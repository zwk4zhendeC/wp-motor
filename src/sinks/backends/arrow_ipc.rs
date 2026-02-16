use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use orion_conf::ErrorOwe;
use serde_json::json;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use wp_arrow::convert::records_to_batch;
use wp_arrow::ipc::encode_ipc;
use wp_arrow::schema::{FieldDef, parse_wp_type};
use wp_conf::connectors::{ConnectorDef, ConnectorScope, ParamMap, SinkDefProvider};
use wp_connector_api::SinkResult;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkBuildCtx, SinkFactory, SinkHandle,
    SinkSpec as ResolvedSinkSpec,
};
use wp_model_core::model::DataRecord;

// ---------------------------------------------------------------------------
// Target address parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Unix(String),
    Tcp(String, u16),
}

fn parse_target(s: &str) -> anyhow::Result<Target> {
    if let Some(path) = s.strip_prefix("unix://") {
        if path.is_empty() {
            anyhow::bail!("unix:// target must include a socket path");
        }
        Ok(Target::Unix(path.to_string()))
    } else if let Some(addr) = s.strip_prefix("tcp://") {
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("tcp:// target must be tcp://host:port"))?;
        let port: u16 = port
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid port in tcp:// target"))?;
        if host.is_empty() {
            anyhow::bail!("tcp:// target must include a host");
        }
        Ok(Target::Tcp(host.to_string(), port))
    } else {
        anyhow::bail!(
            "unsupported target scheme: must start with unix:// or tcp://, got: {}",
            s
        );
    }
}

// ---------------------------------------------------------------------------
// Field definition parsing from params
// ---------------------------------------------------------------------------

fn parse_fields_from_params(params: &ParamMap) -> anyhow::Result<Vec<FieldDef>> {
    let fields_val = params
        .get("fields")
        .ok_or_else(|| anyhow::anyhow!("missing required param: fields"))?;
    let arr = fields_val
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("fields must be an array"))?;

    let mut defs = Vec::with_capacity(arr.len());
    for item in arr {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("each field must have a string 'name'"))?;
        let type_str = item
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("each field must have a string 'type'"))?;
        let nullable = item
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let wp_type = parse_wp_type(type_str)
            .map_err(|e| anyhow::anyhow!("field '{}' has invalid type: {}", name, e))?;
        defs.push(FieldDef::new(name, wp_type).with_nullable(nullable));
    }
    Ok(defs)
}

// ---------------------------------------------------------------------------
// source_id persistence
// ---------------------------------------------------------------------------

fn load_or_create_source_id(work_root: &Path) -> std::io::Result<u64> {
    let path = work_root.join(".source_id");
    if path.exists() {
        let bytes = std::fs::read(&path)?;
        if bytes.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source_id file is too short",
            ));
        }
        Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
    } else {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let id: u64 = rand::random();
        std::fs::write(&path, id.to_le_bytes())?;
        Ok(id)
    }
}

// ---------------------------------------------------------------------------
// ArrowIpcSink
// ---------------------------------------------------------------------------

pub struct ArrowIpcSink {
    stream: Box<dyn AsyncWrite + Unpin + Send + Sync>,
    tag: String,
    field_defs: Vec<FieldDef>,
    source_id: u64,
    batch_seq: u64,
    sent_cnt: u64,
}

impl ArrowIpcSink {
    async fn connect(
        target: &Target,
        tag: String,
        field_defs: Vec<FieldDef>,
        source_id: u64,
    ) -> anyhow::Result<Self> {
        let stream: Box<dyn AsyncWrite + Unpin + Send + Sync> = match target {
            Target::Unix(path) => {
                let s = tokio::net::UnixStream::connect(path).await?;
                Box::new(s)
            }
            Target::Tcp(host, port) => {
                let addr = format!("{}:{}", host, port);
                let s = tokio::net::TcpStream::connect(&addr).await?;
                Box::new(s)
            }
        };
        log::info!("arrow_ipc sink connected: target={:?}", target);
        Ok(Self {
            stream,
            tag,
            field_defs,
            source_id,
            batch_seq: 0,
            sent_cnt: 0,
        })
    }

    async fn send_batch(&mut self, records: &[DataRecord]) -> SinkResult<()> {
        self.batch_seq += 1;

        let batch = records_to_batch(records, &self.field_defs)
            .map_err(|e| anyhow::anyhow!("{}", e))
            .owe_res()?;

        let payload = encode_ipc(self.source_id, self.batch_seq, &self.tag, &batch)
            .map_err(|e| anyhow::anyhow!("{}", e))
            .owe_res()?;

        // Length-prefixed frame: [4 bytes LE u32] [payload]
        let frame_len = payload.len() as u32;
        self.stream
            .write_all(&frame_len.to_le_bytes())
            .await
            .owe_res()?;
        self.stream.write_all(&payload).await.owe_res()?;
        self.stream.flush().await.owe_res()?;

        self.sent_cnt = self.sent_cnt.saturating_add(1);
        if self.sent_cnt == 1 {
            log::info!(
                "arrow_ipc sink first-send: tag={} batch_seq={} rows={} payload_bytes={}",
                self.tag,
                self.batch_seq,
                records.len(),
                payload.len(),
            );
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for ArrowIpcSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.stream.flush().await.owe_res()?;
        self.stream.shutdown().await.owe_res()?;
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        // M05 will implement reconnect logic
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for ArrowIpcSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.send_batch(std::slice::from_ref(data)).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        let records: Vec<DataRecord> = data.iter().map(|a| a.as_ref().clone()).collect();
        self.send_batch(&records).await
    }
}

#[async_trait]
impl AsyncRawDataSink for ArrowIpcSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        // Arrow IPC sink only handles structured DataRecords; raw data is ignored.
        Ok(())
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Ok(())
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Ok(())
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ArrowIpcFactory
// ---------------------------------------------------------------------------

pub struct ArrowIpcFactory;

#[async_trait]
impl SinkFactory for ArrowIpcFactory {
    fn kind(&self) -> &'static str {
        "arrow-ipc"
    }

    fn validate_spec(&self, spec: &ResolvedSinkSpec) -> SinkResult<()> {
        let target_str = spec
            .params
            .get("target")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: target"))
            .owe_conf()?;
        parse_target(target_str).owe_conf()?;

        spec.params
            .get("tag")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: tag"))
            .owe_conf()?;

        parse_fields_from_params(&spec.params).owe_conf()?;
        Ok(())
    }

    async fn build(
        &self,
        spec: &ResolvedSinkSpec,
        ctx: &SinkBuildCtx,
    ) -> SinkResult<SinkHandle> {
        let target_str = spec
            .params
            .get("target")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: target"))
            .owe_conf()?;
        let target = parse_target(target_str).owe_conf()?;

        let tag = spec
            .params
            .get("tag")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing required param: tag"))
            .owe_conf()?
            .to_string();

        let field_defs = parse_fields_from_params(&spec.params).owe_conf()?;
        let source_id = load_or_create_source_id(&ctx.work_root).owe_res()?;

        let sink = ArrowIpcSink::connect(&target, tag, field_defs, source_id)
            .await
            .owe_res()?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for ArrowIpcFactory {
    fn sink_def(&self) -> ConnectorDef {
        let mut params = ParamMap::new();
        params.insert("target".into(), json!("unix:///var/run/warpfusion.sock"));
        params.insert("tag".into(), json!("default"));
        params.insert("fields".into(), json!([]));
        ConnectorDef {
            id: "arrow_ipc_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["target".into(), "tag".into(), "fields".into()],
            default_params: params,
            origin: Some("builtin:arrow_ipc_sink".into()),
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // parse_target
    // -----------------------------------------------------------------------

    #[test]
    fn parse_target_unix() {
        let t = parse_target("unix:///var/run/warpfusion.sock").unwrap();
        assert_eq!(t, Target::Unix("/var/run/warpfusion.sock".into()));
    }

    #[test]
    fn parse_target_tcp() {
        let t = parse_target("tcp://127.0.0.1:9800").unwrap();
        assert_eq!(t, Target::Tcp("127.0.0.1".into(), 9800));
    }

    #[test]
    fn parse_target_invalid_scheme() {
        assert!(parse_target("http://example.com").is_err());
    }

    #[test]
    fn parse_target_unix_empty_path() {
        assert!(parse_target("unix://").is_err());
    }

    #[test]
    fn parse_target_tcp_missing_port() {
        assert!(parse_target("tcp://127.0.0.1").is_err());
    }

    #[test]
    fn parse_target_tcp_empty_host() {
        assert!(parse_target("tcp://:9800").is_err());
    }

    // -----------------------------------------------------------------------
    // parse_fields_from_params
    // -----------------------------------------------------------------------

    #[test]
    fn parse_fields_from_json() {
        let mut params = ParamMap::new();
        params.insert(
            "fields".into(),
            json!([
                { "name": "sip", "type": "ip" },
                { "name": "dport", "type": "digit" },
                { "name": "action", "type": "chars", "nullable": false },
            ]),
        );
        let defs = parse_fields_from_params(&params).unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].name, "sip");
        assert!(defs[0].nullable);
        assert_eq!(defs[1].name, "dport");
        assert!(!defs[2].nullable);
    }

    #[test]
    fn parse_fields_invalid_type() {
        let mut params = ParamMap::new();
        params.insert(
            "fields".into(),
            json!([{ "name": "x", "type": "unknown_type" }]),
        );
        assert!(parse_fields_from_params(&params).is_err());
    }

    #[test]
    fn parse_fields_missing_name() {
        let mut params = ParamMap::new();
        params.insert("fields".into(), json!([{ "type": "chars" }]));
        assert!(parse_fields_from_params(&params).is_err());
    }

    #[test]
    fn parse_fields_missing_type() {
        let mut params = ParamMap::new();
        params.insert("fields".into(), json!([{ "name": "x" }]));
        assert!(parse_fields_from_params(&params).is_err());
    }

    // -----------------------------------------------------------------------
    // source_id persistence
    // -----------------------------------------------------------------------

    #[test]
    fn source_id_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = load_or_create_source_id(dir.path()).unwrap();
        let id2 = load_or_create_source_id(dir.path()).unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn source_id_stable_across_calls() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<u64> = (0..5)
            .map(|_| load_or_create_source_id(dir.path()).unwrap())
            .collect();
        assert!(ids.windows(2).all(|w| w[0] == w[1]));
    }

    // -----------------------------------------------------------------------
    // Integration: roundtrip via Unix socket
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_roundtrip_unix() {
        use tokio::io::AsyncReadExt;
        use wp_arrow::ipc::decode_ipc;
        use wp_model_core::model::{Field, FieldStorage};

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("test.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_str = sock_path.to_string_lossy().to_string();
        let srv = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            // Read length prefix
            let mut len_buf = [0u8; 4];
            conn.read_exact(&mut len_buf).await.unwrap();
            let frame_len = u32::from_le_bytes(len_buf) as usize;
            // Read payload
            let mut payload = vec![0u8; frame_len];
            conn.read_exact(&mut payload).await.unwrap();
            payload
        });

        let source_id = load_or_create_source_id(dir.path()).unwrap();
        let field_defs = vec![
            FieldDef::new("name", wp_arrow::schema::WpDataType::Chars),
            FieldDef::new("count", wp_arrow::schema::WpDataType::Digit),
        ];

        let target = Target::Unix(sock_path_str);
        let mut sink = ArrowIpcSink::connect(&target, "test-tag".into(), field_defs, source_id)
            .await
            .unwrap();

        let rec = DataRecord::from(vec![
            FieldStorage::from(Field::from_chars("name", "alice")),
            FieldStorage::from(Field::from_digit("count", 42)),
        ]);
        sink.send_batch(&[rec]).await.unwrap();

        let payload = srv.await.unwrap();
        let frame = decode_ipc(&payload).unwrap();
        assert_eq!(frame.source_id, source_id);
        assert_eq!(frame.batch_seq, 1);
        assert_eq!(frame.tag, "test-tag");
        assert_eq!(frame.batch.num_rows(), 1);
        assert_eq!(frame.batch.num_columns(), 2);
    }

    // -----------------------------------------------------------------------
    // Integration: roundtrip via TCP
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_roundtrip_tcp() {
        use tokio::io::AsyncReadExt;
        use wp_arrow::ipc::decode_ipc;
        use wp_model_core::model::{Field, FieldStorage};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let srv = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut len_buf = [0u8; 4];
            conn.read_exact(&mut len_buf).await.unwrap();
            let frame_len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; frame_len];
            conn.read_exact(&mut payload).await.unwrap();
            payload
        });

        let dir = tempfile::tempdir().unwrap();
        let source_id = load_or_create_source_id(dir.path()).unwrap();
        let field_defs = vec![
            FieldDef::new("action", wp_arrow::schema::WpDataType::Chars),
        ];

        let target = Target::Tcp("127.0.0.1".into(), port);
        let mut sink = ArrowIpcSink::connect(&target, "tcp-tag".into(), field_defs, source_id)
            .await
            .unwrap();

        let rec = DataRecord::from(vec![FieldStorage::from(Field::from_chars("action", "allow"))]);
        sink.send_batch(&[rec]).await.unwrap();

        let payload = srv.await.unwrap();
        let frame = decode_ipc(&payload).unwrap();
        assert_eq!(frame.source_id, source_id);
        assert_eq!(frame.batch_seq, 1);
        assert_eq!(frame.tag, "tcp-tag");
        assert_eq!(frame.batch.num_rows(), 1);
    }

    // -----------------------------------------------------------------------
    // batch_seq increments
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_records_batch_seq_increments() {
        use tokio::io::AsyncReadExt;
        use wp_arrow::ipc::decode_ipc;
        use wp_model_core::model::{Field, FieldStorage};

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("seq.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_str = sock_path.to_string_lossy().to_string();
        let srv = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut seqs = Vec::new();
            for _ in 0..3 {
                let mut len_buf = [0u8; 4];
                conn.read_exact(&mut len_buf).await.unwrap();
                let frame_len = u32::from_le_bytes(len_buf) as usize;
                let mut payload = vec![0u8; frame_len];
                conn.read_exact(&mut payload).await.unwrap();
                let frame = decode_ipc(&payload).unwrap();
                seqs.push(frame.batch_seq);
            }
            seqs
        });

        let source_id = load_or_create_source_id(dir.path()).unwrap();
        let field_defs = vec![FieldDef::new("v", wp_arrow::schema::WpDataType::Chars)];
        let target = Target::Unix(sock_path_str);
        let mut sink = ArrowIpcSink::connect(&target, "seq".into(), field_defs, source_id)
            .await
            .unwrap();

        for _ in 0..3 {
            let rec = DataRecord::from(vec![FieldStorage::from(Field::from_chars("v", "x"))]);
            sink.send_batch(&[rec]).await.unwrap();
        }

        let seqs = srv.await.unwrap();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    // -----------------------------------------------------------------------
    // Empty records
    // -----------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn sink_empty_records() {
        use tokio::io::AsyncReadExt;
        use wp_arrow::ipc::decode_ipc;

        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("empty.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();

        let sock_path_str = sock_path.to_string_lossy().to_string();
        let srv = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut len_buf = [0u8; 4];
            conn.read_exact(&mut len_buf).await.unwrap();
            let frame_len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; frame_len];
            conn.read_exact(&mut payload).await.unwrap();
            payload
        });

        let source_id = load_or_create_source_id(dir.path()).unwrap();
        let field_defs = vec![FieldDef::new("x", wp_arrow::schema::WpDataType::Chars)];
        let target = Target::Unix(sock_path_str);
        let mut sink = ArrowIpcSink::connect(&target, "empty".into(), field_defs, source_id)
            .await
            .unwrap();

        // Send empty records — should still produce a 0-row batch
        sink.send_batch(&[]).await.unwrap();

        let payload = srv.await.unwrap();
        let frame = decode_ipc(&payload).unwrap();
        assert_eq!(frame.batch.num_rows(), 0);
        assert_eq!(frame.batch_seq, 1);
    }
}
