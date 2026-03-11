use crate::{runtime::prelude::*, types::EventBatchRecv, types::EventBatchSend};

use super::act_parser::ActParser;
use crate::runtime::actor::command::CmdSubscriber;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::error::TrySendError;
use wp_connector_api::SourceBatch;
use wp_log::{error_ctrl, info_ctrl};

pub struct ActorWork {
    name: String,
    dat_r: EventBatchRecv,
    cmd_r: CmdSubscriber,
    mon_s: MonSend,
    actor: ActParser,
}

impl ActorWork {
    pub fn new<S: Into<String>>(
        name: S,
        dat_r: EventBatchRecv,
        cmd_r: CmdSubscriber,
        mon_s: MonSend,
        actor: ActParser,
    ) -> Self {
        ActorWork {
            name: name.into(),
            dat_r,
            cmd_r,
            mon_s,
            actor,
        }
    }
    pub async fn proc(&mut self, setting: ParseOption) -> WparseResult<()> {
        info_ctrl!("actor({}) work start", self.name);

        if let Err(e) = self
            .actor
            .parse_events(&self.cmd_r, &mut self.dat_r, &self.mon_s, setting)
            .await
        {
            error_ctrl!("actor({}) work error: {}", self.name, e);
            return Err(e);
        }
        info_ctrl!("actor({}) work end", self.name);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ParseWorkerSender {
    pub dat_s: EventBatchSend,
}

impl ParseWorkerSender {
    pub fn new(dat_s: EventBatchSend) -> Self {
        Self { dat_s }
    }
}

#[derive(Clone, Debug)]
pub struct ParseDispatchRouter {
    senders: Arc<RwLock<Vec<ParseWorkerSender>>>,
    next_idx: Arc<AtomicUsize>,
    reloading: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug)]
pub enum ParseDispatchResult {
    Sent,
    Full(SourceBatch),
    Reloading(SourceBatch),
    Closed(SourceBatch),
}

impl ParseDispatchRouter {
    pub fn new(senders: Vec<ParseWorkerSender>) -> Self {
        Self {
            senders: Arc::new(RwLock::new(senders)),
            next_idx: Arc::new(AtomicUsize::new(0)),
            reloading: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    pub fn replace(&self, senders: Vec<ParseWorkerSender>) {
        let mut guard = self
            .senders
            .write()
            .expect("parse dispatch router poisoned on replace");
        *guard = senders;
        self.next_idx.store(0, Ordering::Relaxed);
        self.reloading.store(false, Ordering::Relaxed);
    }

    pub fn begin_reload(&self) {
        let mut guard = self
            .senders
            .write()
            .expect("parse dispatch router poisoned on begin_reload");
        guard.clear();
        self.next_idx.store(0, Ordering::Relaxed);
        self.reloading.store(true, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Vec<ParseWorkerSender> {
        self.senders
            .read()
            .expect("parse dispatch router poisoned on snapshot")
            .clone()
    }

    pub fn len(&self) -> usize {
        self.senders
            .read()
            .expect("parse dispatch router poisoned on len")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn try_send_round_robin(&self, batch: SourceBatch) -> ParseDispatchResult {
        let senders = self.snapshot();
        if senders.is_empty() {
            if self.reloading.load(Ordering::Relaxed) {
                return ParseDispatchResult::Reloading(batch);
            }
            return ParseDispatchResult::Closed(batch);
        }

        let start = self.next_idx.fetch_add(1, Ordering::Relaxed) % senders.len();
        let mut pending = batch;
        let mut saw_full = false;

        for offset in 0..senders.len() {
            let idx = (start + offset) % senders.len();
            match senders[idx].dat_s.try_send(pending) {
                Ok(()) => return ParseDispatchResult::Sent,
                Err(TrySendError::Full(batch)) => {
                    pending = batch;
                    saw_full = true;
                }
                Err(TrySendError::Closed(batch)) => {
                    pending = batch;
                }
            }
        }

        if saw_full {
            ParseDispatchResult::Full(pending)
        } else {
            ParseDispatchResult::Closed(pending)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::event_id::next_event_id;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use wp_connector_api::{SourceBatch, SourceEvent, Tags};
    use wp_model_core::raw::RawData;

    fn make_batch(tag: &str) -> SourceBatch {
        let mut tags = Tags::new();
        tags.set("tag", tag.to_string());
        vec![SourceEvent::new(
            next_event_id(),
            tag,
            RawData::from_string(tag.to_string()),
            Arc::new(tags),
        )]
    }

    #[test]
    fn router_returns_reloading_when_begin_reload_clears_senders() {
        let (tx, _rx) = mpsc::channel(4);
        let router = ParseDispatchRouter::new(vec![ParseWorkerSender::new(tx)]);
        router.begin_reload();

        let result = router.try_send_round_robin(make_batch("reload"));
        assert!(
            matches!(result, ParseDispatchResult::Reloading(_)),
            "begin_reload 后空 router 应显式标记为 Reloading"
        );
    }

    #[test]
    fn router_returns_closed_when_all_parser_senders_are_closed() {
        let (tx, rx) = mpsc::channel(4);
        let router = ParseDispatchRouter::new(vec![ParseWorkerSender::new(tx)]);
        drop(rx);

        let result = router.try_send_round_robin(make_batch("closed"));
        assert!(
            matches!(result, ParseDispatchResult::Closed(_)),
            "非 reload 场景下 parser sender 全关闭应返回 Closed"
        );
    }
}
