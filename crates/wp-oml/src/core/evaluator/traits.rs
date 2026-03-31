use async_trait::async_trait;
use wp_error::parse_error::OMLCodeResult;
use wp_model_core::model::FieldStorage;

use crate::core::prelude::*;

pub trait FieldCollector {
    fn collect_item(&self, name: &str, src: &DataRecordRef<'_>, dst: &DataRecord)
    -> Vec<DataField>;
}

pub trait BatchFetcher {
    fn extract_batch(
        &self,
        target: &BatchEvalTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Vec<DataField>;
}

#[enum_dispatch]
pub trait ValueProcessor {
    fn value_cacu(&self, in_val: DataField) -> DataField;

    /// Process value with FieldStorage support for zero-copy optimization.
    /// Default implementation converts to DataField and uses value_cacu.
    /// Override this for operations that can preserve FieldStorage::Shared.
    fn value_cacu_storage(&self, in_val: FieldStorage) -> FieldStorage {
        let field = in_val.into_owned();
        let result = self.value_cacu(field);
        FieldStorage::from_owned(result)
    }
}

#[async_trait]
pub trait AsyncExpEvaluator {
    async fn eval_proc_async(
        &self,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
        cache: &mut FieldQueryCache,
    );
}

#[async_trait]
impl AsyncExpEvaluator for EvalExp {
    async fn eval_proc_async(
        &self,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
        cache: &mut FieldQueryCache,
    ) {
        match self {
            EvalExp::Single(x) => {
                x.eval_proc_async(src, dst, cache).await;
            }
            EvalExp::Batch(x) => {
                x.eval_proc_async(src, dst, cache).await;
            }
        }
    }
}
#[allow(dead_code)]
pub trait LibUseAble {
    fn search(&self, lib_n: &str, cond: &DataField, need: &str) -> Option<DataField>;
}

#[async_trait]
pub trait ConfADMExt {
    async fn load(path: &str) -> OMLCodeResult<Self>
    where
        Self: Sized;
}

#[async_trait]
pub trait AsyncDataTransformer {
    async fn transform_async(&self, data: DataRecord, cache: &mut FieldQueryCache) -> DataRecord;

    async fn transform_ref_async(
        &self,
        data: &DataRecord,
        cache: &mut FieldQueryCache,
    ) -> DataRecord;

    async fn transform_batch_async(
        &self,
        records: Vec<DataRecord>,
        cache: &mut FieldQueryCache,
    ) -> Vec<DataRecord> {
        let mut out = Vec::with_capacity(records.len());
        for record in records {
            out.push(self.transform_async(record, cache).await);
        }
        out
    }

    async fn transform_batch_ref_async(
        &self,
        records: &[DataRecord],
        cache: &mut FieldQueryCache,
    ) -> Vec<DataRecord> {
        let mut out = Vec::with_capacity(records.len());
        for record in records {
            out.push(self.transform_ref_async(record, cache).await);
        }
        out
    }
}
