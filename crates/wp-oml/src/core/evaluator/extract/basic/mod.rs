use crate::core::AsyncFieldExtractor;
use crate::core::prelude::*;
use crate::{core::FieldCollector, language::DirectAccessor};
use async_trait::async_trait;
use wp_model_core::model::FieldStorage;
mod batch;
mod read;
mod take;

impl FieldCollector for DirectAccessor {
    fn collect_item(
        &self,
        name: &str,
        src: &DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Vec<DataField> {
        match self {
            DirectAccessor::Take(o) => o.collect_item(name, src, dst),
            DirectAccessor::Read(o) => o.collect_item(name, src, dst),
        }
    }
}

#[allow(dead_code)]
impl DirectAccessor {
    pub(crate) fn extract_one(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<DataField> {
        match self {
            DirectAccessor::Take(o) => o.extract_one(target, src, dst),
            DirectAccessor::Read(o) => o.extract_one(target, src, dst),
        }
    }

    pub(crate) fn extract_storage(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<FieldStorage> {
        self.extract_one(target, src, dst)
            .map(FieldStorage::from_owned)
    }

    pub(crate) fn extract_more(
        &self,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
        cache: &mut FieldQueryCache,
    ) -> Vec<DataField> {
        match self {
            DirectAccessor::Take(o) => o.extract_more(src, dst, cache),
            DirectAccessor::Read(o) => o.extract_more(src, dst, cache),
        }
    }

    pub(crate) fn support_batch(&self) -> bool {
        match self {
            DirectAccessor::Take(o) => o.support_batch(),
            DirectAccessor::Read(o) => o.support_batch(),
        }
    }
}

#[async_trait]
impl AsyncFieldExtractor for DirectAccessor {
    async fn extract_one_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<DataField> {
        match self {
            DirectAccessor::Take(o) => o.extract_one_async(target, src, dst).await,
            DirectAccessor::Read(o) => o.extract_one_async(target, src, dst).await,
        }
    }

    async fn extract_storage_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<FieldStorage> {
        match self {
            DirectAccessor::Take(o) => o.extract_storage_async(target, src, dst).await,
            DirectAccessor::Read(o) => o.extract_storage_async(target, src, dst).await,
        }
    }

    async fn extract_more_async(
        &self,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
        cache: &mut FieldQueryCache,
    ) -> Vec<DataField> {
        match self {
            DirectAccessor::Take(o) => o.extract_more_async(src, dst, cache).await,
            DirectAccessor::Read(o) => o.extract_more_async(src, dst, cache).await,
        }
    }

    fn support_batch_async(&self) -> bool {
        match self {
            DirectAccessor::Take(o) => o.support_batch_async(),
            DirectAccessor::Read(o) => o.support_batch_async(),
        }
    }
}
