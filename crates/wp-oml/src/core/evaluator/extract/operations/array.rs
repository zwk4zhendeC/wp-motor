use crate::core::prelude::*;
use crate::language::ArrOperation;
use async_trait::async_trait;

use wp_model_core::model::{DataField, DataRecord, FieldStorage};

use crate::core::AsyncFieldExtractor;

impl ArrOperation {
    pub(crate) fn extract_one(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<DataField> {
        let target_name = target.name().clone().unwrap_or("_".to_string());
        let arr = self.dat_crate.collect_item(target_name.as_str(), src, dst);
        if !arr.is_empty() {
            Some(DataField::from_arr(target_name, arr))
        } else {
            None
        }
    }

    pub(crate) fn extract_storage(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<FieldStorage> {
        self.extract_one(target, src, dst)
            .map(FieldStorage::from_owned)
    }

    pub(crate) fn extract_more(
        &self,
        _src: &mut DataRecordRef<'_>,
        _dst: &DataRecord,
        _cache: &mut FieldQueryCache,
    ) -> Vec<DataField> {
        Vec::new()
    }

    pub(crate) fn support_batch(&self) -> bool {
        false
    }
}

#[async_trait]
impl AsyncFieldExtractor for ArrOperation {
    async fn extract_one_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<DataField> {
        self.extract_one(target, src, dst)
    }

    async fn extract_storage_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<FieldStorage> {
        self.extract_storage(target, src, dst)
    }

    async fn extract_more_async(
        &self,
        _src: &mut DataRecordRef<'_>,
        _dst: &DataRecord,
        _cache: &mut FieldQueryCache,
    ) -> Vec<DataField> {
        Vec::new()
    }
}
