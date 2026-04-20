use crate::core::prelude::*;
use crate::language::RecordOperation;
use async_trait::async_trait;
use wp_model_core::model::{DataField, DataRecord, FieldStorage};

use crate::core::AsyncFieldExtractor;

impl RecordOperation {
    pub(crate) fn extract_one(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<DataField> {
        match self.dat_get.extract_one(target, src, dst) {
            Some(x) => Some(x),
            None => {
                if let Some(default_acq) = &self.default_val {
                    let name = target.name().clone().unwrap_or("_".to_string());
                    // Use extract_storage to preserve zero-copy for Arc variants
                    let storage = default_acq.extract_storage(target, src, dst);
                    return storage.map(|s| {
                        let mut field = s.into_owned();
                        field.set_name(name);
                        field
                    });
                }
                None
            }
        }
    }

    pub(crate) fn extract_storage(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<FieldStorage> {
        // Try primary extraction first
        if let Some(storage) = self.dat_get.extract_storage(target, src, dst) {
            return Some(storage);
        }

        // Fall back to default value with zero-copy support
        if let Some(default_acq) = &self.default_val {
            let name = target.name().clone().unwrap_or("_".to_string());
            let storage = default_acq.extract_storage(target, src, dst);
            return storage.map(|mut s| {
                if s.is_shared() {
                    // ✅ Zero-copy: modify cur_name without cloning Arc
                    s.set_name(name);
                    s
                } else {
                    // Owned: extract and modify underlying field
                    let mut field = s.into_owned();
                    field.set_name(name);
                    FieldStorage::from_owned(field)
                }
            });
        }

        None
    }

    pub(crate) fn extract_more(
        &self,
        _src: &mut DataRecordRef<'_>,
        _dst: &mut DataRecord,
        _cache: &mut FieldQueryCache,
    ) -> Vec<DataField> {
        Vec::new()
    }

    pub(crate) fn support_batch(&self) -> bool {
        false
    }
}

#[async_trait]
impl AsyncFieldExtractor for RecordOperation {
    async fn extract_one_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<DataField> {
        match self.dat_get.extract_one_async(target, src, dst).await {
            Some(x) => Some(x),
            None => {
                if let Some(default_acq) = &self.default_val {
                    let name = target.name().clone().unwrap_or("_".to_string());
                    let storage = default_acq.extract_storage_async(target, src, dst).await;
                    return storage.map(|s| {
                        let mut field = s.into_owned();
                        field.set_name(name);
                        field
                    });
                }
                None
            }
        }
    }

    async fn extract_storage_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &mut DataRecord,
    ) -> Option<FieldStorage> {
        if let Some(storage) = self.dat_get.extract_storage_async(target, src, dst).await {
            return Some(storage);
        }

        if let Some(default_acq) = &self.default_val {
            let name = target.name().clone().unwrap_or("_".to_string());
            let storage = default_acq.extract_storage_async(target, src, dst).await;
            return storage.map(|mut s| {
                if s.is_shared() {
                    s.set_name(name);
                    s
                } else {
                    let mut field = s.into_owned();
                    field.set_name(name);
                    FieldStorage::from_owned(field)
                }
            });
        }

        None
    }
}
