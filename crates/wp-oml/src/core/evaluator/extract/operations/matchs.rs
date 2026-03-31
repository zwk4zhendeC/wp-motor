use crate::core::prelude::*;
use crate::language::MatchAble;
use crate::language::MatchOperation;
use crate::language::MatchSource;
use async_trait::async_trait;
use wp_model_core::model::{DataField, DataRecord, DataType, FieldStorage};

use crate::core::AsyncFieldExtractor;

#[allow(dead_code)]
impl MatchOperation {
    pub(crate) fn extract_one(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<DataField> {
        match self.dat_crate() {
            MatchSource::Single(dat) => {
                let key = dat.field_name().clone().unwrap_or(target.to_string());
                let cur = EvaluationTarget::new(key, DataType::Auto);
                if let Some(x) = dat.extract_one(&cur, src, dst) {
                    for i in self.items() {
                        if i.is_match(&x) {
                            return i.result().extract_one(target, src, dst);
                        }
                    }
                }
            }
            MatchSource::Multi(sources) => {
                let mut vals: Vec<DataField> = Vec::with_capacity(sources.len());
                for s in sources.iter() {
                    let k = s.field_name().clone().unwrap_or(target.to_string());
                    let c = EvaluationTarget::new(k, DataType::Auto);
                    if let Some(v) = s.extract_one(&c, src, dst) {
                        vals.push(v);
                    } else {
                        // If any source fails to extract, skip matching
                        if let Some(default) = self.default() {
                            return default.result().extract_one(target, src, dst);
                        }
                        return None;
                    }
                }
                let refs: Vec<&DataField> = vals.iter().collect();
                for i in self.items() {
                    if i.is_match(refs.as_slice()) {
                        return i.result().extract_one(target, src, dst);
                    }
                }
            }
        }
        if let Some(default) = self.default() {
            return default.result().extract_one(target, src, dst);
        }
        None
    }

    pub(crate) fn extract_storage(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<FieldStorage> {
        // Use extract_storage instead of extract_one to preserve zero-copy for Arc variants
        match self.dat_crate() {
            MatchSource::Single(dat) => {
                let key = dat.field_name().clone().unwrap_or(target.to_string());
                let cur = EvaluationTarget::new(key, DataType::Auto);
                if let Some(x) = dat.extract_one(&cur, src, dst) {
                    for i in self.items() {
                        if i.is_match(&x) {
                            // Call extract_storage to enable zero-copy for FieldArc/ObjArc
                            return i.result().extract_storage(target, src, dst);
                        }
                    }
                }
            }
            MatchSource::Multi(sources) => {
                let mut vals: Vec<DataField> = Vec::with_capacity(sources.len());
                for s in sources.iter() {
                    let k = s.field_name().clone().unwrap_or(target.to_string());
                    let c = EvaluationTarget::new(k, DataType::Auto);
                    if let Some(v) = s.extract_one(&c, src, dst) {
                        vals.push(v);
                    } else {
                        if let Some(default) = self.default() {
                            return default.result().extract_storage(target, src, dst);
                        }
                        return None;
                    }
                }
                let refs: Vec<&DataField> = vals.iter().collect();
                for i in self.items() {
                    if i.is_match(refs.as_slice()) {
                        // Call extract_storage to enable zero-copy for FieldArc/ObjArc
                        return i.result().extract_storage(target, src, dst);
                    }
                }
            }
        }
        if let Some(default) = self.default() {
            // Call extract_storage to enable zero-copy for FieldArc/ObjArc
            return default.result().extract_storage(target, src, dst);
        }
        None
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
impl AsyncFieldExtractor for MatchOperation {
    async fn extract_one_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<DataField> {
        match self.dat_crate() {
            MatchSource::Single(dat) => {
                let key = dat.field_name().clone().unwrap_or(target.to_string());
                let cur = EvaluationTarget::new(key, DataType::Auto);
                if let Some(x) = dat.extract_one_async(&cur, src, dst).await {
                    for i in self.items() {
                        if i.is_match(&x) {
                            return i.result().extract_one_async(target, src, dst).await;
                        }
                    }
                }
            }
            MatchSource::Multi(sources) => {
                let mut vals: Vec<DataField> = Vec::with_capacity(sources.len());
                for s in sources.iter() {
                    let k = s.field_name().clone().unwrap_or(target.to_string());
                    let c = EvaluationTarget::new(k, DataType::Auto);
                    if let Some(v) = s.extract_one_async(&c, src, dst).await {
                        vals.push(v);
                    } else {
                        if let Some(default) = self.default() {
                            return default.result().extract_one_async(target, src, dst).await;
                        }
                        return None;
                    }
                }
                let refs: Vec<&DataField> = vals.iter().collect();
                for i in self.items() {
                    if i.is_match(refs.as_slice()) {
                        return i.result().extract_one_async(target, src, dst).await;
                    }
                }
            }
        }
        if let Some(default) = self.default() {
            return default.result().extract_one_async(target, src, dst).await;
        }
        None
    }

    async fn extract_storage_async(
        &self,
        target: &EvaluationTarget,
        src: &mut DataRecordRef<'_>,
        dst: &DataRecord,
    ) -> Option<FieldStorage> {
        match self.dat_crate() {
            MatchSource::Single(dat) => {
                let key = dat.field_name().clone().unwrap_or(target.to_string());
                let cur = EvaluationTarget::new(key, DataType::Auto);
                if let Some(x) = dat.extract_one_async(&cur, src, dst).await {
                    for i in self.items() {
                        if i.is_match(&x) {
                            return i.result().extract_storage_async(target, src, dst).await;
                        }
                    }
                }
            }
            MatchSource::Multi(sources) => {
                let mut vals: Vec<DataField> = Vec::with_capacity(sources.len());
                for s in sources.iter() {
                    let k = s.field_name().clone().unwrap_or(target.to_string());
                    let c = EvaluationTarget::new(k, DataType::Auto);
                    if let Some(v) = s.extract_one_async(&c, src, dst).await {
                        vals.push(v);
                    } else {
                        if let Some(default) = self.default() {
                            return default
                                .result()
                                .extract_storage_async(target, src, dst)
                                .await;
                        }
                        return None;
                    }
                }
                let refs: Vec<&DataField> = vals.iter().collect();
                for i in self.items() {
                    if i.is_match(refs.as_slice()) {
                        return i.result().extract_storage_async(target, src, dst).await;
                    }
                }
            }
        }
        if let Some(default) = self.default() {
            return default
                .result()
                .extract_storage_async(target, src, dst)
                .await;
        }
        None
    }
}
