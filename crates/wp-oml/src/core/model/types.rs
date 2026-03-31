use super::super::AsyncDataTransformer;
use crate::core::prelude::*;
use crate::language::{DataModel, StubModel};
use async_trait::async_trait;

#[async_trait]
impl AsyncDataTransformer for StubModel {
    async fn transform_async(&self, data: DataRecord, _cache: &mut FieldQueryCache) -> DataRecord {
        data
    }

    async fn transform_ref_async(
        &self,
        data: &DataRecord,
        _cache: &mut FieldQueryCache,
    ) -> DataRecord {
        data.clone()
    }
}

#[async_trait]
impl AsyncDataTransformer for DataModel {
    async fn transform_async(&self, data: DataRecord, cache: &mut FieldQueryCache) -> DataRecord {
        match self {
            DataModel::Stub(null_model) => null_model.transform_async(data, cache).await,
            DataModel::Object(obj_model) => obj_model.transform_async(data, cache).await,
        }
    }

    async fn transform_ref_async(
        &self,
        data: &DataRecord,
        cache: &mut FieldQueryCache,
    ) -> DataRecord {
        match self {
            DataModel::Stub(null_model) => null_model.transform_ref_async(data, cache).await,
            DataModel::Object(obj_model) => obj_model.transform_ref_async(data, cache).await,
        }
    }

    async fn transform_batch_async(
        &self,
        records: Vec<DataRecord>,
        cache: &mut FieldQueryCache,
    ) -> Vec<DataRecord> {
        match self {
            DataModel::Stub(null_model) => null_model.transform_batch_async(records, cache).await,
            DataModel::Object(obj_model) => obj_model.transform_batch_async(records, cache).await,
        }
    }

    async fn transform_batch_ref_async(
        &self,
        records: &[DataRecord],
        cache: &mut FieldQueryCache,
    ) -> Vec<DataRecord> {
        match self {
            DataModel::Stub(null_model) => {
                null_model.transform_batch_ref_async(records, cache).await
            }
            DataModel::Object(obj_model) => {
                obj_model.transform_batch_ref_async(records, cache).await
            }
        }
    }
}
