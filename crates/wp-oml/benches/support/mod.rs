use std::cell::RefCell;

use oml::core::AsyncDataTransformer;
use oml::language::ObjModel;
use oml::parser::oml_parse_raw;
use tokio::runtime::{Builder, Runtime};
use wp_knowledge::cache::FieldQueryCache;
use wp_model_core::model::DataRecord;

thread_local! {
    static BENCH_RUNTIME: RefCell<Runtime> = RefCell::new(
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime for wp-oml benches"),
    );
}

fn with_runtime<F, R>(f: F) -> R
where
    F: FnOnce(&mut Runtime) -> R,
{
    BENCH_RUNTIME.with(|runtime| f(&mut runtime.borrow_mut()))
}

pub fn parse_model(code: &str) -> ObjModel {
    let mut code_ref = code;
    with_runtime(|runtime| {
        runtime
            .block_on(oml_parse_raw(&mut code_ref))
            .expect("parse OML model for bench")
    })
}

#[allow(dead_code)]
pub trait BenchTransformExt: AsyncDataTransformer + Sync {
    fn transform(&self, data: DataRecord, cache: &mut FieldQueryCache) -> DataRecord {
        with_runtime(|runtime| runtime.block_on(self.transform_async(data, cache)))
    }

    fn transform_ref(&self, data: &DataRecord, cache: &mut FieldQueryCache) -> DataRecord {
        with_runtime(|runtime| runtime.block_on(self.transform_ref_async(data, cache)))
    }

    fn transform_batch(
        &self,
        records: Vec<DataRecord>,
        cache: &mut FieldQueryCache,
    ) -> Vec<DataRecord> {
        with_runtime(|runtime| runtime.block_on(self.transform_batch_async(records, cache)))
    }

    fn transform_batch_ref(
        &self,
        records: &[DataRecord],
        cache: &mut FieldQueryCache,
    ) -> Vec<DataRecord> {
        with_runtime(|runtime| runtime.block_on(self.transform_batch_ref_async(records, cache)))
    }
}

impl<T> BenchTransformExt for T where T: AsyncDataTransformer + Sync + ?Sized {}
