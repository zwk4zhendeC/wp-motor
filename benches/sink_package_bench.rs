use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;
use wp_engine::sinks::{ProcMeta, SinkPackage, SinkRecUnit};
use wp_model_core::model::{DataField, DataRecord};

/// 创建指定大小的 SinkPackage
fn build_package(count: usize) -> SinkPackage {
    let units = (0..count).map(|idx| {
        let mut record = DataRecord::default();
        record.append(DataField::from_chars("timestamp", "2024-01-01T00:00:00Z"));
        record.append(DataField::from_chars("level", "INFO"));
        record.append(DataField::from_chars("message", format!("Log message {}", idx)));
        record.append(DataField::from_chars("host", "server-01"));
        record.append(DataField::from_chars("service", "web-api"));
        
        SinkRecUnit::new(
            idx as u64,
            ProcMeta::Rule("/benchmark/rule".to_string()),
            Arc::new(record),
        )
    });
    SinkPackage::from_units(units)
}

/// 基准测试：创建 SinkPackage
fn bench_package_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("package_creation");
    
    for size in [10, 100, 1000, 5000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let package = build_package(black_box(size));
                black_box(package);
            });
        });
    }
    
    group.finish();
}

/// 基准测试：迭代 SinkPackage
fn bench_package_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("package_iteration");
    
    for size in [10, 100, 1000, 5000].iter() {
        let package = build_package(*size);
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &package, |b, package| {
            b.iter(|| {
                let mut count = 0;
                for unit in package.iter() {
                    count += unit.data().len();
                }
                black_box(count);
            });
        });
    }
    
    group.finish();
}

/// 基准测试：克隆 SinkPackage
fn bench_package_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("package_clone");
    
    for size in [10, 100, 1000, 5000].iter() {
        let package = build_package(*size);
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &package, |b, package| {
            b.iter(|| {
                let cloned = package.clone();
                black_box(cloned);
            });
        });
    }
    
    group.finish();
}

/// 基准测试：提取和更新元数据
fn bench_package_meta_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("package_meta_update");
    
    for size in [10, 100, 1000].iter() {
        let package = build_package(*size);
        
        group.bench_with_input(BenchmarkId::from_parameter(size), &package, |b, package| {
            b.iter(|| {
                let mut pkg = package.clone();
                pkg.update_meta(ProcMeta::Rule("/updated/rule".to_string()));
                black_box(pkg);
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_package_creation,
    bench_package_iteration,
    bench_package_clone,
    bench_package_meta_update
);
criterion_main!(benches);
