use crate::stat::MonSend;
use crate::stat::runtime_counters;
use wp_stat::DataDim;
use wp_stat::StatRecorder;
use wp_stat::StatReq;

use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::error::TrySendError;
use std::collections::HashMap;
use wp_data_fmt::{Raw, RecordFormatter};
use wp_model_core::model::DataRecord;
use wp_stat::ReportVariant;
use wp_stat::StatCollector;

#[derive(Clone)]
struct DataCollectorGroup {
    collect: Vec<String>,
    indices: Vec<usize>,
}

#[derive(Clone)]
pub struct MetricCollectors {
    pub(crate) items: Vec<StatCollector>,
    unit_collectors_end: usize,
    has_data_dims: bool,
    data_groups: Vec<DataCollectorGroup>,
}

pub fn extract_metric_dimensions(tdo: &DataRecord, collects: &[String]) -> DataDim {
    let mut data = DataDim::with_capacity(collects.len());
    let formatter = Raw;
    for key in collects {
        let value = tdo.field(key.as_str());
        data.push(value.map(|x| formatter.fmt_field(x).to_string()));
    }
    data
}
impl StatRecorder<Option<&DataRecord>> for MetricCollectors {
    fn record_begin(&mut self, target: &str, dat: Option<&DataRecord>) {
        for idx in 0..self.unit_collectors_end {
            self.items[idx].record_begin(target, ());
        }

        match dat {
            Some(tdo) => {
                let (items, groups) = (&mut self.items, &self.data_groups);
                for group in groups.iter() {
                    if let Some((last, rest)) = group.indices.split_last() {
                        let data = extract_metric_dimensions(tdo, group.collect.as_slice());
                        for idx in rest {
                            items[*idx].record_begin(target, data.clone());
                        }
                        items[*last].record_begin(target, data);
                    }
                }
            }
            None => {
                for idx in self.unit_collectors_end..self.items.len() {
                    self.items[idx].record_begin(target, ());
                }
            }
        }
    }

    fn record_end(&mut self, rule: &str, dat: Option<&DataRecord>) {
        for idx in 0..self.unit_collectors_end {
            self.items[idx].record_end(rule, ());
        }

        match dat {
            Some(tdo) => {
                let (items, groups) = (&mut self.items, &self.data_groups);
                for group in groups.iter() {
                    if let Some((last, rest)) = group.indices.split_last() {
                        let data = extract_metric_dimensions(tdo, group.collect.as_slice());
                        for idx in rest {
                            items[*idx].record_end(rule, data.clone());
                        }
                        items[*last].record_end(rule, data);
                    }
                }
            }
            None => {
                for idx in self.unit_collectors_end..self.items.len() {
                    self.items[idx].record_end(rule, ());
                }
            }
        }
    }

    fn record_task(&mut self, rule: &str, dat: Option<&DataRecord>) {
        for idx in 0..self.unit_collectors_end {
            self.items[idx].record_task(rule, ());
        }

        match dat {
            Some(tdo) => {
                let (items, groups) = (&mut self.items, &self.data_groups);
                for group in groups.iter() {
                    if let Some((last, rest)) = group.indices.split_last() {
                        let data = extract_metric_dimensions(tdo, group.collect.as_slice());
                        for idx in rest {
                            items[*idx].record_task(rule, data.clone());
                        }
                        items[*last].record_task(rule, data);
                    }
                }
            }
            None => {
                for idx in self.unit_collectors_end..self.items.len() {
                    self.items[idx].record_task(rule, ());
                }
            }
        }
    }
}

impl StatRecorder<&str> for MetricCollectors {
    fn record_begin(&mut self, target: &str, dat_key: &str) {
        for requ in self.items.iter_mut() {
            requ.record_begin(target, DataDim::from(dat_key));
        }
    }

    fn record_end(&mut self, target: &str, dat_key: &str) {
        for requ in self.items.iter_mut() {
            requ.record_end(target, DataDim::from(dat_key));
        }
    }

    fn record_task(&mut self, target: &str, dat_key: &str) {
        for requ in self.items.iter_mut() {
            requ.record_task(target, DataDim::from(dat_key));
        }
    }
}

impl StatRecorder<()> for MetricCollectors {
    fn record_begin(&mut self, target: &str, dat_key: ()) {
        for requ in self.items.iter_mut() {
            requ.record_begin(target, dat_key);
        }
    }

    fn record_end(&mut self, target: &str, dat_key: ()) {
        for requ in self.items.iter_mut() {
            requ.record_end(target, dat_key);
        }
    }

    fn record_task(&mut self, target: &str, dat_key: ()) {
        for requ in self.items.iter_mut() {
            requ.record_task(target, dat_key);
        }
    }
}

impl MetricCollectors {
    /// Batch record helper for unit `()` dat_key: add `count` occurrences at once.
    pub fn record_task_batch(&mut self, target: &str, count: usize) {
        if count == 0 {
            return;
        }
        for c in self.items.iter_mut() {
            c.record_task_n_unit(target, count);
        }
    }

    pub fn supports_unit_batch(&self) -> bool {
        !self.has_data_dims
    }

    pub fn record_begin_batch_unit(&mut self, target: &str, count: usize) {
        if count == 0 {
            return;
        }
        debug_assert!(
            self.supports_unit_batch(),
            "record_begin_batch_unit requires collectors without dimensions"
        );
        for c in self.items.iter_mut() {
            c.record_begin_n_unit(target, count);
        }
    }

    pub fn record_end_batch_unit(&mut self, target: &str, count: usize) {
        if count == 0 {
            return;
        }
        debug_assert!(
            self.supports_unit_batch(),
            "record_end_batch_unit requires collectors without dimensions"
        );
        for c in self.items.iter_mut() {
            c.record_end_n_unit(target, count);
        }
    }
}

impl MetricCollectors {
    pub fn new(target: String, stat_reqs: Vec<StatReq>) -> Self {
        let mut unit_items = Vec::new();
        let mut data_items = Vec::new();

        for req in stat_reqs {
            if !req.match_target(target.as_str()) {
                continue;
            }
            if req.collect.is_empty() {
                unit_items.push(StatCollector::new(target.clone(), req));
            } else {
                data_items.push(StatCollector::new(target.clone(), req));
            }
        }

        let mut data_groups: Vec<DataCollectorGroup> = Vec::new();
        let mut collect_group_map: HashMap<Vec<String>, usize> = HashMap::new();
        for (rel_idx, collector) in data_items.iter().enumerate() {
            let collect = collector.get_req().collect.clone();
            if let Some(group_idx) = collect_group_map.get(&collect).copied() {
                data_groups[group_idx].indices.push(rel_idx);
            } else {
                let group_idx = data_groups.len();
                collect_group_map.insert(collect.clone(), group_idx);
                data_groups.push(DataCollectorGroup {
                    collect,
                    indices: vec![rel_idx],
                });
            }
        }

        let unit_collectors_end = unit_items.len();
        let has_data_dims = !data_items.is_empty();
        for group in data_groups.iter_mut() {
            for idx in group.indices.iter_mut() {
                *idx += unit_collectors_end;
            }
        }
        unit_items.extend(data_items);

        Self {
            items: unit_items,
            unit_collectors_end,
            has_data_dims,
            data_groups,
        }
    }
    pub fn up_target(&mut self, target: String) {
        for item in self.items.iter_mut() {
            item.up_target(target.clone());
        }
    }
    pub async fn send_stat(&mut self, mon_send: &MonSend) -> Result<(), SendError<ReportVariant>> {
        let batch_time = fast_now();
        for requ in self.items.iter_mut() {
            let slices = requ.collect_stat_with_time(batch_time);
            let r = mon_send.try_send(ReportVariant::Stat(slices));
            // 在 debug 构建下，监控通道关闭被视为严重错误，直接中断，利于尽快发现监控消费端未启动的问题。
            #[cfg(debug_assertions)]
            {
                if let Err(TrySendError::Closed(e)) = &r {
                    let _ = e;
                    debug_assert!(false, "monitor channel closed when sending stat");
                }
            }
            match r {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    runtime_counters::rec_monitor_send_drop_full();
                }
                Err(TrySendError::Closed(s)) => {
                    runtime_counters::rec_monitor_send_drop_closed();
                    return Err(SendError(s));
                }
            }
        }
        Ok(())
    }
}

fn fast_now() -> chrono::NaiveDateTime {
    use chrono::Local;
    Local::now().naive_local()
}


#[cfg(test)]
mod tests {
    use super::*;
    use wp_stat::{StatRecorder, StatTarget};

    #[test]
    fn none_input_still_records_when_collect_fields_present() {
        let mut collectors = MetricCollectors::new(
            "ruleA".to_string(),
            vec![StatReq::simple_test(StatTarget::All, vec!["k".to_string()], 10)],
        );

        collectors.record_begin("ruleA", None);
        collectors.record_end("ruleA", None);

        let report = collectors.items[0].collect_stat();
        assert_eq!(report.get_data().len(), 1);
        assert_eq!(report.get_data()[0].stat.total, 1);
        assert_eq!(report.get_data()[0].stat.success, 1);
    }
}
