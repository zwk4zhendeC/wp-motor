use crate::stat::ReportGenerator;
use crate::stat::reporting::{ReportEngine, create_report_table};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use wp_model_core::model::DataRecord;
use wp_stat::StatReport;
use wp_stat::StatReq;
use wp_stat::{Mergeable, ReportVariant};

#[derive(Clone, Default)]
pub struct MetricSet {
    units: Vec<StatReport>,
    index: HashMap<String, usize>,
}

impl Display for MetricSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for i in &self.units {
            write!(f, "RunStatSet:{}", i)?;
        }
        Ok(())
    }
}
impl MetricSet {
    pub fn units(&self) -> &[StatReport] {
        self.units.as_slice()
    }

    pub fn merge_slice(&mut self, slices: ReportVariant) {
        let ReportVariant::Stat(x) = slices;
        self.merge_unit(x);
    }

    fn merge_key(report: &StatReport) -> String {
        match report.target_identity() {
            Some(target) => format!(
                "{}|{}|1|{}",
                report.get_req().stage,
                report.get_name(),
                target
            ),
            None => format!("{}|{}|0|", report.get_req().stage, report.get_name()),
        }
    }

    fn merge_unit(&mut self, x: StatReport) {
        let key = Self::merge_key(&x);

        if let Some(idx) = self.index.get(&key).copied() {
            if let Some(sum) = self.units.get_mut(idx)
                && x.can_merge(sum)
            {
                sum.merge(x);
                return;
            }
            self.index.remove(&key);
        }

        for (idx, sum) in self.units.iter_mut().enumerate() {
            if x.can_merge(sum) {
                sum.merge(x);
                self.index.insert(key, idx);
                return;
            }
        }

        self.units.push(x);
        self.index.insert(key, self.units.len() - 1);
    }
    pub fn merge(&mut self, other: MetricSet) {
        for oth in other.units {
            self.merge_unit(oth);
        }
    }

    pub fn registry(&mut self, reqs: Vec<StatReq>) {
        for req in reqs {
            let ins = StatReport::req(req);
            self.units.push(ins);
            let idx = self.units.len() - 1;
            let key = Self::merge_key(&self.units[idx]);
            self.index.insert(key, idx);
        }
    }
    pub fn show_table(&mut self) {
        let mut table = create_report_table();
        let mut sorted: Vec<&StatReport> = self.units.iter().collect();
        sorted.sort();
        for sum in sorted {
            sum.generate_report(&mut table, &mut ReportEngine::new());
        }

        println!("\n{}", table);
    }
    pub fn conv_to_tdc(&self) -> Vec<DataRecord> {
        let mut tdc_vec = Vec::new();
        for req in &self.units {
            tdc_vec.append(&mut Vec::<DataRecord>::from(req.clone()));
        }
        tdc_vec
    }
}

#[cfg(test)]
mod tests {
    use crate::stat::metric_collect::MetricCollectors;
    use crate::stat::metric_set::MetricSet;

    use wp_stat::ReportVariant;
    use wp_stat::StatRecorder;
    use wp_stat::StatReq;
    use wp_stat::StatTarget;

    #[test]
    fn test_stat_1() {
        let mut sum = MetricSet::default();
        let reqs = vec![
            StatReq::simple_test2("stat1", StatTarget::All, Vec::new(), 3),
            StatReq::simple_test2("stat2", StatTarget::All, Vec::new(), 10),
        ];
        let mut stat1 = MetricCollectors::new("sink_1".to_string(), reqs.clone());
        //let mut sum  = TopNStatFixtures::new("sink_1".to_string(), p.clone());

        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.2");
        stat1.record_task("sink_1", "10.0.0.3");
        stat1.record_task("sink_1", "10.0.0.4");
        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.5");
        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.3");
        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.1");
        println!("---------------stat1--------------------");
        for mut i in stat1.items {
            let slices = i.collect_stat();
            println!("{}", slices);
            sum.merge_slice(ReportVariant::Stat(slices));
        }
        println!("---------------sum stat1--------------------");
        println!("{}", sum);
        let mut stat2 = MetricCollectors::new("sink_2".to_string(), reqs.clone());
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.2");
        stat2.record_task("sink_2", "10.0.0.3");
        stat2.record_task("sink_2", "10.0.0.4");
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.5");
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.3");
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.1");
        println!("---------------stat2--------------------");
        for mut i in stat2.items {
            let slices = i.collect_stat();
            println!("{}", slices);
            sum.merge_slice(ReportVariant::Stat(slices));
        }

        println!("---------------total--------------------");
        println!("{}", sum);
    }

    #[test]
    fn test_stat_2() {
        let mut sum = MetricSet::default();
        let reqs = vec![
            StatReq::simple_test2("stat1", StatTarget::All, Vec::new(), 3),
            StatReq::simple_test2("stat2", StatTarget::All, Vec::new(), 10),
        ];
        let mut stat1 = MetricCollectors::new("sink_1".to_string(), reqs.clone());

        //let mut sum  = TopNStatFixtures::new("sink_1".to_string(), p.clone());

        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.2");
        stat1.record_task("sink_1", "10.0.0.3");
        stat1.record_task("sink_1", "10.0.0.4");
        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.5");
        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.3");
        stat1.record_task("sink_1", "10.0.0.1");
        stat1.record_task("sink_1", "10.0.0.1");
        println!("---------------stat1--------------------");
        for mut i in stat1.items {
            let slices = i.collect_stat();
            println!("{}", slices);
            sum.merge_slice(ReportVariant::Stat(slices));
        }
        println!("---------------sum stat1--------------------");
        println!("{}", sum);
        let mut stat2 = MetricCollectors::new("sink_2".to_string(), reqs.clone());
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.2");
        stat2.record_task("sink_2", "10.0.0.3");
        stat2.record_task("sink_2", "10.0.0.4");
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.5");
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.3");
        stat2.record_task("sink_2", "10.0.0.1");
        stat2.record_task("sink_2", "10.0.0.1");
        println!("---------------stat2--------------------");
        for mut i in stat2.items {
            let slices = i.collect_stat();
            println!("{}", slices);
            sum.merge_slice(ReportVariant::Stat(slices));
        }

        println!("---------------total--------------------");
        println!("{}", sum);
    }
}
