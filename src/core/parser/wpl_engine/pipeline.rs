use crate::core::prelude::*;
use crate::core::sinks::sync_sink::traits::SyncCtrl;
use crate::facade::test_helpers::SinkTerminal;
use crate::sinks::SinkGroupAgent;
use crate::stat::MonSend;
use crate::stat::metric_collect::MetricCollectors;
use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use wp_parse_api::{DataResult, RawData};
use wp_stat::StatRecorder;
use wp_stat::StatReq;
use wpl::WparseResult;
use wpl::{AnnotationFunc, AnnotationType};
use wpl::{OPTIMIZE_TIMES, WplEvaluator};

#[derive(Getters, Clone)]
pub struct WplPipeline {
    parser: WplEvaluator,
    fun_vec: Vec<AnnotationType>,
    pub hit_cnt: usize,
    pub access_cnt: usize,
    pub index: usize,
    output: Vec<SinkGroupAgent>,
    send_rr: Arc<AtomicUsize>,
    wpl_key: String,
    s_name: String,
    stat_ext: MetricCollectors,
}

impl WplPipeline {
    pub fn new(
        index: usize,
        wpl_key: String,
        fun_vec: Vec<AnnotationType>,
        parser: WplEvaluator,
        output: Vec<SinkGroupAgent>,
        stat_reqs: Vec<StatReq>,
    ) -> Self {
        //let s_name = name.split('/').last().unwrap_or(&name);
        let s_name = wpl_key.clone();
        let stat_ext = MetricCollectors::new(wpl_key.clone(), stat_reqs);

        Self {
            parser,
            fun_vec,
            index,
            wpl_key,
            output,
            send_rr: Arc::new(AtomicUsize::new(0)),
            hit_cnt: 0,
            access_cnt: 0,
            s_name,
            stat_ext,
        }
    }

    pub fn short_name(&self) -> &str {
        self.s_name.as_str()
    }

    fn next_output_index(&self) -> usize {
        self.send_rr.fetch_add(1, AtomicOrdering::Relaxed) % self.output().len()
    }

    pub fn get_rolled_end(&self) -> &SinkTerminal {
        let idx = self.next_output_index();
        self.output[idx].end_point()
    }
    pub fn proc(&mut self, data: &SourceEvent, oth_suc_len: usize) -> DataResult {
        self.access_cnt += 1;
        match self
            .parser
            .proc(data.event_id, data.payload.clone(), oth_suc_len)
        {
            Ok((mut record, left)) => {
                self.stat_ext.record_begin(self.wpl_key.as_str(), None);
                for func in self.fun_vec.iter() {
                    func.proc(data, &mut record)?;
                }
                self.stat_ext
                    .record_end(self.wpl_key.as_str(), Some(&record));
                Ok((record, RawData::from_string(left)))
            }
            Err(e) => Err(e),
        }
    }
    pub async fn send_stat(&mut self, mon_send: &MonSend) -> WparseResult<()> {
        self.stat_ext.send_stat(mon_send).await.owe_sys()?;
        Ok(())
    }
    pub fn stop(&mut self) {
        for out in &mut self.output {
            out.end_mut().stop().expect("stop error");
        }
    }
}

//#[derive(Clone)]

impl Eq for WplPipeline {}

impl PartialEq<Self> for WplPipeline {
    fn eq(&self, _other: &Self) -> bool {
        todo!()
    }
}

impl PartialOrd<Self> for WplPipeline {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for WplPipeline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_hit = OPTIMIZE_TIMES - self.hit_cnt;
        let other_hit = OPTIMIZE_TIMES - other.hit_cnt;
        self_hit.cmp(&other_hit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wp_conf::structure::{FixedGroup, SinkGroupConf};

    fn build_pipeline(output_cnt: usize) -> WplPipeline {
        let evaluator = WplEvaluator::from_code("rule demo { ( _ ) }").expect("build wpl");
        let mut output = Vec::with_capacity(output_cnt);
        for _ in 0..output_cnt {
            output.push(SinkGroupAgent::new(
                SinkGroupConf::Fixed(FixedGroup::default_ins()),
                SinkTerminal::null(),
            ));
        }
        WplPipeline::new(
            0,
            "demo/rule".to_string(),
            Vec::new(),
            evaluator,
            output,
            Vec::new(),
        )
    }

    #[test]
    fn rolled_index_advances_by_one_per_send_and_ignores_hit_cnt() {
        let mut p = build_pipeline(3);
        p.hit_cnt = 100;
        let first = p.next_output_index();

        p.hit_cnt = 1;
        let second = p.next_output_index();
        let third = p.next_output_index();

        assert_eq!((first + 1) % 3, second);
        assert_eq!((second + 1) % 3, third);
    }

    #[test]
    fn rolled_index_is_shared_across_clones() {
        let p = build_pipeline(2);
        let p2 = p.clone();

        let first = p.next_output_index();
        let second = p2.next_output_index();

        assert_eq!((first + 1) % 2, second);
    }
}
