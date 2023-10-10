use lazy_static::lazy_static;
use prometheus::{Gauge, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};
use std::time::{Duration, Instant};

lazy_static! {
    pub static ref COMPACT_COST_TIME: IntCounter =
        IntCounter::new("compact", "cost").expect("metric can be created");
    pub static ref ZERO_LEVEL_WAIT: IntCounter =
        IntCounter::new("zero_wait", "cost").expect("metric can be created");
    pub static ref METRIC_WRITE_REQUEST: IntCounterVec =
        IntCounterVec::new(Opts::new("write_request", "Write Request Cost"), &["cost"])
            .expect("metric can be created");
}

pub fn register_custom_metrics(reg: &Registry) {
    reg.register(Box::new(COMPACT_COST_TIME.clone()))
        .expect("collector can be registered");
    reg.register(Box::new(ZERO_LEVEL_WAIT.clone()))
        .expect("collector can be registered");
}

pub fn stats() {}

lazy_static! {
    static ref EV: EvMetrics = EvMetrics {
        lsm_size: IntGaugeVec::new(prometheus::Opts::new("badger_lsm_size_bytes", "lsm size bytes by direct"), &["direct"]).unwrap(),
        vlog_size: IntGauge::new("vlog_size", "vlog size bytes").unwrap(),
        pending_writes: Gauge::new("pending_writes_total", "pending writes total").unwrap(),
        num_reads: IntCounter::new("num_reads", "number of reads").unwrap(),
        num_writes: IntCounter::new("num_writes", "number of writes").unwrap(),
        num_bytes_read: IntCounter::new("num_bytes_read", "bytes of read").unwrap(),
        num_bytes_written: IntCounter::new("num_bytes_written", "bytes of written").unwrap(),
        num_lsm_gets: IntCounter::new("num_lsm_gets", "number of lsm gets").unwrap(),
        num_lsm_bloom_hits: IntCounter::new("num_bloom_hits", "number of bloom hits").unwrap(),
        num_blocked_puts: IntCounter::new("num_blocked_hits", "number of blocked hits").unwrap(),
        num_mem_tables_gets: IntCounter::new("num_mem_tables", "number of the memtable gets")
            .unwrap(),
        num_gets: IntCounter::new("num_gets", "number of gets").unwrap(),
        block_hash_calc_cost: IntCounter::new(
            "block_hash_calc_cost",
            "block hash calc cost for bloom"
        )
        .unwrap(),
    };
}

pub struct EvMetrics {
    pub lsm_size: IntGaugeVec,
    pub vlog_size: IntGauge,
    pub pending_writes: Gauge,

    /// These are cumulative
    pub num_reads: IntCounter,
    pub num_writes: IntCounter,
    pub num_bytes_read: IntCounter,
    pub num_bytes_written: IntCounter,
    pub num_lsm_gets: IntCounter,
    pub num_lsm_bloom_hits: IntCounter,
    pub num_gets: IntCounter,
    pub num_blocked_puts: IntCounter,
    /// number of the memtable gets
    pub num_mem_tables_gets: IntCounter,
    pub block_hash_calc_cost: IntCounter,
}

pub fn get_metrics() -> &'static EvMetrics {
    &EV
}

#[test]
fn t_stats() {
    let reg = Registry::new();
    super::event::register_custom_metrics(&reg);
    COMPACT_COST_TIME.inc_by(19);
    println!("{}ms", COMPACT_COST_TIME.get());
}
