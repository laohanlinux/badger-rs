use crate::table::table::Table;
use lazy_static::lazy_static;
use prometheus::{Gauge, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};
use std::fmt;
use std::fmt::Formatter;
use std::time::{Duration, Instant};

lazy_static! {
    static ref EV: EvMetrics = EvMetrics {
        lsm_size: IntGaugeVec::new(
            prometheus::Opts::new("badger_lsm_size_bytes", "lsm size bytes by direct"),
            &["direct"]
        )
        .unwrap(),
        vlog_size: IntGauge::new("vlog_size", "vlog size bytes").unwrap(),
        pending_writes: IntGauge::new("pending_writes_total", "pending writes total").unwrap(),
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
        num_puts: IntCounter::new("num_puts", "number of puts").unwrap(),
        block_hash_calc_cost: IntCounter::new(
            "block_hash_calc_cost",
            "block hash calc cost for bloom"
        )
        .unwrap(),
    };
}

#[derive(Debug)]
pub struct EvMetrics {
    pub lsm_size: IntGaugeVec,
    pub vlog_size: IntGauge,
    pub pending_writes: IntGauge,

    /// These are cumulative
    pub num_reads: IntCounter,
    pub num_writes: IntCounter,
    pub num_bytes_read: IntCounter,
    pub num_bytes_written: IntCounter,
    pub num_lsm_gets: IntCounter,
    pub num_lsm_bloom_hits: IntCounter,
    pub num_gets: IntCounter,
    pub num_puts: IntCounter,
    pub num_blocked_puts: IntCounter,
    /// number of the memtable gets
    pub num_mem_tables_gets: IntCounter,
    pub block_hash_calc_cost: IntCounter,
}

impl fmt::Display for EvMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use tabled::{Table, Tabled};

        #[derive(Tabled)]
        struct KeyPair {
            label: String,
            value: String,
        }
        let mut kv = vec![];
        kv.push(KeyPair {
            label: "num_reads".to_owned(),
            value: self.num_reads.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_writes".to_owned(),
            value: self.num_writes.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_bytes_read".to_owned(),
            value: self.num_bytes_read.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_bytes_written".to_owned(),
            value: self.num_bytes_written.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_lsm_gets".to_owned(),
            value: self.num_lsm_gets.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_lsm_bloom_hits".to_owned(),
            value: self.num_lsm_bloom_hits.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_gets".to_owned(),
            value: self.num_gets.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_puts".to_owned(),
            value: self.num_puts.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_blocked_puts".to_owned(),
            value: self.num_blocked_puts.get().to_string(),
        });
        kv.push(KeyPair {
            label: "num_mem_tables_gets".to_owned(),
            value: self.num_mem_tables_gets.get().to_string(),
        });
        kv.push(KeyPair {
            label: "block_hash_calc_cost".to_owned(),
            value: self.block_hash_calc_cost.get().to_string(),
        });
        let table_str = Table::new(kv).to_string();
        f.write_str(&table_str)
    }
}

pub fn get_metrics() -> &'static EvMetrics {
    &EV
}