use std::time::{Duration, Instant};
use metrics::{counter, describe_counter};

/// An `EventLog` provides a log of events associated with a specific object.
pub trait EventLog {
    /// Formats its arguments with fmt.Sprintf and adds the
    /// result to the event log.
    fn printf(&self);

    /// Like printf. but it marks this event as an error.
    fn errorf(&self);

    /// Declares that this event log is complete.
    /// The event log should not be used after calling this method.
    fn finish(&self);
}


pub fn cost() {
    let start = Instant::now();
    let delta = start.elapsed();
}

pub fn wait_lsm_into_disk(n: Duration) {
    counter!("wait_lsm_into_disk", n.as_millis() as u64);
}

pub fn compact_cost_time(n: Duration) {
    counter!("compact_cost_time", n.as_millis() as u64);
}

pub fn stats() {}

#[test]
fn t_stats() {
    {
        let recorder = metrics_prometheus::install();

        // Either use `metrics` crate interfaces.
        metrics::increment_counter!("count", "whose" => "mine", "kind" => "owned");
        metrics::increment_counter!("count", "whose" => "mine", "kind" => "ref");
        metrics::increment_counter!("count", "kind" => "owned", "whose" => "dummy");

        // Or construct and provide `prometheus` metrics directly.
        recorder.register_metric(prometheus::Gauge::new("value", "help").unwrap());
    }
    let _ = metrics_prometheus::try_install();
    let report = prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::default_registry().gather()).unwrap();

    println!("{}", report);
}