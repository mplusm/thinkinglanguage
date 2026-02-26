// ThinkingLanguage — Metrics collection and Prometheus exposition

use std::collections::HashMap;
use std::fmt;

/// A metrics registry that collects counters, gauges, and histograms.
#[derive(Debug, Clone, Default)]
pub struct MetricsRegistry {
    counters: HashMap<String, u64>,
    gauges: HashMap<String, f64>,
    histograms: HashMap<String, Vec<f64>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment a counter by 1.
    pub fn inc(&mut self, name: &str) {
        *self.counters.entry(name.to_string()).or_insert(0) += 1;
    }

    /// Increment a counter by a specific amount.
    pub fn inc_by(&mut self, name: &str, amount: u64) {
        *self.counters.entry(name.to_string()).or_insert(0) += amount;
    }

    /// Set a gauge value.
    pub fn set_gauge(&mut self, name: &str, value: f64) {
        self.gauges.insert(name.to_string(), value);
    }

    /// Record a histogram observation.
    pub fn observe(&mut self, name: &str, value: f64) {
        self.histograms
            .entry(name.to_string())
            .or_default()
            .push(value);
    }

    /// Get a counter value.
    pub fn counter(&self, name: &str) -> u64 {
        self.counters.get(name).copied().unwrap_or(0)
    }

    /// Get a gauge value.
    pub fn gauge(&self, name: &str) -> Option<f64> {
        self.gauges.get(name).copied()
    }

    /// Get histogram observations.
    pub fn histogram(&self, name: &str) -> Option<&Vec<f64>> {
        self.histograms.get(name)
    }

    /// Render metrics in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let mut output = String::new();

        // Counters
        let mut counter_names: Vec<_> = self.counters.keys().collect();
        counter_names.sort();
        for name in counter_names {
            let value = self.counters[name];
            output.push_str(&format!("# TYPE {name} counter\n"));
            output.push_str(&format!("{name} {value}\n"));
        }

        // Gauges
        let mut gauge_names: Vec<_> = self.gauges.keys().collect();
        gauge_names.sort();
        for name in gauge_names {
            let value = self.gauges[name];
            output.push_str(&format!("# TYPE {name} gauge\n"));
            output.push_str(&format!("{name} {value}\n"));
        }

        // Histograms (summary stats)
        let mut hist_names: Vec<_> = self.histograms.keys().collect();
        hist_names.sort();
        for name in hist_names {
            let values = &self.histograms[name];
            if values.is_empty() {
                continue;
            }
            let count = values.len() as f64;
            let sum: f64 = values.iter().sum();

            output.push_str(&format!("# TYPE {name} summary\n"));
            output.push_str(&format!("{name}_count {}\n", values.len()));
            output.push_str(&format!("{name}_sum {sum}\n"));
            output.push_str(&format!("{name}_avg {}\n", sum / count));
        }

        output
    }
}

impl fmt::Display for MetricsRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.render_prometheus())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let mut metrics = MetricsRegistry::new();
        metrics.inc("events_processed");
        metrics.inc("events_processed");
        metrics.inc_by("events_processed", 3);
        assert_eq!(metrics.counter("events_processed"), 5);
        assert_eq!(metrics.counter("nonexistent"), 0);
    }

    #[test]
    fn test_gauge() {
        let mut metrics = MetricsRegistry::new();
        metrics.set_gauge("pipeline_duration_ms", 1234.5);
        assert_eq!(metrics.gauge("pipeline_duration_ms"), Some(1234.5));
        assert_eq!(metrics.gauge("nonexistent"), None);
    }

    #[test]
    fn test_histogram() {
        let mut metrics = MetricsRegistry::new();
        metrics.observe("latency_ms", 10.0);
        metrics.observe("latency_ms", 20.0);
        metrics.observe("latency_ms", 30.0);

        let hist = metrics.histogram("latency_ms").unwrap();
        assert_eq!(hist.len(), 3);
    }

    #[test]
    fn test_prometheus_rendering() {
        let mut metrics = MetricsRegistry::new();
        metrics.inc_by("requests_total", 42);
        metrics.set_gauge("active_connections", 5.0);
        metrics.observe("request_duration_ms", 100.0);
        metrics.observe("request_duration_ms", 200.0);

        let output = metrics.render_prometheus();
        assert!(output.contains("# TYPE requests_total counter"));
        assert!(output.contains("requests_total 42"));
        assert!(output.contains("# TYPE active_connections gauge"));
        assert!(output.contains("active_connections 5"));
        assert!(output.contains("# TYPE request_duration_ms summary"));
        assert!(output.contains("request_duration_ms_count 2"));
        assert!(output.contains("request_duration_ms_sum 300"));
    }
}
