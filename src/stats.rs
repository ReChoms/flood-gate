use std::time::Duration;

#[derive(Debug, PartialEq)]
pub struct PercentileStats {
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

#[derive(Debug, PartialEq)]
pub struct BenchmarkReport {
    pub protocol: String,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub total_duration: Duration,
    pub percentiles: PercentileStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SloCriteria {
    pub max_p99_ms: f64,
    pub max_failed_requests: usize,
    pub min_rps: f64,
}

#[derive(Debug, PartialEq)]
pub struct SloBreach {
    pub reason: String,
}

impl BenchmarkReport {
    pub fn throughput_rps(&self) -> f64 {
        let duration_secs = self.total_duration.as_secs_f64();
        if duration_secs <= 0.0 {
            return 0.0;
        }
        self.total_requests as f64 / duration_secs
    }

    pub fn check_slo(&self, criteria: &SloCriteria) -> Result<(), SloBreach> {
        if self.percentiles.p99_ms > criteria.max_p99_ms {
            return Err(SloBreach {
                reason: format!(
                    "p99 {:.2}ms exceeded threshold {:.2}ms",
                    self.percentiles.p99_ms, criteria.max_p99_ms
                ),
            });
        }

        if self.failed_requests > criteria.max_failed_requests {
            return Err(SloBreach {
                reason: format!(
                    "{} failed requests exceeded threshold {}",
                    self.failed_requests, criteria.max_failed_requests
                ),
            });
        }

        let throughput = self.throughput_rps();
        if throughput < criteria.min_rps {
            return Err(SloBreach {
                reason: format!(
                    "throughput {:.2} req/s below floor {:.2} req/s",
                    throughput, criteria.min_rps
                ),
            });
        }

        Ok(())
    }
}

pub fn render_ascii_table(report: &BenchmarkReport) -> String {
    let duration_secs = report.total_duration.as_secs_f64();
    let throughput = report.throughput_rps();
    format!(
        "=== Floodgate Benchmark: {} ===\nRequests: {} total ({} ok, {} err)\nDuration: {:.2}s ({:.1} req/s)\nLatencies:\n  p50: {:.2}ms\n  p90: {:.2}ms\n  p95: {:.2}ms\n  p99: {:.2}ms",
        report.protocol, report.total_requests, report.successful_requests, report.failed_requests,
        duration_secs, throughput,
        report.percentiles.p50_ms, report.percentiles.p90_ms, report.percentiles.p95_ms, report.percentiles.p99_ms,
    )
}

fn sample_at_percentile(sorted_samples: &[Duration], percentile: f64) -> Duration {
    let rank = ((percentile / 100.0) * sorted_samples.len() as f64).ceil() as usize;
    let target_index = rank.saturating_sub(1).min(sorted_samples.len() - 1);
    sorted_samples[target_index]
}

impl PercentileStats {
    pub fn compute(latencies: &[Duration]) -> Self {
        if latencies.is_empty() {
            return Self { p50_ms: 0.0, p90_ms: 0.0, p95_ms: 0.0, p99_ms: 0.0 };
        }
        let mut sorted_latencies = latencies.to_vec();
        sorted_latencies.sort_unstable();
        Self {
            p50_ms: sample_at_percentile(&sorted_latencies, 50.0).as_secs_f64() * 1000.0,
            p90_ms: sample_at_percentile(&sorted_latencies, 90.0).as_secs_f64() * 1000.0,
            p95_ms: sample_at_percentile(&sorted_latencies, 95.0).as_secs_f64() * 1000.0,
            p99_ms: sample_at_percentile(&sorted_latencies, 99.0).as_secs_f64() * 1000.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentiles() {
        let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let stats = PercentileStats::compute(&samples);
        assert_eq!(stats.p50_ms, 50.0);
        assert_eq!(stats.p90_ms, 90.0);
        assert_eq!(stats.p95_ms, 95.0);
        assert_eq!(stats.p99_ms, 99.0);
    }

    #[test]
    fn test_empty_samples_return_zeros() {
        let stats = PercentileStats::compute(&[]);
        assert_eq!(stats.p50_ms, 0.0);
        assert_eq!(stats.p90_ms, 0.0);
        assert_eq!(stats.p95_ms, 0.0);
        assert_eq!(stats.p99_ms, 0.0);
    }

    #[test]
    fn test_single_sample() {
        let stats = PercentileStats::compute(&[Duration::from_millis(42)]);
        assert_eq!(stats.p50_ms, 42.0);
        assert_eq!(stats.p90_ms, 42.0);
        assert_eq!(stats.p95_ms, 42.0);
        assert_eq!(stats.p99_ms, 42.0);
    }

    #[test]
    fn test_render_ascii_table() {
        let report = BenchmarkReport {
            protocol: "REST".to_string(),
            total_requests: 100,
            successful_requests: 100,
            failed_requests: 0,
            total_duration: Duration::from_secs(2),
            percentiles: PercentileStats { p50_ms: 15.0, p90_ms: 25.0, p95_ms: 30.0, p99_ms: 45.0 },
        };
        let table = render_ascii_table(&report);
        assert!(table.contains("REST"));
        assert!(table.contains("p50: 15.00ms"));
    }

    #[test]
    fn test_slo_breach_detection() {
        let report = BenchmarkReport {
            protocol: "REST".to_string(),
            total_requests: 100,
            successful_requests: 98,
            failed_requests: 2,
            total_duration: Duration::from_secs(1),
            percentiles: PercentileStats { p50_ms: 5.0, p90_ms: 10.0, p95_ms: 20.0, p99_ms: 55.0 },
        };
        let passing_criteria = SloCriteria { max_p99_ms: 60.0, max_failed_requests: 5, min_rps: 50.0 };
        assert!(report.check_slo(&passing_criteria).is_ok());

        let latency_breach = SloCriteria { max_p99_ms: 50.0, max_failed_requests: 5, min_rps: 50.0 };
        assert_eq!(report.check_slo(&latency_breach).unwrap_err().reason, "p99 55.00ms exceeded threshold 50.00ms");

        let error_breach = SloCriteria { max_p99_ms: 60.0, max_failed_requests: 0, min_rps: 50.0 };
        assert_eq!(report.check_slo(&error_breach).unwrap_err().reason, "2 failed requests exceeded threshold 0");

        let rps_breach = SloCriteria { max_p99_ms: 60.0, max_failed_requests: 5, min_rps: 200.0 };
        assert_eq!(report.check_slo(&rps_breach).unwrap_err().reason, "throughput 100.00 req/s below floor 200.00 req/s");
    }
}
