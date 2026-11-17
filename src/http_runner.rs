use crate::stats::BenchmarkReport;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct HttpBenchmarkConfig {
    pub url: String,
    pub method: String,
    pub body: Option<String>,
    pub total_requests: usize,
    pub concurrency: usize,
    pub target_rps: Option<u32>,
}

fn build_request(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    body: Option<&str>,
) -> reqwest::RequestBuilder {
    let builder = match method {
        "POST" => client.post(url),
        _ => client.get(url),
    };
    match body {
        Some(payload) => builder.header("Content-Type", "application/json").body(payload.to_string()),
        None => builder,
    }
}

async fn execute_timed_request(builder: reqwest::RequestBuilder) -> (bool, Duration) {
    let start = Instant::now();
    let is_ok = builder.send().await.map_or(false, |resp| resp.status().is_success());
    (is_ok, start.elapsed())
}

fn create_pacer(target_rps: Option<u32>, concurrency: usize) -> Option<tokio::time::Interval> {
    let rps = target_rps?;
    let worker_rps = (rps as f64 / concurrency.max(1) as f64).max(0.001);
    Some(tokio::time::interval(Duration::from_secs_f64(1.0 / worker_rps)))
}

async fn run_worker(
    client: reqwest::Client,
    config: HttpBenchmarkConfig,
    counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> Vec<(bool, Duration)> {
    let mut samples = Vec::new();
    let mut ticker = create_pacer(config.target_rps, config.concurrency);
    while counter.fetch_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
        if let Some(ref mut interval) = ticker {
            interval.tick().await;
        }
        let req = build_request(&client, &config.url, &config.method, config.body.as_deref());
        samples.push(execute_timed_request(req).await);
    }
    samples
}

fn aggregate_results(total_requests: usize, duration: Duration, results: Vec<(bool, Duration)>) -> BenchmarkReport {
    let mut successful = 0;
    let mut latencies = Vec::with_capacity(results.len());
    for (ok, latency) in results {
        if ok { successful += 1; }
        latencies.push(latency);
    }
    BenchmarkReport {
        protocol: "HTTP/1.1".to_string(),
        total_requests,
        successful_requests: successful,
        failed_requests: total_requests.saturating_sub(successful),
        total_duration: duration,
        percentiles: crate::stats::PercentileStats::compute(&latencies),
    }
}

pub async fn run_http_benchmark(config: &HttpBenchmarkConfig) -> BenchmarkReport {
    let (client, start) = (reqwest::Client::new(), Instant::now());
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(config.total_requests));
    let mut handles = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency.max(1) {
        handles.push(tokio::spawn(run_worker(client.clone(), config.clone(), counter.clone())));
    }
    let mut results = Vec::with_capacity(config.total_requests);
    for handle in handles {
        if let Ok(mut worker_samples) = handle.await {
            results.append(&mut worker_samples);
        }
    }
    aggregate_results(config.total_requests, start.elapsed(), results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_http_runner_mixed_responses_and_latencies() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for i in 0..5 {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let mut buf = [0u8; 512];
                    let _ = socket.read(&mut buf).await;
                    let status = if i < 3 { "200 OK" } else { "500 Internal Server Error" };
                    let response = format!("HTTP/1.1 {}\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK", status);
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            }
        });
        let config = HttpBenchmarkConfig {
            url: format!("http://{}", addr),
            method: "POST".to_string(),
            body: Some(r#"{"query":"test"}"#.to_string()),
            total_requests: 5,
            concurrency: 2,
            target_rps: None,
        };
        let report = run_http_benchmark(&config).await;
        assert_eq!(report.successful_requests, 3);
        assert_eq!(report.failed_requests, 2);
        assert!(report.percentiles.p50_ms > 0.0);
    }

    #[tokio::test]
    async fn test_http_runner_rate_pacing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..4 {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let mut buf = [0u8; 512];
                    let _ = socket.read(&mut buf).await;
                    let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK").await;
                }
            }
        });
        let config = HttpBenchmarkConfig {
            url: format!("http://{}", addr),
            method: "GET".to_string(),
            body: None,
            total_requests: 4,
            concurrency: 1,
            target_rps: Some(20),
        };
        let report = run_http_benchmark(&config).await;
        assert_eq!(report.successful_requests, 4);
        assert!(report.total_duration >= Duration::from_millis(120));
    }
}
