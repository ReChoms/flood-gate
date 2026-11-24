use crate::pacer::await_scheduled_turn;
use crate::stats::BenchmarkReport;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::time::Duration;
use tokio::time::Instant;

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

async fn execute_timed_request(
    request_builder: reqwest::RequestBuilder,
    scheduled_start: Instant,
) -> (bool, Duration) {
    let request_succeeded = request_builder
        .send()
        .await
        .map_or(false, |http_response| http_response.status().is_success());
    (request_succeeded, scheduled_start.elapsed())
}

async fn run_worker(
    http_client: reqwest::Client,
    benchmark_settings: HttpBenchmarkConfig,
    next_request_index: Arc<AtomicUsize>,
    benchmark_epoch: Instant,
) -> Vec<(bool, Duration)> {
    let mut latency_samples = Vec::new();
    loop {
        let request_index = next_request_index.fetch_add(1, Ordering::Relaxed);
        if request_index >= benchmark_settings.total_requests {
            break;
        }
        let scheduled_start = await_scheduled_turn(
            benchmark_epoch,
            request_index,
            benchmark_settings.target_rps,
        )
        .await;
        let http_request = build_request(
            &http_client,
            &benchmark_settings.url,
            &benchmark_settings.method,
            benchmark_settings.body.as_deref(),
        );
        latency_samples.push(execute_timed_request(http_request, scheduled_start).await);
    }
    latency_samples
}

pub async fn run_http_benchmark(benchmark_settings: &HttpBenchmarkConfig) -> BenchmarkReport {
    let (http_client, benchmark_epoch) = (reqwest::Client::new(), Instant::now());
    let next_request_index = Arc::new(AtomicUsize::new(0));
    let mut worker_handles = Vec::with_capacity(benchmark_settings.concurrency);
    for _ in 0..benchmark_settings.concurrency.max(1) {
        worker_handles.push(tokio::spawn(run_worker(
            http_client.clone(),
            benchmark_settings.clone(),
            next_request_index.clone(),
            benchmark_epoch,
        )));
    }
    let mut all_latency_samples = Vec::with_capacity(benchmark_settings.total_requests);
    for worker_handle in worker_handles {
        if let Ok(mut worker_samples) = worker_handle.await {
            all_latency_samples.append(&mut worker_samples);
        }
    }
    BenchmarkReport::from_samples(
        "HTTP/1.1",
        benchmark_settings.total_requests,
        benchmark_epoch.elapsed(),
        &all_latency_samples,
    )
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

    async fn spawn_http_mock(count: usize, first_delay: Duration) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for i in 0..count {
                if let Ok((mut socket, _)) = listener.accept().await {
                    let mut buf = [0u8; 512];
                    let _ = socket.read(&mut buf).await;
                    if i == 0 && !first_delay.is_zero() {
                        tokio::time::sleep(first_delay).await;
                    }
                    let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK").await;
                }
            }
        });
        addr
    }

    #[tokio::test]
    async fn test_http_runner_coordinated_omission() {
        let addr = spawn_http_mock(3, Duration::from_millis(150)).await;
        let config = HttpBenchmarkConfig {
            url: format!("http://{addr}"),
            method: "GET".to_string(),
            body: None,
            total_requests: 3,
            concurrency: 1,
            target_rps: Some(20),
        };
        let report = run_http_benchmark(&config).await;
        assert_eq!(report.successful_requests, 3);
        assert!(
            report.percentiles.p50_ms >= 50.0,
            "expected p50 >= 50ms, got {}",
            report.percentiles.p50_ms
        );
    }

    #[tokio::test]
    async fn test_http_runner_uniform_pacing_across_workers() {
        let addr = spawn_http_mock(4, Duration::ZERO).await;
        let config = HttpBenchmarkConfig {
            url: format!("http://{addr}"),
            method: "GET".to_string(),
            body: None,
            total_requests: 4,
            concurrency: 4,
            target_rps: Some(20),
        };
        let report = run_http_benchmark(&config).await;
        assert_eq!(report.successful_requests, 4);
        assert!(
            report.total_duration >= Duration::from_millis(130),
            "expected >= 130ms, got {:?}",
            report.total_duration
        );
    }
}
