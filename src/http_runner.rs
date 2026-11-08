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

pub async fn run_http_benchmark(_config: &HttpBenchmarkConfig) -> BenchmarkReport {
    todo!()
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
}
