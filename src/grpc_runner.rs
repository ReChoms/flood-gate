use crate::stats::BenchmarkReport;
use bytes::{Buf, BufMut, Bytes};
use http::uri::PathAndQuery;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::time::Duration;
use tokio::time::{Instant, Interval};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::{Channel, Endpoint};
use tonic::Status;

#[derive(Debug, Clone)]
pub struct GrpcBenchmarkConfig {
    pub url: String,
    pub rpc_path: String,
    pub payloads: Vec<Bytes>,
    pub total_requests: usize,
    pub concurrency: usize,
    pub channels: usize,
    pub target_rps: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default)]
struct RawBytesCodec;

impl Encoder for RawBytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

impl Decoder for RawBytesCodec {
    type Item = ();
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        src.advance(src.remaining());
        Ok(Some(()))
    }
}

impl Codec for RawBytesCodec {
    type Encode = Bytes;
    type Decode = ();
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        *self
    }

    fn decoder(&mut self) -> Self::Decoder {
        *self
    }
}

async fn build_channel_pool(url: &str, channel_count: usize) -> Result<Vec<Channel>, String> {
    let endpoint = Endpoint::from_shared(url.to_owned()).map_err(|err| err.to_string())?;
    let mut channels = Vec::with_capacity(channel_count);
    for _ in 0..channel_count {
        channels.push(endpoint.connect().await.map_err(|err| err.to_string())?);
    }
    Ok(channels)
}

fn create_pacer(target_rps: Option<u32>, concurrency: usize) -> Option<Interval> {
    let rps = target_rps?;
    let worker_rps = (rps as f64 / concurrency.max(1) as f64).max(0.001);
    Some(tokio::time::interval(Duration::from_secs_f64(1.0 / worker_rps)))
}

async fn run_grpc_worker(
    channel: Channel,
    path: PathAndQuery,
    payloads: Arc<Vec<Bytes>>,
    next_idx: Arc<AtomicUsize>,
    total_requests: usize,
    target_rps: Option<u32>,
    concurrency: usize,
) -> Vec<(bool, Duration)> {
    let (mut client, mut ticker, mut samples) = (tonic::client::Grpc::new(channel), create_pacer(target_rps, concurrency), Vec::new());
    loop {
        let idx = next_idx.fetch_add(1, Ordering::Relaxed);
        if idx >= total_requests { break; }
        let start = match ticker { Some(ref mut interval) => interval.tick().await, None => Instant::now() };
        let payload = payloads[idx % payloads.len()].clone();
        let is_ok = match client.ready().await {
            Ok(()) => client.unary(tonic::Request::new(payload), path.clone(), RawBytesCodec).await.is_ok(),
            Err(_) => false,
        };
        samples.push((is_ok, start.elapsed()));
    }
    samples
}

pub async fn run_grpc_benchmark(config: &GrpcBenchmarkConfig) -> Result<BenchmarkReport, String> {
    if config.payloads.is_empty() {
        return Err("at least one gRPC payload is required".to_string());
    }
    let path: PathAndQuery = config.rpc_path.parse().map_err(|err| format!("invalid rpc path '{}': {err}", config.rpc_path))?;
    let channels = build_channel_pool(&config.url, config.channels.max(1)).await?;
    let (payloads, next_idx, worker_count) = (Arc::new(config.payloads.clone()), Arc::new(AtomicUsize::new(0)), config.concurrency.max(1));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let channel = channels[worker_id % channels.len()].clone();
        handles.push(tokio::spawn(run_grpc_worker(channel, path.clone(), payloads.clone(), next_idx.clone(), config.total_requests, config.target_rps, worker_count)));
    }
    let mut samples = Vec::with_capacity(config.total_requests);
    for handle in handles {
        if let Ok(mut worker_samples) = handle.await { samples.append(&mut worker_samples); }
    }
    Ok(BenchmarkReport::from_samples("gRPC", config.total_requests, start.elapsed(), &samples))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{convert::Infallible, future::{ready, Ready}, sync::{atomic::{AtomicUsize, Ordering}, Arc}, task::{Context, Poll}};
    use tonic::body::BoxBody;
    use tonic::codegen::{BoxFuture, Service};
    use tonic::server::{Grpc, NamedService};
    use tonic::transport::{server::TcpIncoming, Server};

    #[derive(Clone)]
    struct MockEchoService { seen: Arc<AtomicUsize> }

    impl NamedService for MockEchoService {
        const NAME: &'static str = "demo.Echo";
    }

    impl Service<tonic::Request<()>> for MockEchoService {
        type Response = tonic::Response<Bytes>;
        type Error = Status;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
        fn call(&mut self, _req: tonic::Request<()>) -> Self::Future {
            if self.seen.fetch_add(1, Ordering::SeqCst) < 3 {
                ready(Ok(tonic::Response::new(Bytes::new())))
            } else {
                ready(Err(Status::internal("mock error")))
            }
        }
    }

    impl Service<http::Request<BoxBody>> for MockEchoService {
        type Response = http::Response<BoxBody>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Infallible>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
        fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
            let svc = self.clone();
            Box::pin(async move { Ok(Grpc::new(RawBytesCodec).unary(svc, req).await) })
        }
    }

    #[tokio::test]
    async fn test_grpc_runner_mixed_responses() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        tokio::spawn(Server::builder().add_service(MockEchoService { seen: seen.clone() }).serve_with_incoming(incoming));

        let config = GrpcBenchmarkConfig {
            url: format!("http://{addr}"),
            rpc_path: "/demo.Echo/Say".to_string(),
            payloads: vec![Bytes::from_static(b"\x0a\x02hi"), Bytes::from_static(b"\x0a\x02yo")],
            total_requests: 5, concurrency: 2, channels: 1, target_rps: None,
        };
        let report = run_grpc_benchmark(&config).await.unwrap();
        assert_eq!(seen.load(Ordering::SeqCst), 5);
        assert_eq!(report.protocol, "gRPC");
        assert_eq!(report.successful_requests, 3);
        assert_eq!(report.failed_requests, 2);
        assert!(report.percentiles.p50_ms > 0.0);
    }
}
