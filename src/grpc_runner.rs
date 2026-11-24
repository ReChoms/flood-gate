use crate::pacer::await_scheduled_turn;
use crate::stats::BenchmarkReport;
use bytes::{Buf, BufMut, Bytes};
use http::uri::PathAndQuery;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::time::Duration;
use tokio::time::Instant;
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

struct GrpcWorkerContext {
    rpc_path: PathAndQuery,
    encoded_payloads: Arc<Vec<Bytes>>,
    next_request_index: Arc<AtomicUsize>,
    total_requests: usize,
    target_requests_per_second: Option<u32>,
    benchmark_epoch: Instant,
}

async fn execute_timed_grpc_request(
    grpc_client: &mut tonic::client::Grpc<Channel>,
    rpc_path: PathAndQuery,
    request_payload: Bytes,
    scheduled_start: Instant,
) -> (bool, Duration) {
    let request_succeeded = match grpc_client.ready().await {
        Ok(()) => grpc_client.unary(tonic::Request::new(request_payload), rpc_path, RawBytesCodec).await.is_ok(),
        Err(_) => false,
    };
    (request_succeeded, scheduled_start.elapsed())
}

async fn run_grpc_worker(
    grpc_channel: Channel,
    worker_context: Arc<GrpcWorkerContext>,
) -> Vec<(bool, Duration)> {
    let (mut grpc_client, mut latency_samples) = (tonic::client::Grpc::new(grpc_channel), Vec::new());
    loop {
        let request_index = worker_context.next_request_index.fetch_add(1, Ordering::Relaxed);
        if request_index >= worker_context.total_requests {
            break;
        }
        let scheduled_start = await_scheduled_turn(worker_context.benchmark_epoch, request_index, worker_context.target_requests_per_second).await;
        let request_payload = worker_context.encoded_payloads[request_index % worker_context.encoded_payloads.len()].clone();
        latency_samples.push(execute_timed_grpc_request(&mut grpc_client, worker_context.rpc_path.clone(), request_payload, scheduled_start).await);
    }
    latency_samples
}

async fn prepare_grpc_session(
    benchmark_settings: &GrpcBenchmarkConfig,
) -> Result<(Vec<Channel>, PathAndQuery), String> {
    if benchmark_settings.payloads.is_empty() {
        return Err("at least one gRPC payload is required".to_string());
    }
    let rpc_path: PathAndQuery = benchmark_settings.rpc_path.parse().map_err(|parse_error| format!("invalid rpc path '{}': {parse_error}", benchmark_settings.rpc_path))?;
    let channel_pool = build_channel_pool(&benchmark_settings.url, benchmark_settings.channels.max(1)).await?;
    Ok((channel_pool, rpc_path))
}

pub async fn run_grpc_benchmark(benchmark_settings: &GrpcBenchmarkConfig) -> Result<BenchmarkReport, String> {
    let (channel_pool, rpc_path) = prepare_grpc_session(benchmark_settings).await?;
    let worker_count = benchmark_settings.concurrency.max(1);
    let benchmark_epoch = Instant::now();
    let worker_context = Arc::new(GrpcWorkerContext {
        rpc_path,
        encoded_payloads: Arc::new(benchmark_settings.payloads.clone()),
        next_request_index: Arc::new(AtomicUsize::new(0)),
        total_requests: benchmark_settings.total_requests,
        target_requests_per_second: benchmark_settings.target_rps,
        benchmark_epoch,
    });
    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_index in 0..worker_count {
        let grpc_channel = channel_pool[worker_index % channel_pool.len()].clone();
        worker_handles.push(tokio::spawn(run_grpc_worker(grpc_channel, worker_context.clone())));
    }
    let mut all_latency_samples = Vec::with_capacity(benchmark_settings.total_requests);
    for worker_handle in worker_handles {
        if let Ok(mut worker_samples) = worker_handle.await {
            all_latency_samples.append(&mut worker_samples);
        }
    }
    Ok(BenchmarkReport::from_samples("gRPC", benchmark_settings.total_requests, benchmark_epoch.elapsed(), &all_latency_samples))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{convert::Infallible, sync::{atomic::{AtomicUsize, Ordering}, Arc}, task::{Context, Poll}};
    use tonic::body::BoxBody;
    use tonic::codegen::{BoxFuture, Service};
    use tonic::server::{Grpc, NamedService};
    use tonic::transport::{server::TcpIncoming, Server};

    type ResponseRule = Arc<dyn Fn(usize) -> (Duration, Result<(), Status>) + Send + Sync>;

    #[derive(Clone)]
    struct MockEchoService {
        seen_requests: Arc<AtomicUsize>,
        response_rule: ResponseRule,
    }

    impl NamedService for MockEchoService {
        const NAME: &'static str = "demo.Echo";
    }

    impl Service<tonic::Request<()>> for MockEchoService {
        type Response = tonic::Response<Bytes>;
        type Error = Status;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
        fn call(&mut self, _req: tonic::Request<()>) -> Self::Future {
            let request_index = self.seen_requests.fetch_add(1, Ordering::SeqCst);
            let (response_delay, response_status) = (self.response_rule)(request_index);
            Box::pin(async move {
                if !response_delay.is_zero() {
                    tokio::time::sleep(response_delay).await;
                }
                response_status.map(|()| tonic::Response::new(Bytes::new()))
            })
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

    async fn spawn_grpc_mock<RuleFn>(response_rule: RuleFn) -> (std::net::SocketAddr, Arc<AtomicUsize>)
    where
        RuleFn: Fn(usize) -> (Duration, Result<(), Status>) + Send + Sync + 'static,
    {
        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_address = tcp_listener.local_addr().unwrap();
        let seen_requests = Arc::new(AtomicUsize::new(0));
        let mock_service = MockEchoService { seen_requests: seen_requests.clone(), response_rule: Arc::new(response_rule) };
        let incoming_stream = TcpIncoming::from_listener(tcp_listener, true, None).unwrap();
        tokio::spawn(Server::builder().add_service(mock_service).serve_with_incoming(incoming_stream));
        (server_address, seen_requests)
    }

    #[tokio::test]
    async fn test_grpc_runner_mixed_responses() {
        let (server_address, seen_requests) = spawn_grpc_mock(|request_index| {
            let status = if request_index < 3 { Ok(()) } else { Err(Status::internal("mock error")) };
            (Duration::ZERO, status)
        })
        .await;

        let config = GrpcBenchmarkConfig {
            url: format!("http://{server_address}"),
            rpc_path: "/demo.Echo/Say".to_string(),
            payloads: vec![Bytes::from_static(b"\x0a\x02hi"), Bytes::from_static(b"\x0a\x02yo")],
            total_requests: 5, concurrency: 2, channels: 1, target_rps: None,
        };
        let report = run_grpc_benchmark(&config).await.unwrap();
        assert_eq!(seen_requests.load(Ordering::SeqCst), 5);
        assert_eq!(report.protocol, "gRPC");
        assert_eq!(report.successful_requests, 3);
        assert_eq!(report.failed_requests, 2);
        assert!(report.percentiles.p50_ms > 0.0);
    }

    #[tokio::test]
    async fn test_grpc_runner_uniform_pacing_across_workers() {
        let (server_address, seen_requests) = spawn_grpc_mock(|_| (Duration::ZERO, Ok(()))).await;

        let config = GrpcBenchmarkConfig {
            url: format!("http://{server_address}"),
            rpc_path: "/demo.Echo/Say".to_string(),
            payloads: vec![Bytes::from_static(b"\x0a\x02hi")],
            total_requests: 4,
            concurrency: 4,
            channels: 1,
            target_rps: Some(20),
        };
        let report = run_grpc_benchmark(&config).await.unwrap();
        assert_eq!(seen_requests.load(Ordering::SeqCst), 4);
        assert_eq!(report.successful_requests, 4);
        assert!(
            report.total_duration >= Duration::from_millis(130),
            "expected >= 130ms, got {:?}",
            report.total_duration
        );
    }
}
