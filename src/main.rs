use std::path::{Path, PathBuf};
use std::process::ExitCode;
use clap::{Args, Parser, Subcommand};
use floodgate::grpc_runner::{run_grpc_benchmark, GrpcBenchmarkConfig};
use floodgate::http_runner::{run_http_benchmark, HttpBenchmarkConfig};
use floodgate::proto_loader::compile_grpc_payloads;
use floodgate::stats::{render_ascii_table, BenchmarkReport, SloCriteria};

#[derive(Parser, Debug)]
#[command(name = "floodgate")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Http(HttpArgs),
    Grpc(GrpcArgs),
}

#[derive(Args, Debug, Clone)]
pub struct SloArgs {
    #[arg(long)]
    pub max_p99_ms: Option<f64>,
    #[arg(long)]
    pub max_failed: Option<usize>,
    #[arg(long)]
    pub min_rps: Option<f64>,
}

#[derive(Args, Debug)]
pub struct HttpArgs {
    #[arg(long)]
    pub url: String,
    #[arg(long, default_value = "GET")]
    pub method: String,
    #[arg(long)]
    pub body: Option<String>,
    #[arg(short = 'n', long = "requests", default_value_t = 100)]
    pub total_requests: usize,
    #[arg(short = 'c', long = "concurrency", default_value_t = 10)]
    pub concurrency: usize,
    #[arg(long)]
    pub target_rps: Option<u32>,
    #[command(flatten)]
    pub slo: SloArgs,
}

#[derive(Args, Debug, Clone)]
pub struct GrpcArgs {
    #[arg(long)]
    pub url: String,
    #[arg(long)]
    pub proto: PathBuf,
    #[arg(long)]
    pub call: String,
    #[arg(long)]
    pub data: Vec<String>,
    #[arg(long)]
    pub data_file: Option<PathBuf>,
    #[arg(short = 'n', long = "requests", default_value_t = 100)]
    pub total_requests: usize,
    #[arg(short = 'c', long = "concurrency", default_value_t = 10)]
    pub concurrency: usize,
    #[arg(long, default_value_t = 1)]
    pub channels: usize,
    #[arg(long)]
    pub target_rps: Option<u32>,
    #[command(flatten)]
    pub slo: SloArgs,
}

fn build_http_config(args: &HttpArgs) -> HttpBenchmarkConfig {
    HttpBenchmarkConfig {
        url: args.url.clone(),
        method: args.method.clone(),
        body: args.body.clone(),
        total_requests: args.total_requests,
        concurrency: args.concurrency,
        target_rps: args.target_rps,
    }
}

fn build_slo_criteria(args: &SloArgs) -> Option<SloCriteria> {
    if args.max_p99_ms.is_none() && args.max_failed.is_none() && args.min_rps.is_none() {
        return None;
    }
    Some(SloCriteria {
        max_p99_ms: args.max_p99_ms.unwrap_or(f64::INFINITY),
        max_failed_requests: args.max_failed.unwrap_or(usize::MAX),
        min_rps: args.min_rps.unwrap_or(0.0),
    })
}

fn evaluate_and_print_report(report: &BenchmarkReport, slo: &SloArgs) -> ExitCode {
    eprintln!("{}", render_ascii_table(report));
    if let Ok(json) = report.to_json() {
        println!("{json}");
    }
    let Some(criteria) = build_slo_criteria(slo) else {
        return ExitCode::SUCCESS;
    };
    if let Err(breach) = report.check_slo(&criteria) {
        eprintln!("SLO breach: {}", breach.reason);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn run_http_command(args: &HttpArgs) -> ExitCode {
    let config = build_http_config(args);
    let report = run_http_benchmark(&config).await;
    evaluate_and_print_report(&report, &args.slo)
}

fn collect_json_payloads(inline: &[String], data_file: Option<&Path>) -> Result<Vec<String>, String> {
    let mut payloads = inline.to_vec();
    if let Some(path) = data_file {
        let contents = std::fs::read_to_string(path).map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        payloads.extend(contents.lines().map(str::trim).filter(|line| !line.is_empty()).map(str::to_string));
    }
    if payloads.is_empty() {
        return Err("provide at least one --data or --data-file entry".to_string());
    }
    Ok(payloads)
}

fn prepare_grpc_config(args: &GrpcArgs) -> Result<GrpcBenchmarkConfig, String> {
    let raw_payloads = collect_json_payloads(&args.data, args.data_file.as_deref())?;
    let payload_refs: Vec<&str> = raw_payloads.iter().map(String::as_str).collect();
    let compiled = compile_grpc_payloads(&args.proto, &args.call, &payload_refs)?;
    Ok(GrpcBenchmarkConfig {
        url: args.url.clone(),
        rpc_path: compiled.rpc_path,
        payloads: compiled.payloads,
        total_requests: args.total_requests,
        concurrency: args.concurrency,
        channels: args.channels,
        target_rps: args.target_rps,
    })
}

async fn run_grpc_command(args: &GrpcArgs) -> ExitCode {
    let args_owned = args.clone();
    let config = match tokio::task::spawn_blocking(move || prepare_grpc_config(&args_owned)).await {
        Ok(Ok(grpc_config)) => grpc_config,
        Ok(Err(err)) => { eprintln!("gRPC setup error: {err}"); return ExitCode::FAILURE; }
        Err(err) => { eprintln!("gRPC setup task failed: {err}"); return ExitCode::FAILURE; }
    };
    match run_grpc_benchmark(&config).await {
        Ok(report) => evaluate_and_print_report(&report, &args.slo),
        Err(err) => { eprintln!("gRPC benchmark failed: {err}"); ExitCode::FAILURE }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Http(args) => run_http_command(&args).await,
        Commands::Grpc(args) => run_grpc_command(&args).await,
    }
}

#[test]
fn test_cli_http_defaults() {
    let cli = Cli::try_parse_from(["floodgate", "http", "--url", "http://127.0.0.1"]).unwrap();
    let Commands::Http(args) = cli.command else { panic!("expected http"); };
    assert_eq!(args.concurrency, 10);
    assert_eq!(args.total_requests, 100);
    assert!(build_slo_criteria(&args.slo).is_none());
}

#[test]
fn test_build_config_and_slo() {
    let cli = Cli::try_parse_from(["floodgate", "http", "--url", "http://127.0.0.1", "--max-p99-ms", "25.0"]).unwrap();
    let Commands::Http(args) = cli.command else { panic!("expected http"); };
    assert_eq!(build_http_config(&args).url, "http://127.0.0.1");
    assert_eq!(build_slo_criteria(&args.slo).unwrap().max_p99_ms, 25.0);
}

#[cfg(test)]
async fn spawn_mock_server() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let _ = tokio::io::AsyncWriteExt::write_all(
                &mut socket,
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ).await;
        }
    });
    addr
}

#[tokio::test]
async fn test_run_http_command_success() {
    let addr = spawn_mock_server().await;
    let cli = Cli::try_parse_from([
        "floodgate", "http", "--url", &format!("http://{addr}"), "-n", "1", "-c", "1",
    ]).unwrap();
    let Commands::Http(args) = cli.command else { panic!("expected http"); };
    assert_eq!(run_http_command(&args).await, ExitCode::SUCCESS);
}

#[tokio::test]
async fn test_run_http_command_slo_failure() {
    let addr = spawn_mock_server().await;
    let cli = Cli::try_parse_from([
        "floodgate", "http", "--url", &format!("http://{addr}"),
        "-n", "1", "-c", "1", "--max-p99-ms", "0.0",
    ]).unwrap();
    let Commands::Http(args) = cli.command else { panic!("expected http"); };
    assert_eq!(run_http_command(&args).await, ExitCode::FAILURE);
}

#[test]
fn test_cli_grpc_defaults() {
    let cli = Cli::try_parse_from([
        "floodgate", "grpc", "--url", "http://127.0.0.1:50051",
        "--proto", "svc.proto", "--call", "demo.Echo/Say", "--data", r#"{"text":"hi"}"#,
    ]).unwrap();
    let Commands::Grpc(args) = cli.command else { panic!("expected grpc"); };
    assert_eq!(args.channels, 1);
    assert_eq!(args.concurrency, 10);
    assert_eq!(args.total_requests, 100);
    assert_eq!(args.data, vec![r#"{"text":"hi"}"#]);
    assert!(args.data_file.is_none());
}

#[tokio::test]
async fn test_run_grpc_command_missing_payloads_returns_failure() {
    let cli = Cli::try_parse_from([
        "floodgate", "grpc", "--url", "http://127.0.0.1:50051", "--proto", "svc.proto", "--call", "demo.Echo/Say",
    ]).unwrap();
    let Commands::Grpc(args) = cli.command else { panic!("expected grpc"); };
    assert_eq!(run_grpc_command(&args).await, ExitCode::FAILURE);
}

#[test]
fn test_prepare_grpc_config_merges_data_and_file() {
    let temp_dir = std::env::temp_dir();
    let proto_path = temp_dir.join("floodgate_cli_test.proto");
    let data_path = temp_dir.join("floodgate_cli_test.jsonl");
    std::fs::write(&proto_path, "syntax = \"proto3\"; package demo; service Echo { rpc Say (Msg) returns (Msg); } message Msg { string text = 1; }").unwrap();
    std::fs::write(&data_path, "\n  {\"text\":\"yo\"}  \n").unwrap();
    let cli = Cli::try_parse_from([
        "floodgate", "grpc", "--url", "http://127.0.0.1:50051", "--proto", proto_path.to_str().unwrap(),
        "--call", "demo.Echo/Say", "--data", r#"{"text":"hi"}"#, "--data-file", data_path.to_str().unwrap(), "--channels", "4",
    ]).unwrap();
    let Commands::Grpc(args) = cli.command else { panic!("expected grpc"); };
    let config = prepare_grpc_config(&args).unwrap();
    let _ = std::fs::remove_file(&proto_path);
    let _ = std::fs::remove_file(&data_path);
    assert_eq!(config.rpc_path, "/demo.Echo/Say");
    assert_eq!(config.channels, 4);
    assert_eq!(config.payloads, vec![bytes::Bytes::from_static(b"\x0a\x02hi"), bytes::Bytes::from_static(b"\x0a\x02yo")]);
}
