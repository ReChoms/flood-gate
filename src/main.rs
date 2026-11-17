use std::process::ExitCode;
use clap::{Args, Parser, Subcommand};
use floodgate::http_runner::HttpBenchmarkConfig;
use floodgate::stats::SloCriteria;

#[derive(Parser, Debug)]
#[command(name = "floodgate")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Http(HttpArgs),
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
    #[arg(long)]
    pub max_p99_ms: Option<f64>,
    #[arg(long)]
    pub max_failed: Option<usize>,
    #[arg(long)]
    pub min_rps: Option<f64>,
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

fn build_slo_criteria(args: &HttpArgs) -> Option<SloCriteria> {
    if args.max_p99_ms.is_none() && args.max_failed.is_none() && args.min_rps.is_none() {
        return None;
    }
    Some(SloCriteria {
        max_p99_ms: args.max_p99_ms.unwrap_or(f64::INFINITY),
        max_failed_requests: args.max_failed.unwrap_or(usize::MAX),
        min_rps: args.min_rps.unwrap_or(0.0),
    })
}

async fn run_http_command(_args: &HttpArgs) -> ExitCode {
    todo!()
}

fn main() {
    let _cli = Cli::parse();
}

#[test]
fn test_cli_http_defaults() {
    let cli = Cli::try_parse_from(["floodgate", "http", "--url", "http://127.0.0.1"]).unwrap();
    let Commands::Http(args) = cli.command;
    assert_eq!(args.concurrency, 10);
    assert_eq!(args.total_requests, 100);
    assert!(build_slo_criteria(&args).is_none());
}

#[test]
fn test_build_config_and_slo() {
    let cli = Cli::try_parse_from(["floodgate", "http", "--url", "http://127.0.0.1", "--max-p99-ms", "25.0"]).unwrap();
    let Commands::Http(args) = cli.command;
    assert_eq!(build_http_config(&args).url, "http://127.0.0.1");
    assert_eq!(build_slo_criteria(&args).unwrap().max_p99_ms, 25.0);
}

#[tokio::test]
async fn test_run_http_command_success() {
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
    let args = HttpArgs {
        url: format!("http://{addr}"),
        method: "GET".to_string(),
        body: None,
        total_requests: 1,
        concurrency: 1,
        target_rps: None,
        max_p99_ms: None,
        max_failed: Some(0),
        min_rps: None,
    };
    assert_eq!(run_http_command(&args).await, ExitCode::SUCCESS);
}
