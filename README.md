# Floodgate

High-throughput, zero-GC, dual-protocol load generator and latency profiler written in pure Rust.

Designed as a unified modern replacement for `wrk` (HTTP/1.1) and `ghz` (gRPC), built specifically for microsecond-accurate latency profiling and CI performance regression testing.

## Core Capabilities

1. **Zero Garbage Collection Skew**: Pure Rust runtime with zero GC pauses, ensuring p99/p99.9 measurements reflect true server/kernel latencies without client-side measurement jitter.
2. **Anti-Coordinated Omission**: Uses a global open-loop arrival schedule (`src/pacer.rs` via `tokio::time::sleep_until` on `epoch + request_index / rps`) and measures elapsed latency from each request's scheduled start time so server stalls never hide queueing delays.
3. **Multi-Dimensional CI SLO Guardrails**: Validates composite criteria (latency ceiling, error budget, throughput floor) via `SloCriteria` to prevent the Fast-Failure Paradox in automated pipelines.
4. **Unified Dual Protocol**: Stress-tests both HTTP/1.1 REST JSON endpoints and schema-agnostic HTTP/2 Protobuf gRPC services from a single CLI (`stdout` JSON lines, `stderr` ASCII summary).

## Architecture

- `src/pacer.rs`: Shared global open-loop arrival schedule (`scheduled_deadline`, `await_scheduled_turn`) used by both HTTP and gRPC workers.
- `src/stats.rs`: Pure compute percentile calculations (p50/p90/p95/p99), ASCII report rendering, JSON serialization, and `SloCriteria` assertions.
- `src/http_runner.rs`: Concurrent and open-loop HTTP/1.1 REST workload dispatch engine (`reqwest`).
- `src/proto_loader.rs`: Startup `.proto` compilation (`protox` + `prost-reflect`) encoding JSON inputs into reusable protobuf payload rings.
- `src/grpc_runner.rs`: Pooled HTTP/2 gRPC workload engine (`tonic` + zero-decode `RawBytesCodec`).
- `src/main.rs`: `clap` CLI (`http` and `grpc` subcommands), `stdout` JSON-line output, `stderr` summary table, and exit-code SLO enforcement.

## Usage

```bash
# HTTP/1.1 benchmark with open-loop pacing and p99 SLO guardrail
cargo run --release -- http \
  --url http://127.0.0.1:8080/health \
  -n 1000 -c 32 --target-rps 100 --max-p99-ms 25

# Schema-agnostic gRPC benchmark using a .proto file and JSON payload
cargo run --release -- grpc \
  --url http://127.0.0.1:50051 \
  --proto ../code_app/proto/query_engine.proto \
  --call query_engine.QueryService/ExecuteSql \
  --data '{"query":"SELECT 1"}' \
  -n 1000 -c 32 --channels 4 --target-rps 100
```
