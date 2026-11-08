# Floodgate

High-throughput, zero-GC, dual-protocol load generator and latency profiler written in pure Rust.

Designed as a unified modern replacement for `wrk` (HTTP/1.1) and `ghz` (gRPC), built specifically for microsecond-accurate latency profiling and CI performance regression testing.

## Core Capabilities

1. **Zero Garbage Collection Skew**: Pure Rust runtime with zero GC pauses, ensuring p99/p99.9 measurements reflect true server/kernel latencies without client-side measurement jitter.
2. **Anti-Coordinated Omission**: Supports open-loop constant-arrival rate scheduling (`tokio::time::interval`) to prevent the closed-loop latency masking inherent in naive load tools.
3. **Multi-Dimensional CI SLO Guardrails**: Validates composite criteria (latency ceiling, error budget, throughput floor) via `SloCriteria` to prevent the Fast-Failure Paradox in automated pipelines.
4. **Unified Dual Protocol**: Seamlessly stress-tests both HTTP/1.1 REST JSON endpoints and HTTP/2 Protobuf gRPC services.

## Architecture

- `src/stats.rs`: Pure compute percentile calculations (p50/p90/p95/p99), ASCII report rendering, and SLO threshold assertions.
- `src/http_runner.rs`: Concurrent and open-loop HTTP/REST workload dispatch engine.
- `src/grpc_runner.rs`: High-throughput gRPC workload engine (upcoming).
- `src/main.rs`: CLI shell with exit-code CI enforcement (upcoming).
