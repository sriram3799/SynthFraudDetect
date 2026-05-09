# SentinelStream — Benchmark Report

> Auto-populated by `benchmarks/integration_test.sh`.
> Do not edit manually — re-run the integration test to refresh.

## Run Metadata

- **Date:** <!-- BENCH_DATE -->
- **Seed:** 42
- **Duration:** 300s at 25,000 tps target

## Results

| Criterion | Target | Actual | Status |
| :--- | :--- | :--- | :--- |
| Throughput | ≥ 25,000 tps | <!-- BENCH_TPS --> tps | <!-- BENCH_TPS_STATUS --> |
| Kafka lag | 0 | <!-- BENCH_LAG --> | <!-- BENCH_LAG_STATUS --> |
| p99 latency | ≤ 80 ms | <!-- BENCH_P99 --> ms | <!-- BENCH_P99_STATUS --> |
| Fraud accuracy | 200/200 (100%) | <!-- BENCH_ACCURACY --> | <!-- BENCH_ACCURACY_STATUS --> |
| DuckDB p99 query | < 1.0 s | <!-- BENCH_DUCKDB --> s | <!-- BENCH_DUCKDB_STATUS --> |

## Latency Breakdown

| Stage | p50 | p95 | p99 | p99.9 |
| :--- | :--- | :--- | :--- | :--- |
| End-to-end | <!-- P50_E2E --> ms | <!-- P95_E2E --> ms | <!-- P99_E2E --> ms | <!-- P999_E2E --> ms |
| Kafka queue | <!-- P50_KAFKA --> ms | <!-- P95_KAFKA --> ms | <!-- P99_KAFKA --> ms | — |
| Flink processing | <!-- P50_FLINK --> ms | <!-- P95_FLINK --> ms | <!-- P99_FLINK --> ms | — |

## DuckDB Query Times

| Model | Query time |
| :--- | :--- |
| `daily_fraud_volume` | <!-- QT_DAILY --> s |
| `top_attacked_merchants` | <!-- QT_MERCHANTS --> s |
| `velocity_breach_users` | <!-- QT_VELOCITY --> s |

## Overall

****FAIL** — see benchmark_report.md**
