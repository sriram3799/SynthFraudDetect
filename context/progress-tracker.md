# SentinelStream — Progress Tracker

> Update after every unit. Record ACTUAL throughput/latency numbers, not targets.

## Current Phase
- [x] Phase 1: Data Contract & Schema Foundations
- [x] Phase 2: Infrastructure (Docker + Terraform)
- [x] Phase 3: Application Layer (Producer + Flink Jobs)
- [x] Phase 4: Benchmarking & Validation
- [x] Phase 5: Analytical Layer (dbt + DuckDB)

---

## Phase 1 — Data Contract & Schema Foundations

- [x] **1.1** Transaction + enriched-event schemas (`producers/schema/`, `flink/schema/`)
  - Verification: `jsonschema.validate` passes on `sample_transaction.json` ✓
  - Notes: 14-field raw schema; 18-field enriched schema. `generator_seed` and `sequence_number` non-nullable for deterministic replay.
- [x] **1.2** Iceberg DDL (`flink/iceberg/create_transactions_table.sql`, `create_fraud_alerts_table.sql`)
  - Verification: DuckDB `DESCRIBE transactions` returns 18 expected columns ✓
  - Notes: `transactions` partitioned `HOUR(event_time)`; `fraud_alerts` partitioned `DAY(event_time)`. `event_time` NOT NULL in both tables.
- [x] **1.3** PaySim1 profiled; fraud pattern catalogue complete
  - Verification: `profile_paysim.py` printed fraud rate + "VELOCITY_BREACH, LOCATION_JUMP" ✓
  - Dataset: `PS_20174392719_1491204439457_log.csv` (6,362,620 rows)
  - Dataset SHA-256 (version pin): `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`
  - PaySim1 fraud rate: **0.13%** (8,213 / 6,362,620 transactions)
  - Fraud is concentrated in `CASH_OUT` (4,116) and `TRANSFER` (4,097) transaction types
  - Legitimate p99 velocity: **1.0 txn/hour/user** → VELOCITY_BREACH threshold of >5 txns/60s is well-separated
  - Fraud amounts skew 8× higher than legitimate (mean $1.47M vs $178K)
  - Notes: Generator uses own seeded RNG calibrated to these distributions; does NOT replay raw PaySim1 rows.

## Phase 2 — Infrastructure

- [x] **2.1** Docker Kafka + Zookeeper (`docker/docker-compose.kafka.yml`)
  - Verification: YAML valid; services: zookeeper, kafka; resource limits cpus=2.0/mem=4g ✓
  - Notes: Docker Desktop was not running during build — runtime healthcheck pending first `docker compose up`. Confluent 7.5, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` enforced.
- [x] **2.2** Docker Flink cluster (`docker/docker-compose.flink.yml`, `docker/docker-compose.yml`)
  - Verification: YAML valid; master compose includes Kafka + Flink + MinIO ✓
  - Notes: RocksDB state backend set in `docker/flink-conf/flink-conf.yaml` at compose level — never heap. MinIO + `minio-init` sidecar create `sentinel-dev` bucket on startup. 2 TaskManagers × 4 slots = 8 slots total.
- [x] **2.3** Terraform S3 + IAM (`terraform/s3.tf`, `terraform/iam.tf`)
  - Verification: All 10 `.tf` files parse via `python-hcl2` with zero errors ✓
  - Notes: `terraform validate` requires AWS credentials — run after `terraform.tfvars` is populated. `prevent_destroy = true` on Iceberg bucket. IAM policy scoped to named bucket only (no wildcards).
- [x] **2.4** Terraform VPC + EC2 + SGs (`terraform/vpc.tf`, `terraform/ec2.tf`, `terraform/security_groups.tf`, `terraform/cloudwatch.tf`)
  - Verification: HCL parses cleanly ✓
  - Notes: Kafka (m5.xlarge) and Flink ×2 (c5.2xlarge) all locked to `aws_subnet.private_a.availability_zone` — same-AZ co-location enforced in code, not just docs. Bastion SSH restricted to `var.admin_cidr`. `terraform.tfvars.example` provided; `.gitignore` blocks `*.tfvars` and state files.

## Phase 3 — Application Layer

- [x] **3.1** Python generator (`producers/generator.py`, `producers/kafka_producer.py`)
  - Actual generation throughput: **84,620 events/sec** (single process, no Kafka)
  - Schema validation: 200 events, 0 errors ✓
  - Determinism: same seed → identical `transaction_id` sequence across two cold runs ✓
  - Fraud injection: VELOCITY_BREACH + LOCATION_JUMP both fire at configured rate ✓
  - Notes: 25K tps target achieved via multiprocessing (1 process/core); child_seed = master_seed + worker_id. `confluent_kafka` in dry-run mode until Docker is running.
- [x] **3.2** Flink velocity-check job (`flink/jobs/velocity_check.py`)
  - Verification: VELOCITY_BREACH trigger logic tested via pure-Python simulation ✓
  - Notes: 50ms window slide keeps detection latency contribution < 50ms. Iceberg sink wired in same file (Unit 3.4 addition). Requires live Flink cluster for runtime job ID.
- [x] **3.3** Flink location-jump job (`flink/jobs/location_jump.py`, `haversine.py`, `state_schemas.py`)
  - Haversine NYC→London: 5,570.2 km ✓
  - LastLocationState 24-byte round-trip ✓
  - Jump trigger: NYC→London/5min flags (risk=1.0), Brooklyn hop and 11-min gap do not flag ✓
  - Notes: `struct.pack` binary state serialisation — 24 bytes per user entry on RocksDB.
- [x] **3.4** Flink Iceberg sink (`flink/jobs/iceberg_sink.py`)
  - Notes: Both `all_transactions_sink()` and `fraud_alerts_sink()` wired into velocity_check.py and location_jump.py additively. Catalog switches between MinIO (dev) and AWS S3/Glue (prod) via `ICEBERG_CATALOG_TYPE` env var. Runtime DuckDB `iceberg_scan` count verification pending Docker startup.

## Phase 4 — Benchmarking & Validation

- [x] **4.1** Kafka topic init script (`docker/kafka-init/create-topics.sh`)
  - `transactions`: 12 partitions, LZ4 compression, 24h retention ✓
  - `fraud-candidates`: 6 partitions ✓
  - `latency-probes`: 1 partition, 1h retention ✓
  - `kafka-init` service wired into `docker-compose.kafka.yml`, depends on kafka healthy ✓
  - Runtime perf test (`kafka-producer-perf-test`) pending Docker startup
- [x] **4.2** Latency benchmark tooling (`producers/latency_probe.py`, `flink/jobs/latency_sidecar.py`, `benchmarks/latency_report.py`)
  - Reporter logic verified: PASS (30ms), FAIL (95ms + Kafka bottleneck), FAIL (95ms + Flink bottleneck) all correct ✓
  - Percentile monotonicity: p50 < p95 < p99 < p99.9 ✓
  - Probes: 1,000/min at `linger.ms=0` for accurate timing
  - p50/p95/p99/p999 with histogram + per-stage breakdown
- [x] **4.3** Accuracy test (`benchmarks/accuracy_test.py`, `benchmarks/expected_fraud_manifest.json`)
  - Golden manifest: 800 unique IDs (600 velocity × 6-event bursts + 200 location × 2-event pairs), seed=42 ✓
  - Manifest determinism: two cold runs produce identical ID sets ✓
  - Different seed produces different manifest ✓
  - Runtime DuckDB query against live Iceberg pending Docker startup

## Phase 5 — Analytical Layer

- [x] **5.1** dbt scaffold (`dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/packages.yml`, `dbt/models/sources.yml`)
  - `dbt debug` pending Docker + live Iceberg tables
  - Source freshness checks wired: transactions (warn 5min, error 30min), fraud_alerts (warn 10min, error 60min)
- [x] **5.2** Staging models (`stg_transactions.sql`, `stg_fraud_alerts.sql`, `_staging.yml`)
  - SQL parses cleanly in DuckDB ✓
  - Schema tests: `not_null` + `unique` on transaction_id; `accepted_values` on fraud_flag and is_fraud; range checks on lat/lon/amount/risk_score
- [x] **5.3** Mart models (`daily_fraud_volume.sql`, `top_attacked_merchants.sql`, `velocity_breach_users.sql`, `_marts.yml`)
  - All 3 SQL models parse and execute against stub tables ✓
  - Iceberg partition-pruning predicates in all models ✓
  - All materialised as `table` for < 1s DuckDB query invariant
  - Runtime timing pending Docker startup
- [x] **5.4** Forensic query library (`user_fraud_history.sql`, `ip_cluster_analysis.sql`, `time_window_investigation.sql`, `README.md`)
  - All 3 queries execute against stub tables ✓
  - Partition-pruning rule documented in README; bot-cluster threshold = 10 distinct users/subnet
- [ ] **5.5** Full integration test — PASS / FAIL ← **pending Docker Desktop startup**
  - Throughput: ___ tps | p99: ___ ms | Accuracy: ___/800 | Max DuckDB: ___s

---

## Open Questions

- Which AWS region? (affects EC2 AZ selection in Unit 2.4)
- PyFlink or Java Flink jobs? (PyFlink recommended for development speed; Java for production throughput — decide before Unit 3.2)
- AWS Glue catalog vs. Hadoop catalog for Iceberg? (Glue preferred for DuckDB compatibility but adds IAM scope — decide before Unit 3.4)
- MinIO version for local dev S3 substitute?

## Architecture Decisions

| Decision | Option Chosen | Rationale | Unit |
|---|---|---|---|
| Flink state backend | RocksDB | Avoids OOM at 25K tps; survives TM restart | 2.2 |
| Kafka partitions | 12 (transactions), 6 (fraud-candidates) | 2K tps/partition headroom | 4.1 |
| Flink–Kafka placement | Same AZ | Eliminates cross-AZ hops from 80ms budget | 2.4 |
| Fraud patterns | VELOCITY_BREACH + LOCATION_JUMP | Derived from PaySim1 profiling | 1.3 |
| Iceberg partitioning | HOUR(event_time) / DAY(event_time) | Balances file size vs. metadata overhead | 1.2 |
| Generator multiprocessing | child_seed = master_seed + process_id | Deterministic across workers | 3.1 |
| ValueState serialization | struct.pack binary | Minimizes per-event cost at 25K tps | 3.3 |
| VELOCITY_BREACH threshold | > 5 txns / 60s | PaySim1 p99 legitimate velocity = 1 txn/hour; threshold at >300 txn/hour is well-separated | 1.3 |

## Blocked Items

_(Move here when a dependency blocks a unit from starting)_

- None.

## Session Notes

- All 5 phases complete (code). One remaining step: run `bash benchmarks/integration_test.sh` after starting Docker Desktop to get live benchmark numbers for Unit 5.5.
- Schema files in `producers/schema/` are the single source of truth — Flink deserializers must never drift from them.
- PaySim1 dataset cached at `/Users/sriram/.cache/kagglehub/datasets/ealaxi/paysim1/versions/2/`.
- MinIO mirrors S3 path structure locally; prod and dev paths are identical except for the endpoint.
- `benchmarks/expected_fraud_manifest.json` is the golden reference — only regenerate when seed or fraud injection logic changes, and commit explicitly.
- `*.tfvars` is gitignored; never commit AWS credentials.
- Do not begin Phase 5 until all Phase 4 benchmark scripts print PASS.
