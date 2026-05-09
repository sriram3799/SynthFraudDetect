# Architecture Context

## Stack

| Layer | Technology | Role |
| :--- | :--- | :--- |
| **Data Generation** | Python (Faker/Custom) | Generates synthetic high-frequency transaction streams with fraud patterns. |
| **Ingestion** | Apache Kafka | Handles the real-time ingestion of synthetic transaction events. |
| **Stream Processing** | Apache Flink | Executes stateful fraud detection logic and sliding-window risk scoring. |
| **Table Format** | Apache Iceberg | Provides a reliable, transactional data lake for storing flagged transactions. |
| **Cloud Storage** | AWS S3 | Long-term storage for transaction logs and model training datasets. |
| **Transformation** | dbt (data build tool) | Models fraud metrics (e.g., chargeback rates) on DuckDB. |
| **Query Engine** | DuckDB | Powers sub-second forensic analysis and risk reporting. |
| **Infrastructure** | Terraform | Automates the deployment of AWS resources and networking. |

## System Boundaries

- `terraform/` — Manages the AWS VPC, S3 buckets, and EC2 instances hosting the Dockerized fraud engine.
- `flink-fraud-logic/` — Contains the core Java/Python Flink jobs that identify "velocity" and "location-jump" patterns.
- `synthetic-generator/` — Owns the logic for simulating realistic e-commerce traffic, including valid and fraudulent behaviors.
- `dbt_analytics/` — Responsible for aggregating detected fraud events into high-level risk dashboards.

## Storage Model

- **Apache Iceberg (S3)**: The primary historical record. It stores all "flagged" and "cleared" transactions for audit trails and future model training.
- **Flink State Store**: Holds ephemeral, in-memory transaction windows (e.g., "last 5 minutes of purchases per User ID") for real-time comparison.
- **DuckDB**: Serves as the analytical layer for analysts to query structured fraud reports locally and at high speed.

## Auth and Access Model

- **Role-Based Access (IAM)**: Services interact via IAM instance profiles; the Flink job has specific `S3:PutObject` permissions to write to the Iceberg lake.
- **Secure Networking**: All Kafka brokers and Flink task managers are isolated within private subnets, accessible only via a bastion host or VPN.
- **Audit Logging**: Every query made via DuckDB and every deployment via Terraform is logged to AWS CloudWatch for security compliance.

## Invariants

1. **Detection Latency**: The p99 time from transaction entry to fraud-flagging must remain under **80ms** to support real-time "decline" decisions.
2. **Deterministic Simulation**: Given the same seed, the synthetic generator must produce identical "fraud" events for testing and benchmarking.
3. **Data Integrity**: Flagged fraudulent transactions must be written to Iceberg with ACID guarantees; a system crash must not lose an "alert."
4. **Environment Parity**: The Dockerized local environment must mirror the AWS production environment's resource limits and Kafka configurations.

## Fraud Pattern Definitions

Two patterns are implemented. These thresholds were selected after PaySim1 profiling (see `producers/data_profiling/paysim_analysis.md`).

| Pattern | ID | Detection Rule |
| :--- | :--- | :--- |
| Velocity Breach | `VELOCITY_BREACH` | > 5 transactions in a 60-second sliding window for the same `user_id` |
| Location Jump | `LOCATION_JUMP` | > 500 km displacement between consecutive transactions for the same `user_id` within 10 minutes (Haversine distance) |

`risk_score` for `VELOCITY_BREACH` is proportional to the excess transaction count above the threshold.  
`risk_score` for `LOCATION_JUMP` is proportional to the computed travel speed (km/h); physically impossible speeds score near 1.0.

## Iceberg Table Layout

### S3 Path Conventions
```
s3://<iceberg_bucket>/iceberg/transactions/    — all enriched events (fraud + legitimate)
s3://<iceberg_bucket>/iceberg/fraud_alerts/   — fraud-only events (is_fraud = TRUE)
```
Local dev (MinIO): `s3://sentinel-dev/iceberg/transactions/` and `.../fraud_alerts/`

### Partition Strategy
| Table | Partition | Rationale |
| :--- | :--- | :--- |
| `transactions` | `HOUR(event_time)` | ~128 MB/file at 25K tps; enables DuckDB 1-hour window pruning |
| `fraud_alerts` | `DAY(event_time)` | < 2% fraud rate keeps file sizes healthy at daily granularity; 30-day audit retention |

### DDL Files
- `flink/iceberg/create_transactions_table.sql`
- `flink/iceberg/create_fraud_alerts_table.sql`

`event_time` is **NOT NULL** in both tables. The Flink Iceberg sink must reject any record missing this field before write.

## Docker ↔ AWS Resource Parity

| Service | Docker resource limit | AWS EC2 target |
| :--- | :--- | :--- |
| Kafka broker | `cpus: 2.0`, `mem_limit: 4g` | m5.xlarge (4 vCPU, 16 GB) |
| Flink TaskManager (×2) | `cpus: 4.0`, `mem_limit: 8g` | c5.2xlarge (8 vCPU, 16 GB) |
| Flink JobManager | `cpus: 1.0`, `mem_limit: 2g` | Collocated on one c5.2xlarge |

All Kafka and Flink EC2 nodes are placed in **the same Availability Zone** (AZ-a) to eliminate cross-AZ network latency from the 80ms budget.

## Environment Variables

All secrets are supplied at runtime via `terraform.tfvars` (gitignored) or container env files. Never hardcoded.

| Variable | Used by | Description |
| :--- | :--- | :--- |
| `AWS_REGION` | Terraform, Flink | Target AWS region |
| `ICEBERG_BUCKET_NAME` | Terraform, Flink, dbt | S3 bucket name for the Iceberg warehouse |
| `KAFKA_BOOTSTRAP_SERVERS` | Producer, Flink | Comma-separated broker list |
| `FLINK_INSTANCE_PROFILE_ARN` | Terraform output | IAM profile ARN attached to Flink EC2 nodes |