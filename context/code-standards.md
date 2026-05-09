# Code Standards

## General

- **Modular Logic**: Keep Flink transformations and dbt models small and single-purpose to simplify testing and debugging.
- **Idempotency**: Ensure that all data processing steps (ingestion through storage) can be safely retried without creating duplicate records or inconsistent states.
- **Root Cause Resolution**: Address performance bottlenecks or connectivity issues within the infrastructure layer rather than implementing "sleep" timers or manual retries in the application code.

## SQL & dbt

- **Strict Modeling**: All dbt models must include schema tests (not null, unique) for primary keys to ensure integrity within the data lake.
- **Performance-First SQL**: Avoid expensive joins on raw Iceberg tables; utilize partitioning and DuckDB’s columnar efficiency to minimize I/O.
- **Documentation**: Every dbt model must have a corresponding description in the `.yml` file explaining the business logic and the market data fields involved.

## Infrastructure (Terraform)

- **State Integrity**: Never modify AWS resources manually via the Console; all changes must be reconciled through Terraform to prevent state drift.
- **Resource Tagging**: Apply consistent tags (Project, Environment, Owner) to all provisioned resources for cost tracking and management.
- **Credential Safety**: Avoid hardcoding secrets; use environment variables or secure parameter stores for any sensitive authentication tokens.

## Python & Scripting

- **Type Hinting**: All Python scripts (producers/utilities) must use explicit type hints to improve maintainability and catch errors early.
- **Graceful Shutdowns**: Implement proper signal handling in Kafka producers and consumers to ensure connections are closed and buffers are flushed during container stops.
- **Validation at Boundaries**: Validate incoming market data packets against expected schemas before pushing them into the Kafka stream.

## Data and Storage

- **Iceberg Partitioning**: Partition data by event time (e.g., hourly or daily) to optimize query performance and metadata management.
- **ACID Compliance**: All writes to S3 must go through the Iceberg API to maintain transactional integrity; never write raw files directly to the data lake folders.
- **Storage Tiering**: Use S3 lifecycle policies to move older, infrequently accessed historical data to lower-cost storage classes.

## File Organization

- `terraform/` — Contains AWS provider configurations, IAM policies, and VPC network definitions.
- `flink/` — Houses the Java or Python-based Flink job definitions for real-time windowing logic.
- `dbt/` — Contains the SQL models, macros, and schema tests for the analytical transformation layer.
- `docker/` — Stores the Dockerfiles and compose YAMLs for local development and containerized service orchestration.
- `producers/` — Includes the logic for fetching or simulating the market tick data feed.