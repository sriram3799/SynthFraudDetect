# AI Workflow Rules

## Approach

Build this project using a **data-first, spec-driven workflow**. This architecture relies on the precise orchestration of distributed systems; therefore, development must proceed by first defining the data contract and infrastructure state in the context files. Every implementation step must align with the performance benchmarks (80ms p99 latency) and structural invariants (Iceberg ACID compliance) defined in the architecture. Do not skip infrastructure provisioning steps to write application logic.

## Scoping Rules

- Work on one pipeline stage at a time (e.g., Kafka to Flink, then Flink to S3).
- Prefer small, verifiable data flow increments over large "black box" deployments.
- Do not modify infrastructure (Terraform) and processing logic (Flink) in the same implementation step.

## When to Split Work

Split an implementation step if it combines:

- **Infrastructure & Application Logic:** (e.g., Provisioning S3 buckets and writing Flink Sink logic).
- **Multiple System Boundaries:** (e.g., Adjusting Kafka retention and dbt transformation logic).
- **Data Schema Changes:** Any change that requires a migration of the Iceberg table format must be its own isolated unit of work.

If a data packet cannot be traced from the start to the end of the current unit (e.g., from Kafka producer to Flink output), the scope is too broad—split it.

## Handling Missing Requirements

- Do not invent "placeholder" data formats or simulated behaviors not defined in the context files.
- If the throughput or latency requirements for a new feature are ambiguous, resolve them in `architecture.md` before coding.
- If a specific AWS permission or network rule is missing, document it as a blocker in `progress-tracker.md` before attempting a deployment.

## Protected Files

Do not modify the following unless explicitly instructed:

- `terraform/.terraform.lock.hcl` — Managed by Terraform CLI.
- `dbt_project/dbt_packages/*` — Managed by dbt deps.
- Any auto-generated Iceberg metadata files or raw S3 parquet snapshots.

## Keeping Docs in Sync

Update the relevant context file whenever implementation changes:

- **Performance Baselines:** If a change impacts the 25K events/sec or 80ms latency targets.
- **Environment Variables:** Update `architecture.md` if new AWS or Kafka secrets are required.
- **DAG Flow:** If the order of transformations in dbt or the windowing logic in Flink is altered.

## Before Moving to the Next Unit

1. The data flows correctly through the unit (verified via logs or DuckDB query).
2. No invariant defined in `architecture.md` (e.g., Exactly-once processing) was violated.
3. `progress-tracker.md` reflects the specific throughput achieved during the unit test.
4. **Terraform Validation:** `terraform validate` and `terraform plan` show zero unexpected changes.
5. **Docker Health:** All containers in the stack report a `healthy` status.