# SentinelStream — Forensic Query Library

Parameterised DuckDB SQL runbooks for security analyst investigation.
These are **not** dbt models — they run directly against live Iceberg tables
via DuckDB's `iceberg_scan()` table function.

---

## Connection Prerequisites

### Local dev (MinIO)
All queries default to the local MinIO endpoint. Run them as-is after
`docker compose up -d`.

### AWS prod
Export these variables before running, then replace `s3://sentinel-dev`
with your actual bucket name:

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
# Modify SET s3_endpoint / s3_use_ssl lines in each query accordingly
```

---

## The Partition-Pruning Rule

> **Every forensic query MUST include at least one `event_time` predicate.**

Without it, DuckDB scans every Parquet file across all partitions — at
25K tps that is gigabytes of data per query. With a predicate, only the
matching Iceberg partitions are opened.

| Table | Partition granularity | Minimum predicate |
| :--- | :--- | :--- |
| `transactions` | `HOUR(event_time)` | `event_time >= now() - INTERVAL N HOURS` |
| `fraud_alerts` | `DAY(event_time)` | `event_time >= now() - INTERVAL N DAYS` |

If you need a full historical scan for research purposes, acknowledge the
cost by adding `-- FULL SCAN: intentional` as a comment.

---

## Queries

### `user_fraud_history.sql`

Complete transaction timeline for a single user.

```bash
duckdb -c "$(sed 's/{{ user_id }}/user_00042_00017/g' \
    dbt/forensics/user_fraud_history.sql)"
```

**Returns:** All transactions for the user in the last 7 days, ordered by
`event_time ASC`. Includes IP, coordinates, device fingerprint, and fraud flags.
Expect ≥ 6 rows for any user that triggered a `VELOCITY_BREACH` burst.

---

### `ip_cluster_analysis.sql`

Identify /24 subnets with suspicious multi-user activity.

```bash
duckdb -c "$(cat dbt/forensics/ip_cluster_analysis.sql)"
```

**Returns:** Top 100 /24 subnets in the last 24 hours, ranked by `distinct_users`.
Subnets with > 10 distinct users are flagged `SUSPECT_CLUSTER`.
Typical query time: < 1 second (24-hour partition window).

---

### `time_window_investigation.sql`

All fraud events within a specific time window. Use during incident response.

```bash
duckdb -c "$(sed \
    -e 's/{{ start_time }}/2025-05-06 00:00:00/g' \
    -e 's/{{ end_time }}/2025-05-06 06:00:00/g' \
    dbt/forensics/time_window_investigation.sql)"
```

**Returns:** All `fraud_alerts` rows in the window, ordered by `event_time ASC`,
`risk_score DESC`. The `start_time` / `end_time` values drive Iceberg DAY
partition selection — narrower windows are faster.

---

## Adding New Forensic Queries

1. Create `dbt/forensics/<descriptive_name>.sql`.
2. Include the MinIO connection preamble (copy from an existing query).
3. Add at least one `event_time` partition-pruning predicate; add the comment
   `-- FULL SCAN: intentional` if omitting it is deliberate.
4. Document the query in this README under a new `###` heading.
5. Verify `time duckdb -c "$(cat dbt/forensics/<name>.sql)"` returns in < 1s
   under a representative data volume.
