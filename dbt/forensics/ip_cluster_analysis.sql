-- IP cluster analysis forensic query.
--
-- Groups transactions by /24 IPv4 subnet and identifies subnets with an
-- unusually high number of distinct users — a strong signal for bot farms,
-- credential-stuffing infrastructure, or shared proxy abuse.
--
-- A subnet with > 10 distinct users is flagged as a SUSPECT_CLUSTER.
-- Adjust the threshold (10) for your environment's traffic profile.
--
-- REQUIRED: event_time filter for partition pruning (see README.md).
--
-- Usage:
--   duckdb -c "$(cat dbt/forensics/ip_cluster_analysis.sql)"

INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

WITH raw AS (
    SELECT
        -- Extract /24 subnet prefix (first three octets of an IPv4 address)
        regexp_extract(ip_address, '^(\d{1,3}\.\d{1,3}\.\d{1,3})\.\d+$', 1)
                                            AS subnet_24,
        user_id,
        transaction_id,
        is_fraud,
        event_time
    FROM iceberg_scan('s3://sentinel-dev/iceberg/transactions/')
    -- Partition pruning predicate: MANDATORY
    WHERE event_time >= now() - INTERVAL 24 HOURS
      AND ip_address LIKE '%.%.%.%'    -- IPv4 only; extend for IPv6 as needed
),

aggregated AS (
    SELECT
        subnet_24,
        count(distinct user_id)             AS distinct_users,
        count(distinct transaction_id)      AS total_transactions,
        sum(CASE WHEN is_fraud THEN 1 ELSE 0 END)
                                            AS fraud_transactions,
        round(
            sum(CASE WHEN is_fraud THEN 1 ELSE 0 END)::DOUBLE
            / nullif(count(distinct transaction_id), 0) * 100,
            2
        )                                   AS fraud_rate_pct,
        min(event_time)                     AS first_seen,
        max(event_time)                     AS last_seen
    FROM raw
    WHERE subnet_24 IS NOT NULL AND subnet_24 != ''
    GROUP BY subnet_24
)

SELECT
    subnet_24,
    distinct_users,
    total_transactions,
    fraud_transactions,
    fraud_rate_pct,
    CASE WHEN distinct_users > 10 THEN 'SUSPECT_CLUSTER' ELSE 'NORMAL' END
                                            AS cluster_status,
    first_seen,
    last_seen
FROM aggregated
ORDER BY distinct_users DESC, fraud_rate_pct DESC
LIMIT 100;
