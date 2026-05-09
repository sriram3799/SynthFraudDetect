-- User fraud history forensic query.
--
-- Returns the complete transaction timeline for a single suspect user,
-- ordered by event_time ascending. Includes full enriched metadata for
-- cross-referencing IP addresses, device fingerprints, and locations.
--
-- REQUIRED: Replace the {{ user_id }} placeholder before running.
-- REQUIRED: Always include the event_time filter to leverage Iceberg partition pruning.
--
-- Usage:
--   duckdb -c "$(sed 's/{{ user_id }}/user_00042_00017/g' \
--     dbt/forensics/user_fraud_history.sql)"

INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

SELECT
    transaction_id,
    event_time,
    merchant_id,
    round(amount_usd, 2)                        AS amount_usd,
    currency,
    risk_score,
    fraud_flags,
    is_fraud,
    ip_address,
    round(latitude, 6)                          AS latitude,
    round(longitude, 6)                         AS longitude,
    device_fingerprint,
    session_id,
    processing_time_ms
FROM iceberg_scan('s3://sentinel-dev/iceberg/transactions/')
WHERE user_id = '{{ user_id }}'
  -- Partition pruning predicate: always include an event_time range.
  -- Remove or widen this filter only if investigating historical incidents.
  AND event_time >= now() - INTERVAL 7 DAYS
ORDER BY
    event_time ASC;
