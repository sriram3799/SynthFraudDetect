-- Time-window investigation forensic query.
--
-- Returns all flagged fraud events within a specific time window.
-- Use this during incident response to reconstruct the full scope of an attack.
--
-- REQUIRED: Replace {{ start_time }} and {{ end_time }} with ISO-8601 timestamps
--   before running. These values drive Iceberg partition pruning.
--
-- Example timestamps:
--   start_time: '2025-05-06 00:00:00'
--   end_time:   '2025-05-06 06:00:00'
--
-- Usage:
--   duckdb -c "$(sed \
--     -e 's/{{ start_time }}/2025-05-06 00:00:00/g' \
--     -e 's/{{ end_time }}/2025-05-06 06:00:00/g' \
--     dbt/forensics/time_window_investigation.sql)"

INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

SELECT
    event_time,
    transaction_id,
    user_id,
    merchant_id,
    round(amount_usd, 2)                    AS amount_usd,
    currency,
    fraud_flags,
    round(risk_score, 3)                    AS risk_score,
    ip_address,
    round(latitude, 6)                      AS latitude,
    round(longitude, 6)                     AS longitude,
    device_fingerprint,
    processing_time_ms
FROM iceberg_scan('s3://sentinel-dev/iceberg/fraud_alerts/')
WHERE is_fraud = true
  -- Partition pruning predicates: MANDATORY — drives DAY partition selection
  AND event_time >= TIMESTAMPTZ '{{ start_time }}'
  AND event_time <  TIMESTAMPTZ '{{ end_time }}'
ORDER BY
    event_time     ASC,
    risk_score     DESC;
