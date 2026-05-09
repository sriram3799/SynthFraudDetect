-- Iceberg table for fraud-ONLY enriched events (is_fraud = TRUE).
-- Partition strategy: DAY(event_time) — fraud events are < 2% of total volume,
-- so hourly partitions would produce tiny files. Daily granularity keeps files
-- at a healthy size while still supporting DuckDB day-range pruning.
-- This table is the primary source for dbt mart models and forensic queries.

CREATE TABLE IF NOT EXISTS sentinel.fraud_alerts (
    -- Identity fields
    transaction_id      VARCHAR         NOT NULL,
    user_id             VARCHAR         NOT NULL,
    merchant_id         VARCHAR         NOT NULL,
    sequence_number     BIGINT          NOT NULL,
    generator_seed      BIGINT          NOT NULL,

    -- Transaction payload
    amount_usd          DOUBLE          NOT NULL,
    currency            VARCHAR(3)      NOT NULL,
    event_time          TIMESTAMP       NOT NULL,   -- partition key

    -- Geographic and device context
    ip_address          VARCHAR         NOT NULL,
    latitude            DOUBLE          NOT NULL,
    longitude           DOUBLE          NOT NULL,
    device_fingerprint  VARCHAR         NOT NULL,
    session_id          VARCHAR         NOT NULL,

    -- Fraud enrichment (all rows here have is_fraud = TRUE)
    risk_score          DOUBLE          NOT NULL,
    fraud_flags         ARRAY<STRING>   NOT NULL,
    is_fraud            BOOLEAN         NOT NULL,
    processing_time_ms  BIGINT          NOT NULL,
    flink_processing_timestamp TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'write.format.default'          = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes'  = '134217728',
    'write.upsert.enabled'          = 'false',
    'write.distribution-mode'       = 'hash',
    'write.wap.enabled'             = 'false',
    'history.expire.max-snapshot-age-ms' = '2592000000'  -- 30 days (audit trail)
);
