-- Iceberg table for ALL enriched transaction events (fraud and legitimate).
-- Partition strategy: HOUR(event_time) keeps individual files to ~128MB at 25K tps
-- while still allowing DuckDB to prune down to a 1-hour window in forensic queries.
-- event_time is NOT NULL enforced — the Flink sink must reject records missing it.

CREATE TABLE IF NOT EXISTS sentinel.transactions (
    -- Identity fields (from TransactionEvent)
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

    -- Flink enrichment fields
    risk_score          DOUBLE          NOT NULL,   -- [0.0, 1.0]
    fraud_flags         ARRAY<STRING>   NOT NULL,   -- e.g. ['VELOCITY_BREACH']
    is_fraud            BOOLEAN         NOT NULL,   -- denormalized for partition filter
    processing_time_ms  BIGINT          NOT NULL,   -- latency probe field
    flink_processing_timestamp TIMESTAMP NOT NULL   -- dbt source loaded_at_field
)
USING iceberg
PARTITIONED BY (hours(event_time))
TBLPROPERTIES (
    'write.format.default'          = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes'  = '134217728',  -- 128 MB
    'write.upsert.enabled'          = 'false',
    'write.distribution-mode'       = 'hash',
    'write.wap.enabled'             = 'false',
    'history.expire.max-snapshot-age-ms' = '604800000'  -- 7 days snapshot retention
);
