"""
Flink SQL job: fraud-candidates → Iceberg fraud_alerts.

Reads from the `fraud-candidates` Kafka topic and inserts into the
sentinel.fraud_alerts Iceberg table via the Flink SQL Table API.

Why SQL instead of DataStream IcebergSink?
  PyFlink 1.18.0 ships no Python bindings for pyflink.datastream.connectors.iceberg.
  The recommended path for Iceberg writes in Python is the Table API (Flink SQL),
  which uses the iceberg-flink-runtime JAR through the catalog factory.

The job runs as a streaming INSERT and is submitted via:
  flink run --python iceberg_writer.py
It runs indefinitely on the cluster (detached after submission).
"""

import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    warehouse    = os.environ.get("ICEBERG_WAREHOUSE",      "s3://sentinel-dev/iceberg/")
    s3_endpoint  = os.environ.get("S3_ENDPOINT",            "http://minio:9000")
    aws_key      = os.environ.get("AWS_ACCESS_KEY_ID",      "minioadmin")
    aws_secret   = os.environ.get("AWS_SECRET_ACCESS_KEY",  "minioadmin")

    t_env = TableEnvironment.create(
        EnvironmentSettings.new_instance().in_streaming_mode().build()
    )
    # Use a single task slot so the job plays nicely with the 4-slot TaskManagers.
    t_env.get_config().set("parallelism.default", "1")

    # ------------------------------------------------------------------
    # 1. Register Iceberg Hadoop catalog
    #    Uses iceberg-flink-runtime JAR which is on the Flink lib/ classpath.
    # ------------------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE CATALOG sentinel_catalog WITH (
            'type'                 = 'iceberg',
            'catalog-type'         = 'hadoop',
            'warehouse'            = '{warehouse}',
            'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'          = '{s3_endpoint}',
            's3.path-style-access' = 'true',
            's3.access-key-id'     = '{aws_key}',
            's3.secret-access-key' = '{aws_secret}'
        )
    """)

    # ------------------------------------------------------------------
    # 2. Kafka source for fraud-candidates.
    #    Declares only the 18 fields that belong in fraud_alerts.
    #    The JSON connector silently ignores extra fields (probe, etc.).
    # ------------------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE TABLE fraud_candidates_src (
            transaction_id              STRING,
            user_id                     STRING,
            merchant_id                 STRING,
            amount_usd                  DOUBLE,
            currency                    STRING,
            event_time                  BIGINT,
            ip_address                  STRING,
            latitude                    DOUBLE,
            longitude                   DOUBLE,
            device_fingerprint          STRING,
            session_id                  STRING,
            generator_seed              BIGINT,
            sequence_number             BIGINT,
            risk_score                  DOUBLE,
            fraud_flags                 ARRAY<STRING NOT NULL>,
            is_fraud                    BOOLEAN,
            processing_time_ms          BIGINT,
            flink_processing_timestamp  BIGINT
        ) WITH (
            'connector'                       = 'kafka',
            'topic'                           = 'fraud-candidates',
            'properties.bootstrap.servers'    = '{kafka_brokers}',
            'properties.group.id'             = 'sentinel-iceberg-writer',
            'format'                          = 'json',
            'json.ignore-parse-errors'        = 'true',
            'scan.startup.mode'               = 'earliest-offset'
        )
    """)

    # ------------------------------------------------------------------
    # 3. Stream-INSERT into Iceberg.
    #    Only write rows where is_fraud = true (they all should be, but
    #    guard against any stray non-fraud candidates from Kafka retries).
    # ------------------------------------------------------------------
    t_env.execute_sql("""
        INSERT INTO `sentinel_catalog`.`sentinel`.`fraud_alerts`
        SELECT
            transaction_id,
            user_id,
            merchant_id,
            amount_usd,
            currency,
            event_time,
            ip_address,
            latitude,
            longitude,
            device_fingerprint,
            session_id,
            generator_seed,
            sequence_number,
            risk_score,
            fraud_flags,
            is_fraud,
            processing_time_ms,
            flink_processing_timestamp
        FROM fraud_candidates_src
        WHERE is_fraud = true
    """)
    # execute_sql() for a streaming INSERT submits the job and returns immediately;
    # the job continues running on the cluster after this script exits.


if __name__ == "__main__":
    main()
