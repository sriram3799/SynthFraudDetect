"""
Shared Iceberg sink builder for SentinelStream Flink jobs.

Provides two sinks:
  - all_transactions_sink()  → sentinel.transactions  (all enriched events)
  - fraud_alerts_sink()      → sentinel.fraud_alerts   (is_fraud=True only)

Used by velocity_check.py and location_jump.py (Unit 3.4 additions).
The catalog is configured via iceberg-catalog.properties and environment
variables so the same code runs against MinIO locally and S3 in prod.
"""

import os

from pyflink.common import Types
from pyflink.datastream.connectors.iceberg import (
    IcebergSink,
    CatalogLoader,
    TableLoader,
)


# ---------------------------------------------------------------------------
# Enriched event schema as a Flink RowType
# Must stay in sync with flink/schema/enriched_event.json
# ---------------------------------------------------------------------------

ENRICHED_ROW_TYPE = Types.ROW_NAMED(
    [
        "transaction_id", "user_id", "merchant_id", "amount_usd", "currency",
        "event_time", "ip_address", "latitude", "longitude",
        "device_fingerprint", "session_id", "generator_seed", "sequence_number",
        "risk_score", "fraud_flags", "is_fraud", "processing_time_ms",
        "flink_processing_timestamp",
    ],
    [
        Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
        Types.LONG(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(),
        Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
        Types.DOUBLE(), Types.OBJECT_ARRAY(Types.STRING()), Types.BOOLEAN(),
        Types.LONG(), Types.LONG(),
    ],
)


def _catalog_loader() -> CatalogLoader:
    catalog_type = os.environ.get("ICEBERG_CATALOG_TYPE", "hadoop")
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3://sentinel-dev/iceberg/")
    region = os.environ.get("AWS_REGION", "us-east-1")

    props: dict[str, str] = {
        "type": catalog_type,
        "warehouse": warehouse,
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
        "s3.path-style-access": "true",
    }

    if catalog_type == "glue":
        props["glue.region"] = region

    return CatalogLoader.custom(
        "org.apache.iceberg.flink.FlinkCatalogFactory",
        props,
    )


def all_transactions_sink() -> IcebergSink:
    """Sink for ALL enriched events (fraud + legitimate)."""
    return (
        IcebergSink.for_row_data(
            TableLoader.from_catalog(
                _catalog_loader(),
                "sentinel",          # database
                "transactions",      # table
            )
        )
        .with_row_type(ENRICHED_ROW_TYPE)
        .with_upsert(False)
        .build()
    )


def fraud_alerts_sink() -> IcebergSink:
    """Sink for fraud-only events (is_fraud = True)."""
    return (
        IcebergSink.for_row_data(
            TableLoader.from_catalog(
                _catalog_loader(),
                "sentinel",
                "fraud_alerts",
            )
        )
        .with_row_type(ENRICHED_ROW_TYPE)
        .with_upsert(False)
        .build()
    )
