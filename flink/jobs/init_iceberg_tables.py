"""
One-shot Iceberg table initializer using boto3.

Writes minimal valid Iceberg v2 metadata directly to MinIO, bypassing all
JVM/Hadoop classpath dependencies. Creates two tables with the schema that
exactly matches ENRICHED_ROW_TYPE in iceberg_sink.py:

  sentinel.fraud_alerts   — fraud-only events
  sentinel.transactions   — all enriched events

Each table is unpartitioned (no event_date computed column), which is fine
for dev/test. Flink's IcebergSink appends new snapshot files after first
write; DuckDB iceberg_scan() works as soon as version-hint.text exists.

Environment variables (same as docker-compose.flink.yml):
  S3_ENDPOINT            default: http://minio:9000
  AWS_ACCESS_KEY_ID      default: minioadmin
  AWS_SECRET_ACCESS_KEY  default: minioadmin
"""

import json
import os
import time
import uuid

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Schema — must match ENRICHED_ROW_TYPE in iceberg_sink.py exactly.
# Iceberg types: string, double, long, boolean, list
# ---------------------------------------------------------------------------

_FIELDS = [
    {"id": 1,  "name": "transaction_id",             "required": True, "type": "string"},
    {"id": 2,  "name": "user_id",                    "required": True, "type": "string"},
    {"id": 3,  "name": "merchant_id",                "required": True, "type": "string"},
    {"id": 4,  "name": "amount_usd",                 "required": True, "type": "double"},
    {"id": 5,  "name": "currency",                   "required": True, "type": "string"},
    {"id": 6,  "name": "event_time",                 "required": True, "type": "long"},
    {"id": 7,  "name": "ip_address",                 "required": True, "type": "string"},
    {"id": 8,  "name": "latitude",                   "required": True, "type": "double"},
    {"id": 9,  "name": "longitude",                  "required": True, "type": "double"},
    {"id": 10, "name": "device_fingerprint",         "required": True, "type": "string"},
    {"id": 11, "name": "session_id",                 "required": True, "type": "string"},
    {"id": 12, "name": "generator_seed",             "required": True, "type": "long"},
    {"id": 13, "name": "sequence_number",            "required": True, "type": "long"},
    {"id": 14, "name": "risk_score",                 "required": True, "type": "double"},
    {"id": 15, "name": "fraud_flags",                "required": True, "type": {
        "type": "list",
        "element-id": 101,
        "element": "string",
        "element-required": True,
    }},
    {"id": 16, "name": "is_fraud",                   "required": True, "type": "boolean"},
    {"id": 17, "name": "processing_time_ms",         "required": True, "type": "long"},
    {"id": 18, "name": "flink_processing_timestamp", "required": True, "type": "long"},
]


def _metadata(location: str, now_ms: int) -> dict:
    """Return a minimal valid Iceberg format-version 2 table metadata dict."""
    return {
        "format-version": 2,
        "table-uuid": str(uuid.uuid4()),
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": now_ms,
        "last-column-id": 18,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [],
                "fields": _FIELDS,
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        "current-snapshot-id": -1,
        "refs": {},
        "snapshots": [],
        "statistics": [],
        "snapshot-log": [],
        "metadata-log": [],
    }


def main() -> None:
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    aws_key     = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret  = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    bucket      = "sentinel-dev"

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    now_ms = int(time.time() * 1000)

    tables = [
        (
            "iceberg/sentinel/fraud_alerts",
            "s3://sentinel-dev/iceberg/sentinel/fraud_alerts",
        ),
        (
            "iceberg/sentinel/transactions",
            "s3://sentinel-dev/iceberg/sentinel/transactions",
        ),
    ]

    for prefix, location in tables:
        table_name = prefix.split("/")[-1]
        meta_key    = f"{prefix}/metadata/v1.metadata.json"
        hint_key    = f"{prefix}/metadata/version-hint.text"

        # Skip if version-hint already exists (idempotent).
        try:
            s3.head_object(Bucket=bucket, Key=hint_key)
            print(f"[init_iceberg] Table sentinel.{table_name} already exists — skipping.")
            continue
        except ClientError:
            pass  # object not found — proceed to create

        meta_json = json.dumps(_metadata(location, now_ms), indent=2).encode("utf-8")

        s3.put_object(Bucket=bucket, Key=meta_key,  Body=meta_json, ContentType="application/json")
        s3.put_object(Bucket=bucket, Key=hint_key,  Body=b"1")
        print(f"[init_iceberg] Table sentinel.{table_name} ready.")

    print("[init_iceberg] All DDL complete. Iceberg tables are ready.")


if __name__ == "__main__":
    main()
