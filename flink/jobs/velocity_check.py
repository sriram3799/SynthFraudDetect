"""
Flink job: VELOCITY_BREACH detection.

Consumes the `transactions` Kafka topic, keys by user_id, and uses a
sliding event-time window (60s window, 50ms slide) to count transactions
per user. Any user exceeding 5 transactions in the window is flagged with
VELOCITY_BREACH and emitted to the `fraud-candidates` Kafka topic.

The 50ms slide — rather than the default 1s — keeps the worst-case window
firing latency contribution to < 50ms, staying within the 80ms p99 budget.

Output to Iceberg is added in Unit 3.4 (iceberg_sink.py) without touching
the detection logic here.
"""

import json
import os
from typing import Iterable

from pyflink.common import Types, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSourceBuilder,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction

from watermark_strategy import sentinel_watermark_strategy
from latency_sidecar import LatencySidecarFunction

KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "transactions"
OUTPUT_TOPIC = "fraud-candidates"
LATENCY_PROBES_TOPIC = "latency-probes"
CONSUMER_GROUP = "sentinel-velocity-check"

VELOCITY_WINDOW_SEC = 60
VELOCITY_SLIDE_MS = 50       # 50ms slide keeps detection latency < 50ms
VELOCITY_THRESHOLD = 5       # flag if count exceeds this


class VelocityWindowFunction(ProcessWindowFunction):
    """
    Counts transactions per user in the window.
    Emits a fraud-candidate JSON record if count > VELOCITY_THRESHOLD.
    """

    def process(
        self,
        key: str,
        context: "ProcessWindowFunction.Context",
        elements: Iterable[dict],
    ) -> Iterable[str]:
        events = list(elements)
        count = len(events)
        if count <= VELOCITY_THRESHOLD:
            return

        # Use the most recent event as the representative record.
        # Convert to plain dict (Row objects don't support ** unpacking).
        latest = max(events, key=lambda e: e["event_time"])
        latest_dict = latest.as_dict() if hasattr(latest, "as_dict") else dict(latest)
        excess = count - VELOCITY_THRESHOLD
        risk_score = min(1.0, round(0.5 + excess * 0.1, 3))

        candidate = {
            **latest_dict,
            "fraud_flags": ["VELOCITY_BREACH"],
            "is_fraud": True,
            "risk_score": risk_score,
            "processing_time_ms": (
                context.current_processing_time() - latest["event_time"]
            ),
            "flink_processing_timestamp": context.current_processing_time(),
        }
        yield json.dumps(candidate)


def build_transaction_row_type() -> Types:
    return Types.ROW_NAMED(
        [
            "transaction_id", "user_id", "merchant_id", "amount_usd", "currency",
            "event_time", "ip_address", "latitude", "longitude",
            "device_fingerprint", "session_id", "generator_seed", "sequence_number",
            "probe", "emit_timestamp_ns",
        ],
        [
            Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.STRING(),
            Types.LONG(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(),
            Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
            Types.BOOLEAN(), Types.LONG(),
        ],
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(4)

    # ------------------------------------------------------------------ #
    # Source: Kafka `transactions` topic
    # ------------------------------------------------------------------ #
    source = (
        KafkaSourceBuilder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id(CONSUMER_GROUP)
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(build_transaction_row_type())
            .build()
        )
        .build()
    )

    raw_stream = env.from_source(
        source=source,
        watermark_strategy=sentinel_watermark_strategy(),
        source_name="KafkaTransactionsSource",
    )

    # ------------------------------------------------------------------ #
    # Latency sidecar: annotate probe events and forward to latency-probes
    # Runs as a separate branch so it doesn't affect detection throughput.
    # Only velocity_check handles the sidecar (latency-probes is 1-partition).
    # ------------------------------------------------------------------ #
    probe_stream = (
        raw_stream
        .filter(lambda row: bool(row["probe"]))
        .map(LatencySidecarFunction(), output_type=Types.STRING())
    )
    probe_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(LATENCY_PROBES_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    probe_stream.sink_to(probe_sink)

    # ------------------------------------------------------------------ #
    # Detection: sliding event-time window, keyed by user_id
    # Exclude probe events — they carry synthetic data, not real users.
    # ------------------------------------------------------------------ #
    txn_stream = raw_stream.filter(lambda row: not row["probe"])
    fraud_stream = (
        txn_stream
        .key_by(lambda row: row["user_id"], key_type=Types.STRING())
        .window(
            SlidingEventTimeWindows.of(
                Time.seconds(VELOCITY_WINDOW_SEC),
                Time.milliseconds(VELOCITY_SLIDE_MS),
            )
        )
        .process(VelocityWindowFunction(), output_type=Types.STRING())
    )

    # ------------------------------------------------------------------ #
    # Sink: Kafka `fraud-candidates` topic
    # ------------------------------------------------------------------ #
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    fraud_stream.sink_to(sink)

    env.execute("sentinel-velocity-check")


if __name__ == "__main__":
    main()
