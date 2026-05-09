"""
Flink job: LOCATION_JUMP detection.

Consumes the `transactions` Kafka topic, maintains a per-user ValueState
holding (last_lat, last_lon, last_event_time_ms) serialised as 24 bytes
via struct.pack, and emits a LOCATION_JUMP fraud candidate when:

  haversine_distance(last_pos, current_pos) > 500 km
  AND
  (current_event_time - last_event_time) < 10 minutes

risk_score is proportional to the implied travel speed (km/h); physically
impossible speeds (e.g. > 800 km/h) score near 1.0.

Output to Iceberg is added in Unit 3.4 without modifying this logic.
"""

import json
import os
from typing import Iterable

from pyflink.common import Types
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
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from haversine import haversine_km
from state_schemas import LastLocationState
from watermark_strategy import sentinel_watermark_strategy

KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "transactions"
OUTPUT_TOPIC = "fraud-candidates"
CONSUMER_GROUP = "sentinel-location-jump"

JUMP_DISTANCE_KM = 500.0      # minimum displacement to flag
JUMP_WINDOW_MS = 10 * 60_000  # 10 minutes in milliseconds
MAX_PLAUSIBLE_SPEED_KMH = 800.0   # commercial aircraft speed — above this → risk_score 1.0


class LocationJumpFunction(KeyedProcessFunction):
    """
    Stateful per-user function that tracks the last known position and
    emits a fraud candidate when an impossible location change is detected.
    """

    def open(self, runtime_context: object) -> None:
        descriptor = ValueStateDescriptor(
            "last_location",
            Types.PRIMITIVE_ARRAY(Types.BYTE()),
        )
        self._state = runtime_context.get_state(descriptor)

    def process_element(
        self,
        event: dict,
        ctx: "KeyedProcessFunction.Context",
    ) -> Iterable[str]:
        raw = self._state.value()

        cur_lat: float = event["latitude"]
        cur_lon: float = event["longitude"]
        cur_ts: int = event["event_time"]

        if raw is not None:
            prev = LastLocationState.from_bytes(bytes(raw))
            elapsed_ms = cur_ts - prev.event_time_ms
            distance_km = haversine_km(prev.latitude, prev.longitude, cur_lat, cur_lon)

            if 0 < elapsed_ms <= JUMP_WINDOW_MS and distance_km > JUMP_DISTANCE_KM:
                elapsed_h = elapsed_ms / 3_600_000.0
                speed_kmh = distance_km / elapsed_h if elapsed_h > 0 else float("inf")
                risk_score = min(1.0, round(speed_kmh / MAX_PLAUSIBLE_SPEED_KMH, 3))

                # Convert Row to plain dict before ** unpacking (Row is not a mapping).
                event_dict = event.as_dict() if hasattr(event, "as_dict") else dict(event)
                # ctx.timestamp() is event-time of the current element, not wall-clock;
                # use TimerService.current_processing_time() for real latency.
                processing_time = ctx.timer_service().current_processing_time()
                candidate = {
                    **event_dict,
                    "fraud_flags": ["LOCATION_JUMP"],
                    "is_fraud": True,
                    "risk_score": risk_score,
                    "processing_time_ms": processing_time - event["event_time"],
                    "flink_processing_timestamp": processing_time,
                }
                yield json.dumps(candidate)

        # Always update state to the current position
        self._state.update(
            list(LastLocationState(cur_lat, cur_lon, cur_ts).to_bytes())
        )


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

    fraud_stream = (
        raw_stream
        .key_by(lambda row: row["user_id"], key_type=Types.STRING())
        .process(LocationJumpFunction(), output_type=Types.STRING())
    )

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

    env.execute("sentinel-location-jump")


if __name__ == "__main__":
    main()
