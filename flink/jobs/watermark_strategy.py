"""
Shared watermark strategy for all SentinelStream Flink jobs.

200ms out-of-orderness tolerance: enough to absorb network jitter between the
Kafka producer and Flink, without adding more than ~200ms to the latency budget
(leaving ~80ms for actual detection + Iceberg commit overhead).
"""

from pyflink.common import WatermarkStrategy
from pyflink.common import Duration


def sentinel_watermark_strategy() -> WatermarkStrategy:
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_millis(200))
        .with_timestamp_assigner(
            # event_time field is epoch ms — convert to long for Flink watermark
            lambda event, _: event["event_time"]
        )
    )
