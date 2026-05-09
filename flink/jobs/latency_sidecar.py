"""
Flink latency sidecar.

Attaches as a SideOutput on both detection jobs. For each event where
`probe == true`, it appends two additional timestamps:
  - flink_received_ns: nanoseconds when Flink processed the event
  - iceberg_commit_ns: nanoseconds after the Iceberg write (approximated via
    Flink's processing time; exact Iceberg commit callback not available in
    PyFlink 1.18 without a custom sink wrapper)

Emits the annotated probe record to the `latency-probes` Kafka topic.
latency_report.py reads this topic to compute per-stage breakdowns.

Usage: imported and called from velocity_check.py and location_jump.py.
"""

import json
import time
from typing import Iterator

from pyflink.datastream.functions import MapFunction


class LatencySidecarFunction(MapFunction):
    """
    Annotates probe events with Flink-side timestamps.
    Non-probe events are passed through unchanged (returned as-is).
    """

    def map(self, record) -> str:
        # record is a PyFlink Row from JsonRowDeserializationSchema.
        # Row is not a dict subclass but supports subscript access and .as_dict().
        if hasattr(record, "as_dict"):
            event = record.as_dict()
        elif isinstance(record, dict):
            event = dict(record)
        else:
            try:
                event = json.loads(record)
            except (json.JSONDecodeError, TypeError):
                return str(record)

        if not event.get("probe"):
            return json.dumps(event)

        now_ns = time.time_ns()
        event["flink_received_ns"] = now_ns
        # Iceberg commit adds ~5–15ms overhead on top of Flink processing;
        # we approximate it here and correct with a calibration offset in
        # latency_report.py once a baseline is measured.
        event["iceberg_commit_ns"] = now_ns
        event["stage_breakdown"] = {
            "kafka_queue_ms": round(
                (now_ns - event["emit_timestamp_ns"]) / 1_000_000, 2
            ),
        }
        return json.dumps(event)
