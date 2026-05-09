"""
Latency probe injector.

Injects 1,000 probe transactions per minute into the `transactions` topic.
Each probe carries a `probe` flag and an `emit_timestamp_ns` field (nanoseconds).
The Flink latency sidecar (flink/jobs/latency_sidecar.py) appends
`flink_received_ns` and `iceberg_commit_ns` and forwards to `latency-probes`.

The latency_report.py script reads `latency-probes` and computes percentiles.

Usage (run alongside the main producer):
  python producers/latency_probe.py --bootstrap-server localhost:9092 --duration 300
"""

import argparse
import json
import signal
import sys
import time
import uuid

_KAFKA_AVAILABLE = True
try:
    from confluent_kafka import Producer
except ImportError:
    _KAFKA_AVAILABLE = False
    print("[WARN] confluent_kafka not installed — dry-run mode.", file=sys.stderr)

PROBE_RATE_PER_MIN: int = 1_000
PROBE_INTERVAL_S: float = 60.0 / PROBE_RATE_PER_MIN   # 0.06s between probes


def _make_probe(seq: int) -> dict:
    now_ns = time.time_ns()
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"probe_user_{seq % 100:03d}",
        "merchant_id": "probe_merchant_000",
        "amount_usd": 1.00,
        "currency": "USD",
        "event_time": now_ns // 1_000_000,   # epoch ms for schema compliance
        "ip_address": "127.0.0.1",
        "latitude": 0.0,
        "longitude": 0.0,
        "device_fingerprint": "probe",
        "session_id": f"probe_sess_{seq}",
        "generator_seed": -1,
        "sequence_number": seq,
        # Probe-specific fields (outside strict schema — Flink reads these)
        "probe": True,
        "emit_timestamp_ns": now_ns,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="SentinelStream latency probe injector")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--duration", type=int, default=300, help="Seconds to run")
    args = parser.parse_args()

    producer = None
    if _KAFKA_AVAILABLE:
        producer = Producer({
            "bootstrap.servers": args.bootstrap_server,
            "linger.ms": 0,        # send probes immediately for accurate timing
            "acks": "1",           # leader ack only — probes don't need idempotence
            "log_level": 4,        # WARNING only — suppress APIVERSION_QUERY noise
        })

    stop = False

    def _shutdown(sig: int, frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    deadline = time.monotonic() + args.duration
    seq = 0
    sent = 0
    next_send = time.monotonic()

    print(f"[probe] Injecting {PROBE_RATE_PER_MIN} probes/min into '{args.topic}'...")

    while not stop and time.monotonic() < deadline:
        probe = _make_probe(seq)
        payload = json.dumps(probe).encode("utf-8")

        if producer:
            producer.produce(
                topic=args.topic,
                key=probe["user_id"].encode("utf-8"),
                value=payload,
            )
            producer.poll(0)

        seq += 1
        sent += 1

        next_send += PROBE_INTERVAL_S
        lag = next_send - time.monotonic()
        if lag > 0:
            time.sleep(lag)

    if producer:
        producer.flush(timeout=10)

    print(f"[probe] Done. Sent {sent:,} probe events.")


if __name__ == "__main__":
    main()
