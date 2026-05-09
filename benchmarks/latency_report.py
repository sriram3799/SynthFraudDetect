"""
End-to-end latency reporter.

Reads the `latency-probes` Kafka topic, computes p50/p95/p99/p999 latencies
across three stages, and prints a histogram.

SUCCESS criterion: p99 end-to-end <= 80ms.
If p99 > 80ms, the report identifies which stage is the bottleneck so
tuning targets the right component.

Usage:
  python benchmarks/latency_report.py \\
      --bootstrap-server localhost:9092 \\
      --topic latency-probes \\
      --timeout 30
"""

import argparse
import json
import sys
from collections import defaultdict
from typing import Optional

_KAFKA_AVAILABLE = True
try:
    from confluent_kafka import Consumer, KafkaError
except ImportError:
    _KAFKA_AVAILABLE = False

LATENCY_TARGET_MS: float = 80.0

BAR_WIDTH: int = 40
HISTOGRAM_BUCKETS: list[float] = [10, 20, 30, 40, 50, 60, 70, 80, 100, 150, 200, 500]


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    # Linear interpolation: index in [0, n-1]
    idx = (p / 100.0) * (len(sorted_vals) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def _bar(value: float, max_val: float) -> str:
    filled = int((value / max_val) * BAR_WIDTH) if max_val > 0 else 0
    return "█" * filled + "░" * (BAR_WIDTH - filled)


def _histogram(values: list[float], label: str) -> None:
    if not values:
        print(f"  {label}: no data")
        return
    counts: dict[float, int] = defaultdict(int)
    for v in values:
        for bucket in HISTOGRAM_BUCKETS:
            if v <= bucket:
                counts[bucket] += 1
                break
        else:
            counts[HISTOGRAM_BUCKETS[-1]] += 1

    max_count = max(counts.values()) if counts else 1
    print(f"\n  {label} distribution (n={len(values):,}):")
    prev = 0.0
    for bucket in HISTOGRAM_BUCKETS:
        c = counts.get(bucket, 0)
        print(
            f"  ≤{bucket:>5.0f}ms  {_bar(c, max_count)}  {c:>5,}  "
            f"({c/len(values)*100:5.1f}%)"
        )
        prev = bucket


def collect_probes(
    bootstrap_servers: str,
    topic: str,
    timeout_s: int,
) -> list[dict]:
    if not _KAFKA_AVAILABLE:
        print("[WARN] confluent_kafka not installed — cannot read probes.", file=sys.stderr)
        return []

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "sentinel-latency-reporter",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    probes: list[dict] = []
    deadline = __import__("time").monotonic() + timeout_s
    empty_polls = 0

    try:
        while __import__("time").monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                empty_polls += 1
                if empty_polls >= 5:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[ERROR] Kafka: {msg.error()}", file=sys.stderr)
                continue
            empty_polls = 0
            try:
                probes.append(json.loads(msg.value().decode("utf-8")))
            except json.JSONDecodeError:
                pass
    finally:
        consumer.close()

    return probes


def compute_and_print(probes: list[dict]) -> bool:
    if not probes:
        print("[latency_report] No probe records found.")
        return False

    e2e: list[float] = []
    kafka_q: list[float] = []
    flink_proc: list[float] = []

    for p in probes:
        emit_ns: Optional[int] = p.get("emit_timestamp_ns")
        recv_ns: Optional[int] = p.get("flink_received_ns")
        commit_ns: Optional[int] = p.get("iceberg_commit_ns")

        if emit_ns and commit_ns:
            e2e.append((commit_ns - emit_ns) / 1_000_000)
        if emit_ns and recv_ns:
            kafka_q.append((recv_ns - emit_ns) / 1_000_000)
        if recv_ns and commit_ns:
            flink_proc.append((commit_ns - recv_ns) / 1_000_000)

    print("=" * 60)
    print("  SENTINELSTREAM — END-TO-END LATENCY REPORT")
    print(f"  Probe records analysed: {len(probes):,}")
    print("=" * 60)

    for label, values in [
        ("End-to-end (emit → Iceberg commit)", e2e),
        ("Kafka queue (emit → Flink receive)", kafka_q),
        ("Flink processing (receive → commit)", flink_proc),
    ]:
        if not values:
            continue
        p50 = _percentile(values, 50)
        p95 = _percentile(values, 95)
        p99 = _percentile(values, 99)
        p999 = _percentile(values, 99.9)
        print(f"\n  {label}")
        print(f"    p50:   {p50:7.2f} ms")
        print(f"    p95:   {p95:7.2f} ms")
        print(f"    p99:   {p99:7.2f} ms")
        print(f"    p99.9: {p999:7.2f} ms")

    print()

    if not e2e:
        print("  [INCONCLUSIVE] No end-to-end samples.")
        return False

    p99_e2e = _percentile(e2e, 99)
    passed = p99_e2e <= LATENCY_TARGET_MS

    _histogram(e2e, "End-to-end latency")

    print()
    print("=" * 60)
    if passed:
        print(f"  RESULT: PASS  — p99 {p99_e2e:.2f}ms ≤ {LATENCY_TARGET_MS}ms target")
    else:
        print(f"  RESULT: FAIL  — p99 {p99_e2e:.2f}ms > {LATENCY_TARGET_MS}ms target")
        # Identify bottleneck stage
        p99_kafka = _percentile(kafka_q, 99) if kafka_q else 0
        p99_flink = _percentile(flink_proc, 99) if flink_proc else 0
        if p99_kafka > p99_flink:
            print(f"  BOTTLENECK: Kafka queue ({p99_kafka:.2f}ms p99)")
            print("  → Check producer batch.size / linger.ms, topic partition count")
        else:
            print(f"  BOTTLENECK: Flink processing ({p99_flink:.2f}ms p99)")
            print("  → Check window slide interval, RocksDB serialisation, Iceberg commit batch size")
    print("=" * 60)

    return passed


def main() -> None:
    parser = argparse.ArgumentParser(description="SentinelStream latency reporter")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="latency-probes")
    parser.add_argument("--timeout", type=int, default=30, help="Seconds to wait for messages")
    args = parser.parse_args()

    probes = collect_probes(args.bootstrap_server, args.topic, args.timeout)
    passed = compute_and_print(probes)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
