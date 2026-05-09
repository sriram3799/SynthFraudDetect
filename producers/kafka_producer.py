"""
Kafka producer for SentinelStream synthetic transaction events.

Runs one OS process per CPU core; each process owns an independent
TransactionGenerator seeded at (master_seed + worker_id), which together
reproduce the full deterministic sequence for a given master seed.

Usage
-----
  python producers/kafka_producer.py \\
      --bootstrap-server localhost:9092 \\
      --topic transactions \\
      --tps 25000 \\
      --seed 42 \\
      --duration 60

Special injection flags (for testing):
  --inject-velocity-burst <user_id>   Force a velocity burst for that user
  --inject-location-jump  <user_id>   Force a location-jump pair for that user
  --fraud-only-mode                   Emit 100 VELOCITY + 100 LOCATION events then exit
"""

import argparse
import json
import multiprocessing
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

import jsonschema

from config import ProducerConfig
from generator import TransactionGenerator

try:
    from confluent_kafka import Producer, KafkaException
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False
    print("[WARN] confluent_kafka not installed — running in dry-run mode (no Kafka output).",
          file=sys.stderr)


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

_SCHEMA_PATH = Path(__file__).parent / "schema" / "transaction_event.json"
_SCHEMA: dict = json.loads(_SCHEMA_PATH.read_text())
_VALIDATOR = jsonschema.Draft7Validator(_SCHEMA)


def _validate(event: dict) -> bool:
    errors = list(_VALIDATOR.iter_errors(event))
    if errors:
        print(f"[SCHEMA ERROR] {errors[0].message}", file=sys.stderr)
        return False
    return True


# ---------------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------------

_REPORT_INTERVAL = 5.0   # seconds between throughput log lines


def _worker(
    worker_id: int,
    config: ProducerConfig,
    tps_per_worker: int,
    stop_event: multiprocessing.Event,  # type: ignore[type-arg]
    inject_velocity_user: Optional[str] = None,
    inject_location_user: Optional[str] = None,
    fraud_only_mode: bool = False,
) -> None:
    # Re-register SIGTERM/SIGINT so the worker exits cleanly on container stop
    def _shutdown(signum: int, frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    producer: Optional[object] = None
    if _KAFKA_AVAILABLE:
        producer = Producer({
            "bootstrap.servers": config.bootstrap_servers,
            "linger.ms": config.linger_ms,
            "batch.size": config.batch_size,
            "compression.type": "lz4",
            "acks": "all",                       # required for exactly-once guarantee
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 100,
        })

    gen = TransactionGenerator(
        seed=config.seed,
        worker_id=worker_id,
        fraud_injection_rate=config.fraud_injection_rate,
    )

    start_time = time.monotonic()
    deadline = start_time + config.duration_seconds if config.duration_seconds > 0 else float("inf")
    last_report = start_time
    sent = 0
    errors = 0

    def _delivery_report(err: object, msg: object) -> None:
        nonlocal errors
        if err is not None:
            errors += 1
            print(f"[WORKER {worker_id}] Delivery error: {err}", file=sys.stderr)

    # ---- Special injection modes ----------------------------------------
    if inject_velocity_user:
        events = gen._velocity_burst(int(time.time() * 1000), seq=0)
        for ev in events:
            ev["user_id"] = inject_velocity_user
            _ship(producer, config.topic_name, ev, _delivery_report)
        if producer and _KAFKA_AVAILABLE:
            producer.flush()  # type: ignore[attr-defined]
        print(f"[WORKER {worker_id}] Injected velocity burst for {inject_velocity_user}")
        return

    if inject_location_user:
        events = gen._location_jump_pair(int(time.time() * 1000), seq=0)
        for ev in events:
            ev["user_id"] = inject_location_user
            _ship(producer, config.topic_name, ev, _delivery_report)
        if producer and _KAFKA_AVAILABLE:
            producer.flush()  # type: ignore[attr-defined]
        print(f"[WORKER {worker_id}] Injected location jump for {inject_location_user}")
        return

    if fraud_only_mode:
        _run_fraud_only(gen, producer, config, _delivery_report, worker_id)
        return

    # ---- Normal streaming loop ------------------------------------------
    interval = 1.0 / tps_per_worker
    next_send = time.monotonic()

    for event in gen.stream(target_tps=tps_per_worker):
        if stop_event.is_set() or time.monotonic() >= deadline:
            break

        if not _validate(event):
            continue

        _ship(producer, config.topic_name, event, _delivery_report)
        sent += 1

        # Periodic flush + rate limiting
        if producer and _KAFKA_AVAILABLE and sent % 500 == 0:
            producer.poll(0)  # type: ignore[attr-defined]

        # Rate limiting: sleep until next_send
        next_send += interval
        lag = next_send - time.monotonic()
        if lag > 0:
            time.sleep(lag)

        # Progress report
        now = time.monotonic()
        if now - last_report >= _REPORT_INTERVAL:
            elapsed = now - start_time
            actual_tps = int(sent / elapsed)
            fraud_count = gen.stats.fraud_emitted
            print(
                f"[WORKER {worker_id}] "
                f"Throughput: {actual_tps} tps | "
                f"Sent: {sent:,} | "
                f"Errors: {errors} | "
                f"Fraud injected: {fraud_count}",
                flush=True,
            )
            last_report = now

    # Flush remaining messages before exit
    if producer and _KAFKA_AVAILABLE:
        producer.flush(timeout=30)  # type: ignore[attr-defined]
    print(f"[WORKER {worker_id}] Exiting. Total sent: {sent:,}, errors: {errors}")


def _run_fraud_only(
    gen: TransactionGenerator,
    producer: object,
    config: "ProducerConfig",
    delivery_cb: object,
    worker_id: int,
) -> None:
    """Emit exactly 100 VELOCITY_BREACH + 100 LOCATION_JUMP events then stop."""
    now_ms = int(time.time() * 1_000)
    for i in range(100):
        burst = gen._velocity_burst(now_ms + i * 1_000, seq=i * 10)
        for ev in burst:
            _ship(producer, config.topic_name, ev, delivery_cb)
    for i in range(100):
        pair = gen._location_jump_pair(now_ms + i * 2_000, seq=1000 + i * 10)
        for ev in pair:
            _ship(producer, config.topic_name, ev, delivery_cb)
    if producer and _KAFKA_AVAILABLE:
        producer.flush(timeout=30)  # type: ignore[attr-defined]
    print(f"[WORKER {worker_id}] fraud-only-mode: emitted 100 VELOCITY + 100 LOCATION events.")


def _ship(
    producer: object,
    topic: str,
    event: dict,
    delivery_cb: object,
) -> None:
    if not _KAFKA_AVAILABLE or producer is None:
        return
    payload = json.dumps(event).encode("utf-8")
    while True:
        try:
            producer.produce(  # type: ignore[attr-defined]
                topic=topic,
                key=event["user_id"].encode("utf-8"),
                value=payload,
                on_delivery=delivery_cb,
            )
            break
        except BufferError:
            producer.poll(0.1)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SentinelStream synthetic transaction producer")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--tps", type=int, default=25_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duration", type=int, default=0, help="Seconds to run (0=forever)")
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    parser.add_argument("--workers", type=int, default=0, help="0=one per CPU core")
    parser.add_argument("--inject-velocity-burst", metavar="USER_ID", default=None)
    parser.add_argument("--inject-location-jump", metavar="USER_ID", default=None)
    parser.add_argument("--fraud-only-mode", action="store_true")
    args = parser.parse_args()

    config = ProducerConfig(
        bootstrap_servers=args.bootstrap_server,
        topic_name=args.topic,
        target_tps=args.tps,
        seed=args.seed,
        duration_seconds=args.duration,
        fraud_injection_rate=args.fraud_rate,
        num_workers=args.workers or os.cpu_count() or 1,
    )

    n_workers = config.num_workers
    tps_per_worker = max(1, config.target_tps // n_workers)

    stop_event = multiprocessing.Event()

    # Propagate SIGINT/SIGTERM from the main process to all workers
    def _handle_signal(signum: int, frame: object) -> None:
        print("\n[MAIN] Shutdown signal received — stopping workers...", flush=True)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Special single-worker injection modes bypass multiprocessing
    if args.inject_velocity_burst or args.inject_location_jump or args.fraud_only_mode:
        _worker(
            worker_id=0,
            config=config,
            tps_per_worker=tps_per_worker,
            stop_event=stop_event,
            inject_velocity_user=args.inject_velocity_burst,
            inject_location_user=args.inject_location_jump,
            fraud_only_mode=args.fraud_only_mode,
        )
        return

    processes: list[multiprocessing.Process] = []
    for wid in range(n_workers):
        p = multiprocessing.Process(
            target=_worker,
            args=(wid, config, tps_per_worker, stop_event),
            daemon=True,
            name=f"sentinel-producer-{wid}",
        )
        p.start()
        processes.append(p)

    print(
        f"[MAIN] Started {n_workers} worker(s) targeting "
        f"{config.target_tps:,} tps on topic '{config.topic_name}'",
        flush=True,
    )

    for p in processes:
        p.join()

    print("[MAIN] All workers exited.")


if __name__ == "__main__":
    main()
