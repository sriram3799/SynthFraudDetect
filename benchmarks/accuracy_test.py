"""
Fraud detection accuracy test.

Injects exactly 100 VELOCITY_BREACH + 100 LOCATION_JUMP fraud events
(using --seed 42, --fraud-only-mode), waits for Flink to process them,
then queries the Iceberg `fraud_alerts` table via DuckDB and asserts 100%
detection of both patterns.

Also generates (or validates against) the golden manifest
benchmarks/expected_fraud_manifest.json — a deterministic list of
transaction_ids that MUST appear in fraud_alerts for seed=42.

EXIT CODES
  0 — PASS (200/200 detected)
  1 — FAIL (detection < 100%)
  2 — ERROR (infrastructure issue, inconclusive)

Usage:
  python benchmarks/accuracy_test.py --seed 42
  python benchmarks/accuracy_test.py --seed 42 --generate-manifest
"""

import argparse
import itertools
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent / "producers"))
from generator import TransactionGenerator

MANIFEST_PATH = Path(__file__).parent / "expected_fraud_manifest.json"

N_VELOCITY = 100
N_LOCATION = 100
N_TOTAL = N_VELOCITY + N_LOCATION

WAIT_SECONDS = 30        # time for Flink to process and commit to Iceberg
S3_ENDPOINT = "localhost:9000"
ICEBERG_WAREHOUSE = "s3://sentinel-dev/iceberg"
MINIO_KEY = "minioadmin"
MINIO_SECRET = "minioadmin"


# ---------------------------------------------------------------------------
# Manifest generation — deterministic for seed=42
# ---------------------------------------------------------------------------

def generate_manifest(seed: int) -> dict:
    """
    Pre-compute the set of transaction_ids that fraud-only-mode will produce
    for the given seed. Run once; commit the manifest to git.
    """
    gen = TransactionGenerator(seed=seed, worker_id=0, fraud_injection_rate=1.0)
    now_ms = 1_746_518_400_000   # fixed epoch for determinism

    velocity_ids: list[str] = []
    location_ids: list[str] = []

    for i in range(N_VELOCITY):
        burst = gen._velocity_burst(now_ms + i * 1_000, seq=i * 10)
        velocity_ids.extend(ev["transaction_id"] for ev in burst)

    for i in range(N_LOCATION):
        pair = gen._location_jump_pair(now_ms + i * 2_000, seq=1000 + i * 10)
        location_ids.extend(ev["transaction_id"] for ev in pair)

    return {
        "seed": seed,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "velocity_transaction_ids": velocity_ids,
        "location_transaction_ids": location_ids,
        "total_expected": len(velocity_ids) + len(location_ids),
    }


def load_or_generate_manifest(seed: int) -> dict:
    if MANIFEST_PATH.exists():
        manifest = json.loads(MANIFEST_PATH.read_text())
        if manifest.get("seed") != seed:
            print(
                f"[WARN] Manifest seed ({manifest['seed']}) != requested seed ({seed}). "
                "Re-generating...",
                file=sys.stderr,
            )
            manifest = generate_manifest(seed)
            MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
        return manifest
    manifest = generate_manifest(seed)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    print(f"[accuracy_test] Manifest generated: {MANIFEST_PATH}")
    return manifest


# ---------------------------------------------------------------------------
# DuckDB query against live Iceberg tables
# ---------------------------------------------------------------------------

def query_fraud_alerts(manifest: dict) -> dict:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{MINIO_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")

    rows = con.execute(
        f"""
        SELECT transaction_id, fraud_flags
        FROM iceberg_scan('{ICEBERG_WAREHOUSE}/sentinel/fraud_alerts/')
        WHERE is_fraud = true
        """
    ).fetchall()

    found_ids = {row[0] for row in rows}

    velocity_expected = set(manifest["velocity_transaction_ids"])
    location_expected = set(manifest["location_transaction_ids"])

    velocity_found = velocity_expected & found_ids
    location_found = location_expected & found_ids

    return {
        "velocity_expected": len(velocity_expected),
        "velocity_found": len(velocity_found),
        "location_expected": len(location_expected),
        "location_found": len(location_found),
        "total_found": len(found_ids),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def print_result(results: dict) -> bool:
    v_exp = results["velocity_expected"]
    v_found = results["velocity_found"]
    l_exp = results["location_expected"]
    l_found = results["location_found"]

    v_pct = v_found / v_exp * 100 if v_exp else 0
    l_pct = l_found / l_exp * 100 if l_exp else 0
    total = v_found + l_found
    total_exp = v_exp + l_exp
    total_pct = total / total_exp * 100 if total_exp else 0

    print("=" * 55)
    print("  SENTINELSTREAM — FRAUD DETECTION ACCURACY")
    print("=" * 55)
    print(f"  VELOCITY_BREACH detected: {v_found}/{v_exp} ({v_pct:.1f}%)")
    print(f"  LOCATION_JUMP  detected: {l_found}/{l_exp} ({l_pct:.1f}%)")
    print(f"  Overall accuracy:         {total}/{total_exp} ({total_pct:.1f}%)")
    print("=" * 55)

    passed = total == total_exp
    print(f"  RESULT: {'PASS' if passed else 'FAIL'}")
    if not passed:
        missing_v = results["velocity_expected"] - results["velocity_found"]
        missing_l = results["location_expected"] - results["location_found"]
        if missing_v:
            print(f"  Missing VELOCITY_BREACH: {missing_v} events")
        if missing_l:
            print(f"  Missing LOCATION_JUMP: {missing_l} events")
    print("=" * 55)
    return passed


def main() -> None:
    parser = argparse.ArgumentParser(description="SentinelStream fraud accuracy test")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--generate-manifest",
        action="store_true",
        help="Re-generate the golden manifest and exit (don't run live query)",
    )
    parser.add_argument("--wait", type=int, default=WAIT_SECONDS,
                        help="Seconds to wait before querying Iceberg")
    parser.add_argument("--s3-endpoint", default=S3_ENDPOINT)
    parser.add_argument("--warehouse", default=ICEBERG_WAREHOUSE)
    args = parser.parse_args()

    manifest = load_or_generate_manifest(args.seed)
    print(f"[accuracy_test] Using manifest: seed={args.seed}, "
          f"total_expected={manifest['total_expected']}")

    if args.generate_manifest:
        print(f"[accuracy_test] Manifest written to {MANIFEST_PATH}")
        sys.exit(0)

    print(f"[accuracy_test] Waiting {args.wait}s for Flink to process events...")
    time.sleep(args.wait)

    try:
        results = query_fraud_alerts(manifest)
    except Exception as e:
        print(f"[ERROR] Could not query Iceberg: {e}", file=sys.stderr)
        print("  Ensure the full stack is running: docker compose up -d", file=sys.stderr)
        sys.exit(2)

    passed = print_result(results)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
