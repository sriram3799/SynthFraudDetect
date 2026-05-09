"""
PaySim1 dataset profiler.

Downloads the PaySim1 dataset via kagglehub, computes key statistical
properties needed to calibrate the SentinelStream synthetic generator,
and writes a human-readable analysis report to paysim_analysis.md.

Run once before building the synthetic generator (Unit 1.3).
The dataset version hash pinned in the output must match what the
generator uses to ensure deterministic fraud-pattern calibration.
"""

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import kagglehub
import pandas as pd
import numpy as np


REPORT_PATH = Path(__file__).parent / "paysim_analysis.md"
FRAUD_PATTERNS = ["VELOCITY_BREACH", "LOCATION_JUMP"]


def _file_hash(path: str) -> str:
    """SHA-256 of the first dataset file found, for version pinning."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_paysim(dataset_path: str) -> pd.DataFrame:
    csv_files = list(Path(dataset_path).glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {dataset_path}")
    csv_path = csv_files[0]
    print(f"Loading dataset: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df, csv_path


def profile_fraud_rate(df: pd.DataFrame) -> dict[str, Any]:
    total = len(df)
    fraud_col = "isFraud" if "isFraud" in df.columns else df.columns[-2]
    n_fraud = int(df[fraud_col].sum())
    fraud_rate = round(n_fraud / total * 100, 4)
    return {
        "total_transactions": total,
        "fraud_transactions": n_fraud,
        "fraud_rate_pct": fraud_rate,
        "fraud_column": fraud_col,
    }


def profile_transaction_types(df: pd.DataFrame, fraud_col: str) -> dict[str, Any]:
    type_col = "type" if "type" in df.columns else None
    if type_col is None:
        return {}
    counts = df[type_col].value_counts().to_dict()
    fraud_by_type = df[df[fraud_col] == 1][type_col].value_counts().to_dict()
    return {
        "transaction_types": {k: int(v) for k, v in counts.items()},
        "fraud_by_type": {k: int(v) for k, v in fraud_by_type.items()},
    }


def profile_amount_distributions(df: pd.DataFrame, fraud_col: str) -> dict[str, Any]:
    amount_col = "amount" if "amount" in df.columns else None
    if amount_col is None:
        return {}
    legit = df[df[fraud_col] == 0][amount_col]
    fraud = df[df[fraud_col] == 1][amount_col]

    def stats(series: pd.Series) -> dict[str, float]:
        return {
            "mean": round(float(series.mean()), 2),
            "median": round(float(series.median()), 2),
            "std": round(float(series.std()), 2),
            "p25": round(float(series.quantile(0.25)), 2),
            "p75": round(float(series.quantile(0.75)), 2),
            "p95": round(float(series.quantile(0.95)), 2),
            "p99": round(float(series.quantile(0.99)), 2),
            "max": round(float(series.max()), 2),
        }

    return {
        "amount_legit": stats(legit),
        "amount_fraud": stats(fraud),
    }


def profile_velocity_patterns(df: pd.DataFrame, fraud_col: str) -> dict[str, Any]:
    """
    Approximates velocity patterns using the PaySim 'step' column (1 step = 1 hour).
    Counts transactions per (nameOrig, step) to estimate max txns-per-hour per user.
    """
    origin_col = "nameOrig" if "nameOrig" in df.columns else None
    step_col = "step" if "step" in df.columns else None
    if origin_col is None or step_col is None:
        return {"note": "Velocity profiling skipped — required columns not found."}

    per_user_per_hour = (
        df.groupby([origin_col, step_col]).size().reset_index(name="count")
    )
    max_per_hour = int(per_user_per_hour["count"].max())
    p99_per_hour = float(per_user_per_hour["count"].quantile(0.99))

    fraud_users = df[df[fraud_col] == 1][origin_col].unique()
    fraud_velocity = per_user_per_hour[
        per_user_per_hour[origin_col].isin(fraud_users)
    ]["count"]

    return {
        "max_txns_per_user_per_hour": max_per_hour,
        "p99_txns_per_user_per_hour": round(p99_per_hour, 2),
        "fraud_user_max_txns_per_hour": (
            int(fraud_velocity.max()) if len(fraud_velocity) > 0 else 0
        ),
        "velocity_breach_threshold_chosen": 5,
        "velocity_breach_window_sec": 60,
        "velocity_breach_rationale": (
            "Threshold of >5 txns/60s maps to >300 txns/hour — well above the "
            f"p99 legitimate rate of {round(p99_per_hour, 1)} txns/hour observed in PaySim1."
        ),
    }


def write_report(
    report_path: Path,
    fraud_stats: dict[str, Any],
    type_stats: dict[str, Any],
    amount_stats: dict[str, Any],
    velocity_stats: dict[str, Any],
    dataset_version_hash: str,
    csv_filename: str,
) -> None:
    lines = [
        "# PaySim1 Dataset Analysis",
        "",
        "> Auto-generated by `profile_paysim.py`. Do not edit manually.",
        "> Re-run if the dataset or fraud thresholds change; commit the diff explicitly.",
        "",
        "## Dataset Version",
        "",
        f"- **Source file:** `{csv_filename}`",
        f"- **SHA-256 (version pin):** `{dataset_version_hash}`",
        "",
        "## Fraud Rate",
        "",
        f"- Total transactions: **{fraud_stats['total_transactions']:,}**",
        f"- Fraud transactions: **{fraud_stats['fraud_transactions']:,}**",
        f"- **PaySim1 fraud rate: {fraud_stats['fraud_rate_pct']:.2f}%**",
        "",
        "## Transaction Types",
        "",
    ]

    if type_stats.get("transaction_types"):
        lines += ["| Type | Count | Fraud Count |", "| :--- | ---: | ---: |"]
        for t, count in sorted(
            type_stats["transaction_types"].items(), key=lambda x: -x[1]
        ):
            fraud_count = type_stats["fraud_by_type"].get(t, 0)
            lines.append(f"| {t} | {count:,} | {fraud_count:,} |")
    lines.append("")

    lines += [
        "## Amount Distributions (USD equivalent)",
        "",
        "| Statistic | Legitimate | Fraud |",
        "| :--- | ---: | ---: |",
    ]
    if amount_stats:
        legit = amount_stats["amount_legit"]
        fraud = amount_stats["amount_fraud"]
        for stat in ["mean", "median", "std", "p25", "p75", "p95", "p99", "max"]:
            lines.append(f"| {stat} | {legit[stat]:,.2f} | {fraud[stat]:,.2f} |")
    lines.append("")

    lines += [
        "## Velocity Pattern Analysis",
        "",
        f"- Max transactions / user / hour (all): **{velocity_stats.get('max_txns_per_user_per_hour', 'N/A')}**",
        f"- p99 transactions / user / hour: **{velocity_stats.get('p99_txns_per_user_per_hour', 'N/A')}**",
        f"- Fraud user max transactions / hour: **{velocity_stats.get('fraud_user_max_txns_per_hour', 'N/A')}**",
        "",
        "### VELOCITY_BREACH Threshold Decision",
        "",
        f"- **Threshold:** > {velocity_stats.get('velocity_breach_threshold_chosen', 5)} transactions in a "
        f"{velocity_stats.get('velocity_breach_window_sec', 60)}-second sliding window per `user_id`",
        f"- **Rationale:** {velocity_stats.get('velocity_breach_rationale', '')}",
        "",
        "## Fraud Patterns Catalogued",
        "",
        "**Fraud patterns catalogued: VELOCITY_BREACH, LOCATION_JUMP**",
        "",
        "| Pattern ID | Trigger Condition | `risk_score` Basis |",
        "| :--- | :--- | :--- |",
        "| `VELOCITY_BREACH` | > 5 txns in 60s sliding window (keyed by `user_id`) | Proportional to excess txn count above threshold |",
        "| `LOCATION_JUMP` | > 500 km displacement in < 10 min between consecutive txns (same `user_id`) | Proportional to computed travel speed (km/h); physically impossible speed → score ≈ 1.0 |",
        "",
        "## Generator Calibration Notes",
        "",
        "- The synthetic generator does **not** replay raw PaySim1 rows.",
        "- It uses its own seeded RNG, calibrated to match the distributions above.",
        "- `VELOCITY_BREACH` is injected as a burst of 6 transactions in < 60s for a designated `user_id`.",
        "- `LOCATION_JUMP` is injected by teleporting a designated `user_id` > 500 km between two consecutive events separated by < 10 minutes.",
        "- Fraud injection rate is configurable (default: 2% of total volume).",
        "",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to: {report_path}")


def main() -> None:
    print("Downloading PaySim1 dataset via kagglehub...")
    dataset_path = kagglehub.dataset_download("ealaxi/paysim1")
    print(f"Dataset path: {dataset_path}")

    df, csv_path = load_paysim(dataset_path)
    version_hash = _file_hash(str(csv_path))
    print(f"Dataset SHA-256: {version_hash}")

    fraud_stats = profile_fraud_rate(df)
    print(f"PaySim1 fraud rate: {fraud_stats['fraud_rate_pct']:.2f}%")

    type_stats = profile_transaction_types(df, fraud_stats["fraud_column"])
    amount_stats = profile_amount_distributions(df, fraud_stats["fraud_column"])
    velocity_stats = profile_velocity_patterns(df, fraud_stats["fraud_column"])

    write_report(
        REPORT_PATH,
        fraud_stats,
        type_stats,
        amount_stats,
        velocity_stats,
        version_hash,
        csv_path.name,
    )

    print(f"Fraud patterns catalogued: {', '.join(FRAUD_PATTERNS)}")


if __name__ == "__main__":
    main()
