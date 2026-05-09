#!/usr/bin/env bash
# Creates all SentinelStream Kafka topics on first cluster boot.
# Run by the kafka-init service in docker-compose.kafka.yml.
# Idempotent: re-running against an existing cluster is safe.

set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"

wait_for_broker() {
  echo "[kafka-init] Waiting for broker at ${BROKER}..."
  until kafka-broker-api-versions --bootstrap-server "${BROKER}" >/dev/null 2>&1; do
    sleep 2
  done
  echo "[kafka-init] Broker is ready."
}

create_topic() {
  local name="$1"
  local partitions="$2"
  local retention_ms="$3"
  local retention_bytes="${4:-536870912}"   # default 512MB per partition

  if kafka-topics --bootstrap-server "${BROKER}" --list | grep -qx "${name}"; then
    echo "[kafka-init] Topic '${name}' already exists — skipping."
  else
    kafka-topics \
      --bootstrap-server "${BROKER}" \
      --create \
      --topic "${name}" \
      --partitions "${partitions}" \
      --replication-factor "${REPLICATION}" \
      --config retention.ms="${retention_ms}" \
      --config retention.bytes="${retention_bytes}" \
      --config segment.ms=3600000 \
      --config compression.type=lz4 \
      --config min.insync.replicas=1
    echo "[kafka-init] Created topic '${name}' (${partitions} partitions, max ${retention_bytes}B/partition)."
  fi
}

wait_for_broker

# transactions: 12 partitions — 2K tps/partition headroom at 25K tps target
# 512MB/partition × 12 = 6GB max — well within Docker VM budget
create_topic "transactions"     12 86400000 536870912

# fraud-candidates: 6 partitions — lower volume (< 2% of transactions)
create_topic "fraud-candidates"  6 86400000 134217728  # 128MB/partition

# latency-probes: 1 partition — low-volume probe stream, order matters
create_topic "latency-probes"    1 3600000  10485760   # 10MB total

echo "[kafka-init] All topics ready."
kafka-topics --bootstrap-server "${BROKER}" --list
