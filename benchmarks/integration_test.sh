#!/usr/bin/env bash
# SentinelStream full integration test.
# Brings up the Docker stack, runs 25K tps for 300s, then checks all four
# success criteria simultaneously. Writes results to benchmark_report.md.
#
# EXIT CODES
#   0 — all four criteria PASS
#   1 — one or more criteria FAIL
#   2 — infrastructure error (stack didn't come up)
#
# Usage:
#   bash benchmarks/integration_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPORT="${SCRIPT_DIR}/benchmark_report.md"

SEED=42
TPS=25000
DURATION=300
BOOTSTRAP="localhost:9092"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }
info() { echo -e "${YELLOW}[INFO]${NC} $*"; }

OVERALL=0

# ---------------------------------------------------------------------------
# 1. Start Docker stack
# ---------------------------------------------------------------------------
info "Starting Docker stack..."
docker compose -f "${PROJECT_ROOT}/docker/docker-compose.yml" up -d

info "Waiting for all services to be healthy (max 120s)..."
deadline=$((SECONDS + 120))
while [ $SECONDS -lt $deadline ]; do
    unhealthy=$(docker compose -f "${PROJECT_ROOT}/docker/docker-compose.yml" ps --format json \
        2>/dev/null | python3 -c "
import json, sys
data = sys.stdin.read().strip()
try:
    services = json.loads(data)
    if isinstance(services, dict):
        services = [services]
except json.JSONDecodeError:
    services = [json.loads(l) for l in data.splitlines() if l.strip()]
unhealthy = [s for s in services if s.get('Health','') not in ('healthy','')]
print(len(unhealthy))
" 2>/dev/null || echo "999")
    if [ "$unhealthy" -eq 0 ]; then
        pass "All containers healthy."
        break
    fi
    sleep 5
done

if [ "$unhealthy" -ne 0 ] 2>/dev/null; then
    fail "Containers did not reach healthy state within 120s."
    docker compose -f "${PROJECT_ROOT}/docker/docker-compose.yml" ps
    exit 2
fi

# ---------------------------------------------------------------------------
# 2. Run producer for 300s at 25K tps
# ---------------------------------------------------------------------------
info "Running producer: seed=${SEED}, tps=${TPS}, duration=${DURATION}s..."
python3 "${PROJECT_ROOT}/producers/kafka_producer.py" \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic transactions \
    --tps "${TPS}" \
    --seed "${SEED}" \
    --duration "${DURATION}" &
PRODUCER_PID=$!

# Also run latency probes in parallel
python3 "${PROJECT_ROOT}/producers/latency_probe.py" \
    --bootstrap-server "${BOOTSTRAP}" \
    --duration "${DURATION}" &
PROBE_PID=$!

wait "${PRODUCER_PID}"
wait "${PROBE_PID}"
info "Producer run complete."

# ---------------------------------------------------------------------------
# 3. Check [1]: Throughput + Kafka lag
# ---------------------------------------------------------------------------
info "Checking Kafka consumer lag..."
LAG=$(docker exec sentinel-kafka kafka-consumer-groups \
    --bootstrap-server "localhost:9092" \
    --describe --group sentinel-velocity-check 2>/dev/null \
    | awk 'NR>1 { sum += $6 } END { print sum+0 }')

if [ "${LAG}" -eq 0 ]; then
    pass "Criterion [1] Kafka lag: ${LAG} (target: 0)"
else
    fail "Criterion [1] Kafka lag: ${LAG} (target: 0)"
    OVERALL=1
fi

# ---------------------------------------------------------------------------
# 4. Check [2]: p99 latency ≤ 80ms
# ---------------------------------------------------------------------------
info "Running latency report..."
if python3 "${SCRIPT_DIR}/latency_report.py" \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic latency-probes \
    --timeout 30; then
    pass "Criterion [2] p99 latency ≤ 80ms"
else
    fail "Criterion [2] p99 latency > 80ms — see report above for bottleneck"
    OVERALL=1
fi

# ---------------------------------------------------------------------------
# 5. Check [3]: Fraud accuracy 200/200
# ---------------------------------------------------------------------------
info "Injecting fraud-only test events (seed=${SEED})..."
python3 "${PROJECT_ROOT}/producers/kafka_producer.py" \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic transactions \
    --seed "${SEED}" \
    --fraud-only-mode

info "Running accuracy test (waiting 30s for Flink)..."
if python3 "${SCRIPT_DIR}/accuracy_test.py" --seed "${SEED}" --wait 30; then
    pass "Criterion [3] Fraud accuracy: 200/200"
else
    fail "Criterion [3] Fraud accuracy < 200/200"
    OVERALL=1
fi

# ---------------------------------------------------------------------------
# 6. Check [4]: DuckDB query times < 1.0s
# ---------------------------------------------------------------------------
info "Checking DuckDB mart query times..."
DUCKDB_FAIL=0

check_query() {
    local label="$1"
    local query="$2"
    local t_start
    t_start=$(date +%s%N)
    duckdb -c "${query}" > /dev/null 2>&1 || true
    local t_end
    t_end=$(date +%s%N)
    local elapsed_ms=$(( (t_end - t_start) / 1000000 ))
    local elapsed_s
    elapsed_s=$(echo "scale=3; ${elapsed_ms}/1000" | bc)
    if [ "${elapsed_ms}" -lt 1000 ]; then
        pass "  ${label}: ${elapsed_s}s"
    else
        fail "  ${label}: ${elapsed_s}s (target: < 1.0s)"
        DUCKDB_FAIL=1
    fi
}

check_query "daily_fraud_volume" \
    "INSTALL iceberg; LOAD iceberg; SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SELECT * FROM sentinel_analytics.daily_fraud_volume ORDER BY day DESC LIMIT 7;"

check_query "top_attacked_merchants" \
    "INSTALL iceberg; LOAD iceberg; SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SELECT * FROM sentinel_analytics.top_attacked_merchants LIMIT 10;"

check_query "velocity_breach_users" \
    "INSTALL iceberg; LOAD iceberg; SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SELECT * FROM sentinel_analytics.velocity_breach_users LIMIT 50;"

if [ "${DUCKDB_FAIL}" -eq 0 ]; then
    pass "Criterion [4] All DuckDB queries < 1.0s"
else
    fail "Criterion [4] One or more DuckDB queries exceeded 1.0s"
    OVERALL=1
fi

# ---------------------------------------------------------------------------
# 7. Summary
# ---------------------------------------------------------------------------
echo ""
echo "=================================================="
if [ "${OVERALL}" -eq 0 ]; then
    echo -e "${GREEN}  INTEGRATION TEST: PASS — all 4 criteria met${NC}"
    sed -i '' "s|<!-- OVERALL_STATUS -->|**PASS** — all 4 criteria met on $(date -u +%Y-%m-%d)|" "${REPORT}"
else
    echo -e "${RED}  INTEGRATION TEST: FAIL — see failures above${NC}"
    sed -i '' "s|<!-- OVERALL_STATUS -->|**FAIL** — see benchmark_report.md|" "${REPORT}"
fi
echo "=================================================="
echo ""

exit "${OVERALL}"
