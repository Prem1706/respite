#!/usr/bin/env bash
# Benchmarks Respite against real Redis with redis-benchmark and prints a
# Markdown table. Needs redis-server and redis-benchmark on PATH.
#
#   bench/bench.sh            # no persistence
#   AOF=1 bench/bench.sh      # both servers with appendonly + fsync everysec
set -euo pipefail

cd "$(dirname "$0")/.."
REQUESTS=${REQUESTS:-500000}
CLIENTS=${CLIENTS:-50}
TESTS=${TESTS:-set,get,incr}
REDIS_PORT=6391
RESPITE_PORT=6392
TMP=$(mktemp -d)

go build -o "$TMP/respite" ./cmd/respite

if [[ "${AOF:-0}" == 1 ]]; then
  redis_args=(--appendonly yes --appendfsync everysec)
  respite_args=(-aof "$TMP/respite.aof" -appendfsync everysec)
else
  redis_args=(--appendonly no)
  respite_args=(-aof "")
fi

redis-server --port $REDIS_PORT --save "" --dir "$TMP" "${redis_args[@]}" >/dev/null &
REDIS_PID=$!
"$TMP/respite" -addr ":$RESPITE_PORT" "${respite_args[@]}" 2>/dev/null &
RESPITE_PID=$!
trap 'kill $REDIS_PID $RESPITE_PID 2>/dev/null; wait 2>/dev/null; rm -rf "$TMP"' EXIT
sleep 1

# Prints "test,rps,p50,p99" lines for one server and pipeline depth.
run() {
  redis-benchmark -p "$1" -t "$TESTS" -n "$REQUESTS" -c "$CLIENTS" -P "$2" -r 100000 --csv -q |
    tail -n +2 | tr -d '"' | awk -F, '{ printf "%s,%d,%s,%s\n", $1, $2, $5, $7 }'
}

echo "Requests: $REQUESTS, clients: $CLIENTS, AOF: ${AOF:-0}, CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
echo
echo "| Command | Pipeline | Redis ops/s | Respite ops/s | Respite vs Redis | Redis p50 / p99 ms | Respite p50 / p99 ms |"
echo "|---|---|---|---|---|---|---|"
for pipeline in 1 16; do
  paste -d, <(run $REDIS_PORT "$pipeline") <(run $RESPITE_PORT "$pipeline") |
    awk -F, -v p="$pipeline" '{
      printf "| %s | %s | %\047d | %\047d | %.0f%% | %s / %s | %s / %s |\n", $1, p, $2, $6, 100*$6/$2, $3, $4, $7, $8
    }'
done
