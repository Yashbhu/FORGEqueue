#!/bin/bash
# Bring up ForgeQueue with telemetry and a Prometheus UI.
#
#   ./deploy/observability.sh
#
# Starts the gateway, the worker and Prometheus, then prints the URLs.
# Ctrl-C stops all three. Redis must be on localhost:6379; Postgres is
# optional and the journal stays disabled without it.
set -eu

cd "$(dirname "$0")/.."

# Prometheus defaults to :9090, which is already the gateway's metrics
# port, so the UI gets its own. Anything else here silently fails to bind.
PROM_PORT=${PROM_PORT:-9095}
PROM_CONFIG=${PROM_CONFIG:-deploy/prometheus.yml}
# Outside the repo on purpose, so Prometheus does not litter git status.
PROM_DATA=${PROM_DATA:-$HOME/.forgequeue/prometheus}
GRAFANA_PORT=${GRAFANA_PORT:-3000}
GRAFANA_HOME=${GRAFANA_HOME:-/opt/homebrew/opt/grafana/share/grafana}
# ForgeQueue's own config, not the stock brew one. The stock file leaves
# anonymous auth off, which makes every API call 401 and hides the
# provisioned dashboards entirely.
GRAFANA_INI=${GRAFANA_INI:-deploy/grafana/grafana.ini}

if ! redis-cli ping > /dev/null 2>&1; then
  echo "redis is not responding on localhost:6379" >&2
  exit 1
fi

if ! command -v prometheus > /dev/null 2>&1; then
  echo "prometheus is not installed: brew install prometheus" >&2
  exit 1
fi

redis-cli -n 0 FLUSHDB > /dev/null

# Build first, then run the binaries directly. Under `go run` the process
# being started is the go tool, and the compiled binary is its child: killing
# the recorded PID tears down the go tool and leaves the real server
# listening. That silently stacks up stray workers, which then compete for
# the same queue and make the telemetry look wrong.
BIN_DIR=$(mktemp -d)
go build -o "$BIN_DIR/gateway" ./cmd/gateway
go build -o "$BIN_DIR/worker" ./cmd/worker

# Grafana is optional. Its install is large, so the stack still comes up
# without it and Prometheus alone still gives you the graphs.
HAVE_GRAFANA=no
if command -v grafana > /dev/null 2>&1 && [ -f "$GRAFANA_INI" ]; then
  HAVE_GRAFANA=yes
fi

cleanup() {
  # Reverse order, and never let one failure hide the others.
  for p in ${GRAFANA_PID:-} ${PROM_PID:-} ${WORKER_PID:-} ${GATEWAY_PID:-}; do
    [ -n "$p" ] || continue
    kill "$p" 2>/dev/null || true
  done

  # Then insist. A single TERM is not always enough to tear the stack down
  # when the script is not in the foreground, and a leftover worker is worse
  # than a crash: it keeps competing for the same queue and quietly makes the
  # telemetry disagree with reality.
  sleep 1
  for p in ${GRAFANA_PID:-} ${PROM_PID:-} ${WORKER_PID:-} ${GATEWAY_PID:-}; do
    [ -n "$p" ] || continue
    kill -0 "$p" 2>/dev/null && kill -9 "$p" 2>/dev/null || true
  done

  rm -rf "$BIN_DIR"
}
trap cleanup EXIT INT TERM

FORGEQUEUE_METRICS_ADDR=:9090 "$BIN_DIR/gateway" > /tmp/fq-gateway.log 2>&1 &
GATEWAY_PID=$!

FORGEQUEUE_METRICS_ADDR=:9091 "$BIN_DIR/worker" > /tmp/fq-worker.log 2>&1 &
WORKER_PID=$!

# Give the Go processes time to bind before Prometheus starts scraping, so
# the first scrape is not a misleading "target down".
sleep 6

# An explicit --storage.tsdb.path is not optional here. Left unset, Prometheus
# defaults to ./data relative to the working directory, which means it writes
# a TSDB directory into the repo (showing up in git status) and loses all
# history if you run from somewhere else. This path survives restarts, so the
# graphs still have the previous run's data after a Ctrl-C and restart.
mkdir -p "$PROM_DATA"

prometheus \
  --config.file="$PROM_CONFIG" \
  --storage.tsdb.path="$PROM_DATA" \
  --storage.tsdb.retention.time=2h \
  --web.listen-address=":${PROM_PORT}" \
  > /tmp/fq-prometheus.log 2>&1 &
PROM_PID=$!

# Do not claim the UI is up until it answers. Prometheus exits on a bind
# failure, and without this check the script happily prints a dead URL.
for _ in $(seq 1 20); do
  if curl -sf "localhost:${PROM_PORT}/-/ready" > /dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$PROM_PID" 2>/dev/null; then
    echo "prometheus exited during startup:" >&2
    tail -5 /tmp/fq-prometheus.log >&2
    exit 1
  fi
  sleep 1
done

if ! curl -sf "localhost:${PROM_PORT}/-/ready" > /dev/null 2>&1; then
  echo "prometheus never became ready on :${PROM_PORT}" >&2
  exit 1
fi

# Grafana next, pointed at this checkout's provisioning directory. The
# dashboards provider path is interpolated from the environment so nothing
# absolute is committed.
if [ "$HAVE_GRAFANA" = yes ]; then
  export FORGEQUEUE_DASHBOARDS_DIR="$PWD/deploy/grafana/dashboards"
  GRAFANA_DATA="$BIN_DIR/grafana-data"
  mkdir -p "$GRAFANA_DATA/plugins" "$BIN_DIR/grafana-logs"

  grafana server \
    --config="$GRAFANA_INI" \
    --homepath="$GRAFANA_HOME" \
    --packaging=brew \
    "cfg:default.paths.data=$GRAFANA_DATA" \
    "cfg:default.paths.logs=$BIN_DIR/grafana-logs" \
    "cfg:default.paths.plugins=$GRAFANA_DATA/plugins" \
    "cfg:default.paths.provisioning=$PWD/deploy/grafana/provisioning" \
    > /tmp/fq-grafana.log 2>&1 &
  GRAFANA_PID=$!

  # First boot migrates its SQLite schema, which takes a few seconds. Wait
  # for the API rather than sleeping a guessed interval.
  for _ in $(seq 1 45); do
    if curl -sf "localhost:${GRAFANA_PORT}/api/health" > /dev/null 2>&1; then
      break
    fi
    if ! kill -0 "$GRAFANA_PID" 2>/dev/null; then
      echo "grafana exited during startup:" >&2
      tail -5 /tmp/fq-grafana.log >&2
      break
    fi
    sleep 1
  done
fi

GRAFANA_LINE=""
if [ "$HAVE_GRAFANA" = yes ]; then
  GRAFANA_LINE="  Grafana UI       http://localhost:${GRAFANA_PORT}  (no login)"
fi

cat <<EOF

  ForgeQueue is up.

  Gateway metrics   http://localhost:9090/metrics
  Worker metrics    http://localhost:9091/metrics
  Prometheus UI     http://localhost:${PROM_PORT}   <- graphs, not raw text
${GRAFANA_LINE}

  Generate some load, then watch it land in the UI:

    go run ./cmd/demoenqueue

  Logs: /tmp/fq-gateway.log  /tmp/fq-worker.log  /tmp/fq-prometheus.log

  Ctrl-C to stop.


EOF

# Block here on purpose. Without this the script reaches EOF, the EXIT trap
# fires, and it kills the processes it just started.
#
# Each PID is waited on separately so the trap runs as soon as Ctrl-C
# arrives, rather than after the last child exits.
#
# Note for testing: bash sets SIGINT to ignore in background jobs, so
# `kill -INT` on a backgrounded script is a no-op and will look like a
# cleanup bug. Send a real Ctrl-C to the foreground process group instead.
for _pid in ${GRAFANA_PID:-} ${PROM_PID:-} ${WORKER_PID:-} ${GATEWAY_PID:-}; do
  [ -n "$_pid" ] || continue
  wait "$_pid" 2>/dev/null || true
done
