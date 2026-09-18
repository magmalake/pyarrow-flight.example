#!/usr/bin/env bash
#
# What the boundary costs: one column of a real Iceberg table, read by Daft in
# this process and over Flight from two servers.
#
#   pixi run transports
#   TAXI_TABLE=/path/to/warehouse/taxi/trips pixi run transports
#
# It brings up both servers, runs client/07_transports.py against them and
# tears them down, for the same reason scripts/check.sh does: a measurement
# that depends on something already running somewhere is not repeatable, and a
# server left behind holds the port for the next one.
#
# The pieces it needs live in sibling checkouts, the way the magmalake tins
# do. Each leg is skipped with a note rather than failing the run, so the
# in-process number is available without a Flight server and vice versa.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TAXI_TABLE="${TAXI_TABLE:-$ROOT/../taxibench.example/build/warehouse/warehouse/taxi/trips}"
IB_LIB="${IB_LIB:-$ROOT/../iceberg.mojo/build/libibscan.dylib}"
FLIGHT_MOJO="${FLIGHT_MOJO:-$ROOT/../flight.mojo}"
COLUMN="${TAXI_COLUMN:-trip_distance}"
PYARROW_PORT="${PYARROW_PORT:-18816}"
MOJO_PORT="${MOJO_PORT:-18815}"
# The Mojo tins dlopen their codec shims from $CONDA_PREFIX/lib; a server
# started outside its own environment looks in the wrong place and aborts on
# the first compressed page.
SHIM_PREFIX="${MAGMALAKE_SHIM_PREFIX:-$ROOT/../iceberg.mojo/.pixi/envs/default}"

[ "$(uname -s)" = "Darwin" ] || IB_LIB="${IB_LIB%.dylib}.so"

PIDS=()
cleanup() { for pid in ${PIDS+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

wait_for() {  # <port> <what>
    for _ in $(seq 1 60); do
        if python -c "import socket;socket.create_connection(('127.0.0.1',$1),0.5)" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "warning: $2 never came up on $1, skipping that leg" >&2
    return 1
}

if [ ! -d "$TAXI_TABLE" ]; then
    echo "error: no table at $TAXI_TABLE" >&2
    echo "  load it with 'pixi run load' in taxibench.example, or set TAXI_TABLE" >&2
    exit 1
fi

# Leg 1: this process. Needs iceberg.mojo's scan library.
if [ ! -f "$IB_LIB" ]; then
    echo "note: no scan library at $IB_LIB — skipping the in-process leg" >&2
    echo "      build it with 'pixi run carrow-scan-lib' in iceberg.mojo" >&2
    IB_LIB=""
fi

# Leg 2: pyarrow's Flight server over the same Parquet files — the reference
# implementation of the protocol, so that the protocol and one server's
# encoder are not reported as one number.
python server/parquet_server.py "$TAXI_TABLE" --port "$PYARROW_PORT" \
    --column "$COLUMN" > /tmp/parquet-flight.log 2>&1 &
PIDS+=($!)
PYARROW_URI=""
wait_for "$PYARROW_PORT" "the pyarrow server" && \
    PYARROW_URI="grpc://127.0.0.1:$PYARROW_PORT"

# Leg 3: flight.mojo's Iceberg server. It runs under its own pixi environment,
# which is where its codec shims are.
MOJO_URI=""
if [ -x "$FLIGHT_MOJO/build/serve_ice" ]; then
    ( cd "$FLIGHT_MOJO" && pixi run -- ./build/serve_ice "$TAXI_TABLE" \
        --columns "$COLUMN" --split-size 134217728 --port "$MOJO_PORT" ) \
        > /tmp/mojo-flight.log 2>&1 &
    PIDS+=($!)
    wait_for "$MOJO_PORT" "the flight.mojo server" && \
        MOJO_URI="grpc://127.0.0.1:$MOJO_PORT"
else
    echo "note: no $FLIGHT_MOJO/build/serve_ice — skipping the Mojo Flight leg" >&2
    echo "      build it with 'pixi run serve-iceberg' in flight.mojo" >&2
fi

TAXI_TABLE="$TAXI_TABLE" IB_LIB="$IB_LIB" TAXI_COLUMN="$COLUMN" \
MAGMALAKE_SHIM_PREFIX="$SHIM_PREFIX" \
PYARROW_FLIGHT_URI="$PYARROW_URI" MOJO_FLIGHT_URI="$MOJO_URI" \
    python client/07_transports.py
