#!/usr/bin/env bash
#
# Every client example against flight.mojo's Iceberg server, built here from a
# sibling checkout, and every row it serves compared with the table's Parquet
# files read by pyarrow.
#
#   pixi run check-mojo
#   FLIGHT_MOJO=/path/to/flight.mojo pixi run check-mojo
#
# `check` proves the examples are right, against a server nobody here wrote;
# this proves the Mojo server answers them the same way. It needs flight.mojo
# at ../flight.mojo (or $FLIGHT_MOJO) with its own siblings next to it
# (flare_mlake, threads.mojo, iceberg.mojo and the read stack: the layout the
# magmalake tins use, and the one the org's nightly canary checks out), and it
# builds the server with that checkout's pixi environment, so the Mojo
# compiler is flight.mojo's and none is added to this repository.
#
# The table is flight.mojo's own gate fixture (tools/make_fixture.py, written
# by PyIceberg): the reference server's seven rows, plus 600 in a file with
# many small row groups so the plan splits into more endpoints than files.
# A few hundred kilobytes, generated each run; nothing is downloaded.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FLIGHT_MOJO="$(cd "${FLIGHT_MOJO:-$ROOT/../flight.mojo}" 2>/dev/null && pwd)" || {
    echo "error: no flight.mojo checkout at ${FLIGHT_MOJO:-$ROOT/../flight.mojo}" >&2
    echo "  clone https://github.com/magmalake/flight.mojo next to this repository" >&2
    echo "  (with its siblings), or set FLIGHT_MOJO" >&2
    exit 1
}
# The examples connect to 8815, as they do to the reference server.
PORT=8815
WORK="$ROOT/build/check-mojo"
LOG="$WORK/server.log"
rm -rf "$WORK"
mkdir -p "$WORK"

# flight.mojo's pixi, not this one's: this script runs inside `pixi run`, and
# the manifest is named so the nested call cannot pick up ours.
flight_pixi() {  # <pixi run args...>
    pixi run --manifest-path "$FLIGHT_MOJO/pixi.toml" "$@"
}

port_open() {
    python -c "import socket;socket.create_connection(('127.0.0.1',$PORT),0.5)" 2>/dev/null
}

if port_open; then
    echo "error: something is already listening on $PORT; the examples would" >&2
    echo "  talk to it instead of the server this run builds. Stop it first." >&2
    exit 1
fi

echo "=== build flight.mojo's Iceberg server ($FLIGHT_MOJO)"
flight_pixi serve-iceberg

echo "=== write the fixture table"
TABLE="$(flight_pixi -e verify python "$FLIGHT_MOJO/tools/make_fixture.py" "$WORK/warehouse" | tail -1)"
echo "$TABLE"

# The server runs under flight.mojo's environment, which is where flare's and
# the codecs' shims are. `pixi run` stays the parent of the server, so the
# server writes its own PID down and is stopped by it: killing the wrapper
# alone would leave it holding the port for the next run.
echo "=== start it on 127.0.0.1:$PORT"
cat > "$WORK/serve.sh" <<EOF
echo \$\$ > "$WORK/server.pid"
cd "$FLIGHT_MOJO"
exec ./build/serve_ice "$TABLE" --port $PORT
EOF
flight_pixi bash "$WORK/serve.sh" > "$LOG" 2>&1 &
WRAPPER=$!
cleanup() {
    if [ -s "$WORK/server.pid" ]; then
        kill "$(cat "$WORK/server.pid")" 2>/dev/null || true
    fi
    kill "$WRAPPER" 2>/dev/null || true
    wait "$WRAPPER" 2>/dev/null || true
    return 0
}
trap cleanup EXIT

up=""
for _ in $(seq 1 120); do
    if port_open; then up=1; break; fi
    kill -0 "$WRAPPER" 2>/dev/null || break
    sleep 1
done
if [ -z "$up" ]; then
    echo "error: the flight.mojo server did not come up; its log:" >&2
    cat "$LOG" >&2
    exit 1
fi

fail() {
    echo "--- server log:" >&2
    cat "$LOG" >&2
    exit 1
}

for f in client/01_read_a_table.py client/02_fan_out.py client/03_hand_off.py \
         client/04_errors.py client/05_daft_scan.py client/06_pushdown.py; do
    echo "=== $f"
    python "$f" || { echo "error: $f failed against the flight.mojo server" >&2; fail; }
done

echo "=== compare every row with the table's files, read by pyarrow"
python scripts/compare_mojo.py "$WORK/warehouse" || fail

echo
echo "all examples passed against the flight.mojo server, and served the table as pyarrow reads it"
