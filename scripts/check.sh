#!/usr/bin/env bash
# Run every client example against the reference server.
#
# Waits for the port rather than sleeping a guessed number of seconds: a fixed
# sleep is the kind of gate that passes locally and fails on a slower runner.
set -euo pipefail

python server/reference_server.py > /tmp/reference.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do
  if python -c "import socket;socket.create_connection(('127.0.0.1',8815),0.5)" 2>/dev/null; then
    break
  fi
  kill -0 "$SRV" 2>/dev/null || { echo "server died:" >&2; cat /tmp/reference.log >&2; exit 1; }
  sleep 1
done

for f in client/01_read_a_table.py client/02_fan_out.py client/03_hand_off.py \
         client/04_errors.py client/05_daft_scan.py client/06_pushdown.py; do
  echo "=== $f"
  python "$f"
done
echo
echo "all examples passed against the reference server"
