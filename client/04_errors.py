"""What failure looks like, so it is recognisable when it happens.

Every case below is one this example's authors hit while building the server.
Each prints what came back, and the script exits non-zero if an answer is not
the one both servers here are meant to give: a bad ticket is the *client's*
mistake, so it must come back as INVALID_ARGUMENT or NOT_FOUND, never as
INTERNAL, which is for the server's own faults.

    pixi run example-errors
"""
import sys

import pyarrow as pa
import pyarrow.flight as fl

surprises = []


def expect(label, call, want):
    """Run `call`; `want` is the exception class expected, or None for rows."""
    try:
        got = call()
    except Exception as e:  # noqa: BLE001
        print(f"{label:<19}-> {type(e).__name__}: {str(e)[:60]}…")
        if want is None or not isinstance(e, want):
            surprises.append(f"{label}: {type(e).__name__}")
        return
    print(f"{label:<19}-> {got.num_rows} rows (empty, not an error)")
    if want is not None or got.num_rows != 0:
        surprises.append(f"{label}: {got.num_rows} rows")


# 1. Nothing listening.
try:
    fl.connect("grpc://127.0.0.1:9999").get_flight_info(
        fl.FlightDescriptor.for_path("x")
    )
except fl.FlightUnavailableError as e:
    print(f"server down        -> FlightUnavailableError: {str(e)[:60]}…")

client = fl.connect("grpc://127.0.0.1:8815")

# 2. A method the server does not implement. UNIMPLEMENTED is a legal answer
#    and a client is expected to cope, not crash.
#
#    Note there is no `FlightUnimplementedError`: pyarrow maps gRPC's
#    UNIMPLEMENTED onto `ArrowNotImplementedError`, which is not in the
#    `flight` namespace at all. The `Flight*Error` list is shorter than the
#    gRPC status list -- NOT_FOUND arrives as `ArrowKeyError` and
#    INVALID_ARGUMENT as `ArrowInvalid`, both below -- so catching `Exception`
#    is the honest catch-all.
expect("ListFlights", lambda: list(client.list_flights()),
       pa.lib.ArrowNotImplementedError)

# A ticket is opaque bytes, and a real client never takes one apart. This
# script does, to build the bad ones below: both servers here use
# `<snapshot>|<start>|<length>|<path>`.
info = client.get_flight_info(fl.FlightDescriptor.for_path("x"))
snapshot, start, length, _ = info.endpoints[0].ticket.ticket.decode().split("|", 3)


def do_get(raw):
    return lambda: client.do_get(fl.Ticket(raw)).read_all()


# 3. A snapshot the table does not have -- never had, or has since expired.
#    There is nothing to read it against, so the answer is NOT_FOUND: plan
#    again with GetFlightInfo. Coming back empty instead would let a client
#    that fans out assemble a table with that part silently missing.
expect("unknown snapshot", do_get(b"0|0|0|/nonexistent.parquet"),
       pa.lib.ArrowKeyError)

# 4. A stale ticket: the table's own snapshot, naming a piece that snapshot
#    does not have. That piece holds none of the snapshot's rows, so the
#    stream is empty -- a worker holding a stale ticket comes back empty
#    rather than failing the query.
expect("stale ticket",
       do_get(f"{snapshot}|{start}|{length}|/nonexistent.parquet".encode()),
       None)

# 5. A malformed ticket is a different thing from either, and says so:
#    INVALID_ARGUMENT. Retrying it is pointless.
expect("malformed ticket", do_get(b"not-a-ticket"), pa.lib.ArrowInvalid)

if surprises:
    print(f"\nunexpected answers: {surprises}")
    sys.exit(1)
