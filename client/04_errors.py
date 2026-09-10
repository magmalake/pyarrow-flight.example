"""What failure looks like, so it is recognisable when it happens.

Every case below is one this example's authors hit while building the server.

    pixi run example-errors
"""
import pyarrow as pa
import pyarrow.flight as fl

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
#    gRPC status list, so catching `Exception` is the honest catch-all.
try:
    list(client.list_flights())
except pa.lib.ArrowNotImplementedError as e:
    print(f"ListFlights        -> ArrowNotImplementedError: {str(e)[:44]}…")
except Exception as e:
    print(f"ListFlights        -> {type(e).__name__}: {str(e)[:44]}…")

# 3. A ticket the server cannot place. This server returns an empty stream for
#    a ticket naming a file the current snapshot has no task for -- a worker
#    holding a stale ticket should come back empty rather than fail a query.
try:
    got = client.do_get(fl.Ticket(b"0|0|0|/nonexistent.parquet")).read_all()
    print(f"stale ticket       -> {got.num_rows} rows (empty, not an error)")
except Exception as e:
    print(f"stale ticket       -> {type(e).__name__}: {str(e)[:50]}…")

# 4. A malformed ticket is a different thing from a stale one, and should say so.
try:
    client.do_get(fl.Ticket(b"not-a-ticket")).read_all()
except Exception as e:
    print(f"malformed ticket   -> {type(e).__name__}: {str(e)[:60]}…")
