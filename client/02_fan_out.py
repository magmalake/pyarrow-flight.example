"""Fetch every endpoint in parallel and concatenate.

This is the point of a server advertising more than one endpoint: they are
disjoint, so a client may read them concurrently and the union is the table.

    pixi run example-fanout
"""
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:8815")
info = client.get_flight_info(fl.FlightDescriptor.for_path("taxi"))


def fetch(endpoint):
    # A `location` means "fetch it from there"; an empty list means "from the
    # server you asked". A single-process server leaves it empty; a distributed
    # one fills it in, and *this loop does not change* — which is what makes
    # the same client work against both.
    where = endpoint.locations[0].uri if endpoint.locations else None
    conn = fl.connect(where) if where else client
    return conn.do_get(endpoint.ticket).read_all()


with ThreadPoolExecutor(max_workers=len(info.endpoints)) as pool:
    parts = list(pool.map(fetch, info.endpoints))

table = pa.concat_tables(parts)

for i, part in enumerate(parts):
    print(f"  endpoint {i}: {part.num_rows} rows")
print(f"\nunion: {table.num_rows} rows across {len(parts)} endpoints")

# Worth asserting rather than assuming: the union has to match what
# GetFlightInfo advertised, or the endpoints do not partition the table.
assert table.num_rows == info.total_records, (
    f"union has {table.num_rows} rows, GetFlightInfo said {info.total_records}"
)
print("union matches total_records")
