"""Compare what flight.mojo's server serves with what pyarrow reads itself.

Run by scripts/check-mojo.sh, against flight.mojo's server on 127.0.0.1:8815
serving its gate fixture, with the fixture's warehouse as the argument. The
client examples check properties that hold for any table (the endpoints
partition it, the groups add up); this checks values, which is what a wrong
validity bitmap or a misread timestamp gets past properties with.

The oracle is pyarrow reading the table's Parquet files directly, so nothing
here is transcribed from either server's output. The schema is compared with
the reference server's, which declares the same five columns, and the error
cases client/04_errors.py prints are asserted.

    python scripts/compare_mojo.py build/check-mojo/warehouse
"""

import pathlib
import sys

import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq

sys.path.insert(0, "server")
from reference_server import CHUNKS, SCHEMA  # noqa: E402

problems = []


def mismatch(what):
    problems.append(what)
    print(f"MISMATCH: {what}")


client = fl.connect("grpc://127.0.0.1:8815")
info = client.get_flight_info(fl.FlightDescriptor.for_path("taxi"))
parts = [client.do_get(ep.ticket).read_all() for ep in info.endpoints]
table = pa.concat_tables(parts)
print(f"{info.total_records} records advertised, {table.num_rows} fetched "
      f"over {len(info.endpoints)} endpoints")

# The schema: names, types and nullability as the reference server declares
# them. Field metadata (Iceberg's field ids) is the server's own business.
for want, got_info, got_stream in zip(SCHEMA, info.schema, table.schema):
    for where, got in (("GetFlightInfo", got_info), ("DoGet", got_stream)):
        if (got.name, got.type, got.nullable) != (want.name, want.type, want.nullable):
            mismatch(f"{where} field {got.name}: {got.type}, nullable={got.nullable}; "
                     f"the reference has {want.name}: {want.type}, nullable={want.nullable}")
if len(info.schema) != len(SCHEMA):
    mismatch(f"{len(info.schema)} fields, the reference has {len(SCHEMA)}")

if table.num_rows != info.total_records:
    mismatch(f"GetFlightInfo said {info.total_records} rows, DoGet gave {table.num_rows}")
if len(info.endpoints) <= 3:
    mismatch(f"{len(info.endpoints)} endpoints for 3 data files: the plan did not split")

rows = table.to_pylist()
ids = [r["id"] for r in rows]
if len(ids) != len(set(ids)):
    mismatch(f"ids served twice: {sorted({i for i in ids if ids.count(i) > 1})}")
by_id = {r["id"]: r for r in rows}

# Every row, value for value, against the table's files read by pyarrow. A
# fresh warehouse holds exactly the table's data files: two appends and the
# file registered with add_files.
files = sorted(pathlib.Path(sys.argv[1]).rglob("*.parquet"))
oracle = pa.concat_tables([pq.read_table(f) for f in files]).to_pylist()
print(f"oracle: {len(oracle)} rows in {len(files)} Parquet files")
if len(files) != 3:
    mismatch(f"expected the fixture's 3 data files, found {len(files)}")
for want in oracle:
    got = by_id.pop(want["id"], None)
    if got != want:
        mismatch(f"id {want['id']}: served {got}, the file has {want}")
if by_id:
    mismatch(f"rows served that no file has: ids {sorted(by_id)[:10]}")

# The reference server's rows are the first seven of the fixture.
if not {r["id"] for r in pa.concat_tables(CHUNKS).to_pylist()} <= set(ids):
    mismatch("the reference server's ids 1..7 are not all served")

# The error cases, which client/04_errors.py prints rather than asserts.
try:
    stale = client.do_get(fl.Ticket(b"0|0|0|/nonexistent.parquet")).read_all()
    if stale.num_rows != 0:
        mismatch(f"a stale ticket returned {stale.num_rows} rows, not an empty stream")
except Exception as e:  # noqa: BLE001
    mismatch(f"a stale ticket raised {type(e).__name__}: {e}; it should come back empty")
try:
    client.do_get(fl.Ticket(b"not-a-ticket")).read_all()
    mismatch("a malformed ticket was answered instead of refused")
except Exception:  # noqa: BLE001
    pass
try:
    list(client.list_flights())
    mismatch("ListFlights answered; the server implements only the read path")
except pa.lib.ArrowNotImplementedError:
    pass
except Exception as e:  # noqa: BLE001
    mismatch(f"ListFlights raised {type(e).__name__}, not UNIMPLEMENTED: {e}")

if problems:
    print(f"\n{len(problems)} mismatch(es): the flight.mojo server is wrong")
    sys.exit(1)
print(f"flight.mojo serves every row as pyarrow reads it ({len(oracle)}), "
      "the reference schema, and the expected errors")
