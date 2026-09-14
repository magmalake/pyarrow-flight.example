"""Query a Flight server with Daft, through the connector in `daft_flight/`.

This is the "keep your engine, replace the reader" case. Daft does the
scheduling, the predicate and the aggregation; the server does the reading;
the connector is a translation between Flight's two questions and Daft's.

The query below is deliberately not a `read_all()` — it filters and groups, so
the work happens in Daft's engine over batches that came off a Flight stream.

    pixi run example-daft
"""

import sys

sys.path.insert(0, ".")

import daft

from daft_flight import FlightSource

source = FlightSource("grpc://127.0.0.1:8815", dataset="t")
print(source.display_name())
print("schema:", source.schema)
print("total_records:", source.total_records)

df = source.read()

# One endpoint per unit of work became one Daft task per unit of work, so this
# runs over the endpoints in parallel without anything here asking for that.
result = (
    df.where(df["ok"])
    .groupby("region")
    .agg(daft.col("id").count().alias("trips"))
    .sort("region")
)
result.show()

rows = {r["region"]: r["trips"] for r in result.to_pylist()}
print("by region:", rows)

# Checked as a property rather than against fixed values, because this runs
# against two different servers with two different tables — the reference one
# and the Mojo Iceberg one, which is larger. The grouped counts must add up to
# the number of rows that pass the same filter, or something was lost or
# double-counted between the endpoints.
ok_rows = df.where(df["ok"]).count_rows()
assert sum(rows.values()) == ok_rows, f"groups sum to {sum(rows.values())}, filter gives {ok_rows}"

if source.total_records == 7:  # the reference fixture: ids 1..7, ok on 1,3,5,6
    assert rows == {"apac": 1, "eu": 3}, f"reference fixture: got {rows}"

# A limit is the one pushdown the connector forwards, because it is the only
# one Flight can express: stop reading the stream. The answer has to be right
# regardless of which endpoint served it.
limited = df.select("id").limit(3).to_pylist()
assert len(limited) == 3, f"expected 3 rows, got {len(limited)}"

# Count is not pushed down — Daft counts the rows the tasks returned, which
# must agree with what GetFlightInfo promised before any data moved.
counted = df.count_rows()
assert counted == source.total_records, (
    f"GetFlightInfo said {source.total_records}, the scan returned {counted}"
)

print(f"\n05 OK — {counted} rows over {len(source._endpoints)} endpoint(s)")
