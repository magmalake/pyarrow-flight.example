"""Hand the result to the tools you already use.

The reason Flight is worth the trouble: what arrives is Arrow, so passing it on
costs nothing. pandas is the exception that proves it — that conversion is a
real copy, because pandas is not Arrow underneath.

    pixi run example-handoff
"""
import duckdb
import polars as pl
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:8815")
info = client.get_flight_info(fl.FlightDescriptor.for_path("taxi"))
arrivals = client.do_get(info.endpoints[0].ticket).read_all()

# Polars: zero copy. It is Arrow underneath, so this rebinds buffers.
frame = pl.from_arrow(arrivals)
print("polars:")
print(frame.head(3))

# DuckDB: queries the Arrow table in place, no load step. The variable name is
# picked up from the enclosing scope by replacement scan — so it must not be a
# SQL keyword, which is why this is not called `table`.
print("\nduckdb:")
print(duckdb.sql("SELECT region, count(*) AS n FROM arrivals GROUP BY region ORDER BY n DESC").df())

# pandas: a genuine conversion. Fine, but it is the one that costs.
print("\npandas:")
print(arrivals.to_pandas().head(3))
