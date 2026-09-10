"""Read a table from a Flight server. Nothing here knows the server is Mojo.

    pixi run example-basic
"""
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:8815")

# A descriptor names *what* you want. `for_path` is the usual form; `for_command`
# carries opaque bytes if a server accepts a query rather than a name.
info = client.get_flight_info(fl.FlightDescriptor.for_path("taxi"))

print(f"rows advertised : {info.total_records}")
print(f"endpoints       : {len(info.endpoints)}")
print(f"schema          : {info.schema}")

# An endpoint is one unit of work. Its ticket is opaque -- the server decides
# what it means, and a client never interprets it.
table = client.do_get(info.endpoints[0].ticket).read_all()
print(f"\nfirst endpoint  : {table.num_rows} rows")
print(table.slice(0, 5).to_pandas())
