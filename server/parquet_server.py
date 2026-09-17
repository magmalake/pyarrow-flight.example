"""A pyarrow Flight server over the same Parquet files, for measurement.

`reference_server.py` serves seven rows and exists so a broken example can be
told apart from a broken server. This one serves a real table -- the same
79.5M-row taxi table `flight.mojo`'s `serve-iceberg` serves -- and exists for
one reason: to say what Arrow Flight itself costs.

Without it, timing a client against the Mojo server measures the protocol and
that server's encoder together, and reports the sum as "Flight". pyarrow's
Flight server is a mature implementation of the same protocol over the same
bytes, so the difference between the two is the encoder and the difference
between either and an in-process read is the boundary.

One endpoint per data file, which is what a scan planner would give. The
projection is applied by the server, so what crosses the wire is one column.

    python server/parquet_server.py <table-dir> [--port 8816] [--column trip_distance]
"""

import argparse
import glob
import json
import os

import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq


def data_files(table_dir: str) -> list[str]:
    """Every Parquet file under the table's data directory, sorted.

    Reading the directory rather than the manifests is deliberate: this is not
    an Iceberg implementation and should not look like one. It serves the same
    bytes, which is all the comparison needs.
    """
    pattern = os.path.join(table_dir, "data", "**", "*.parquet")
    return sorted(glob.glob(pattern, recursive=True))


class ParquetFlight(fl.FlightServerBase):
    def __init__(self, location: str, table_dir: str, column: str) -> None:
        super().__init__(location=location)
        self._files = data_files(table_dir)
        if not self._files:
            raise SystemExit(f"no Parquet files under {table_dir}/data")
        self._column = column
        full = pq.ParquetFile(self._files[0]).schema_arrow
        self._schema = pa.schema([full.field(column)])
        self._rows = sum(pq.ParquetFile(f).metadata.num_rows for f in self._files)

    def get_flight_info(self, context, descriptor):
        endpoints = [
            fl.FlightEndpoint(json.dumps({"file": f}).encode(), [])
            for f in self._files
        ]
        return fl.FlightInfo(self._schema, descriptor, endpoints, self._rows, -1)

    def get_schema(self, context, descriptor):
        return fl.SchemaResult(self._schema)

    def do_get(self, context, ticket):
        path = json.loads(ticket.ticket.decode())["file"]
        reader = pq.ParquetFile(path).iter_batches(columns=[self._column])
        return fl.GeneratorStream(self._schema, reader)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("table")
    ap.add_argument("--port", type=int, default=8816)
    ap.add_argument("--column", default="trip_distance")
    # Flight speaks gRPC, and gRPC speaks Unix sockets: on one machine that
    # takes the loopback stack out without changing a line of client code
    # beyond the URI.
    ap.add_argument("--unix", help="serve on this Unix socket instead of a TCP port")
    args = ap.parse_args()
    location = f"grpc+unix://{args.unix}" if args.unix else f"grpc://127.0.0.1:{args.port}"
    server = ParquetFlight(location, args.table, args.column)
    print(
        f"pyarrow flight server on {location}:"
        f" {len(server._files)} files, {server._rows:,} rows, column {args.column}",
        flush=True,
    )
    server.serve()
