"""A pyarrow Flight server, for checking the client examples against.

Deliberately not the interesting one. It exists so an example that fails
against the Mojo server can be told apart from an example that was wrong to
begin with: run it here first, and if it passes, the client is fine.

    pixi run reference-server
"""
from datetime import datetime

import pyarrow as pa
import pyarrow.flight as fl

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
        pa.field("amount", pa.float64(), nullable=True),
        pa.field("ok", pa.bool_(), nullable=False),
        pa.field("ts", pa.timestamp("us"), nullable=True),
    ]
)

# Two chunks, so the server has something real to advertise as two endpoints.
CHUNKS = [
    pa.table(
        {
            "id": [1, 2, 3, 4],
            "region": ["eu", "us", "eu", "us"],
            "amount": [1.5, None, 3.5, 4.5],
            "ok": [True, False, True, False],
            "ts": [datetime(2023, 11, d) for d in (14, 15, 16, 17)],
        },
        schema=SCHEMA,
    ),
    pa.table(
        {
            "id": [5, 6, 7],
            "region": ["apac", "eu", "apac"],
            "amount": [5.5, 6.5, None],
            "ok": [True, True, False],
            "ts": [datetime(2023, 12, 1), datetime(2024, 1, 1), datetime(2023, 11, 14)],
        },
        schema=SCHEMA,
    ),
]


class Reference(fl.FlightServerBase):
    def get_flight_info(self, context, descriptor):
        # One endpoint per chunk, no location: "fetch from the server you
        # asked", which is what a single-process server means.
        endpoints = [
            fl.FlightEndpoint(str(i).encode(), []) for i in range(len(CHUNKS))
        ]
        return fl.FlightInfo(
            SCHEMA,
            descriptor,
            endpoints,
            sum(c.num_rows for c in CHUNKS),
            -1,  # total_bytes unknown
        )

    def do_get(self, context, ticket):
        which = int(ticket.ticket.decode())
        return fl.RecordBatchStream(CHUNKS[which])


if __name__ == "__main__":
    print("reference flight server on 127.0.0.1:8815", flush=True)
    Reference(location="grpc://127.0.0.1:8815").serve()
