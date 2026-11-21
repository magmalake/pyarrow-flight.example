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


SNAPSHOT = 1
"""The one snapshot this table has. Its tickets name it, as flight.mojo's do."""


def ticket_for(i):
    """flight.mojo's ticket format, `<snapshot>|<start>|<length>|<path>`.

    Tickets are opaque to a client, so nothing in client/ depends on this.
    Using the Mojo server's shape is what lets the same bad tickets mean the
    same thing to both servers, and be answered the same way (see `do_get`).
    """
    return f"{SNAPSHOT}|0|{CHUNKS[i].num_rows}|chunk-{i}".encode()


def parse_ticket(raw):
    """(snapshot, start, path), refusing what is not a ticket.

    `ValueError` is what pyarrow answers INVALID_ARGUMENT (`ArrowInvalid` on
    the client), and `KeyError` NOT_FOUND (`ArrowKeyError`); anything else
    would be INTERNAL or UNKNOWN, which is for the server's own faults.
    """
    try:
        snapshot, start, length, path = raw.decode().split("|", 3)
        snapshot, start, length = int(snapshot), int(start), int(length)
    except ValueError:  # UnicodeDecodeError is a ValueError too
        raise ValueError(
            "malformed ticket, expected '<snapshot>|<start>|<length>|<path>'"
        ) from None
    if start < 0 or length < 0 or not path:
        raise ValueError("malformed ticket, a negative range or an empty path")
    return snapshot, start, path


class Reference(fl.FlightServerBase):
    def get_flight_info(self, context, descriptor):
        # One endpoint per chunk, no location: "fetch from the server you
        # asked", which is what a single-process server means.
        endpoints = [
            fl.FlightEndpoint(ticket_for(i), []) for i in range(len(CHUNKS))
        ]
        return fl.FlightInfo(
            SCHEMA,
            descriptor,
            endpoints,
            sum(c.num_rows for c in CHUNKS),
            -1,  # total_bytes unknown
        )

    def do_get(self, context, ticket):
        # The same three answers flight.mojo gives: a malformed ticket is
        # INVALID_ARGUMENT, a snapshot the table does not have is NOT_FOUND,
        # and a ticket at a real snapshot for a piece it does not have is a
        # stale one, answered with an empty stream rather than failing.
        snapshot, start, path = parse_ticket(ticket.ticket)
        if snapshot != SNAPSHOT:
            raise KeyError(f"snapshot {snapshot} is not in the table; plan again")
        for i, chunk in enumerate(CHUNKS):
            if start == 0 and path == f"chunk-{i}":
                return fl.RecordBatchStream(chunk)
        return fl.RecordBatchStream(SCHEMA.empty_table())


if __name__ == "__main__":
    print("reference flight server on 127.0.0.1:8815", flush=True)
    Reference(location="grpc://127.0.0.1:8815").serve()
