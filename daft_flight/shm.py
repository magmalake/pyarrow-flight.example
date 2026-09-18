"""The same Flight plan, with the rows handed over as a mapping.

`source.py` asks a server for rows and the server sends them. This asks the
same server, on the same protocol, and gets back the *name of a segment* the
rows are already in — then maps it. Flight stays the control plane, which is
what it is good at: `GetFlightInfo` still divides the work, a ticket still
names a unit of it, and the client still fans out. Only `DoGet` changes, and
it changes from "here are 28 MiB" to "here is where they are".

That trade is available because a ticket is opaque bytes. A server that knows
its client is on the same host can put a path in it; one that does not, sends
the rows. Nothing else in the plan moves.

## Why a mapping can be free and a socket cannot

Arrow's IPC *file* layout is the in-memory layout, with every buffer 8-byte
aligned. So a consumer that maps the file does not decode it — it points at
the buffers where they lie, and pyarrow hands Daft the same zero-copy arrays
it would have built from a stream it had to read. The C Data Interface cannot
do this across processes: it hands over **pointers**, which mean nothing in
another address space. A mapping is the thing that crosses.

What it does not remove is the producer's write. The rows have to reach the
mapping, and on this path that is a full copy of the column — one, rather
than the encode/send/decode of a stream. Getting rid of that one as well means
the producer *allocating* its Arrow buffers inside the mapping to begin with,
which is a change to the reader rather than to the protocol.

    from daft_flight import ShmSource

    df = ShmSource("grpc://127.0.0.1:8816", dataset="taxi").read()
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.flight as fl
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from daft_flight.source import FlightSource, _next_chunk

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft.io.pushdowns import Pushdowns


class ShmSource(DataSource):
    """Reads a Flight dataset whose `DoGet` answers with a mapping to read."""

    def __init__(self, uri: str, dataset: str = "t") -> None:
        # The plan is Flight's, unchanged — this borrows it rather than
        # reimplementing a second copy of the same two calls.
        self._flight = FlightSource(uri, dataset=dataset)
        self._uri = uri

    @property
    def name(self) -> str:
        return "flight-shm"

    @property
    def schema(self) -> Schema:
        return self._flight.schema

    def display_name(self) -> str:
        return (
            f"ShmSource({self._uri},"
            f" {len(self._flight._endpoints)} endpoints, mapped)"
        )

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        for endpoint in self._flight._endpoints:
            yield _ShmTask(
                uri=endpoint.location or self._uri,
                ticket=endpoint.ticket,
                schema=self._flight.schema,
                arrow_schema=self._flight._arrow_schema,
                limit=pushdowns.limit,
            )


class _ShmTask(DataSourceTask):
    """One `DoGet` that returns a path, and the mapping behind it."""

    def __init__(
        self,
        uri: str,
        ticket: bytes,
        schema: Schema,
        arrow_schema: pa.Schema,
        limit: int | None = None,
    ) -> None:
        self._uri = uri
        self._ticket = ticket
        self._schema = schema
        self._arrow_schema = arrow_schema
        self._limit = limit

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        # Off the event loop, for the reason source.py explains: the server
        # is writing 28 MiB before it answers, and Daft drives every task on
        # one loop.
        path = await asyncio.to_thread(self._fetch_path)
        seen = 0
        # Unlinking a mapped file is legal and keeps the directory from
        # filling up over repeated reads: the mapping holds the inode open
        # until the last reference to it goes away.
        with pa.memory_map(path, "rb") as source:
            reader = pa.ipc.open_file(source)
            os.unlink(path)
            for i in range(reader.num_record_batches):
                batch = reader.get_batch(i)
                if self._limit is not None:
                    remaining = self._limit - seen
                    if remaining <= 0:
                        return
                    if batch.num_rows > remaining:
                        batch = batch.slice(0, remaining)
                seen += batch.num_rows
                yield RecordBatch.from_arrow_record_batches(
                    [batch], self._arrow_schema
                )

    def _fetch_path(self) -> str:
        """The whole of what crosses the wire: one string."""
        with fl.connect(self._uri) as client:
            reader = client.do_get(fl.Ticket(self._ticket))
            chunk = _next_chunk(reader)
            if chunk is None:
                raise RuntimeError("shm server returned no path")
            return chunk.data.column(0)[0].as_py()
