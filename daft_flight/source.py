"""A Daft `DataSource` that reads any Arrow Flight server.

The point of this file is how little is in it. Daft already knows how to
schedule work, apply predicates and run an aggregation; what it needs from a
new source is the answer to two questions — *what does the data look like* and
*where are the pieces* — and Flight answers both in its first call. So this is
a translation, not an integration: `GetFlightInfo` becomes Daft's task list and
`DoGet` becomes a task's batches.

Nothing here knows what the server is written in. It is checked against a
pyarrow reference server and against a Mojo one, and it cannot tell them apart.

    from daft_flight import FlightSource

    df = FlightSource("grpc://127.0.0.1:8815", dataset="taxi").read()
    df.where(df["region"] == "eu").groupby("region").sum("amount").show()

## The plan is fetched once

`GetFlightInfo` is called when the source is constructed, and the endpoints it
returns are the task list for every execution of that DataFrame. That is
deliberate rather than lazy: our server pins a snapshot and puts it in the
ticket, so a plan fetched once describes one consistent version of the table.
Re-planning per execution would silently mix snapshots if a commit landed in
between.

## What is not pushed down

Daft offers `filters`, `columns`, `limit` and a count `aggregation`. Only the
limit is used here, because Flight has no vocabulary for the rest: a ticket is
opaque bytes whose meaning is the server's business, and there is no field in
which to send a predicate. So filters and projection are applied by Daft after
the read — correct, but the bytes still crossed the wire.

A server that wants pushdown has to define it inside its own ticket format, and
then only clients that know that format benefit. That trade — a protocol
everyone speaks against pushdowns nobody speaks — is the honest cost of an
interop seam.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, NamedTuple

import pyarrow.flight as fl
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import pyarrow as pa
    from daft.io.pushdowns import Pushdowns


class _Endpoint(NamedTuple):
    """One unit of work, reduced to what a task needs and can be pickled.

    pyarrow's own endpoint objects are not carried around: a task may be
    executed in another process, so a plain tuple of bytes and strings travels
    better than anything holding a connection.
    """

    ticket: bytes
    location: str | None
    """Where to fetch it. `None` means "the server we asked", which is what an
    endpoint with no location means in Flight, and what a single-process server
    returns."""


class FlightSource(DataSource):
    """Reads a Flight dataset as a Daft DataFrame."""

    def __init__(self, uri: str, dataset: str = "t") -> None:
        self._uri = uri
        with fl.connect(uri) as client:
            info = client.get_flight_info(fl.FlightDescriptor.for_path(dataset))

        self._arrow_schema: pa.Schema = info.schema
        self._schema = Schema.from_pyarrow_schema(info.schema)
        self._total_records = info.total_records
        self._endpoints = [
            _Endpoint(
                ticket=ep.ticket.ticket,
                # A list of locations means "any of these can serve it", so
                # the first is as good as any. Empty means fetch from here.
                location=_uri_of(ep.locations[0]) if ep.locations else None,
            )
            for ep in info.endpoints
        ]

    @property
    def name(self) -> str:
        return "flight"

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def total_records(self) -> int:
        """What the server said before any data moved. `-1` means unknown."""
        return self._total_records

    def display_name(self) -> str:
        placed = sum(1 for e in self._endpoints if e.location is not None)
        where = f", {placed} placed" if placed else ""
        return f"FlightSource({self._uri}, {len(self._endpoints)} endpoints{where})"

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        """One task per endpoint — Flight's split, handed to Daft's scheduler.

        The endpoints are disjoint and their union is the dataset, which is the
        same contract Daft needs from a task list. Nothing has to be invented
        here, and nothing may be merged or dropped.
        """
        for endpoint in self._endpoints:
            yield _FlightTask(
                uri=endpoint.location or self._uri,
                ticket=endpoint.ticket,
                schema=self._schema,
                arrow_schema=self._arrow_schema,
                limit=pushdowns.limit,
            )


class _FlightTask(DataSourceTask):
    """One `DoGet`, streamed batch by batch."""

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
        """Yield batches as they arrive rather than reading the whole stream.

        `read_all()` would be shorter and would hold the entire endpoint in
        memory before Daft saw any of it, which defeats the reason a stream is
        a stream.
        """
        seen = 0
        with fl.connect(self._uri) as client:
            reader = await asyncio.to_thread(
                client.do_get, fl.Ticket(self._ticket)
            )
            while True:
                # Off the event loop. `read_chunk` blocks for as long as the
                # server takes, and Daft drives these tasks on one loop: a
                # blocking call here stalls every other task, so a fan-out
                # over N endpoints runs in exactly the time N sequential
                # reads would take. pyarrow releases the GIL for the transfer
                # and the decode, so a thread per task is real parallelism.
                chunk = await asyncio.to_thread(_next_chunk, reader)
                if chunk is None:
                    return
                batch = chunk.data
                # A per-task limit is the one pushdown that saves real bytes,
                # and it is safe because a limit is "at most": Daft still
                # applies the global one over what the tasks return.
                if self._limit is not None:
                    remaining = self._limit - seen
                    if remaining <= 0:
                        reader.cancel()
                        return
                    if batch.num_rows > remaining:
                        batch = batch.slice(0, remaining)
                seen += batch.num_rows
                yield RecordBatch.from_arrow_record_batches(
                    [batch], self._arrow_schema
                )


def _next_chunk(reader):
    """One chunk, or None at end of stream.

    `StopIteration` must not be raised inside a thread that an async
    generator is awaiting -- it means something else there -- so the sentinel
    crosses the boundary instead of the exception.
    """
    try:
        return reader.read_chunk()
    except StopIteration:
        return None


def _uri_of(location: fl.Location) -> str:
    """pyarrow hands back `uri` as bytes on some versions, str on others."""
    uri = location.uri
    return uri.decode() if isinstance(uri, bytes) else str(uri)
