"""Read Arrow buffers a Mojo producer put in shared memory, without copying.

`shm.py` asks a Flight server for rows and gets back the name of a mapping the
server *wrote the rows into* — pyarrow decoded Parquet, then encoded Arrow IPC
into the mapping. This skips the encode. `iceberg.mojo`'s `ib-shm-publish`
scans a split and writes the Arrow buffers themselves into the mapping, in the
layout they already have, and says where each one landed:

    {"batches": [{"path": "...", "columns": [
        {"name": "trip_distance", "format": "g", "length": 1048576,
         "null_count": 0, "buffers": [null, {"offset": 0, "length": 8388608}]}
    ]}]}

so the consumer maps the file and wraps the buffers where they lie. No decode,
no copy, nothing allocated for the data at all — `pa.foreign_buffer` is a view
and `Array.from_buffers` points at it.

## Offsets, not addresses

Every position in that manifest is an offset from the start of the mapping,
because the mapping lands at a different address here than it did in the
producer. Adding our own base is the entire fix-up, and it is what lets Arrow
buffers cross a process at all — the C Data Interface hands over pointers,
which is exactly what does not survive the trip.

## What is still paid

The producer copies once: its decode wrote the buffers, and publishing moves
them into the mapping. That is the same copy an in-process C Data Interface
export makes, so this is a cross-process handover at the price of an
in-process one. Removing it means the Parquet decode allocating inside the
mapping to begin with, which is a change to the reader rather than to this
protocol.
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import subprocess
from typing import TYPE_CHECKING

import pyarrow as pa
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft.io.pushdowns import Pushdowns


class _Worker:
    """One producer process and its one-line-each-way pipe.

    A pipe is a single conversation, so a worker serves one ticket at a time.
    Long-lived rather than one process per split: a launch per split would
    measure Mojo's startup rather than the handover.
    """

    def __init__(self, binary: str, table: str, column: str, out_dir: str,
                 split_size: int, env: dict[str, str] | None = None) -> None:
        self._proc = subprocess.Popen(
            [binary, table, str(split_size), column, out_dir],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env={**os.environ, **(env or {})},
            text=True,
            bufsize=1,
        )
        self.tickets: list[str] = json.loads(self._proc.stdout.readline())["tickets"]

    def publish(self, ticket: str) -> list[dict]:
        self._proc.stdin.write(ticket + "\n")
        self._proc.stdin.flush()
        line = self._proc.stdout.readline()
        if not line:
            raise RuntimeError("shm publisher exited")
        return json.loads(line)["batches"]

    def close(self) -> None:
        if self._proc.poll() is None:
            self._proc.stdin.close()
            self._proc.wait(timeout=10)


class _Publisher:
    """A pool of producer processes, handed out one per in-flight split.

    One worker would serialise the whole scan behind a single pipe, and the
    Parquet decode is most of what a publish costs — so a single-process
    producer measures how fast one core decodes, not what the handover costs.
    A server on the other side of a socket would use a thread pool for exactly
    the same reason; this is that, spelled with processes because the producer
    is a separate program.
    """

    def __init__(self, binary: str, table: str, column: str, out_dir: str,
                 split_size: int, workers: int = 0,
                 env: dict[str, str] | None = None) -> None:
        os.makedirs(out_dir, exist_ok=True)
        workers = workers or min(8, os.cpu_count() or 4)
        self._workers = [
            _Worker(binary, table, column, out_dir, split_size, env)
            for _ in range(workers)
        ]
        self.tickets = self._workers[0].tickets
        self._free: queue.Queue[_Worker] = queue.Queue()
        for w in self._workers:
            self._free.put(w)

    def publish(self, ticket: str) -> list[dict]:
        """Hand over one ticket; get back where its batches landed."""
        worker = self._free.get()
        try:
            return worker.publish(ticket)
        finally:
            self._free.put(worker)

    def close(self) -> None:
        for w in self._workers:
            w.close()


# Arrow C Data Interface format strings, for the types a flat column can be.
_TYPES = {
    "g": pa.float64(), "f": pa.float32(),
    "l": pa.int64(), "i": pa.int32(), "s": pa.int16(), "c": pa.int8(),
    "L": pa.uint64(), "I": pa.uint32(), "S": pa.uint16(), "C": pa.uint8(),
    "b": pa.bool_(), "u": pa.utf8(), "z": pa.binary(),
    "U": pa.large_utf8(), "Z": pa.large_binary(),
}


def _arrow_type(fmt: str) -> pa.DataType:
    if fmt in _TYPES:
        return _TYPES[fmt]
    if fmt.startswith("ts"):  # tsu:, tsm:, … timestamp with a unit and a zone
        unit = {"s": "s", "m": "ms", "u": "us", "n": "ns"}[fmt[2]]
        zone = fmt.split(":", 1)[1] or None
        return pa.timestamp(unit, tz=zone)
    raise ValueError(f"shm_direct: no type for Arrow format {fmt!r}")


def _column(mapping: pa.Buffer, spec: dict) -> pa.Array:
    """One column, pointing straight into the mapping.

    `foreign_buffer` keeps `mapping` alive as the owner, so the pages stay
    mapped for as long as any array built on them is reachable.
    """
    base = mapping.address
    buffers = [
        None
        if b is None
        else pa.foreign_buffer(base + b["offset"], b["length"], mapping)
        for b in spec["buffers"]
    ]
    return pa.Array.from_buffers(
        _arrow_type(spec["format"]),
        spec["length"],
        buffers,
        null_count=spec["null_count"],
    )


def read_batch(entry: dict) -> pa.RecordBatch:
    """A published batch, as Arrow, with nothing copied."""
    with pa.memory_map(entry["path"], "rb") as source:
        mapping = source.read_buffer(os.path.getsize(entry["path"]))
    columns = [_column(mapping, c) for c in entry["columns"]]
    return pa.RecordBatch.from_arrays(
        columns, names=[c["name"] for c in entry["columns"]]
    )


class ShmDirectSource(DataSource):
    """Reads an Iceberg table published into mappings by `ib-shm-publish`."""

    def __init__(self, publisher: _Publisher) -> None:
        self._publisher = publisher
        first = publisher.publish(publisher.tickets[0])
        self._arrow_schema = read_batch(first[0]).schema
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)
        self._primed = {publisher.tickets[0]: first}

    @property
    def name(self) -> str:
        return "shm-direct"

    @property
    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"ShmDirectSource({len(self._publisher.tickets)} splits, mapped)"

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        for ticket in self._publisher.tickets:
            yield _ShmDirectTask(
                self._publisher, ticket, self._schema, self._arrow_schema,
                self._primed.pop(ticket, None), pushdowns.limit,
            )


class _ShmDirectTask(DataSourceTask):
    def __init__(self, publisher, ticket, schema, arrow_schema, primed, limit) -> None:
        self._publisher, self._ticket = publisher, ticket
        self._schema, self._arrow_schema = schema, arrow_schema
        self._primed, self._limit = primed, limit

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        # Off the event loop: the producer is scanning Parquet before it
        # answers, and Daft drives every task on one loop.
        entries = self._primed or await asyncio.to_thread(
            self._publisher.publish, self._ticket
        )
        seen = 0
        for entry in entries:
            batch = await asyncio.to_thread(read_batch, entry)
            if self._limit is not None:
                remaining = self._limit - seen
                if remaining <= 0:
                    return
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)
            seen += batch.num_rows
            yield RecordBatch.from_arrow_record_batches([batch], self._arrow_schema)
            # The mapping is the consumer's now; the file has done its job.
            try:
                os.unlink(entry["path"])
            except FileNotFoundError:
                pass
