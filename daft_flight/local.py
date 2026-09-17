"""The same Daft source, reading in-process instead of over a socket.

`source.py` fetches a plan over gRPC and a task's rows over gRPC. This fetches
both by calling into a Mojo shared library: the reader runs inside the Python
process and hands Daft its Arrow buffers through the **C Data Interface**, so
nothing is encoded, sent, or decoded.

The two are deliberately interchangeable. A unit of work is the same ticket
string in both, `<snapshot>|<start>|<length>|<path>`, and both produce the same
Daft `DataFrame` — which is what makes them comparable, and what makes the
choice a deployment decision rather than a rewrite.

    from daft_flight import IcebergLocalSource

    df = IcebergLocalSource(LIB, "/warehouse/db/trips").read()

## When to prefer this

Whenever it is available. In one process there is nothing to move: Flight's
encode, socket and decode are pure overhead against a function call, and our
own Flight server copies every value out of Arrow layout and back before it
even reaches the wire. Flight earns its place when the reader cannot live in
the consumer's process — another machine, another language, credentials that
should not leave a service, or a crash that must not take the worker with it.

## What it pushes down

Everything the protocol version of this source cannot. `get_tasks` receives
Daft's pushdowns and hands three of them to the library: the **projection**,
so the Parquet reader decodes the columns asked for and no others; the
**predicate**, translated by `predicate.py` into Iceberg's filter DSL; and the
**limit**, as before.

The predicate is worth the most and is worth it twice. At plan time it prunes
manifests and files on partition values and column statistics, so a task list
comes back shorter -- work that never gets scheduled. At read time what is
left of it becomes the residual, which drops row groups and pages on the
Parquet footer before a byte is decompressed. Neither is available to a reader
that filters after the read, which is what makes this more than an
optimisation of the same shape.

Daft still applies the filter to what comes back. Nothing here claims a filter
was handled, so pushing a predicate down changes what is decoded and never
what is returned -- which is also the property that makes a partial push
(`predicate.py` explains when that happens) safe rather than clever.

## What is being trusted

`dlopen`-ing a native library puts it in *this* process. A fault in it is a
fault in the Python interpreter, and there is no retry boundary. That is the
real cost of the in-process path, and it is worth stating next to the speed.
"""

from __future__ import annotations

import asyncio
import ctypes
import os
from typing import TYPE_CHECKING

import pyarrow as pa
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from daft_flight.predicate import to_iceberg_filter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft.io.pushdowns import Pushdowns

DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024
"""Target bytes per task, matching what a production Iceberg planner uses.

It must be the same for planning and for reading: the read re-plans and selects
the task at the requested offset, so a different division has no task there and
the split comes back empty.
"""


def _use_shims_from(prefix: str) -> None:
    """Point the library's codec shims at the environment that has them.

    The Mojo tins `dlopen` their C shims — `libzstdmojo`, `liblz4mojo`,
    `libbrotlimojo` — from `$CONDA_PREFIX/lib`, resolved at first use. Loading
    the library from *another* environment (this one, which deliberately has
    no Mojo toolchain) therefore looks for them in the wrong place, and the
    failure arrives as a scan that aborts on the first compressed page rather
    than as a missing-library error. zstd is what Iceberg writes Parquet with
    by default, so that is nearly every real table.

    Setting the variable for this process is the whole fix. It has to happen
    before the first call into the library, because the handle is opened once
    and kept for the life of the process.
    """
    os.environ["CONDA_PREFIX"] = prefix


def _bind(lib_path: str) -> ctypes.CDLL:
    """Declare the three entry points, because ctypes guesses `int` otherwise.

    On a 64-bit platform an undeclared `restype` truncates a returned pointer
    to 32 bits, which produces an address that is wrong in a way that looks
    like data corruption rather than a binding mistake.
    """
    lib = ctypes.CDLL(lib_path)
    lib.ib_plan_splits.argtypes = [
        ctypes.c_char_p,  # table directory
        ctypes.c_int64,  # split size
        ctypes.c_char_p,  # filter DSL, empty for none
        ctypes.c_int64,  # snapshot to plan against, 0 for the current one
        ctypes.c_char_p,  # destination buffer
        ctypes.c_int64,
    ]
    lib.ib_plan_splits.restype = ctypes.c_int64
    lib.ib_snapshot.argtypes = [ctypes.c_char_p]
    lib.ib_snapshot.restype = ctypes.c_int64
    lib.ib_schema.argtypes = [ctypes.c_char_p, ctypes.c_int64, ctypes.c_char_p]
    lib.ib_schema.restype = ctypes.c_void_p
    lib.ib_scan_split.argtypes = [
        ctypes.c_char_p,  # table directory
        ctypes.c_char_p,  # ticket
        ctypes.c_int64,  # split size
        ctypes.c_char_p,  # projection, empty for every column
        ctypes.c_char_p,  # filter DSL, the one the plan was made with
    ]
    lib.ib_scan_split.restype = ctypes.c_void_p
    return lib


class IcebergLocalSource(DataSource):
    """Reads an Iceberg table through a Mojo shared library, in this process."""

    def __init__(
        self,
        lib_path: str,
        table_dir: str,
        columns: list[str] | None = None,
        split_size: int = DEFAULT_SPLIT_SIZE,
        shim_prefix: str | None = None,
    ) -> None:
        if shim_prefix is None:
            shim_prefix = os.environ.get("MAGMALAKE_SHIM_PREFIX")
        if shim_prefix:
            _use_shims_from(shim_prefix)
        self._lib_path = lib_path
        self._shim_prefix = shim_prefix
        self._lib = _bind(lib_path)
        self._table_dir = table_dir.encode()
        self._columns = ",".join(columns).encode() if columns else b""
        self._split_size = split_size

        # The plan is not made here any more: it depends on the predicate, and
        # the predicate arrives with `get_tasks`. The snapshot is resolved here
        # instead, and every plan is pinned to it, which keeps the property
        # planning once used to give -- one version of the table for the life
        # of this source, however many times it is planned or executed.
        self._snapshot = self._lib.ib_snapshot(self._table_dir)
        if self._snapshot < 0:
            raise RuntimeError(f"ib_snapshot failed for {table_dir}")

        # The schema costs one row, not a scan — see `ib_schema`.
        addr = self._lib.ib_schema(self._table_dir, split_size, self._columns)
        if not addr:
            raise RuntimeError(f"ib_schema failed for {table_dir}")
        self._arrow_schema = pa.Schema._import_from_c(addr)
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)

    @property
    def name(self) -> str:
        return "iceberg-local"

    @property
    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return (
            f"IcebergLocalSource({self._table_dir.decode()},"
            f" snapshot {self._snapshot}, in-process)"
        )

    def _plan(self, filter_dsl: bytes) -> list[bytes]:
        """The tickets for one predicate, as newline-separated text.

        Two-call protocol: ask for the size, then for the bytes. The library
        has no allocator of ours to use, so the buffer is the caller's.
        """
        needed = self._lib.ib_plan_splits(
            self._table_dir, self._split_size, filter_dsl, self._snapshot, None, 0
        )
        if needed < 0:
            raise RuntimeError(
                f"ib_plan_splits failed for {self._table_dir.decode()}"
            )
        buf = ctypes.create_string_buffer(needed)
        if (
            self._lib.ib_plan_splits(
                self._table_dir,
                self._split_size,
                filter_dsl,
                self._snapshot,
                buf,
                needed,
            )
            != needed
        ):
            raise RuntimeError("ib_plan_splits: plan changed between calls")
        return [t.encode() for t in buf.raw[:needed].decode().split("\n") if t]

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        """One task per split — the same division the Flight server hands out.

        It is the same planner: `ib_plan_splits` is `plan_files()` with the
        tickets rendered as text, so a table divides identically whichever way
        it is read. What differs from the Flight source is that the planner is
        run *here*, with the predicate: a pushed-down filter changes how many
        tasks there are, which is the part a ticket cannot express.
        """
        filter_dsl = to_iceberg_filter(pushdowns.filters).encode()
        names = self._needed(pushdowns)
        columns = ",".join(names).encode() if names is not None else self._columns
        arrow_schema = self._project(names)
        schema = (
            self._schema
            if arrow_schema is self._arrow_schema
            else Schema.from_pyarrow_schema(arrow_schema)
        )
        for ticket in self._plan(filter_dsl):
            yield _LocalTask(
                lib_path=self._lib_path,
                shim_prefix=self._shim_prefix,
                table_dir=self._table_dir,
                ticket=ticket,
                columns=columns,
                filter_dsl=filter_dsl,
                split_size=self._split_size,
                schema=schema,
                arrow_schema=arrow_schema,
                limit=pushdowns.limit,
            )

    def _needed(self, pushdowns: Pushdowns) -> list[str] | None:
        """The columns a task must return: the projection *and* the filter's.

        `pushdowns.columns` is what the query wants out of the source, which is
        not what the source has to read: Daft applies the filter to what comes
        back, so the columns the filter tests have to be in it even when the
        query never selects them. `count(*) where trip_distance > 0` pushes a
        projection of one unrelated column and a predicate on another, and a
        source that reads only the first hands Daft a batch its own filter
        cannot be evaluated against.

        Those columns are read whether or not the predicate was pushed, and
        that is the honest cost of leaving the filter to Daft: the pushdown
        saves pages and row groups, not the column.
        """
        if pushdowns.columns is None:
            return None
        names = list(pushdowns.columns)
        for name in sorted(pushdowns.filter_required_column_names()):
            if name not in names:
                names.append(name)
        return names

    def _project(self, columns: list[str] | None) -> pa.Schema:
        """The schema of what a task will return under this projection.

        A projected read is free to hand back more than was asked for — the
        residual may need a column the projection does not — so the task states
        what it will yield and then selects exactly that out of each batch. The
        order is the one asked for, not the table's.
        """
        if columns is None:
            return self._arrow_schema
        return pa.schema([self._arrow_schema.field(c) for c in columns])


class _LocalTask(DataSourceTask):
    """One `ib_scan_split`, imported as an `ArrowArrayStream`."""

    def __init__(
        self,
        lib_path: str,
        shim_prefix: str | None,
        table_dir: bytes,
        ticket: bytes,
        columns: bytes,
        filter_dsl: bytes,
        split_size: int,
        schema: Schema,
        arrow_schema: pa.Schema,
        limit: int | None = None,
    ) -> None:
        self._lib_path = lib_path
        self._shim_prefix = shim_prefix
        self._table_dir = table_dir
        self._ticket = ticket
        self._columns = columns
        self._filter_dsl = filter_dsl
        self._split_size = split_size
        self._schema = schema
        self._arrow_schema = arrow_schema
        self._limit = limit

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        # Bound per task rather than carrying the handle: a task may run in
        # another process, and `ctypes.CDLL` does not survive the trip. dlopen
        # of an already-loaded library is a refcount bump, not a reload.
        if self._shim_prefix:
            _use_shims_from(self._shim_prefix)
        lib = _bind(self._lib_path)
        # Off the event loop. The library reads the whole split before it
        # returns an address, so this call blocks for as long as the scan
        # takes -- and Daft drives these tasks on one loop, where a blocking
        # call stalls every other task. ctypes releases the GIL for the
        # duration of a foreign call, so a thread per task is real
        # parallelism rather than a queue with extra steps.
        addr = await asyncio.to_thread(
            lib.ib_scan_split,
            self._table_dir,
            self._ticket,
            self._split_size,
            self._columns,
            self._filter_dsl,
        )
        if not addr:
            # An empty split is a legal answer — a stale ticket names a task
            # this plan does not have — and the library reports it the same
            # way it reports a failure. Treat it as no rows.
            return

        reader = pa.RecordBatchReader._import_from_c(addr)
        names = self._arrow_schema.names
        seen = 0
        for batch in reader:
            # What was read is a superset of what was asked for whenever the
            # residual needed a column the projection did not. Selecting by
            # name settles both that and the column order, and moves nothing.
            if batch.schema.names != names:
                batch = batch.select(names)
            if self._limit is not None:
                remaining = self._limit - seen
                if remaining <= 0:
                    break
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)
            seen += batch.num_rows
            yield RecordBatch.from_arrow_record_batches(
                [batch], self._arrow_schema
            )
