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

## What is being trusted

`dlopen`-ing a native library puts it in *this* process. A fault in it is a
fault in the Python interpreter, and there is no retry boundary. That is the
real cost of the in-process path, and it is worth stating next to the speed.
"""

from __future__ import annotations

import ctypes
import os
from typing import TYPE_CHECKING

import pyarrow as pa
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

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
        ctypes.c_char_p,
        ctypes.c_int64,
        ctypes.c_char_p,
        ctypes.c_int64,
    ]
    lib.ib_plan_splits.restype = ctypes.c_int64
    lib.ib_schema.argtypes = [ctypes.c_char_p, ctypes.c_int64, ctypes.c_char_p]
    lib.ib_schema.restype = ctypes.c_void_p
    lib.ib_scan_split.argtypes = [
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_int64,
        ctypes.c_char_p,
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

        # Two-call protocol: ask for the size, then for the bytes. The library
        # has no allocator of ours to use, so the buffer is the caller's.
        needed = self._lib.ib_plan_splits(
            self._table_dir, split_size, None, 0
        )
        if needed < 0:
            raise RuntimeError(f"ib_plan_splits failed for {table_dir}")
        buf = ctypes.create_string_buffer(needed)
        if self._lib.ib_plan_splits(
            self._table_dir, split_size, buf, needed
        ) != needed:
            raise RuntimeError("ib_plan_splits: plan changed between calls")
        self._tickets = [
            t.encode() for t in buf.raw[:needed].decode().split("\n") if t
        ]

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
            f" {len(self._tickets)} splits, in-process)"
        )

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        """One task per split — the same division the Flight server hands out.

        It is the same planner: `ib_plan_splits` is `plan_files()` with the
        tickets rendered as text, so a table divides identically whichever way
        it is read.
        """
        for ticket in self._tickets:
            yield _LocalTask(
                lib_path=self._lib_path,
                shim_prefix=self._shim_prefix,
                table_dir=self._table_dir,
                ticket=ticket,
                columns=self._columns,
                split_size=self._split_size,
                schema=self._schema,
                arrow_schema=self._arrow_schema,
                limit=pushdowns.limit,
            )


class _LocalTask(DataSourceTask):
    """One `ib_scan_split`, imported as an `ArrowArrayStream`."""

    def __init__(
        self,
        lib_path: str,
        shim_prefix: str | None,
        table_dir: bytes,
        ticket: bytes,
        columns: bytes,
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
        addr = lib.ib_scan_split(
            self._table_dir, self._ticket, self._split_size, self._columns
        )
        if not addr:
            # An empty split is a legal answer — a stale ticket names a task
            # this plan does not have — and the library reports it the same
            # way it reports a failure. Treat it as no rows.
            return

        reader = pa.RecordBatchReader._import_from_c(addr)
        seen = 0
        for batch in reader:
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
