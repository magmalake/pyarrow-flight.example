"""The same column, the same engine, three ways across the boundary.

`03_hand_off.py` shows that what arrives is Arrow and passing it on is free.
This asks what it cost to arrive. Daft reads one column of a 79.5M-row Iceberg
table three times over -- in this process through the C Data Interface, and
over Arrow Flight from two different servers -- and reports the wall clock of
each.

Two Flight servers rather than one, because a single one cannot tell you what
you want to know. Timed against `flight.mojo` alone, the number is the
protocol and that server's encoder added together and labelled "Flight".
pyarrow's server over the same Parquet files is a mature implementation of the
same protocol, so the gap between the two is an encoder and the gap between
either and the in-process read is the boundary itself.

    pixi run transports        # brings up both servers and tears them down

Every leg produces the same 79,478,796 rows, which is asserted rather than
assumed: a transport that is fast because it lost rows is not fast.
"""

import asyncio
import glob
import os
import sys
import time

sys.path.insert(0, ".")

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from collections.abc import AsyncIterator

from daft.io import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from daft_flight import FlightSource, IcebergLocalSource, ShmSource

TABLE = os.environ.get("TAXI_TABLE", "")
LIB = os.environ.get("IB_LIB", "")
COLUMN = os.environ.get("TAXI_COLUMN", "trip_distance")
REPEAT = int(os.environ.get("TRANSPORT_REPEAT", "5"))
SPLIT_SIZE = int(os.environ.get("IB_SPLIT_SIZE", 128 * 1024 * 1024))


def measure(label: str, make_source) -> tuple[str, float, int]:
    """p50 of `REPEAT` full reads, after a discarded warm-up.

    The source is built once and re-read, because building it plans the scan
    and planning is not what this measures.
    """
    source = make_source()
    source.read().count_rows()
    samples = []
    for _ in range(REPEAT):
        start = time.perf_counter()
        rows = source.read().count_rows()
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    return label, samples[len(samples) // 2], rows


class _LocalParquet(DataSource):
    """The control: the server's own read, in this process.

    Every Flight leg has pyarrow decoding Parquet on one side and Daft folding
    Arrow on the other. This is the same two halves with nothing between them —
    the identical `iter_batches` call the server makes, handed straight to
    Daft — so the difference against a Flight leg is the boundary and nothing
    else.

    Without it the in-process row would be `iceberg.mojo` reading an Iceberg
    table while the Flight rows are pyarrow reading Parquet files, and the
    ratio between them would carry two changes at once.
    """

    def __init__(self, table_dir: str, column: str) -> None:
        self._files = sorted(
            glob.glob(os.path.join(table_dir, "data", "**", "*.parquet"), recursive=True)
        )
        field = pq.ParquetFile(self._files[0]).schema_arrow.field(column)
        self._arrow_schema = pa.schema([field])
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)
        self._column = column

    @property
    def name(self) -> str:
        return "local-parquet"

    @property
    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"LocalParquet({len(self._files)} files, in-process)"

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        for path in self._files:
            yield _LocalParquetTask(path, self._column, self._schema, self._arrow_schema)


class _LocalParquetTask(DataSourceTask):
    def __init__(self, path, column, schema, arrow_schema) -> None:
        self._path, self._column = path, column
        self._schema, self._arrow_schema = schema, arrow_schema

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        # Off the event loop for the same reason every other task here is:
        # Daft drives them on one loop and a decode blocks it.
        # The server's exact call, batch size and all: `iter_batches` with a
        # column projection. Reading the whole file at once would be a
        # different decode — bigger batches, less per-batch work — and the
        # comparison would stop being about the boundary.
        batches = await asyncio.to_thread(
            lambda: list(
                pq.ParquetFile(self._path).iter_batches(columns=[self._column])
            )
        )
        for batch in batches:
            yield RecordBatch.from_arrow_record_batches([batch], self._arrow_schema)


def ipc_file() -> str:
    """The column as one Arrow IPC file, written once and reused.

    Arrow's IPC *file* layout is the in-memory layout with every buffer 8-byte
    aligned, which is what makes the two legs below mean anything: a consumer
    can map it and point at the buffers where they lie. It stands in for what
    a shared-memory handoff would put in the mapping.
    """
    path = os.environ.get("TAXI_IPC_FILE", "/tmp/taxi-column.arrow")
    if os.path.exists(path):
        return path
    files = sorted(
        glob.glob(os.path.join(TABLE, "data", "**", "*.parquet"), recursive=True)
    )
    schema = pa.schema([pq.ParquetFile(files[0]).schema_arrow.field(COLUMN)])
    with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, schema) as writer:
        for parquet in files:
            for batch in pq.ParquetFile(parquet).iter_batches(columns=[COLUMN]):
                writer.write_batch(batch)
    return path


def read_ipc(path: str, mapped: bool) -> int:
    """Sum the column out of an IPC file, mapped or copied into the heap.

    The sum is not decoration. Mapping a file and never touching it measures
    `mmap` rather than a read; folding every value is what a consumer does,
    and is what leaves the copy as the only difference between the two.
    """
    source = pa.memory_map(path, "rb") if mapped else pa.OSFile(path, "rb")
    with source:
        table = pa.ipc.open_file(source).read_all()
        pc.sum(table.column(0)).as_py()
        return table.num_rows


legs = []
if TABLE:
    legs.append(
        ("in-process (pyarrow, no boundary)",
         lambda: _LocalParquet(TABLE, COLUMN))
    )
if LIB and TABLE:
    legs.append(
        (
            "in-process (C Data Interface)",
            lambda: IcebergLocalSource(
                LIB, TABLE, columns=[COLUMN], split_size=SPLIT_SIZE
            ),
        )
    )
for name, env in (("Flight — pyarrow server", "PYARROW_FLIGHT_URI"),
                  ("Flight — flight.mojo server", "MOJO_FLIGHT_URI")):
    uri = os.environ.get(env)
    if uri:
        legs.append((name, lambda uri=uri: FlightSource(uri, dataset="taxi")))

# The producer writing its Arrow buffers straight into a mapping, with no
# encode at all — `iceberg.mojo` scanning, this process pointing at the result.
publisher_bin = os.environ.get("SHM_PUBLISH_BIN")
if publisher_bin and TABLE and os.path.exists(publisher_bin):
    from daft_flight.shm_direct import ShmDirectSource, _Publisher

    _pub = _Publisher(
        publisher_bin,
        TABLE,
        COLUMN,
        os.environ.get("SHM_DIRECT_DIR", "/tmp/ib-shm"),
        SPLIT_SIZE,
        env={"CONDA_PREFIX": os.environ["MAGMALAKE_SHIM_PREFIX"]}
        if os.environ.get("MAGMALAKE_SHIM_PREFIX")
        else None,
    )
    legs.append(
        ("shared memory, Mojo writes the buffers",
         lambda: ShmDirectSource(_pub))
    )

# The same server, answering with the name of a mapping instead of the rows.
# Flight stays the control plane; only DoGet changes.
shm_uri = os.environ.get("SHM_FLIGHT_URI")
if shm_uri:
    legs.append(
        ("shared mapping, named by Flight",
         lambda uri=shm_uri: ShmSource(uri, dataset="taxi"))
    )

if not legs:
    raise SystemExit("nothing to measure: set IB_LIB and TAXI_TABLE, or a *_FLIGHT_URI")

print(f"{COLUMN} of {os.path.basename(TABLE) or 'the served table'},"
      f" p50 of {REPEAT} reads\n")
results = [measure(label, make) for label, make in legs]
rows = {r[2] for r in results}
baseline = results[0][1]
for label, ms, count in results:
    print(f"  {label:32} {ms:9.1f} ms   {ms / baseline:5.1f}x   {count:,} rows")
assert len(rows) == 1, f"the legs disagree about the row count: {rows}"

# The two below answer a different question and are printed apart on purpose.
# Every leg above decodes Parquet; these read a column that is already Arrow,
# so they price the *handover* on its own — what a consumer pays to reach
# buffers another process produced, once nothing has to be decoded. The gap
# between them is a copy of 636 MB, and that copy is the whole of what a
# shared mapping saves.
if TABLE:
    print("\nthe handover alone — one column, already Arrow, no Parquet in it:")
    path = ipc_file()
    for label, mapped in (
        ("memory-mapped (zero copy)", True),
        ("read into the heap (one copy)", False),
    ):
        read_ipc(path, mapped)
        samples = []
        for _ in range(REPEAT):
            start = time.perf_counter()
            read_ipc(path, mapped)
            samples.append((time.perf_counter() - start) * 1000)
        samples.sort()
        print(f"  Arrow IPC, {label:30} {samples[len(samples) // 2]:9.1f} ms")
