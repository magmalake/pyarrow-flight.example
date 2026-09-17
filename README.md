# pyarrow-flight.example

Reading a Mojo [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
server from Python, with stock `pyarrow` and no Mojo on the client side.

Companion code for **Arrow Flight as an interop seam** on
[magmalake.org](https://magmalake.org), and the practical answer to "how do I
actually get at this from Python".

```sh
pixi run check              # every example, against a reference server
```

Nothing in `client/` knows the server is written in Mojo. That is the point:
Flight is a wire protocol, so a fast reader in a new language becomes something
an existing tool can call.

## The shape of it

Four calls matter. `GetFlightInfo` asks what a dataset looks like and where to
get it; `DoGet` fetches one piece.

```python
import pyarrow.flight as fl

client = fl.connect("grpc://127.0.0.1:8815")
info = client.get_flight_info(fl.FlightDescriptor.for_path("taxi"))
table = client.do_get(info.endpoints[0].ticket).read_all()
```

`info` carries three things worth understanding:

| | |
|---|---|
| `schema` | The Arrow schema, before any data moves — so a client can plan, or refuse. |
| `endpoints` | **One unit of work each.** Disjoint: fetch them in parallel and concatenate. |
| `total_records` | Rows, or `-1` for unknown. A server that knows from metadata should say. |

A **ticket** is opaque bytes. The server decides what one means and a client
never interprets it — ours encodes a snapshot, a byte range and a file path,
but that is nobody else's business.

## Endpoints are the interesting part

A server that returns several endpoints is telling you the work divides. Ours
returns one per Iceberg scan task, so a large data file split into four tasks
becomes four endpoints — see `client/02_fan_out.py`:

```python
with ThreadPoolExecutor(max_workers=len(info.endpoints)) as pool:
    parts = list(pool.map(fetch, info.endpoints))
table = pa.concat_tables(parts)
assert table.num_rows == info.total_records
```

That assertion is the contract: the union of the endpoints is the table, with
nothing repeated and nothing lost. Worth asserting in your own code too — if it
ever fails, a client that fans out silently gets a wrong answer.

Each endpoint may carry **locations**. Empty means "fetch from the server you
asked", which is what a single-process server means. A distributed one fills
them in — and the client loop above does not change, which is the property that
makes the same code work against both.

## Handing the result onward

What arrives is Arrow, so passing it on is usually free (`client/03_hand_off.py`):

```python
pl.from_arrow(arrivals)                      # polars: zero copy, Arrow underneath
duckdb.sql("SELECT … FROM arrivals")          # duckdb: queries it in place
arrivals.to_pandas()                          # pandas: a real conversion, this one costs
```

DuckDB picks the name up from the enclosing scope by replacement scan, so the
variable must not be a SQL keyword — which is why it is not called `table`.

## Things that will bite you

Each of these cost us real time.

**`import pyarrow.flight` fails with "not built with support for 'flight'".**
The conda build splits Flight into a separate library. Install
**`libarrow-flight`** alongside `pyarrow`; the error reads like a missing
feature rather than a missing package.

**There is no `FlightUnimplementedError`.** pyarrow maps gRPC's UNIMPLEMENTED
onto `ArrowNotImplementedError`, which is not in the `flight` namespace at all.
The `Flight*Error` classes cover fewer cases than gRPC's status list, so a
catch-all is the honest way to handle server errors — see `client/04_errors.py`.

**A schema in `FlightInfo` is an encapsulated IPC message**, not a bare
flatbuffer: the continuation marker and length prefix are part of it. This one
is for anyone *writing* a server — send a bare one and every client reports
"Invalid flatbuffers message", because it reads the first four bytes as a
length.

**gRPC clients advertise gzip.** If a server negotiates it and then fails to
compress, the connection is torn down after the response headers and the client
reports UNAVAILABLE — which looks like a network fault and is not one.

## Running against the Mojo server

`pixi run check` uses a Python reference server so the examples can be verified
without a Mojo toolchain — and so that an example failing against the Mojo
server can be told apart from an example that was wrong to begin with.

For the real thing, see [`server/README.md`](server/README.md):
[`flight.mojo`](https://github.com/magmalake/flight.mojo)'s `serve` task hosts a
fixed table, and `serve-iceberg` hosts a real Iceberg table with one endpoint
per scan split — that second one is what makes `02_fan_out.py` interesting.

## Daft

`daft_flight/` is a Daft `DataSource`: `GetFlightInfo` becomes Daft's task
list, `DoGet` becomes a task's batches, and Daft keeps its scheduler, its
predicates and its aggregations.

```python
from daft_flight import FlightSource

df = FlightSource("grpc://127.0.0.1:8815", dataset="taxi").read()
df.where(df["ok"]).groupby("region").agg(daft.col("id").count()).show()
```

One endpoint becomes one task, so a plan spread over worker processes is read
from those workers -- `client/05_daft_scan.py` against a coordinator reports
`6 endpoints, 6 placed` and the rows arrive from two processes.

Over Flight, only the **limit** is pushed down, because it is the only pushdown
Flight can express: a ticket is opaque bytes whose meaning is the server's
business, and there is no field in which to send a predicate. Filters and
projection are applied by Daft after the read -- correct, but the bytes still
crossed the wire. That trade, a protocol everyone speaks against pushdowns
nobody speaks, is the honest cost of an interop seam. The in-process source
below is where that cost goes away.

**`daft` on conda-forge is a different project** -- a library for drawing
probabilistic graphical models. The DataFrame is PyPI-only, which is why it is
a `[pypi-dependencies]` entry here.

### The in-process alternative

`IcebergLocalSource` is the same source reading through a Mojo shared library
in this process (`iceberg.mojo`'s `carrow-scan-lib`), handing Daft its Arrow
buffers over the C Data Interface with nothing encoded, sent or decoded. Same
plan, same ticket format, same DataFrame; only how a task reads differs. It
needs that library built, so `pixi run check` does not cover it.

Prefer it whenever it is available: in one process Flight's encode, socket and
decode are overhead against a function call.

The cost worth knowing before choosing: a fault in a library you `dlopen` is a
fault in *this* process, with no retry boundary. Flight has one; this does not.

#### It pushes down what Flight cannot

There is no ticket here to be opaque about, so Daft's pushdowns reach the
reader. The projection goes to the Parquet decoder, and the **predicate** goes
to Iceberg -- as its filter DSL, translated by `daft_flight/predicate.py`, a
JSON s-expression:

```python
col("tpep_pickup_datetime") >= lit(datetime(2024, 7, 1))
# ["gteq", "tpep_pickup_datetime", "2024-07-01T00:00:00"]
```

A predicate is worth pushing twice over. At **plan** time Iceberg prunes on
partition values and file statistics, so the task list itself comes back
shorter -- on the 79.5M-row taxi table, partitioned by month, a date range
turns 24 splits into 3. At **read** time what is left becomes the residual,
which drops row groups and pages on the Parquet footer before anything is
decompressed. A reader that filters afterwards gets neither.

Daft applies the filter to what comes back regardless, so pushing changes what
is decoded and never what is returned. That is what makes a *partial* push
safe: in `A and B` with only `A` translatable, `A` alone goes down, because `A`
is implied by `A and B` and so cannot prune a row the filter would have kept.
An `or` has no such property and is pushed whole or not at all. Anything
Iceberg has no vocabulary for -- a function, a cast, a comparison between two
columns -- is simply not pushed. `client/06_pushdown.py` is the translation on
its own, and needs neither a server nor Mojo:

```sh
pixi run example-pushdown
```

Measured on that table, same answers on both sides: a quarter of 2024 summed,
225 ms -> 58 ms; a selective three-predicate count, 243 ms -> 168 ms. A
predicate that prunes nothing (`trip_distance > 0`, true of nearly every row)
costs about 10% -- the residual is evaluated and nothing is dropped.

## What is not here

`DoPut`, `DoExchange` and `DoAction` — Flight can write and can run
server-defined verbs. The Mojo server implements the read path
(`GetFlightInfo`, `GetSchema`, `DoGet`) and answers UNIMPLEMENTED to the rest,
which is a legal response a client is expected to tolerate.
