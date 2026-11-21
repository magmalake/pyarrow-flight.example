# The server

The examples need something to talk to. Two options.

**A Mojo server**, which is the point of the exercise:

```sh
git clone https://github.com/magmalake/flight.mojo ../flight.mojo
cd ../flight.mojo && pixi run serve   # 127.0.0.1:8815
```

`flight.mojo` also has `serve-iceberg`, which serves a real Iceberg table and
advertises one endpoint per scan split — that is the one worth pointing
`02_fan_out.py` at. `pixi run check-mojo` here does all of it: builds that
server from `../flight.mojo`, serves a small generated table, runs every
example against it and compares what it served with pyarrow's own read.

**A Python server** (`python server/reference_server.py`), which exists so the
client examples can be checked against an implementation nobody here wrote. If
an example passes against the reference and fails against the Mojo server, the
Mojo server is wrong — that is the whole reason to keep it.
