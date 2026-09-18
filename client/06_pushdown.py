"""What the in-process source pushes down, and what it will not.

`FlightSource` can push only a limit: a ticket is opaque bytes and there is no
field for a predicate. `IcebergLocalSource` is calling a library rather than a
server, so it can push the projection and the predicate too -- and the
predicate is the one that changes how much work there is, because Iceberg
prunes manifests and files with it before a single task exists.

This example needs neither a server nor Mojo: it exercises the translation on
its own, which is where the interesting decisions are. `daft_flight/local.py`
is what hands the result to the library.

    pixi run example-pushdown
"""

import datetime
import sys

sys.path.insert(0, ".")

from daft import col, lit

from daft_flight.predicate import to_iceberg_filter

# What Daft's predicate becomes. The DSL is a JSON s-expression, the grammar
# `iceberg.mojo`'s `TableScan.filter` takes.
TRANSLATED = [
    (col("trip_distance") > 10, '["gt", "trip_distance", 10]'),
    (col("payment_type") == 2, '["eq", "payment_type", 2]'),
    (col("PULocationID").is_in([132, 138]), '["in", "PULocationID", [132, 138]]'),
    (col("airport_fee").is_null(), '["is-null", "airport_fee"]'),
    (
        (col("trip_distance") > 10) & (col("payment_type") == 2),
        '["and", ["gt", "trip_distance", 10], ["eq", "payment_type", 2]]',
    ),
    # A literal is typed at the far end, against the column it is compared
    # with -- so a datetime travels as ISO-8601 and becomes a timestamp there.
    # This is the pushdown that pays: the table is partitioned by month, so a
    # date range drops whole files at plan time.
    (
        col("tpep_pickup_datetime") >= lit(datetime.datetime(2024, 7, 1)),
        '["gteq", "tpep_pickup_datetime", "2024-07-01T00:00:00"]',
    ),
]

# What is not pushed. Each of these is correct in the DataFrame -- Daft applies
# the filter to what comes back either way. All that is lost is the pruning.
NOT_TRANSLATED = [
    col("trip_distance").abs() > 100,  # a function Iceberg has no name for
    col("trip_distance") > col("tip_amount"),  # two columns, not a literal
    col("VendorID").cast("string") == "2",  # a cast changes the comparison
]

for predicate, expected in TRANSLATED:
    actual = to_iceberg_filter(predicate)
    print(f"{str(predicate):52} -> {actual}")
    assert actual == expected, f"expected {expected}, got {actual}"

print()
for predicate in NOT_TRANSLATED:
    actual = to_iceberg_filter(predicate)
    print(f"{str(predicate):52} -> (not pushed)")
    assert actual == "", f"expected nothing to be pushed, got {actual}"

# A conjunction is pushed as far as it translates. Half of an `and` is implied
# by the whole, so pruning on it cannot drop a row the filter would keep --
# and Daft applies the real predicate afterwards regardless.
partial = to_iceberg_filter((col("trip_distance") > 10) & (col("tip_amount").abs() > 1))
print(f"\npartially pushed: {partial}")
assert partial == '["gt", "trip_distance", 10]'

# A disjunction has no such property: half of an `or` is weaker than the whole,
# so it is pushed entire or not at all.
disjunction = to_iceberg_filter((col("trip_distance") > 10) | (col("tip_amount").abs() > 1))
print(f"not pushed at all: {disjunction or '(none)'}")
assert disjunction == ""

print("\npushdown translation ok")
