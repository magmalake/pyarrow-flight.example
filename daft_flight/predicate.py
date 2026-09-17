"""Daft's pushdown predicate, translated into Iceberg's filter DSL.

`source.py` explains why a Flight ticket can carry no predicate: it is opaque
bytes, and there is no field for one. In this process there is no ticket to be
opaque about -- the reader is a function call away -- so the predicate can go
where it is worth something. That is the whole difference between the two
sources, and it is this file.

Iceberg's filter DSL is a JSON s-expression, the same grammar
`TableScan.filter` takes:

    ["and", ["gt", "trip_distance", 10], ["eq", "payment_type", 2]]

Daft hands predicates over as an expression tree and offers `PredicateVisitor`
to walk it, so the translation is a visitor whose result is that JSON. What
each end calls an operator differs by a few letters; what is interesting is
what happens when they do not correspond at all.

## Anything untranslatable becomes `None`

A predicate Iceberg has no vocabulary for -- `abs(x) > 2`, a function, a cast,
a comparison between two columns -- returns `None` and is simply not pushed.
Daft applies the filter to what comes back regardless, so the answer does not
depend on how much was pushed; only the bytes decoded do.

That makes a **conjunction partial**: in `A and B` with only `A`
translatable, `A` alone is pushed. This is safe in one direction and not the
other, and the direction matters -- what is pushed must be *implied* by what
Daft will apply, never stricter than it. `A` is implied by `A and B`, so
pruning on `A` cannot drop a row that satisfies both. A disjunction has no
such property: half of `A or B` is not implied by the whole, so an `or` is
pushed entire or not at all.

Iceberg's `not` is a bitmap flip rather than three-valued logic, so a null row
survives `not (a > 1)` where SQL would drop it. That is a superset, which is
the safe side of the same rule.
"""

from __future__ import annotations

import datetime
import json
from typing import TYPE_CHECKING, Any, NamedTuple

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from daft import DataType
    from daft.expressions import Expression


class _Col(NamedTuple):
    """A bare column reference, as an operand rather than a predicate."""

    name: str


class _Lit(NamedTuple):
    """A literal, already in the JSON form the DSL wants."""

    value: Any


def to_iceberg_filter(predicate: Expression | None) -> str:
    """The DSL for as much of `predicate` as Iceberg can express.

    An empty string means nothing was pushed, which is what the library takes
    for "no predicate" -- so a caller can hand the result straight on without
    a branch.
    """
    if predicate is None:
        return ""
    term = _ToFilter().visit(predicate)
    if not isinstance(term, list):
        return ""
    return json.dumps(term)


def _literal(value: Any) -> _Lit | None:
    """JSON for one literal, or `None` if the DSL cannot take its type.

    `bind` widens what it is given against the column: an int literal on a
    `long`, a string on a `date` or `timestamp` read as ISO-8601. So a date
    goes over as its ISO form and is typed at the far end by the column it is
    compared against, which is the only place the type is actually known.
    """
    if isinstance(value, bool) or isinstance(value, (int, float, str)):
        return _Lit(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return _Lit(value.isoformat())
    # Decimals, binary, durations, lists, structs, None: all representable in
    # a Daft literal and none of them unambiguous here.
    return None


def _compare(op: str, left: Any, right: Any) -> list | None:
    """One comparison, with the column on the left as the DSL expects."""
    flipped = {"lt": "gt", "gt": "lt", "lteq": "gteq", "gteq": "lteq"}
    if isinstance(left, _Col) and isinstance(right, _Lit):
        return [op, left.name, right.value]
    if isinstance(left, _Lit) and isinstance(right, _Col):
        return [flipped.get(op, op), right.name, left.value]
    # Two columns, two literals, or an operand that did not translate.
    return None


class _ToFilter(PredicateVisitor):
    """Daft's expression tree as Iceberg's filter DSL, or `None`.

    Every method returns one of four things: a `list` (a predicate term), a
    `_Col`, a `_Lit`, or `None` for "this cannot be pushed". The operand types
    exist so that a comparison can tell a column from a literal without
    reaching back into Daft's tree.
    """

    # ── operands ──────────────────────────────────────────────────────────
    def visit_col(self, name: str) -> _Col:
        return _Col(name)

    def visit_lit(self, value: Any) -> _Lit | None:
        return _literal(value)

    def visit_alias(self, expr: Expression, alias: str) -> Any:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> None:
        # A cast changes what a comparison means and Iceberg's binder does its
        # own widening against the column. Pushing one would be guessing which
        # of the two conversions the user asked for.
        return None

    def visit_try_cast(self, expr: Expression, dtype: DataType) -> None:
        return None

    def visit_function(self, name: str, args: list[Expression]) -> None:
        return None

    def visit_coalesce(self, args: list[Expression]) -> None:
        return None

    # ── connectives ───────────────────────────────────────────────────────
    def visit_and(self, left: Expression, right: Expression) -> list | None:
        a = self.visit(left)
        b = self.visit(right)
        a = a if isinstance(a, list) else None
        b = b if isinstance(b, list) else None
        if a is not None and b is not None:
            return ["and", a, b]
        # Half of a conjunction is implied by the whole, so pushing it alone
        # prunes without dropping a row the filter would have kept.
        return a if a is not None else b

    def visit_or(self, left: Expression, right: Expression) -> list | None:
        a = self.visit(left)
        b = self.visit(right)
        if isinstance(a, list) and isinstance(b, list):
            return ["or", a, b]
        return None

    def visit_not(self, expr: Expression) -> list | None:
        inner = self.visit(expr)
        return ["not", inner] if isinstance(inner, list) else None

    # ── comparisons ───────────────────────────────────────────────────────
    def visit_equal(self, left: Expression, right: Expression) -> list | None:
        return _compare("eq", self.visit(left), self.visit(right))

    def visit_not_equal(self, left: Expression, right: Expression) -> list | None:
        return _compare("ne", self.visit(left), self.visit(right))

    def visit_less_than(self, left: Expression, right: Expression) -> list | None:
        return _compare("lt", self.visit(left), self.visit(right))

    def visit_less_than_or_equal(
        self, left: Expression, right: Expression
    ) -> list | None:
        return _compare("lteq", self.visit(left), self.visit(right))

    def visit_greater_than(self, left: Expression, right: Expression) -> list | None:
        return _compare("gt", self.visit(left), self.visit(right))

    def visit_greater_than_or_equal(
        self, left: Expression, right: Expression
    ) -> list | None:
        return _compare("gteq", self.visit(left), self.visit(right))

    def visit_between(
        self, expr: Expression, lower: Expression, upper: Expression
    ) -> list | None:
        # The DSL has no `between`, and two bounds are what it means.
        lo = _compare("gteq", self.visit(expr), self.visit(lower))
        hi = _compare("lteq", self.visit(expr), self.visit(upper))
        if lo is None or hi is None:
            return None
        return ["and", lo, hi]

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> list | None:
        column = self.visit(expr)
        if not isinstance(column, _Col):
            return None
        values = []
        for item in items:
            literal = self.visit(item)
            if not isinstance(literal, _Lit):
                return None
            values.append(literal.value)
        return ["in", column.name, values] if values else None

    def visit_is_null(self, expr: Expression) -> list | None:
        column = self.visit(expr)
        return ["is-null", column.name] if isinstance(column, _Col) else None

    def visit_not_null(self, expr: Expression) -> list | None:
        column = self.visit(expr)
        return ["not-null", column.name] if isinstance(column, _Col) else None
