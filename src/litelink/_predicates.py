"""Offset predicates for the Iceberg maintenance passes.

Isolated in their own module so the `ty` suppression in pyproject.toml covers
these five lines rather than the whole of log.py. pyiceberg's
`LiteralPredicate.__init__` accepts `(term, literal)` at runtime — verified —
but ty resolves a different `__init__` through the expression hierarchy and
reports every construction as missing arguments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression


def offset_in(start: int, end: int) -> BooleanExpression:
    """`start <= offset < end` — a compaction or re-cut unit (§6)."""
    return And(
        GreaterThanOrEqual("litelink_offset", start),
        LessThan("litelink_offset", end),
    )


def offset_below(end: int) -> BooleanExpression:
    """`offset < end` — the eviction prefix (§8)."""
    return LessThan("litelink_offset", end)
