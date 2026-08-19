"""Offset predicates for the Iceberg maintenance passes.

Isolated in their own module so the `ty` suppression in pyproject.toml covers
these five lines rather than the whole of log.py. pyiceberg's
`LiteralPredicate.__init__` accepts `(term, literal)` at runtime — verified —
but ty resolves a different `__init__` through the expression hierarchy and
reports every construction as missing arguments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression


def offset_between(lo: int, hi: int) -> BooleanExpression:
    """`lo <= offset <= hi` — the compaction unit (§6)."""
    return And(
        GreaterThanOrEqual("litelink_offset", lo),
        LessThanOrEqual("litelink_offset", hi),
    )


def offset_at_or_below(hi: int) -> BooleanExpression:
    """`offset <= hi` — the eviction prefix (§8)."""
    return LessThanOrEqual("litelink_offset", hi)
