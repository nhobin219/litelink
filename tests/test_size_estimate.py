"""The appender's size estimate against Arrow's own count (#84).

`target_seal_size` and every `extent.bytes` are stated in the bytes of the Arrow
table a seal builds, and compaction sizes merges by them. The estimate must
never be below that — an undercount seals files larger than the target — and
should stay close, so the bar is `1.0 <= estimate / nbytes <= 1.25`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink
from tests.test_types import OTEL, otel_row

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

Row = dict[str, object]

CASES: list[tuple[str, pa.Schema, Callable[[int], Row]]] = [
    (
        "wide 64-bit",
        pa.schema([("a", pa.int64()), ("b", pa.int64()), ("c", pa.float64())]),
        lambda i: {"a": i, "b": i, "c": 0.5},
    ),
    (
        "short strings",
        pa.schema([(f"s{k}", pa.string()) for k in range(4)]),
        lambda i: {f"s{k}": f"v{i % 1000}" for k in range(4)},
    ),
    ("large_string", pa.schema([("s", pa.large_string())]), lambda i: {"s": "abc"}),
    ("unicode", pa.schema([("s", pa.string())]), lambda i: {"s": "☃é" * 5}),
    (
        "narrow",
        pa.schema([(f"i{k}", pa.int32()) for k in range(4)] + [("b", pa.bool_())]),
        lambda i: {**{f"i{k}": i for k in range(4)}, "b": True},
    ),
    ("all bools", pa.schema([(f"b{k}", pa.bool_()) for k in range(8)]), lambda i: {}),
    (
        "sparse",
        pa.schema([(f"n{k}", pa.int64()) for k in range(8)]),
        lambda i: {"n0": i},
    ),
    (
        "bytes",
        pa.schema([("b", pa.binary()), ("f", pa.binary(16))]),
        lambda i: {"b": bytes(20), "f": bytes(16)},
    ),
    (
        "list<int64>",
        pa.schema([("c", pa.list_(pa.int64()))]),
        lambda i: {"c": list(range(10))},
    ),
    (
        "map<string, int64>",
        pa.schema([("c", pa.map_(pa.string(), pa.int64()))]),
        lambda i: {"c": {"a": 1, "b": 2, "c": 3}},
    ),
    (
        "list<string>",
        pa.schema([("c", pa.list_(pa.string()))]),
        lambda i: {"c": list("abcde")},
    ),
    (
        "null structs and lists",
        pa.schema(
            [
                (
                    "c",
                    pa.struct(
                        [
                            pa.field("a", pa.int64()),
                            pa.field("l", pa.list_(pa.string())),
                        ]
                    ),
                ),
                ("d", pa.list_(pa.int64())),
            ]
        ),
        lambda i: {"c": None if i % 2 else {"a": 1, "l": ["x", None]}},
    ),
    ("OTel log record", OTEL, otel_row),
]


@pytest.mark.parametrize(
    ("label", "schema", "make"), CASES, ids=[label for label, _, _ in CASES]
)
def test_the_estimate_is_never_below_arrow_and_close_to_it(
    tmp_path: Path, label: str, schema: pa.Schema, make: Callable[[int], Row]
) -> None:
    """Each case exercises a term the old estimate got wrong.

    Nulls still take a slot, a string or list carries an offset (8 bytes for
    `large_string`), an int32 is 4 bytes and a bool one bit, and a nested value
    is measured as Arrow holds it rather than as its JSON — which undercounted
    `list<int64>` threefold.

    Falsify by counting a null as nothing (sparse falls to 0.33), dropping the
    offsets (short strings fall to 0.60), or measuring nested values by their
    JSON (`list<int64>` falls to 0.32).
    """
    with litelink.new(tmp_path, "s", schema=schema) as log:
        log.extend([make(i) for i in range(1, 2001)])

        (estimate,) = log._buffer._con.execute(
            "SELECT bytes FROM extent WHERE end_offset IS NULL"
        ).fetchone()
        actual = log._buffer.rows_from(None).nbytes

        assert 1.0 <= estimate / actual <= 1.25, f"{estimate} for {actual}"


@pytest.mark.parametrize(
    ("label", "schema", "make"), CASES, ids=[label for label, _, _ in CASES]
)
def test_the_recount_on_open_agrees_with_the_appender(
    tmp_path: Path, label: str, schema: pa.Schema, make: Callable[[int], Row]
) -> None:
    """A reopened log measures its open group the way the appender did.

    The recount runs when an open log has no open group — one older than the
    group table, or one whose sealer closed it just before dying — and seeds
    the group every later append adds to. It used to be a SQL sum over the
    stored text, which measured a nested value by its JSON.
    """
    with litelink.new(tmp_path, "s", schema=schema) as log:
        log.extend([make(i) for i in range(1, 301)])

        (running,) = log._buffer._con.execute(
            "SELECT bytes FROM extent WHERE end_offset IS NULL"
        ).fetchone()

        assert log._buffer._measure_from(0) == running
