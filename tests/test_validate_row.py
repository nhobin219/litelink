"""`litelink.validate_row`: the answer `append` would give, without a log (#77)."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

SCHEMA = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("f64", pa.float64(), nullable=False),
        pa.field("i32", pa.int32()),
        pa.field("f32", pa.float32()),
        pa.field("b", pa.bool_()),
        pa.field("s", pa.string()),
        pa.field("tid", pa.binary(16)),
        pa.field(
            "attrs", pa.map_(pa.string(), pa.struct([pa.field("x", pa.binary())]))
        ),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("vals", pa.list_(pa.float64())),
    ]
)


class Side(StrEnum):
    BUY = "buy"


BASE: dict[str, object] = {"k": 1, "f64": 1.0}

# Accepted and refused rows alike, each chosen because it reaches a different
# part of the decision: the Python checks before the insert, a CHECK, the
# explainer's branches, the driver, and the NaN test after the insert.
ROWS: list[tuple[str, dict[str, object]]] = [
    ("minimal", BASE),
    ("every column", {**BASE, "i32": 7, "f32": 1.5, "b": False, "s": "x"}),
    ("non-nullable absent", {"f64": 1.0}),
    ("non-nullable None", {"k": None, "f64": 1.0}),
    ("empty row", {}),
    ("unknown column", {**BASE, "c": 2}),
    ("misspelled column", {"ky": 1, "f64": 1.0}),
    ("offset supplied", {**BASE, "litelink_offset": 9}),
    ("str into int64", {**BASE, "k": "1"}),
    ("float into int64", {**BASE, "k": 1.5}),
    ("bool into int64", {**BASE, "k": True}),
    ("int64 overflow", {**BASE, "k": 2**63}),
    ("int32 overflow", {**BASE, "i32": 2**31}),
    ("float32 overflow", {**BASE, "f32": 1e39}),
    ("float32 inf", {**BASE, "f32": float("inf")}),
    ("float32 -inf", {**BASE, "f32": float("-inf")}),
    ("float64 inf, non-nullable", {**BASE, "f64": float("inf")}),
    ("int into float64", {**BASE, "f64": 5}),
    ("inexact int into float64", {**BASE, "f64": 2**53 + 1}),
    ("NaN, nullable", {**BASE, "f32": float("nan")}),
    ("NaN, non-nullable", {**BASE, "f64": float("nan")}),
    ("7 into bool", {**BASE, "b": 7}),
    ("1 into bool", {**BASE, "b": 1}),
    ("int into string", {**BASE, "s": 12345}),
    ("StrEnum into string", {**BASE, "s": Side.BUY}),
    ("trace id", {**BASE, "tid": bytes(16)}),
    ("short trace id", {**BASE, "tid": bytes(15)}),
    ("nested", {**BASE, "attrs": {"k": {"x": b"\x00"}}, "tags": ["a"]}),
    ("unknown struct key", {**BASE, "attrs": {"k": {"x": b"", "y": 1}}}),
    ("wrong nested leaf", {**BASE, "attrs": {"k": {"x": "text"}}}),
    ("wrong list item", {**BASE, "tags": ["a", 1]}),
    ("nested fine, other column bad", {**BASE, "attrs": {}, "i32": 2**31}),
    ("NaN in a nested list", {**BASE, "vals": [1.0, float("nan")]}),
    ("inf in a nested list", {**BASE, "vals": [float("inf")]}),
    ("-inf in a nested list", {**BASE, "vals": [float("-inf")]}),
]


def _outcome(call: Callable[[], object]) -> tuple[type[BaseException], str] | None:
    try:
        call()
    except Exception as exc:  # noqa: BLE001 — the point is to compare any of them
        return type(exc), str(exc)

    return None


@pytest.mark.parametrize(("label", "row"), ROWS, ids=[label for label, _ in ROWS])
def test_validate_row_answers_exactly_as_append_does(
    tmp_path: Path, label: str, row: dict[str, object]
) -> None:
    """Same accept, same refusal, same exception type, same message.

    The whole promise of #77: a stream accepts exactly the same rows whether or
    not it has a log. Compared against a real `append` rather than against
    expected strings, so a change to either side that the other does not
    share fails here.

    Falsify by deleting the `_row_bytes` call from `RowProbe.check` (the NaN
    rows diverge), the unknown-column test (the unknown and misspelled rows
    diverge), or the nested encode (the three bad nested rows diverge).
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA) as log:
        appended = _outcome(lambda: log.append(row))
        validated = _outcome(lambda: litelink.validate_row(SCHEMA, row))

    assert validated == appended


@pytest.mark.parametrize(
    "schema",
    [
        pa.schema([pa.field("litelink_offset", pa.int64())]),
        pa.schema([pa.field("n", pa.uint32())]),
    ],
    ids=["reserved column", "uncarried type"],
)
def test_a_schema_new_refuses_is_refused_the_same_way(
    tmp_path: Path, schema: pa.Schema
) -> None:
    """No row could ever be appended under it, so no row validates against it."""
    created = _outcome(lambda: litelink.new(tmp_path, "s", schema=schema))
    validated = _outcome(lambda: litelink.validate_row(schema, {}))

    assert created is not None
    assert validated == created
