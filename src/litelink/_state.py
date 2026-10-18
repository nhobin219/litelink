"""Keyed state committed with `extend`, and its published record (#202).

A caller that decides what to append from facts about earlier appends — a
producer's last sequence number and the offset it landed at — needs those facts
and the rows they describe to commit together, or a crash between two commits
leaves them disagreeing. So the facts go in the buffer, in the append's own
transaction (`Buffer.append`), as opaque `bytes` under a `str` key. litelink
never interprets a value.

This module owns what the two places it lives agree on: the check a caller's
mapping passes before any SQL exists, and the bytes of the file that records
the state as of a published snapshot's last offset. The buffer owns the rows
(`Buffer.state_as_of`), and `publish` owns when a file is written.

**One Parquet file per published commit that carries state, named by a table
property in that commit**, rather than the values inline in the property. Inline
they would be in every `metadata.json`, which every reader of the published
table parses and every commit rewrites — about 9 MB a commit at 4,096 keys of
1.7 KB. A file costs one PUT per publish and is read only by a restore.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from collections.abc import Mapping

# The published table's property naming the current state file, by its path
# relative to the published prefix — the form `LogTable.uri` resolves.
STATE_PROPERTY = "litelink.state"

# The offset the file's state is as of, in the file's own key/value metadata:
# a file read on its own says what it describes.
THROUGH_METADATA = b"litelink.through"

_SCHEMA = pa.schema(
    [pa.field("key", pa.string(), nullable=False), pa.field("value", pa.binary())]
)


def entries(state: Mapping[str, bytes] | None) -> dict[str, bytes]:
    """`state` checked and copied, or empty for None.

    Checked before the append's transaction opens, so a bad entry refuses the
    whole call — rows included — and consumes no offset, as a bad row does.
    """
    if state is None:
        return {}

    checked: dict[str, bytes] = {}
    for key, value in state.items():
        if not isinstance(key, str) or not key:
            msg = f"a state key is a non-empty str, not {key!r}"
            raise TypeError(msg)

        if not isinstance(value, bytes | bytearray | memoryview):
            msg = f"the state value for {key!r} is bytes, not {type(value).__name__}"
            raise TypeError(msg)

        checked[key] = bytes(value)

    return checked


def encode(state: Mapping[str, bytes], through: int) -> bytes:
    """The published record of `state` as of offset `through`."""
    table = pa.table(
        {"key": list(state), "value": list(state.values())}, schema=_SCHEMA
    ).replace_schema_metadata({THROUGH_METADATA: str(through).encode()})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)

    return sink.getvalue().to_pybytes()


def decode(payload: bytes) -> tuple[int, dict[str, bytes]]:
    """`(through, state)` from a record `encode` wrote.

    A record without its offset is refused rather than read as offset 0: it is
    not a file this module wrote, and a restore guessing its age could put
    state behind or ahead of the rows it describes.
    """
    table = pq.read_table(pa.BufferReader(payload))
    raw = (table.schema.metadata or {}).get(THROUGH_METADATA)
    if raw is None:
        msg = "a published state file without its offset: not one litelink wrote"
        raise ValueError(msg)

    keys = table.column("key").to_pylist()
    values = table.column("value").to_pylist()

    return int(raw), dict(zip(keys, values, strict=True))
