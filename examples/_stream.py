"""Shared shape for the example scripts, so writer and reader agree.

An event-capture schema of the kind SPEC §2 describes: when it happened, when
we learned it, a key, and an opaque payload. `ingest_ts` is stamped by the
application, never by the library — §2 is explicit that a library cannot know
which of several defensible "ingest times" a caller meant.
"""

from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Iterator

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("ingest_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("payload", pa.large_binary()),
    ]
)

# §7: predicates prune only on a LEADING sort column, so this declares that
# time-bounded reads are the cheap ones and per-key scans are not.
SORT_BY = ("event_ts", "key")

NAME = "sensors"
KEYS = [f"sensor-{i:02d}" for i in range(24)]


def observations(
    payload_bytes: int = 400, seed: int | None = None
) -> Iterator[dict[str, object]]:
    """An endless stream of plausible rows."""
    rng = random.Random(seed)
    while True:
        now = time.time_ns()
        yield {
            "event_ts": now,
            "ingest_ts": now,
            "key": rng.choice(KEYS),
            "payload": rng.randbytes(payload_bytes),
        }
