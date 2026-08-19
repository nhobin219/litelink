"""Shared rig for the performance benchmarks.

Internal: these exist to answer "did that change cost us anything", not to show
anyone how to use the library. `examples/` is for that.
"""

from __future__ import annotations

import random
import shutil
import time
from datetime import timedelta
from typing import TYPE_CHECKING, TypeVar

import pyarrow as pa

from litelink import Log, LogConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

T = TypeVar("T")

NAME = "bench"

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("ingest_ts", pa.int64(), nullable=False),
        pa.field("icao24", pa.string()),
        pa.field("altitude_ft", pa.int64()),
        pa.field("speed_kt", pa.float64()),
        # Sized by --payload, and present only to vary the row width these
        # benchmarks measure throughput against. A real schema would not carry
        # it; §3's numbers are stated per row, so the width has to be a knob.
        pa.field("note", pa.string()),
    ]
)
SORT_BY = ("event_ts", "icao24")

# The column list the raw-SQLite comparison has to mirror exactly, or it is not
# measuring the same insert.
COLUMNS = ("event_ts", "ingest_ts", "icao24", "altitude_ft", "speed_kt", "note")

# Large enough that nothing seals underneath a measurement by accident.
NEVER_SEAL = LogConfig(target_size=1 << 40, snapshot_retention=timedelta(days=1))


def observations(
    payload_bytes: int = 400, seed: int = 0
) -> Iterator[dict[str, object]]:
    rng = random.Random(seed)
    aircraft = [f"{0xA00000 + i * 7919:06x}" for i in range(48)]
    while True:
        now = time.time_ns()
        yield {
            "event_ts": now,
            "ingest_ts": now,
            "icao24": rng.choice(aircraft),
            "altitude_ft": rng.randrange(1_000, 41_000),
            "speed_kt": round(rng.uniform(120, 520), 1),
            "note": f"{rng.getrandbits(4 * payload_bytes):0{payload_bytes}x}",
        }


def best_of(runs: int, fn: Callable[[], object]) -> float:
    """Minimum of `runs` timings.

    Minimum rather than mean: the noise here is all additive (scheduling, page
    cache misses), so the fastest run is the closest estimate of the real cost.
    """
    return min(_once(fn) for _ in range(runs))


def _once(fn: Callable[[], object]) -> float:
    started = time.perf_counter()
    fn()

    return time.perf_counter() - started


def best_of_setup(
    runs: int, setup: Callable[[], T], work: Callable[[T], object]
) -> float:
    """Like `best_of`, but with per-run setup excluded from the timing.

    Necessary for anything comparing against a floor: creating a Log builds an
    Iceberg catalog and table, and charging that to write throughput made
    litelink look 100% slower than raw SQLite when the real gap is single
    digits.
    """
    timings = []
    for _ in range(runs):
        subject = setup()
        started = time.perf_counter()
        work(subject)
        timings.append(time.perf_counter() - started)

    return min(timings)


def fresh_log(root: Path, config: LogConfig | None = None) -> Log:
    shutil.rmtree(root, ignore_errors=True)

    return Log.new(
        root, NAME, schema=SCHEMA, sort_by=SORT_BY, config=config or NEVER_SEAL
    )
