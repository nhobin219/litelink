"""Shared shape for the example scripts, so writer and reader agree.

An ADS-B feed: aircraft broadcasting their position, arriving over a websocket
as tabular JSON faster than anyone wants to think about, and needing to be
durable the moment it arrives and queryable a moment later.

The frame is parsed into columns rather than stored whole. That is the point of
declaring a schema — every field prunes from Iceberg statistics, so a query for
one aircraft over one minute never reads the rest.

`ingest_ts` is stamped by the application, never by the library. §2 is explicit
that a library cannot know which of several defensible "ingest times" a caller
meant, and a position feed is exactly where it matters: the moment the aircraft
transmitted, the moment the receiver decoded it, and the moment it was committed
are three different numbers, and only the application knows which its analytics
mean. Keeping both is what makes a point-in-time read possible — `event_ts` for
"where was it", `ingest_ts` for "what did we know, and when".
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
        pa.field("icao24", pa.string()),
        pa.field("callsign", pa.string()),
        pa.field("altitude_ft", pa.int64()),
        pa.field("speed_kt", pa.float64()),
        pa.field("heading_deg", pa.float64()),
    ]
)

# §7: predicates prune only on a LEADING sort column, so this declares that
# time-bounded reads are the cheap ones and per-aircraft scans are not. A feed
# mostly asked "everything in this minute" wants exactly this; one mostly asked
# "this tail number, all day" wants ("icao24", "event_ts") instead — and
# changing it later rewrites every file.
SORT_BY = ("event_ts", "icao24")

NAME = "positions"

# Real ICAO 24-bit addresses are hex; these are made up.
AIRCRAFT = [(f"{0xA00000 + i * 7919:06x}", f"UAL{100 + i}") for i in range(48)]


def observations(seed: int | None = None) -> Iterator[dict[str, object]]:
    """An endless stream of plausible position reports."""
    rng = random.Random(seed)
    state = {
        icao: (
            rng.randrange(28_000, 41_000),
            rng.uniform(380, 520),
            rng.uniform(0, 360),
        )
        for icao, _ in AIRCRAFT
    }

    while True:
        icao, callsign = rng.choice(AIRCRAFT)
        altitude, speed, heading = state[icao]
        altitude = max(1_000, altitude + rng.choice((-100, 0, 0, 100)))
        speed = max(120.0, speed + rng.gauss(0, 1.5))
        heading = (heading + rng.gauss(0, 0.4)) % 360
        state[icao] = (altitude, speed, heading)

        yield {
            # Transmitted a moment before we saw it, as a real feed is.
            "event_ts": time.time_ns() - rng.randrange(0, 900_000_000),
            "ingest_ts": time.time_ns(),
            "icao24": icao,
            "callsign": callsign,
            "altitude_ft": altitude,
            "speed_kt": round(speed, 1),
            "heading_deg": round(heading, 1),
        }
