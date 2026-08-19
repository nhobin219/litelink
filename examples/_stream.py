"""Shared shape for the example scripts, so writer and reader agree.

A market-data feed of the kind litelink is for: a websocket delivering tabular
JSON faster than anyone wants to think about, which has to be durable the
moment it arrives and queryable a moment later.

`ingest_ts` is stamped by the application, never by the library — §2 is
explicit that a library cannot know which of several defensible "ingest times"
a caller meant, and a market feed is exactly where that matters: the exchange
timestamp, the moment the frame arrived, and the moment it was committed are
three different numbers and only the application knows which one its analytics
mean.
"""

from __future__ import annotations

import json
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
        pa.field("symbol", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("size", pa.int64()),
        # The raw frame, kept verbatim. §9: nothing is ever lost to a schema
        # mistake, because a column can be re-promoted out of this at any time.
        pa.field("payload", pa.string()),
    ]
)

# §7: predicates prune only on a LEADING sort column, so this declares that
# time-bounded reads are the cheap ones and per-symbol scans are not. A desk
# that mostly asks "everything in this minute" wants exactly this; one that
# mostly asks "this symbol, all day" wants ("symbol", "event_ts") instead.
SORT_BY = ("event_ts", "symbol")

NAME = "trades"

SYMBOLS = ["AAPL", "MSFT", "NVDA", "SPY", "TSLA", "AMZN", "GOOG", "META"]


def observations(seed: int | None = None) -> Iterator[dict[str, object]]:
    """An endless stream of plausible trade prints."""
    rng = random.Random(seed)
    last = dict.fromkeys(SYMBOLS, 100.0)

    while True:
        symbol = rng.choice(SYMBOLS)
        last[symbol] = max(1.0, last[symbol] * (1 + rng.gauss(0, 0.0004)))
        price = round(last[symbol], 4)
        size = rng.choice((100, 200, 300, 500, 1_000))
        event = time.time_ns() - rng.randrange(0, 5_000_000)

        yield {
            "event_ts": event,
            "ingest_ts": time.time_ns(),
            "symbol": symbol,
            "price": price,
            "size": size,
            "payload": json.dumps(
                {"T": "t", "S": symbol, "p": price, "s": size, "t": event}
            ),
        }
