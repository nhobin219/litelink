"""The whole library, against a live public feed, in one process.

    uv run --group dev python examples/websocket.py

No producer to start, no credentials, no maintainer, no threads. Bitstamp
publishes BTC/USD trades over an unauthenticated websocket; this subscribes,
appends each one, and seals when there is enough to seal. That is the entire
loop — everything else in this file is argument parsing and a closing query.

`seal_due` is an indexed read of one row when there is nothing to do, so
calling it per message costs almost nothing, and when a group is queued it
writes that one file and returns.

**It blocks the event loop, and it is the SEAL that does it** — not the append.
Measured: an append runs at a 405 us median, a `seal_due` that actually writes
a file at 43 ms. So a trade arriving mid-seal waits for it. At this feed's rate
that is invisible; the fix at real rates is to run the seal in another process,
which is what `adsb/maintainer.py` is and why it exists.

`adsb/` is the other end of the range: four processes, one per storage role,
against a synthetic feed that can be driven as hard as you like.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

import pyarrow as pa
import websockets

from litelink import Log, LogConfig

FEED = "wss://ws.bitstamp.net"
CHANNEL = "live_trades_btcusd"

# Every field the feed sends that is worth a column. §7 prunes on Iceberg
# statistics, so a query for one minute of trades never reads the rest — which
# is the reason to declare a schema rather than store the frame whole.
SCHEMA = pa.schema(
    [
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("price", pa.float64()),
        pa.field("amount", pa.float64()),
        # 0 buy, 1 sell, as the feed spells it.
        pa.field("side", pa.int64()),
    ]
)


def row(trade: dict) -> dict:
    """One frame, as columns. Microseconds, because the feed sends them."""
    return {
        "trade_id": int(trade["id"]),
        "event_ts": int(trade["microtimestamp"]),
        "price": float(trade["price"]),
        "amount": float(trade["amount"]),
        "side": int(trade["type"]),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-ws"))
    parser.add_argument("--seconds", type=float, default=30.0)
    args = parser.parse_args()

    # Small, so a short run seals more than once and the file count moves. A
    # real deployment leaves this at the default.
    config = LogConfig(target_seal_size=16 * 1024, compact_min_files=2)
    try:
        log = Log.open(args.root, "trades")
    except FileNotFoundError:
        log = Log.new(args.root, "trades", schema=SCHEMA, config=config)

    with log:
        print(f"capturing {CHANNEL} into {args.root}/trades for {args.seconds:g}s")
        deadline = time.monotonic() + args.seconds
        async with websockets.connect(FEED) as feed:
            await feed.send(
                json.dumps({"event": "bts:subscribe", "data": {"channel": CHANNEL}})
            )
            while time.monotonic() < deadline:
                try:
                    frame = json.loads(await asyncio.wait_for(feed.recv(), timeout=5))
                except TimeoutError:
                    continue

                # The feed also sends subscription acks and reconnect notices.
                if frame.get("event") != "trade":
                    continue

                log.append(row(frame["data"]))
                log.seal_due()

        # `seal()`, not `seal_due()`: the loop above drains groups the appender
        # already CUT, and the open group is not one of them. An orderly
        # shutdown closes it, which is the one moment that is the right thing
        # to do.
        while log.seal() is not None:
            pass

        print(f"  {log.end_offset() - 1:,} trades, {log.table_files()} file(s)")
        summary = log.sql(
            "SELECT count(*) AS trades, min(price) AS low, max(price) AS high,"
            " round(sum(amount), 4) AS btc FROM log"
        ).read_all()
        print(f"  {summary.to_pylist()[0]}")


if __name__ == "__main__":
    asyncio.run(main())
