"""Capture a websocket feed in-line: no maintainer, no threads, no processes.

    uv run --group dev python examples/websocket.py [--url URL] [--seconds N]

Every other example splits the work up, because a real deployment should: a
seal is CPU-bound pure Python and starves anything sharing its interpreter, so
`maintainer.py` runs it elsewhere. This is the other end of the range — the
smallest thing that is still a durable, queryable log — and the whole loop is:

    async for message in websocket:
        log.append(decode(message))
        log.seal_due()

`seal_due` is the entire maintenance story here. It is an indexed read of one
row when there is nothing to do, so calling it per message costs almost
nothing, and when a group is queued it writes that one file and returns. No
lease dance, because there is only one process; no thread, because there is
nothing to overlap with.

**With no --url it serves its own feed**, from `_stream.observations`, over a
loopback websocket in this same event loop. So it runs offline, and the test
suite can exercise it without reaching the network.

**This blocks the event loop, and it is the SEAL that does it — not the
append.** Measured on this file's own config: `append` runs at a 405 us median,
while a `seal_due` that actually writes a file takes 43 ms, two orders of
magnitude more. So a frame arriving during a seal waits for it, and at real
rates that is where the backpressure shows up. Moving `append` to a thread —
the obvious fix, and the wrong one — leaves the stall exactly where it is.

What fixes it is running the seal elsewhere, which is what `maintainer.py`
does and why it exists. Until then this shape holds as long as the gap between
seals is comfortably longer than a seal takes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import time
from pathlib import Path
from typing import TYPE_CHECKING

from _stream import NAME, SCHEMA, SORT_BY, observations

from litelink import Log, LogConfig

if TYPE_CHECKING:
    import websockets


async def serve(host: str, port: int) -> websockets.Server:
    """A local feed, so this example needs no network and no credentials."""
    import websockets

    async def publish(connection: websockets.ServerConnection) -> None:
        feed = observations(seed=7)
        with contextlib.suppress(Exception):
            while True:
                await connection.send(json.dumps(next(feed)))
                # Fast enough to seal several files in a short run, slow enough
                # that the demo is watchable.
                await asyncio.sleep(0.0005)

    return await websockets.serve(publish, host, port)


async def capture(url: str, log: Log, seconds: float) -> tuple[int, int]:
    """Append every frame until `seconds` elapse. Returns (appended, sealed)."""
    import websockets

    appended = 0
    sealed = 0
    deadline = time.monotonic() + seconds
    async with websockets.connect(url) as connection:
        while time.monotonic() < deadline:
            try:
                message = await asyncio.wait_for(connection.recv(), timeout=1.0)
            except TimeoutError:
                continue

            # One row, one durable append. `append` returns the offset it was
            # given, which is what a caller records if it needs to correlate
            # with anything outside the log.
            log.append(json.loads(message))
            appended += 1
            # In-line, per message. Nothing else seals, so leaving this out
            # means rows accumulate in SQLite for ever — durable and readable
            # the whole time, but never reaching Parquet.
            if log.seal_due() is not None:
                sealed += 1

    return appended, sealed


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-ws"))
    parser.add_argument("--url", help="feed to read; omitted, one is served here")
    parser.add_argument("--seconds", type=float, default=10.0)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    # Small, so a short run seals more than once and the demo shows a file
    # count moving. A real deployment leaves this at the default.
    config = LogConfig(target_seal_size=64 * 1024, compact_min_files=2)
    server = None if args.url else await serve("127.0.0.1", args.port)
    url = args.url or f"ws://127.0.0.1:{args.port}"

    # Open-or-new, the same shape `capture.py` uses and for the same reason:
    # `Log.new` alone raises on the second run, which is a poor first
    # impression for the example that is meant to be the easy one.
    try:
        log = Log.open(args.root, NAME)
    except FileNotFoundError:
        log = Log.new(args.root, NAME, schema=SCHEMA, sort_by=SORT_BY, config=config)

    with log:
        print(f"capturing {url} into {args.root}/{NAME} for {args.seconds:g}s")
        appended, sealed = await capture(url, log, args.seconds)

        # `seal()`, not `seal_due()`. The loop above drains groups the appender
        # has already CUT; the open group is not one of them, so ending on
        # `seal_due` left the tail in SQLite — measured, 320 of 3,599 rows —
        # while this print claimed everything was in Parquet. `seal` closes the
        # open group first, which is exactly what an orderly shutdown wants and
        # what nothing on the hot path should do.
        while log.seal() is not None:
            sealed += 1

        print(f"  appended {appended:,} rows, sealed {sealed} file(s)")
        print(f"  table holds {log.table_rows():,} rows in {log.table_files()} file(s)")
        busiest = log.sql(
            "SELECT callsign, count(*) AS reports, max(altitude_ft) AS ceiling"
            " FROM log GROUP BY callsign ORDER BY reports DESC LIMIT 3"
        ).read_all()
        print(f"  busiest callsigns: {busiest.to_pylist()}")

    if server is not None:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
