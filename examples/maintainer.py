"""The maintainer: everything that is not the append.

    uv run python examples/maintainer.py [--root DIR] [--maintain-every SECONDS]

There are two roles, and this is the second one. The **writer** appends. The
**maintainer** does the rest of the storage work: sealing the buffer into
Parquet, then compacting, evicting and expiring what that produces.

Sealing is maintenance. It is not a third role — it is the first thing the
maintainer does with what the writer leaves behind.

**Why not the writer's own thread.** A seal is CPU-bound pure Python — most of
its commit is pyiceberg copying table metadata — so a sealing thread starves
the appending one through the GIL even while holding no lock. Appends measured
45.2 ms behind an in-process seal. A separate process does not share the GIL,
and the `lease` table is what makes handing the role over safe.

Both are plain methods called on this loop's own schedule — `seal_due` often,
because it is an indexed read of one row when there is nothing to do, and
`maintain` rarely, because it reads table metadata. The library owns neither
the thread nor the interval; it has no business deciding how often your
storage process wakes up.

**Why sealing is not its own process.** It is the same kind of work as the
rest: off the hot path, writing to the same Iceberg table, not
latency-critical the way an append is. Sharing a GIL with compaction costs
nothing that matters, and splitting them costs something real — `_table_lock`
serialises a seal's commit against a maintenance pass *within* a process, and
nothing does across processes. Run as two processes, they raced on Iceberg's
delete-after-commit metadata cleanup and each warned about files the other had
already removed.

The two leases (`seal`, `maintain`) stay separate anyway, because they guard
different recovery records — `sealing` and `compacting` — and whoever replays
one must not replay the other. That also means splitting this process in two
later needs no code change, if a long compaction ever delays sealing enough to
matter. A delayed seal costs latency, not file size: the cut was recorded when
the rows arrived.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from _stream import NAME

from litelink import Log


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument("--seal-every", type=float, default=0.25, help="seconds")
    parser.add_argument("--maintain-every", type=float, default=10.0, help="seconds")
    args = parser.parse_args()

    # open(), never new(): the shape, the sort order and the config all come
    # from the log itself. A maintainer that restated them could disagree with
    # the writer, and the log is the one that is right.
    #
    # No "does it exist" check either — open() already refuses a missing log,
    # and repeating that here would mean an example knowing which file to look
    # for, which is the library's business and not the caller's.
    try:
        log = Log.open(args.root, NAME)
    except FileNotFoundError as exc:
        raise SystemExit(f"{exc}\nstart `just demo-capture` first") from exc

    print(f"maintaining {NAME} in {args.root} — pid {os.getpid()}")
    print(
        f"sealing every {args.seal_every:.2f}s, maintaining every "
        f"{args.maintain_every:.0f}s"
    )
    print("run alongside `just demo-capture`. Ctrl-C to hand the leases back.\n")

    # One thread, one loop, two calls at two cadences. Nothing here is a
    # daemon, nothing is signalled, and stopping is just leaving the loop.
    due = 0.0
    try:
        while True:
            log.seal_due()
            due += args.seal_every
            if due >= args.maintain_every:
                due = 0.0
                _maintain(log, args.root)

            time.sleep(args.seal_every)
    except KeyboardInterrupt:
        print("\nreleasing the seal and maintain leases")
    finally:
        log.close()


def _maintain(log: Log, root: Path) -> None:
    started = time.monotonic()
    try:
        # Seals too — sealing is maintenance. The loop calls `seal_due`
        # separately only because it is cheap enough to run far more often.
        log.maintain()
    except RuntimeError as exc:
        # Another process holds the maintain lease. Not worth dying over: it
        # means someone else is already doing this.
        print(f"  skipped: {exc}")
        return

    print(
        f"  table={log.table_rows():>10,} rows"
        f"  buffered={log.buffered_rows():>9,}"
        f"  files={log.table_files():>4}"
        f"  disk={_disk(root) / 1e6:>7.1f} MB"
        f"  ({(time.monotonic() - started) * 1000:>5.0f} ms)"
    )


def _disk(root: Path) -> int:
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file())


if __name__ == "__main__":
    main()
