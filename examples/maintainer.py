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
import threading
import time
from pathlib import Path

from _stream import NAME

from litelink import Log


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument("--maintain-every", type=float, default=10.0, help="seconds")
    args = parser.parse_args()

    if not (args.root / "catalog.db").exists():
        raise SystemExit(
            f"no log at {args.root} — start `just demo-capture` first, "
            "which is what creates it"
        )

    # open(), never new(): the shape, the sort order and the config all come
    # from the log itself. A maintainer that restated them could disagree with
    # the writer, and the log is the one that is right.
    log = Log.open(args.root, NAME)

    print(f"maintaining {NAME} in {args.root} — pid {os.getpid()}")
    print(f"sealing continuously, maintaining every {args.maintain_every:.0f}s")
    print("run alongside `just demo-capture`. Ctrl-C to hand the leases back.\n")

    stop = threading.Event()
    # The seal loop blocks, so it takes the thread and the rest takes the main
    # loop. Which way round does not matter; both are this one role's work.
    sealer = threading.Thread(target=log.run_sealer, args=(stop,), daemon=True)
    sealer.start()

    try:
        while not stop.wait(args.maintain_every):
            _maintain(log, args.root)
    except KeyboardInterrupt:
        print("\nreleasing the seal and maintain leases")
    finally:
        stop.set()
        sealer.join(timeout=30)
        # Raises anything the sealing thread recorded — it has no caller of its
        # own to raise to, so this is where a failed seal surfaces.
        log.close()


def _maintain(log: Log, root: Path) -> None:
    started = time.monotonic()
    try:
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
