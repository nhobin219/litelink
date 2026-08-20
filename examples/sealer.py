"""The sealer: turn the buffer into Parquet, in its own process.

    uv run python examples/sealer.py [--root DIR]

Nothing here configures itself as the sealer. It opens the same log the writer
has open and asks for the `seal` role; the `lease` row decides, and whoever
holds it does the work. Start this while `capture.py` is already sealing on its
own thread and the thread simply starts losing — no restart, no coordination,
no flag on the writer. Stop this and the writer takes the role back once the
lease lapses.

**Why a process rather than the writer's thread.** A seal is CPU-bound pure
Python — most of its commit is pyiceberg copying table metadata — so a sealing
*thread* starves the appending one through the GIL even while holding no lock.
Measured: appends stalled 45.2 ms behind an in-process seal. A separate process
does not share the GIL, which is the whole reason the lease exists.

Safe to run against a live writer because every hand-off is a SQLite row, not a
Python object: the queue says which offsets to cut, `sealing` says who is
writing what, and WAL serialises the two processes. See docs/RUNTIME.md.
"""

from __future__ import annotations

import argparse
import os
import threading
from pathlib import Path

from _stream import NAME

from litelink import Log


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    args = parser.parse_args()

    if not (args.root / "catalog.db").exists():
        raise SystemExit(
            f"no log at {args.root} — start `just demo-capture` first, "
            "which is what creates it"
        )

    # open(), never new(): the shape, the sort order and the config all come
    # from the log itself. A sealer that restated them could disagree with the
    # writer, and the log is the one that is right.
    log = Log.open(args.root, NAME)

    print(f"sealing {NAME} in {args.root} — pid {os.getpid()}")
    print("run this alongside demo-capture; Ctrl-C to hand the role back.\n")

    stop = threading.Event()
    watcher = threading.Thread(target=_report, args=(log, stop), daemon=True)
    watcher.start()

    try:
        # Blocks until `stop` is set. The poll asks the queue whether anything
        # is waiting — one indexed row read — so an idle sealer costs nothing.
        log.run_sealer(stop)
    except KeyboardInterrupt:
        print("\nreleasing the seal lease")
    finally:
        stop.set()
        log.close()


def _report(log: Log, stop: threading.Event) -> None:
    """Print what the sealer has moved, so the split is visible from here too."""
    while not stop.wait(5.0):
        try:
            print(
                f"  table={log.table_rows():>10,} rows"
                f"  buffered={log.buffered_rows():>9,}"
                f"  files={len(log._table.data_files()):>4}"
            )
        except Exception as exc:  # noqa: BLE001 - reporting must not kill the sealer
            print(f"  [report] {exc}")


if __name__ == "__main__":
    main()
