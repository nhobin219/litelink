"""The maintainer: reclaim disk, in its own process.

    uv run python examples/maintainer.py [--root DIR] [--every SECONDS]

Separate from the sealer on purpose, and not just for tidiness. They are
different roles holding different leases, so:

  - either can be restarted, or crash, without stopping the other
  - a compaction that takes seconds cannot delay a seal, and vice versa
  - a slow maintenance pass never touches the append path

That separation is the lesson this library takes from doing it the other way.
Persistence, compaction, deletion and query planning are four concerns, and
coupling them means the slowest one sets the latency of the rest.

`maintain()` is compact + evict + expire (§6, §8, §12). It never unlinks a file
it has not first written to `pending_delete`, so reclaiming disk is a keyed read
of that table rather than a directory walk looking for things nobody claimed.
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
    parser.add_argument("--every", type=float, default=10.0, help="seconds")
    args = parser.parse_args()

    if not (args.root / "catalog.db").exists():
        raise SystemExit(
            f"no log at {args.root} — start `just demo-capture` first, "
            "which is what creates it"
        )

    log = Log.open(args.root, NAME)

    print(f"maintaining {NAME} in {args.root} — pid {os.getpid()}")
    print(f"compact + evict + expire every {args.every:.0f}s. Ctrl-C to stop.\n")

    try:
        while True:
            before = _disk(args.root)
            started = time.monotonic()
            try:
                log.maintain()
            except RuntimeError as exc:
                # Another maintainer holds the lease. Not an error worth dying
                # over — it means someone else is already doing this.
                print(f"  skipped: {exc}")
            else:
                after = _disk(args.root)
                print(
                    f"  {(time.monotonic() - started) * 1000:>7.0f} ms"
                    f"  files={len(log._table.data_files()):>4}"
                    f"  disk={after / 1e6:>7.1f} MB  ({(after - before) / 1e6:+.1f})"
                )

            time.sleep(args.every)
    except KeyboardInterrupt:
        print("\nreleasing the maintain lease")
    finally:
        log.close()


def _disk(root: Path) -> int:
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file())


if __name__ == "__main__":
    main()
