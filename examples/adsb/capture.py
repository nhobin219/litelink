"""The writer: append, and nothing else.

    uv run python examples/adsb/capture.py [--root DIR] [--rate ROWS_PER_SECOND]

One thread appending, and every append durable when `extend()` returns — there
is no in-memory buffer to flush, which is the failure the README opens with.

**Everything else is `maintainer.py`**, a separate process: sealing the buffer
into Parquet, then compacting, evicting and expiring what that produces.
Sealing is maintenance, not a third role — it is the first thing done with what
this process leaves behind.

Nothing here seals, and there is no setting that would make it. Appending
records where the next file should be cut and stops; writing that file is the
maintainer's job. Run this alone and the rows stay in SQLite, durable and
readable — `scan()` unions the buffer with the table, so a reader sees them
whether or not anything has sealed yet. They reach Parquet when the maintainer
starts, at exactly the cuts recorded while it was not running. Only the buffer
grows in the meantime, which is worth seeing.

Ctrl-C to stop. Nothing committed is lost, and nothing queued is either: a cut
that has been recorded but not yet sealed is picked up by whoever opens the log
next.
"""

from __future__ import annotations

import argparse
import itertools
import time
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from _stream import NAME, SCHEMA, SORT_BY, observations

import litelink
from litelink import LogConfig
from litelink._s3 import S3Options

if TYPE_CHECKING:
    from collections.abc import Callable

# Six tries against the library's own ten-second wait: a minute of patience,
# which covers the longest publish this repo has measured.
_SETTLE_ATTEMPTS = 6


def _settle(change: Callable[[], None], what: str) -> None:
    """Apply a settings change, waiting out whatever maintenance is doing.

    Both setters take a whole-log claim — `validate` refuses a PAIR, so the two
    halves have to be decided together — which means they queue behind ordinary
    maintenance. The library waits ten seconds and then reports rather than
    hanging, and that is the right library behaviour: a wedged log should
    surface. But a publish holds its claim for the whole push, measured at 83 s
    over sixteen files, and a merge over a large run at 20 s — so a writer that
    treated the report as fatal would crash on any restart that happened to
    land during one, which is exactly when restarts are most likely.

    The writer is a long-running process with nothing better to do here, so it
    waits. Loudly, because a wait that never ends should look like one.
    """
    for attempt in range(_SETTLE_ATTEMPTS):
        try:
            change()

            return
        except RuntimeError as exc:
            if attempt == _SETTLE_ATTEMPTS - 1:
                raise

            print(f"waiting on maintenance before {what}: {exc}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument("--rate", type=float, default=2000.0, help="reports per second")
    parser.add_argument("--batch", type=int, default=20, help="rows per transaction")
    # Off by default, so the demo needs no object storage to run. `just rustfs`
    # starts one locally and prints the URI to pass here; the same flag with an
    # `s3://` bucket on AWS is the entire difference between the two.
    parser.add_argument(
        "--published",
        default=None,
        help="s3:// prefix for the published tier (see `just rustfs`)",
    )
    # Only meaningful with a published table: it is how long a file stays on local
    # disk AFTER the published table has it, and I4 will not let it be evicted before.
    # Off by default because it needs a binary the demo does not install. With
    # it on, the maintainer writes the config and runs the sidecar itself.
    parser.add_argument(
        "--replicate",
        action="store_true",
        help="ship the WAL continuously (needs litestream on PATH, and --published)",
    )
    parser.add_argument(
        "--staging-retention",
        type=float,
        default=60.0,
        help="seconds to keep published files locally",
    )
    args = parser.parse_args()

    # Small for a demo — seal often, so the reader sees the table grow within
    # seconds — and larger than that where the published table is concerned, because
    # the two want opposite things. A real deployment sizes the seal by §7's
    # read-latency argument rather than to make a demo lively.
    #
    # Measured against S3: 648 ms to upload a 9 kB file. Almost all of that is
    # the round trip, not the bytes, so halving file size doubles the cost of
    # publishing the same stream. Compaction is what bridges it: seal at 1 MiB
    # so the buffer stays shallow, convert to 8 MiB so the published table receives
    # eight times fewer objects for the same data.
    config = LogConfig(
        target_seal_size=1024 * 1024,
        target_compact_size=8 * 1024 * 1024,
        compact_min_files=3,
        staging_snapshot_retention=timedelta(seconds=30),
        # None without a published table, because with nowhere to push to a retention
        # is a policy for deleting the only copy — which `WriteHandle.new` refuses to
        # be told by accident.
        staging_retention=(
            timedelta(seconds=args.staging_retention) if args.published else None
        ),
        wal_replication=args.replicate,
    )

    # new() creates and takes the shape; open() recovers it. A service that
    # restarts wants the second, and must not fail because the log is already
    # there — so the choice is made by whether open() finds one. Asked of the
    # library rather than of the filesystem: which file proves a log exists is
    # the library's business, and an example that checked for it itself would
    # be wrong the day that changes.
    # Credentials are never in the config, and never in the log: `S3Options()`
    # with nothing set reads AWS_ENDPOINT_URL / AWS_ACCESS_KEY_ID /
    # AWS_SECRET_ACCESS_KEY / AWS_REGION at the point of use. `just rustfs`
    # prints the exports for a local endpoint.
    s3 = S3Options()
    try:
        log = litelink.open(args.root, NAME, s3=s3)
        # A log's published table is fixed when it is created (litelink#118),
        # so a restart asking for a different one is a mistake to report, not
        # an instruction: moving a log is `retire()` and a new one. Only when
        # one was asked for — a plain `just demo-capture` on a log created by
        # `just demo-published` keeps the table it has.
        if args.published is not None and (args.published.rstrip("/") != log.published):
            log.close()
            msg = (
                f"{args.root}/{NAME} publishes to {log.published}, not "
                f"{args.published}; a log's published table is fixed when it is "
                f"created"
            )
            raise SystemExit(msg)

        _settle(lambda: log.set_config(config), "applying the config")
    except FileNotFoundError:
        log = litelink.new(
            args.root,
            NAME,
            schema=SCHEMA,
            sort_by=SORT_BY,
            config=config,
            published=args.published,
            s3=s3,
        )

    print(f"capturing {NAME} into {args.root} at ~{args.rate:,.0f} reports/s")
    print("appending only — `just demo-maintain` seals and reclaims disk")
    if args.published:
        print(f"publishing to {args.published} once files are sealed and settled")

    if args.replicate:
        print("WAL replication on — `just demo-maintain` runs the sidecar")

    print("`just demo-tail` watches. Until a maintainer runs, rows stay buffered")
    print("and readable; nothing is lost by starting it late.\n")

    source = observations()
    interval = args.batch / args.rate
    written = 0
    started = time.monotonic()

    try:
        for tick in itertools.count():
            deadline = started + (tick + 1) * interval
            log.extend(itertools.islice(source, args.batch))
            written += args.batch

            if tick % 50 == 0:
                elapsed = time.monotonic() - started
                print(
                    f"  {written:>9,} reports  {written / max(elapsed, 1e-9):>7,.0f}/s"
                    f"  end_offset={log.end_offset():,}"
                )

            # Pace against a fixed schedule rather than sleeping a fixed amount,
            # so a slow batch does not permanently lower the rate.
            time.sleep(max(0.0, deadline - time.monotonic()))
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        log.close()

    on_disk = sum(f.stat().st_size for f in args.root.rglob("*") if f.is_file())
    print(f"{written:,} reports appended, all durable at commit")
    print(f"{args.root}/ holds {on_disk / 1e6:.1f} MB — `just demo-clean` to remove it")
    # Nothing deletes this on exit, deliberately: tail.py reads it after the
    # writer stops, and a demo you cannot inspect afterwards is not much of one.
    # Note the demo leaves staging_retention unset, so the window grows without
    # bound; a real deployment sets it and lets advance() hold the size.
    # Rows still queued or buffered here are not lost — they are durable, and
    # the next process to open the log finds the cuts already recorded.


if __name__ == "__main__":
    main()
