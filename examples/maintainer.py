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
import signal
import subprocess
import threading
import time
from pathlib import Path

from _stream import NAME

from litelink import Log
from litelink._s3 import S3Options


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
        # Credentials from the environment, never from the log — see
        # `capture.py`. Harmless when there is no archive: nothing resolves
        # them unless a push actually happens.
        log = Log.open(args.root, NAME, s3=S3Options())
    except FileNotFoundError as exc:
        raise SystemExit(f"{exc}\nstart `just demo-capture` first") from exc

    print(f"maintaining {NAME} in {args.root} — pid {os.getpid()}")
    if log.archive:
        print(f"pushing settled files to {log.archive}")

    print(
        f"sealing every {args.seal_every:.2f}s, maintaining every "
        f"{args.maintain_every:.0f}s"
    )
    print("run alongside `just demo-capture`. Ctrl-C to hand the leases back.\n")

    # SIGTERM, not just Ctrl-C. Python does not unwind on it — the process
    # simply stops — so without this the `finally` below never runs and a
    # supervisor stopping this service (systemd, `docker stop`, a `kill` from a
    # deploy script) leaves litestream running against a database the next
    # maintainer is about to start replicating. Two instances on one database
    # is the one thing litestream says never to do, and it is reachable the
    # ordinary way a process is stopped. Observed: two orphans accumulated in
    # testing before this was here.
    signal.signal(signal.SIGTERM, _stop)

    # Maintenance runs on its own thread, and this is not a detail. `sync`
    # talks to object storage, so it takes as long as the network takes —
    # measured at 83 s for sixteen files before registration was batched, and
    # still seconds after. Sealing is local, has to keep pace with the writer,
    # and is the only thing that bounds how far the buffer grows. Sharing one
    # thread meant a slow upload stopped sealing: the buffer reached 170,540
    # rows while the table sat at 69,920, and the log looked stalled.
    #
    # Safe because the roles are separate leases and an owner is minted per
    # attempt (see `Log._lease`), so the seal this loop runs and the seal
    # inside `maintain()` exclude each other exactly as two processes would —
    # one is simply refused.
    worker: threading.Thread | None = None
    sidecar = Sidecar(log) if log.config.wal_replication else None
    if sidecar is not None:
        print(f"replicating the WAL — config at {sidecar.config}")

    # One thread, one loop, two calls at two cadences. Nothing here is a
    # daemon, nothing is signalled, and stopping is just leaving the loop.
    due = 0.0
    try:
        while True:
            log.seal_due()
            if sidecar is not None:
                sidecar.keep_running()

            due += args.seal_every
            if due >= args.maintain_every and not pass_running(worker):
                due = 0.0
                worker = threading.Thread(
                    target=_maintain, args=(log, args.root), daemon=True
                )
                worker.start()

            time.sleep(args.seal_every)
    except (KeyboardInterrupt, SystemExit):
        print("\nreleasing the seal and maintain leases")
    finally:
        if sidecar is not None:
            sidecar.stop()

        log.close()


def pass_running(worker: threading.Thread | None) -> bool:
    """Whether the last maintenance pass is still going.

    Skipped rather than queued: passes are idempotent and the next one will
    pick up whatever this one did not, so stacking them behind a slow upload
    would only spend threads to arrive at the same place later.
    """
    return worker is not None and worker.is_alive()


def _stop(signum: int, frame: object) -> None:
    """Turn a signal into an exception, so the cleanup path is the same one.

    `SystemExit` rather than a flag the loop checks: the loop spends most of
    its time in `sleep`, and a flag would leave the sidecar running until the
    current interval elapsed.
    """
    raise SystemExit(128 + signum)


class Sidecar:
    """Runs litestream alongside this process, and restarts it if it dies.

    **Here rather than in the library, on purpose.** Replication is a separate
    process reading the WAL — that is what keeps the network out of the write
    path — and supervising one means process lifecycle: restarts, orphan
    reaping, shutdown ordering. A library doing that would also risk the one
    thing litestream is explicit about, which is that two instances must never
    replicate the same database: a maintainer killed with SIGKILL holds its
    lease for the full TTL and orphans its child, so whoever takes the lease
    next would start a second against the same file.

    None of that goes away by being here. It becomes visible and editable, in
    the file that already decides how often to seal — and if this shape does
    not suit a deployment, `litestream replicate -config` against the same
    generated file is the alternative, with nothing to change in the log.

    Note what this couples: replication now lives as long as the MAINTAINER,
    while the rows it protects come from the WRITER. A deployment that runs a
    writer with no maintainer has `wal_replication=True` and no replication,
    which is the strongest argument for running the sidecar independently.
    """

    def __init__(self, log: Log) -> None:
        self.config = log.write_replication_config()
        self._process: subprocess.Popen[bytes] | None = None

    def keep_running(self) -> None:
        """Start it, or restart it if it has exited. Called every pass.

        Polled rather than signalled, because the loop is already polling and a
        second mechanism to notice a dead child would be a thread whose only
        job is to wait.
        """
        if self._process is not None and self._process.poll() is None:
            return

        if self._process is not None:
            print(f"  litestream exited ({self._process.returncode}), restarting")

        try:
            self._process = subprocess.Popen(  # noqa: S603
                ["litestream", "replicate", "-config", str(self.config)],  # noqa: S607
            )
        except FileNotFoundError:
            raise SystemExit(
                "wal_replication is on but litestream is not on PATH — "
                "see https://litestream.io/install"
            ) from None

    def stop(self) -> None:
        """Terminate it, and wait. An orphan would keep replicating the same
        database the next maintainer is about to start replicating."""
        if self._process is None or self._process.poll() is not None:
            return

        self._process.terminate()
        try:
            self._process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._process.kill()


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

    # After `maintain`, not before. Eviction reads the archive watermark to
    # decide what it is allowed to drop (I4), so a push landing first is what
    # lets the NEXT pass reclaim the disk it freed up.
    #
    # Separate from `maintain()` because it is the one step that can block on a
    # network: everything above is local and finishes in milliseconds, and a
    # loop that could not tell them apart would report an S3 timeout as slow
    # compaction.
    archived = ""
    if log.archive:
        pushed = time.monotonic()
        log.sync()
        archived = f"  sync={(time.monotonic() - pushed) * 1000:>5.0f} ms"

    print(
        f"  table={log.table_rows():>10,} rows"
        f"  buffered={log.buffered_rows():>9,}"
        f"  files={log.table_files():>4}"
        f"  disk={_disk(root) / 1e6:>7.1f} MB"
        f"  ({(time.monotonic() - started) * 1000:>5.0f} ms)"
        f"{archived}"
    )


def _disk(root: Path) -> int:
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file())


if __name__ == "__main__":
    main()
