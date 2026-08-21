"""One storage role, one process.

    uv run python examples/maintainer.py --role seal|compact|reclaim|sync|all

The **writer** appends and does nothing else. Everything else is storage work,
and this runs one piece of it: sealing the buffer into Parquet, converting
sealed files into archive-shaped ones, reclaiming local disk, or pushing to the
archive. `just demo-maintain` starts all four.

**Why one process each.** A seal is CPU-bound pure Python — most of its commit
is pyiceberg copying table metadata — so it starves a thread sharing its
interpreter even while holding no lock. Appends measured 45.2 ms behind an
in-process seal, which is why the writer is its own process. Compaction is the
same work and more of it, so the argument repeats one level down: run
compaction beside sealing and sealing waits on it, and the buffer grows for as
long as it waits. A thread is not enough — it fixes blocking on the network,
not contention for the interpreter.

**What this file used to say, and why it changed.** It argued against
splitting, on two grounds. One is stale: `_table_lock` serialised a seal's
commit against a maintenance pass within a process, and that lock is gone —
both now rest on Iceberg's compare-and-swap retry, which is what makes them
safe across processes too.

The other still happens, and was measured again here rather than assumed away.
Two processes committing to one table race on pyiceberg's delete-after-commit
metadata cleanup, and the loser logs `Failed to delete metadata file` for one
the winner has already removed. A single-process control over the same workload
produced none, so it is the split that causes it.

It is noise rather than damage, and that was checked too: across a four-process
run that logged the race, 817,760 appended rows read back contiguous with no
gap and no duplicate. The commit itself is protected by the CAS retry, and the
metadata files this library depends on are deleted through its own expiry queue
rather than pyiceberg's cleanup — so a cleanup that finds its file already gone
has lost a race to do something that was done.

**The library owns neither the thread nor the interval.** Each role here is a
plain method on its own schedule. `seal_due` runs often because it is an
indexed read of one row when there is nothing to do; the rest run rarely
because they read table metadata. `maintain()` still exists and runs compact,
evict and expire in one call under one lease — `--role all` is that, and is the
right shape when the costs do not justify four processes.

The leases are what make any of this safe. `seal` and `maintain` guard
different recovery records (`sealing`, `compacting`), so whoever replays one
must not replay the other, and an owner is minted per attempt — two processes
and two threads are refused on identical terms.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

from _stream import NAME

from litelink import Log

if TYPE_CHECKING:
    from collections.abc import Callable

from litelink._s3 import S3Options


def seal_pass(log: Log) -> str | None:
    """Drain the seal queue. Reports only when it did something.

    Silence is the healthy state: the queue is usually empty, and a line every
    quarter second saying so would bury the ones that matter.
    """
    before = log.table_files()
    sealed = log.seal_due()
    if sealed is None:
        return None

    return (
        f"sealed through {sealed:,}  "
        f"local files {before} -> {log.table_files()}  "
        f"buffer {log.buffered_rows():,} rows"
    )


def compact_pass(log: Log) -> str | None:
    """Convert sealed files into `target_compact_size` ones."""
    before = log.table_files()
    log.compact()
    after = log.table_files()
    if after == before:
        return None

    return f"converted {before} files -> {after}  local {log.table_rows():,} rows"


def reclaim_pass(log: Log, root: Path) -> str | None:
    """Settle, evict past `local_retention`, expire, delete what came due.

    Settling first because eviction never goes above the watermark (§4a), and
    on a log with no archive nothing else moves it — `sync` is the step that
    moves it when there is one, and does not run here.
    """
    before = log.table_files()
    log.evict()
    log.expire()
    after = log.table_files()
    if after == before:
        return None

    return (
        f"released {before - after} files  local {log.table_rows():,} rows  "
        f"disk {_disk(root) / 1e6:.1f} MB"
    )


def sync_pass(log: Log) -> str | None:
    """Push what compaction has finished with, and record the watermark."""
    before = log.archived_through()
    log.sync()
    after = log.archived_through()
    if after == before:
        return None

    return f"archived through {after:,}  archive files {log.archive_files():,}"


def all_passes(log: Log, root: Path) -> str | None:
    """`maintain()` plus a push: every local pass under one lease, in the order
    it encodes — eviction queues deletions that expiry then drains, so running
    them the other way round only makes files wait a cycle."""
    log.maintain()
    report = (
        f"local {log.table_rows():,} rows in {log.table_files()} files  "
        f"buffer {log.buffered_rows():,} rows  disk {_disk(root) / 1e6:.1f} MB"
    )
    if log.archive:
        log.sync()
        report += f"  archived through {log.archived_through():,}"

    return report


# Cadence per role, and they differ by an order of magnitude because the costs
# do: sealing is an indexed read when idle, conversion reads and rewrites whole
# files, reclaiming is a metadata commit, and a push waits on a network.
ROLES = {
    "seal": 0.25,
    "compact": 10.0,
    "reclaim": 30.0,
    "sync": 10.0,
    "all": 10.0,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument("--role", choices=sorted(ROLES), default="all")
    parser.add_argument(
        "--every", type=float, default=None, help="seconds; per-role default"
    )
    args = parser.parse_args()
    every = args.every if args.every is not None else ROLES[args.role]

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

    if args.role == "sync" and not log.archive:
        # Not an error: `just demo-maintain` starts every role, and a
        # local-only log simply has nothing for this one to do. Exiting quietly
        # beats an error the reader has to learn to ignore.
        print("[   sync] no archive configured, nothing to push", flush=True)
        log.close()

        return

    label = f"[{args.role:>7}]"
    print(f"{label} pid {os.getpid()}, every {every:g}s", flush=True)

    # SIGTERM, not just Ctrl-C. Python does not unwind on it — the process
    # simply stops — so without this the `finally` below never runs and a
    # supervisor stopping this service (systemd, `docker stop`, a `kill` from a
    # deploy script) leaves litestream running against a database the next
    # maintainer is about to start replicating. Two instances on one database
    # is the one thing litestream says never to do, and it is reachable the
    # ordinary way a process is stopped. Observed: two orphans accumulated in
    # testing before this was here.
    signal.signal(signal.SIGTERM, _stop)

    # The sidecar belongs to whichever process is already archive-facing, so it
    # is not started four times over.
    sidecar = (
        Sidecar(log)
        if log.config.wal_replication and args.role in {"sync", "all"}
        else None
    )
    if sidecar is not None and sidecar.owner:
        print(f"{label} replicating the WAL — config at {sidecar.config}", flush=True)
    elif sidecar is not None:
        print(f"{label} another process is replicating the WAL", flush=True)

    passes = {
        "seal": lambda: seal_pass(log),
        "compact": lambda: compact_pass(log),
        "reclaim": lambda: reclaim_pass(log, args.root),
        "sync": lambda: sync_pass(log),
        "all": lambda: all_passes(log, args.root),
    }
    run = passes[args.role]

    global _running
    _running = True
    try:
        while True:
            started = time.monotonic()
            owns = True
            try:
                report = run()
            except RuntimeError as exc:
                # Another owner holds the lease this role needs. Not worth
                # dying over: it means someone else is already doing this.
                owns = False
                report = f"skipped: {exc}"

            if report is not None:
                elapsed = (time.monotonic() - started) * 1000
                print(f"{label} {report}  ({elapsed:.0f} ms)", flush=True)

            if sidecar is not None:
                # Tied to the lease, not to the role. A second maintainer is
                # refused its table work and says so — which reads as safe —
                # while nothing stopped it starting a SECOND litestream against
                # the same databases, the one thing litestream forbids. The
                # process doing the archive work is the one that replicates.
                if owns:
                    sidecar.keep_running()
                else:
                    sidecar.stop()

            time.sleep(every)
    except (KeyboardInterrupt, SystemExit):
        print(f"{label} stopping, leases handed back", flush=True)
    finally:
        _running = False
        if sidecar is not None:
            sidecar.stop()

        log.close()


# Set while the loop is running, so a signal arriving after it has finished
# does not raise into interpreter shutdown — which Python reports as
# "Exception ignored in threading._shutdown" and looks like a crash on the way
# out. Observed on every clean stop before this was here.
_running = False


def _stop(signum: int, frame: object) -> None:
    """Turn a signal into an exception, so the cleanup path is the same one.

    `SystemExit` rather than a flag the loop checks: the loop spends most of
    its time in `sleep`, and a flag would leave the sidecar running until the
    current interval elapsed.
    """
    if not _running:
        return

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
        # An OS lock on a file beside the log, held for this process's whole
        # life. The maintenance lease cannot do this job: it is acquired and
        # RELEASED around each pass, so two maintainers polling every ten
        # seconds both hold it at some point every round and both would keep a
        # sidecar alive — two litestream instances on one database, which is
        # the thing litestream forbids, and silent because each pass succeeds.
        #
        # `flock` because the kernel releases it when the process dies however
        # it dies. A lock FILE would need liveness checks and would survive a
        # SIGKILL as a stale lock nothing could clear.
        self._lock = (log.root / "litestream.lock").open("w")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.owner = True
        except OSError:
            self.owner = False

    def keep_running(self) -> None:
        """Start it, or restart it if it has exited. Called every pass.

        Polled rather than signalled, because the loop is already polling and a
        second mechanism to notice a dead child would be a thread whose only
        job is to wait.
        """
        if not self.owner:
            return

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
    """One pass, phase by phase.

    `log.maintain()` does all of this in one call and is what most deployments
    want. It is split here because the phases cost wildly different amounts and
    a single number hides which one was slow — conversion reads and rewrites
    whole files, eviction and expiry are metadata commits, and sync is the only
    one that can block on a network. An 83 s sync went unnoticed inside a
    combined figure until the buffer had grown to 170,540 rows.

    `seal` reads 0 ms in a healthy log and that is the point: the loop above
    drains the queue every quarter second, so by the time a pass runs there is
    nothing left to seal. A number here means sealing fell behind, which is the
    first thing to know and was previously invisible.
    """
    timings: dict[str, float] = {}
    try:
        for name, phase in (
            ("seal", log.seal_due),
            ("compact", log.compact),
            ("reclaim", _reclaim(log)),
        ):
            started = time.monotonic()
            phase()
            timings[name] = (time.monotonic() - started) * 1000
    except RuntimeError as exc:
        # Another owner holds the maintenance lease. Not worth dying over: it
        # means someone else is already doing this.
        print(f"  skipped: {exc}")
        return

    # After the local passes, not before. Eviction reads the archive watermark
    # to decide what it is allowed to drop (I4), so a push landing first is
    # what lets the NEXT pass reclaim the disk it freed up.
    archived_rows = archived_files = sync_column = ""
    if log.archive:
        pushed = time.monotonic()
        log.sync()
        archived_rows = f" {log.archived_through():>14,}"
        archived_files = f" {log.archive_files():>14,}"
        sync_column = f" {(time.monotonic() - pushed) * 1000:>7.0f}ms"

    print(
        f"{log.table_rows():>13,} {log.buffered_rows():>13,}{archived_rows}"
        f" {log.table_files():>12,}{archived_files}"
        f" {_disk(root) / 1e6:>7.1f}MB"
        f" {timings['seal']:>6.0f}ms {timings['compact']:>8.0f}ms"
        f" {timings['reclaim']:>8.0f}ms{sync_column}"
    )


def _reclaim(log: Log) -> Callable[[], None]:
    """Eviction and expiry as one phase: both are metadata commits that finish
    in milliseconds, and splitting them further would report noise."""

    def run() -> None:
        log.evict()
        log.expire()

    return run


def _disk(root: Path) -> int:
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file())


if __name__ == "__main__":
    main()
