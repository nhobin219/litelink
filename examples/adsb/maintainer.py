"""One storage role, one process.

    uv run python examples/adsb/maintainer.py --role ROLE

The **writer** appends and does nothing else. Everything else is storage work,
split five ways, and this runs one of them; `just demo-maintain` starts all
five. It is the split litelink recommends for a deployment (docs/API.md):

| Role | Runs | Why it is its own process |
| --- | --- | --- |
| `seal` | `seal()` | it is all that bounds the buffer, so nothing may delay it |
| `compact` | `compact()` | the heaviest CPU work; beside `seal` it would delay it |
| `publish` | `publish()` | the network; one push can take a minute |
| `clean` | `evict()`, `reclaim` buffer and staging, `sweep("staging")` | local disk, freed promptly |
| `clean-published` | `reclaim("published")`, `sweep("published")` | deletes and listing on the published table |

**A step gets its own process when it is heavy on CPU or the network.** A seal
is CPU-bound pure Python — most of its commit is pyiceberg copying table
metadata — so it starves a thread sharing its interpreter even while holding
no lock: appends measured 45.2 ms behind an in-process seal, which is why the
writer is its own process. Compaction is the same work and more of it, so the
argument repeats one level down. A thread is not enough — it fixes blocking on
the network, not contention for the interpreter.

**The cleanup steps are not split further.** Eviction and expiry are metadata
commits that finish in milliseconds, and they have an order: eviction queues
the files that `reclaim` then deletes. What does matter is the network, so the
published table's expiry and sweep run apart from local cleanup, and a slow
listing of a bucket never holds up freeing local disk.

**Each extra process committing to the Iceberg table costs a log line.** Two
processes race on pyiceberg's delete-after-commit metadata cleanup, and the
loser logs `Failed to delete metadata file` for one the winner already removed.
It is noise rather than damage: across a run that logged it, 817,760 appended
rows read back contiguous with no gap and no duplicate. The metadata files this
library depends on are deleted through its own expiry queue, not pyiceberg's
cleanup.

**The library owns neither the thread nor the interval.** Each role is plain
method calls on its own schedule. `seal` runs often because it is an indexed
read of one row when there is nothing to do; the rest run rarely because they
read table metadata. `--role all` is `advance()`: the whole pipeline in one
process, the right shape when the costs do not justify five.

The claims are what make any of this safe. Each pass claims the offset RANGE it
is about to work on, so passes on disjoint ranges run at once and only real
overlap serialises — a compaction merging one run and a publish pushing another
have nothing to say to each other. A pass that finds its range claimed SKIPS it
rather than failing: someone else is already doing that work, and what is left
is still there next pass.
"""

from __future__ import annotations

import argparse
import ctypes
import fcntl
import os
import signal
import subprocess
import time
from pathlib import Path

from _stream import NAME
from pyiceberg.exceptions import CommitFailedException

import litelink
from litelink import WriteHandle
from litelink._s3 import S3Options


def seal_pass(log: WriteHandle) -> str | None:
    """Drain the seal queue. Reports only when it did something.

    Silence is the healthy state: the queue is usually empty, and a line every
    quarter second saying so would bury the ones that matter.
    """
    before = log.staging_files()
    sealed = log.seal()
    if sealed is None:
        return None

    return (
        f"sealed through {sealed:,}  "
        f"staging files {before} -> {log.staging_files()}  "
        f"buffer {log.buffered_rows():,} rows"
    )


def compact_pass(log: WriteHandle) -> str | None:
    """Convert sealed files into `target_compact_size` ones."""
    before = log.staging_files()
    log.compact()
    after = log.staging_files()
    if after == before:
        return None

    return f"converted {before} files -> {after}  local {log.staging_rows():,} rows"


def clean_pass(log: WriteHandle, root: Path) -> str | None:
    """Free local disk: evict what the next copy holds, then reclaim the
    buffer and staging and sweep staging. In this order, because eviction
    queues the files `reclaim` deletes.

    Staging eviction never goes past what the published table holds (I4), and
    `publish` is the step that moves that, in its own role. `reclaim("buffer")`
    is a `VACUUM` only when `vacuum_free_ratio` is set.
    """
    before = log.staging_files()
    log.evict()
    log.reclaim("buffer")
    log.reclaim("staging")
    log.sweep("staging")
    after = log.staging_files()
    if after == before:
        return None

    return (
        f"released {before - after} files  local {log.staging_rows():,} rows  "
        f"disk {_disk(root) / 1e6:.1f} MB"
    )


def clean_published_pass(log: WriteHandle) -> None:
    """Expire the published table's snapshots, delete what came due, and sweep
    it. Its own process because on object storage these are network calls, and
    the sweep lists the whole of `metadata/`. Silent: nothing here changes a
    count worth printing."""
    log.reclaim("published")
    log.sweep("published")


def publish_pass(log: WriteHandle) -> str | None:
    """Push what compaction has finished with, and record the watermark."""
    before = log.published_through()
    log.publish()
    after = log.published_through()
    if after == before:
        return None

    return f"published through {after:,}  published files {log.published_files():,}"


def all_passes(log: WriteHandle, root: Path) -> str | None:
    """`advance()`: the whole pipeline, publish included, in the order rows
    move — eviction queues deletions that expiry then drains, so running them
    the other way round only makes files wait a cycle."""
    log.advance()
    report = (
        f"local {log.staging_rows():,} rows in {log.staging_files()} files  "
        f"buffer {log.buffered_rows():,} rows  disk {_disk(root) / 1e6:.1f} MB"
    )
    report += f"  published through {log.published_through():,}"

    return report


# Cadence per role, and they differ by an order of magnitude because the costs
# do: sealing is an indexed read when idle, compaction reads and rewrites whole
# files, local cleanup is metadata commits, and the rest wait on a network. A
# sweep lists at most every four hours however often it is called.
ROLES = {
    "seal": 0.25,
    "compact": 10.0,
    "publish": 10.0,
    "clean": 10.0,
    "clean-published": 60.0,
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
        # `capture.py`. Harmless for a local published table: nothing resolves
        # them unless a push to S3 actually happens.
        log = litelink.open(args.root, NAME, s3=S3Options())
    except FileNotFoundError as exc:
        raise SystemExit(f"{exc}\nstart `just demo-capture` first") from exc

    label = f"[{args.role:>15}]"
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

    # The sidecar belongs to whichever process is already published-facing, so it
    # is not started five times over.
    sidecar = (
        Sidecar(log)
        if log.config.wal_replication and args.role in {"publish", "all"}
        else None
    )
    if sidecar is not None and sidecar.owner:
        print(f"{label} replicating the WAL — config at {sidecar.config}", flush=True)
    elif sidecar is not None:
        print(f"{label} another process is replicating the WAL", flush=True)

    passes = {
        "seal": lambda: seal_pass(log),
        "compact": lambda: compact_pass(log),
        "publish": lambda: publish_pass(log),
        "clean": lambda: clean_pass(log, args.root),
        "clean-published": lambda: clean_published_pass(log),
        "all": lambda: all_passes(log, args.root),
    }
    run = passes[args.role]

    global _running
    _running = True
    try:
        while True:
            started = time.monotonic()
            try:
                report = run()
            except RuntimeError as exc:
                # Another owner holds a claim over the range this role wanted.
                # Not worth dying over: it means someone else is already doing
                # this.
                report = f"skipped: {exc}"

            except CommitFailedException as exc:
                # The branch moved under this pass more times than `_commit`
                # retries. Nothing landed — a failed commit lands nothing — and
                # the work is still there next pass, so a maintainer that died
                # here would be trading a delay for an outage.
                #
                # Reachable in ordinary operation: passes claim ranges, so two
                # maintainers working disjoint offsets commit to one Iceberg
                # branch at once. It is not a RuntimeError, so it needs its own
                # clause.
                report = f"lost the commit race, retrying next pass: {exc}"

            if report is not None:
                elapsed = (time.monotonic() - started) * 1000
                print(f"{label} {report}  ({elapsed:.0f} ms)", flush=True)

            if sidecar is not None:
                # Not gated on any claim. The flock is what stops two
                # instances, and it is held for this process's whole life —
                # whereas a claim is taken and released around every pass, so
                # gating on it meant losing one pass to a busy compactor
                # stopped replication while KEEPING the lock, and nobody
                # replicated for as long as the contention lasted.
                sidecar.keep_running()

            time.sleep(every)
    except (KeyboardInterrupt, SystemExit):
        print(f"{label} stopping, claims handed back", flush=True)
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


# Loaded at import, NOT inside `preexec_fn`. That callback runs in the child
# between fork and exec, where only async-signal-safe work is safe: this
# process is multithreaded by then — s3fs keeps an asyncio loop, pyarrow and
# duckdb keep pools — and a `dlopen` there deadlocks if any thread held the
# loader or allocator lock at the moment of the fork. The parent would then
# block for ever inside `Popen`, holding the replication lock, with nothing
# replicating and no standby able to take over.
_LIBC = ctypes.CDLL("libc.so.6", use_errno=True)


def _die_with_parent() -> None:
    """In the child, between fork and exec: ask for SIGKILL when this process's
    parent dies. Linux-specific (`PR_SET_PDEATHSIG`), and the only thing that
    covers a parent killed with SIGKILL — a handler cannot run for that."""
    _LIBC.prctl(1, signal.SIGKILL)


def _stop(signum: int, frame: object) -> None:
    """Turn a signal into an exception, so the cleanup path is the same one.

    `SystemExit` rather than a flag the loop checks: the loop spends most of
    its time in `sleep`, and a flag would leave the sidecar running until the
    current interval elapsed.
    """
    if not _running:
        return

    raise SystemExit(128 + signum)


def _litestream() -> str:
    """The binary to run, pinned build first.

    `just litestream` puts a pinned release in `.bin/`, and a checkout should
    replicate with the version it pinned rather than whatever the machine
    happens to carry. That is not fastidiousness: litestream v0.5.0 changed the
    config format, and `WriteHandle.replication_config` writes one shape.

    Resolved against the REPO, not the cwd. This is an example run through
    `just` from the repo root today, but a `cd` into `examples/` would
    otherwise silently fall through to PATH — the failure being a different
    litestream, not a missing one, which is the harder one to see.
    """
    # `parents[2]`, because this file is `examples/adsb/maintainer.py`. It was
    # `parent.parent` before the move and resolved to `examples/.bin/`, which
    # never exists — so `--replicate` fell through to whatever was on PATH,
    # silently, which is the failure the paragraph above calls the harder one
    # to see. `_replication.litestream_binary` has always used `parents[2]`.
    pinned = Path(__file__).resolve().parents[2] / ".bin" / "litestream"

    return str(pinned) if os.access(pinned, os.X_OK) else "litestream"


class Sidecar:
    """Runs litestream alongside this process, and restarts it if it dies.

    **Here rather than in the library, on purpose.** Replication is a separate
    process reading the WAL — that is what keeps the network out of the write
    path — and supervising one means process lifecycle: restarts, orphan
    reaping, shutdown ordering. A library doing that would also risk the one
    thing litestream is explicit about, which is that two instances must never
    replicate the same database: a maintainer killed with SIGKILL holds its
    claim for the full TTL and orphans its child, so whoever takes it
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

    def __init__(self, log: WriteHandle) -> None:
        self.config = log.write_replication_config()
        self._process: subprocess.Popen[bytes] | None = None
        # An OS lock on a file beside the log, held for this process's whole
        # life. A range claim cannot do this job: it is acquired and
        # RELEASED around each pass, so two maintainers polling every ten
        # seconds both hold it at some point every round and both would keep a
        # sidecar alive — two litestream instances on one database, which is
        # the thing litestream forbids, and silent because each pass succeeds.
        #
        # `flock` because the kernel releases it when the process dies however
        # it dies. A lock FILE would need liveness checks and would survive a
        # SIGKILL as a stale lock nothing could clear.
        self._lock = (log.root / "litestream.lock").open("w")
        self.owner = self._claim()

    def _claim(self) -> bool:
        """Try for the lock. Retried every pass, not answered once.

        Answered once, a standby never becomes the owner: the process holding
        it exits cleanly, the kernel frees the lock, and the standby goes on
        printing that somebody else is replicating while nobody is.
        """
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False

        return True

    def keep_running(self) -> None:
        """Start it, or restart it if it has exited. Called every pass.

        Polled rather than signalled, because the loop is already polling and a
        second mechanism to notice a dead child would be a thread whose only
        job is to wait.
        """
        if not self.owner:
            self.owner = self._claim()
            if not self.owner:
                return

            print(f"[{'publish':>15}] took over WAL replication", flush=True)

        if self._process is not None and self._process.poll() is None:
            return

        if self._process is not None:
            print(f"  litestream exited ({self._process.returncode}), restarting")

        try:
            self._process = subprocess.Popen(  # noqa: S603
                [_litestream(), "replicate", "-config", str(self.config)],
                # Die with this process, however it dies. Without it a SIGKILL
                # here leaves litestream running while the kernel frees the
                # lock — so a supervisor restarting this service acquires the
                # lock immediately and starts a SECOND instance beside the
                # orphan, which is worse than the race it replaced. The signal
                # handler cannot cover SIGKILL; only the kernel can.
                preexec_fn=_die_with_parent,  # noqa: PLW1509
            )
        except FileNotFoundError:
            raise SystemExit(
                "wal_replication is on but litestream was not found.\n"
                "Fetch the pinned build:  just litestream\n"
                "or install it yourself:  https://litestream.io/install"
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


def _disk(root: Path) -> int:
    return sum(f.stat().st_size for f in root.rglob("*") if f.is_file())


if __name__ == "__main__":
    main()
