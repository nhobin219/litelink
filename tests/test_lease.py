"""Ownership that outlives the process holding it (SPEC §13.6, §11).

§11 records the hazard in both directions: a maintenance process redoing the
writer's in-flight seal, and a writer deleting a maintenance process's
half-written compaction. A lock cannot resolve it, because a lock says nothing
about a process that is no longer running.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from litelink import Log, LogConfig
from litelink._lease import Lease, new_owner

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])


def open_log(root: Path, config: LogConfig | None = None) -> Log:
    if (root / "catalog.db").exists():
        return Log.open(root, "s")

    return Log.new(root, "s", schema=SCHEMA, sort_by=("event_ts",), config=config)


def rows(n: int, start: int = 0) -> list[dict[str, object]]:
    return [{"event_ts": i, "key": f"k{i}"} for i in range(start, start + n)]


def test_a_lease_is_never_a_passenger_in_another_transaction(tmp_path: Path) -> None:
    """A lease write must be its own transaction, or it is not a claim at all.

    The connection is shared, and every buffer write takes `Buffer._lock`
    around an explicit BEGIN. A lease statement issued without that lock lands
    INSIDE whatever transaction is open — an append's — and then commits or
    rolls back with it. A rolled-back append took the lease row with it and
    left its holder believing it held a role the table no longer recorded.

    Observed as two sealers writing the same file, with pyiceberg refusing the
    second: "Cannot add files that are already referenced by table".
    """
    with open_log(tmp_path) as log:
        buffer = log._buffer
        lease = buffer.lease("seal", new_owner())
        acquired = threading.Event()
        taken: list[bool] = []

        def take() -> None:
            taken.append(lease.acquire())
            acquired.set()

        with buffer._lock:
            buffer._con.execute("BEGIN IMMEDIATE")
            grabber = threading.Thread(target=take)
            grabber.start()
            # Without the lock on the lease, this lands inside the transaction
            # below and is undone by it.
            acquired.wait(0.5)
            buffer._con.execute("ROLLBACK")

        grabber.join(5)

        assert taken == [True], f"the lease was never taken: {taken}"
        assert lease.held(), "an unrelated rollback erased a held lease"


def test_a_second_owner_is_refused(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        ours = log._lease("seal")
        theirs = Lease(log._buffer._con, log._buffer._lock, "seal", new_owner())

        assert ours.acquire()
        assert not theirs.acquire(), "two owners held the same role"

        ours.release()

        assert theirs.acquire(), "release did not free it"


def test_one_log_hands_out_a_distinct_owner_every_time(tmp_path: Path) -> None:
    """Which is what lets the lease be the only guard.

    An owner fixed per Log would be re-entered by every thread sharing it, and
    the role would exclude nothing inside a process. Minting per attempt means
    the second caller is a stranger to the row, wherever it is running.
    """
    with open_log(tmp_path) as log:
        ours = log._lease("seal")

        assert ours.acquire()
        assert not log._lease("seal").acquire(), "a second attempt re-entered"
        assert ours.acquire(), "a retry could not re-take the lease it holds"


def test_an_expired_lease_can_be_taken_over(tmp_path: Path) -> None:
    """A killed process must not strand the role forever.

    Safe because the takeover replays rather than duplicates: `sealing` records
    the range and its path before the file exists (I2), so redoing it writes the
    same file to the same name.
    """
    with open_log(tmp_path) as log:
        dying = Lease(
            log._buffer._con, log._buffer._lock, "seal", new_owner(), ttl_ms=1
        )

        assert dying.acquire()
        time.sleep(0.05)
        assert log._lease("seal").acquire(), "an expired lease blocked its successor"


def test_recovery_leaves_another_owners_seal_alone(tmp_path: Path) -> None:
    """§11's hazard, in the direction that loses data.

    A second opener must not redo an in-flight seal it does not own — that is
    how it ends up re-registering a file the owner is about to register itself.
    """

    holder = open_log(tmp_path)
    holder.extend(rows(4))
    path = holder._layout.seal_path(1, 5, "tok")
    holder._buffer.claim_seal(1, 5, path)

    # Someone else holds the seal role and is still working. Taken on the
    # holder's own connection so it outlives the Log opened below.
    other = Lease(holder._buffer._con, holder._buffer._lock, "seal", new_owner())
    assert other.acquire()

    with open_log(tmp_path) as reopened:
        assert reopened._buffer.pending_seal() is not None, (
            "recovery replayed a seal another owner held"
        )

    other.release()

    with open_log(tmp_path) as after:
        assert after._buffer.pending_seal() is None, "did not replay once free"

    holder.close()


def test_maintain_refuses_while_another_owner_holds_it(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        other = Lease(log._buffer._con, log._buffer._lock, "maintain", new_owner())
        assert other.acquire()

        with pytest.raises(RuntimeError, match="maintenance lease"):
            log.maintain()

        other.release()
        log.maintain()


def test_a_rejected_maintain_does_not_strand_the_lease(tmp_path: Path) -> None:
    """A refusal must not lock out the process that could have done the work."""
    log = Log.new(
        tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), archive="s3://bucket/x"
    )
    # sync() refuses without credentials reaching a real endpoint; what matters
    # is that a refusal hands the lease back rather than holding it for its TTL.
    with pytest.raises(Exception):  # noqa: B017, PT011 - any refusal will do
        log.sync()

    other = Lease(log._buffer._con, log._buffer._lock, "maintain", new_owner())

    assert other.acquire(), "the refused call kept the lease"

    log.close()


def test_threads_in_one_process_still_exclude_each_other(tmp_path: Path) -> None:
    """The lease alone, with no in-process flag behind it.

    Two threads calling `seal` are two owners, so one loses on the same row that
    would refuse another process — nothing here knows it is in one process.

    Asserted on the FILES, not on the return values. Both calls report the same
    cut, because the cut is what `seal` promises and both threads asked for the
    same one; what the lease decides is which of them writes it, and writing it
    twice is the failure worth catching.
    """
    with open_log(tmp_path, config=LogConfig(target_seal_size=1 << 30)) as log:
        log.extend(rows(200))
        started = threading.Event()
        results: list[object] = []

        def seal_twice() -> None:
            started.set()
            results.append(log.seal())

        first = threading.Thread(target=seal_twice)
        first.start()
        started.wait(5)
        results.append(log.seal())
        first.join(10)

        assert log.table_files() == 1, "two threads each wrote a file"
        assert log.table_rows() == 200
        assert set(results) == {201}, f"threads disagreed on the cut: {results}"


def test_a_separate_process_can_seal(tmp_path: Path) -> None:
    """The point of the exercise.

    A sealing thread starves the appending one through the GIL, because a seal
    is CPU-bound in pure Python. A sealing process does not share the GIL, and
    the lease is what makes it safe for the sealer to be somewhere else.

    The writer's threshold is set out of reach so that nothing it does can seal;
    everything here is the other process's work.
    """
    config = LogConfig(target_seal_size=1 << 30)
    with Log.new(
        tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), config=config
    ) as log:
        log.extend(rows(500))

        assert log.table_extent() is None, "the writer sealed something itself"

    sealer = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(f"""
                from litelink import Log
                with Log.open({str(tmp_path)!r}, "s") as log:
                    end = log.seal()
                    print("SEALED", end, log.table_rows())
            """),
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert sealer.returncode == 0, sealer.stderr
    assert "SEALED 501 500" in sealer.stdout, f"{sealer.stdout}\n{sealer.stderr}"

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.table_rows() == 500, "the other process's seal is not visible"
        assert reopened.buffered_rows() == 0, "buffer was not cleared"
        assert len(reopened.scan().read_all()) == 500


def test_a_writer_defers_to_a_sealer_in_another_process(tmp_path: Path) -> None:
    """No configuration decides this — the lease does.

    A writer sealing in-process still tries; it simply loses. So an
    operator adds a sealer process without changing the writer, and if that
    process dies the writer takes the role back when the lease lapses.
    """
    # Nothing seals here, so the only seals are the explicit ones below and
    # the assertions are not racing this log's own thread.
    config = LogConfig(target_seal_size=1 << 30)
    with open_log(tmp_path, config=config) as log:
        log.extend(rows(400))

        elsewhere = Lease(log._buffer._con, log._buffer._lock, "seal", new_owner())
        assert elsewhere.acquire(), "could not simulate another sealer"

        # The cut is recorded either way — that is `seal`'s promise and it does
        # not depend on who holds the lease. What the holder owns is the WRITE.
        assert log.seal() == 401, "did not record the cut"
        assert log.table_files() == 0, "wrote a file while another held the role"

        elsewhere.release()

        assert log.seal() == 401, "did not take the role back once free"
        assert log.table_files() == 1, "the queued cut was never written"


def test_a_replayed_seal_hands_the_lease_back(tmp_path: Path) -> None:
    """Every exit from a seal releases, including the one that only recovers.

    The replay path returns early, and returning early past the release left
    the seal role dead for its whole TTL: the drain loop above it exits with
    groups still queued and nothing able to pick them up for 30 seconds.
    """

    config = LogConfig(target_seal_size=1 << 30)
    with open_log(tmp_path, config=config) as log:
        log.extend(rows(50))

        # A seal that committed but never retired its group.
        log._buffer.close_open_group()
        group = log._buffer.pending_group()
        assert group is not None
        start, end = group
        path = log._layout.seal_path(start, end, "tok")
        log._buffer.claim_seal(start, end, path)
        log._write_and_commit(end, path)

        assert log.seal_due() == end, "the replay did not finish the seal"

        # The role must be free again immediately, not in thirty seconds.
        taker = Lease(log._buffer._con, log._buffer._lock, "seal", new_owner())

        assert taker.acquire(), "the replay path kept the seal lease"


def test_a_sort_rewrite_takes_the_maintenance_lease(tmp_path: Path) -> None:
    """A rewrite IS a compaction, so it needs compaction's exclusion.

    Same claim record, same deterministic output path, same commit. Reaching
    that path lease-free let it run beside a `maintain()` in another process:
    two writers to one `compaction_path`, and a single-row `compacting` intent
    each would clear from under the other — leaving a half-written file that
    nothing could name, which is the one thing §12's queue exists to prevent.
    """
    with open_log(tmp_path, config=LogConfig(target_seal_size=1 << 30)) as log:
        log.extend(rows(20))
        log.seal()

        held = Lease(log._buffer._con, log._buffer._lock, "maintain", new_owner())

        assert held.acquire(), "could not simulate another maintainer"

        with pytest.raises(RuntimeError, match="maintenance lease"):
            log.set_sort_by(("key", "event_ts"), rewrite=True)

        assert log._sort_by == ("event_ts",), "the sort order changed anyway"

        held.release()
        log.set_sort_by(("key", "event_ts"), rewrite=True)

        assert log._sort_by == ("key", "event_ts")


def test_a_lapsed_writer_cannot_commit_or_clear_a_successors_claim(
    tmp_path: Path,
) -> None:
    """A stalled writer wakes believing it still owns the seal. It does not.

    Unique per-attempt names removed an accidental fence: under the old shared
    name, a lapsed writer's `register` failed with "already referenced by
    table". With its own name it succeeds, and the range lands in the table
    TWICE — silent duplicate rows, which is worse than the torn file the
    unique names were introduced to prevent.

    Two guards replace that accident. The commit is fenced on the lease, and
    the file is queued for deletion when the fence rejects it so that it stays
    nameable. And `finish_seal` clears only the claim it is given, so a lapsed
    writer cannot wipe the record its successor is working under.
    """
    config = LogConfig(target_seal_size=1 << 30)
    with open_log(tmp_path, config=config) as log:
        log.extend(rows(40))
        log._buffer.close_open_group()
        group = log._buffer.pending_group()

        assert group is not None
        start, end = group
        path = log._layout.seal_path(start, end, "lapsed")
        log._buffer.claim_seal(start, end, path)

        # A lease this writer no longer holds.
        lapsed = log._buffer.lease("seal", new_owner(), ttl_ms=1)

        assert lapsed.acquire()
        time.sleep(0.05)
        stolen = Lease(log._buffer._con, log._buffer._lock, "seal", new_owner())

        assert stolen.acquire(), "could not simulate the takeover"

        with pytest.raises(RuntimeError, match="lost the seal lease"):
            log._write_and_commit(end, path, lapsed)

        assert log.table_files() == 0, "a lapsed writer committed anyway"
        assert path in log._buffer.queued_deletions(), (
            "its file is on disk and reachable from nothing"
        )

        # And it cannot clear a claim that is no longer its own.
        log._buffer.claim_seal(start, end, "someone-elses")

        assert not log._buffer.finish_seal(end, path), "wiped a successor's claim"
        assert log._buffer.pending_seal() is not None
