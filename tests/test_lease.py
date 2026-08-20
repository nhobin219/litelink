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


def test_a_second_owner_is_refused(tmp_path: Path) -> None:
    with open_log(tmp_path) as log:
        ours = log._lease("seal")
        theirs = Lease(log._buffer._con, "seal", new_owner())

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
        dying = Lease(log._buffer._con, "seal", new_owner(), ttl_ms=1)

        assert dying.acquire()
        time.sleep(0.05)
        assert log._lease("seal").acquire(), "an expired lease blocked its successor"


def test_recovery_leaves_another_owners_seal_alone(tmp_path: Path) -> None:
    """§11's hazard, in the direction that loses data.

    A second opener must not redo an in-flight seal it does not own — that is
    how it ends up re-registering a file the owner is about to register itself.
    """
    import datetime

    holder = open_log(tmp_path)
    holder.extend(rows(4))
    path = holder._layout.seal_path(1, 5, datetime.datetime.now(datetime.UTC).date())
    holder._buffer.claim_seal(1, 5, path)

    # Someone else holds the seal role and is still working. Taken on the
    # holder's own connection so it outlives the Log opened below.
    other = Lease(holder._buffer._con, "seal", new_owner())
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
        other = Lease(log._buffer._con, "maintain", new_owner())
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
    with pytest.raises(NotImplementedError):
        log.maintain()

    other = Lease(log._buffer._con, "maintain", new_owner())

    assert other.acquire(), "the refused call kept the lease"

    log.close()


def test_threads_in_one_process_still_exclude_each_other(tmp_path: Path) -> None:
    """The lease alone, with no in-process flag behind it.

    Two threads calling `seal` are two owners, so one loses on the same row that
    would refuse another process — nothing here knows it is in one process.
    """
    with open_log(tmp_path, config=LogConfig(target_size=1 << 30)) as log:
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

    assert sorted(r is None for r in results) == [False, True], (
        f"expected exactly one seal to win, got {results}"
    )


def test_a_separate_process_can_seal(tmp_path: Path) -> None:
    """The point of the exercise.

    A sealing thread starves the appending one through the GIL, because a seal
    is CPU-bound in pure Python. A sealing process does not share the GIL, and
    the lease is what makes it safe for the sealer to be somewhere else.

    The writer's threshold is set out of reach so that nothing it does can seal;
    everything here is the other process's work.
    """
    config = LogConfig(target_size=1 << 30, background_seal=False)
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

    A writer with `background_seal` on still tries; it simply loses. So an
    operator adds a sealer process without changing the writer, and if that
    process dies the writer takes the role back when the lease lapses.
    """
    # Synchronous, so the only seals are the explicit ones below and the
    # assertions are not racing this log's own thread.
    config = LogConfig(target_size=1 << 30, background_seal=False)
    with open_log(tmp_path, config=config) as log:
        log.extend(rows(400))

        elsewhere = Lease(log._buffer._con, "seal", new_owner())
        assert elsewhere.acquire(), "could not simulate another sealer"

        assert log.seal() is None, "sealed while another process held the role"

        elsewhere.release()

        assert log.seal() is not None, "did not take the role back once free"
