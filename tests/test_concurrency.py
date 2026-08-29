"""The concurrency contract, asserted rather than described.

`docs/RUNTIME.md` states what is safe to call from where. These pin the part
that is easy to break by accident: one `Log` reached from many threads, with no
thread calling the same method twice in a row.

That shape is not hypothetical. `fsync` cannot run on an event loop, so an
`async` caller reaches this library through `asyncio.to_thread`, and a pool
hands out a different thread each call. A library that demanded thread affinity
would be unusable from asyncio, which is where a websocket feed lives.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from litelink import Log, LogConfig

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("payload", pa.string()),
    ]
)


def open_log(root: Path, config: LogConfig | None = None) -> Log:
    return Log.new(root, "s", schema=SCHEMA, sort_by=("event_ts",), config=config)


def rows(n: int, *, start: int = 0) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i % 3}", "payload": "p" * 200}
        for i in range(start, start + n)
    ]


def test_one_log_survives_being_passed_around_a_thread_pool(tmp_path: Path) -> None:
    """No affinity: every call may land on a different thread from the last.

    A pool of two against forty submissions guarantees threads are reused for
    unrelated calls and that no call is on the thread that opened the log —
    which is exactly what `asyncio.to_thread` does, and exactly what
    `check_same_thread=True` would forbid.
    """
    config = LogConfig(target_seal_size=8 * 1024, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = []
            for i in range(20):
                futures.append(pool.submit(log.extend, rows(25, start=i * 25)))
                futures.append(pool.submit(lambda: len(log.scan().read_all())))

            # .result() re-raises anything a worker hit, which is the assertion.
            counts = [f.result(timeout=60) for f in futures]

        assert log.seal_due() is not None, "nothing was queued by 500 appends"
        assert log.table_rows() + log.buffered_rows() == 500
        assert max(c for c in counts if isinstance(c, int)) <= 500


def test_appends_and_reads_interleave_without_loss(tmp_path: Path) -> None:
    """A reader thread must never see a row vanish, mid-seal or otherwise.

    §7's boundary is what makes this hold: between the Iceberg commit and the
    buffer delete a row is in both tiers, and the boundary excludes it from
    one.
    """
    config = LogConfig(target_seal_size=8 * 1024, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        stop = threading.Event()
        failures: list[str] = []
        seen: list[int] = []

        def maintain() -> None:
            while not stop.wait(0.002):
                try:
                    log.seal_due()
                except Exception as exc:  # noqa: BLE001 - reported, not raised
                    failures.append(f"seal_due: {exc!r}")
                    return

        def read() -> None:
            while not stop.is_set():
                try:
                    seen.append(len(log.scan().read_all()))
                except Exception as exc:  # noqa: BLE001 - reported, not raised
                    failures.append(f"scan: {exc!r}")
                    return

        workers = [threading.Thread(target=maintain), threading.Thread(target=read)]
        for worker in workers:
            worker.start()

        try:
            for i in range(20):
                log.extend(rows(25, start=i * 25))
        finally:
            stop.set()
            for worker in workers:
                worker.join(30)

        assert not failures, failures
        assert seen, "the reader never completed a scan"
        # Monotonic: a count may lag, but must never go backwards.
        assert seen == sorted(seen), f"rows disappeared mid-flight: {seen}"

        check = log._buffer._con.execute("PRAGMA integrity_check").fetchall()

        assert check == [("ok",)], f"buffer database damaged: {check}"


def test_a_seal_landing_mid_query_neither_loses_nor_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The union's two legs must come from one moment, not two.

    A seal commits its file and THEN deletes the rows it covered, so it has a
    window where a row is in both tiers. A query that resolves the boundary and
    reads the buffer at different moments straddles that window: pairing a new
    snapshot with an old boundary duplicates the rows in the gap, and pairing
    an old snapshot with a buffer the seal has already emptied loses them.

    Forced here rather than raced: the seal runs inside the buffer read, which
    is exactly the interleaving that is otherwise a matter of luck.
    """
    config = LogConfig(target_seal_size=2 * 1024, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        # Both legs must be non-empty, or the union takes its buffer-only
        # branch and the boundary never enters into it.
        log.extend(rows(200))
        log.seal()
        log.extend(rows(200, start=200))
        log.scan().read_all()  # warm the view and the tail cache

        assert log.table_rows() and log.buffered_rows(), "need both legs"

        buffer = log._buffer
        real = buffer.rows_above
        fired = []

        def seal_midway(boundary: int | None) -> object:
            tail = real(boundary)
            if not fired:
                fired.append(log.seal_due())

            return tail

        monkeypatch.setattr(buffer, "rows_above", seal_midway)
        got = log.scan().read_all()
        monkeypatch.undo()

        assert fired and fired[0] is not None, "the seal never ran"

        offsets = got.column("litelink_offset").to_pylist()

        assert len(offsets) == len(set(offsets)), "a row appeared in both legs"
        assert sorted(offsets) == list(range(1, 401)), (
            f"expected offsets 1..400, got {len(offsets)} rows "
            f"spanning {min(offsets)}..{max(offsets)}"
        )


def test_a_reader_is_not_destroyed_by_the_next_query(tmp_path: Path) -> None:
    """A scan returns a LAZY reader, so the caller drains it after we return.

    On a shared DuckDB connection the next query's `register` and
    `CREATE OR REPLACE TEMP VIEW` land underneath a reader still streaming from
    those same names. Measured before the fix: a reader over 200 rows returned
    ZERO after another query ran on the connection — not perturbed, destroyed.

    That is why a concurrent `scan()` could report a row count unrelated to
    what was appended, in either direction. Each query now gets its own cursor:
    an independent connection over the same database, with its own
    registrations and temp views.
    """
    config = LogConfig(target_seal_size=1 << 30, snapshot_retention=timedelta(days=1))
    with open_log(tmp_path, config) as log:
        log.extend(rows(200))

        held = log.scan()  # deliberately not drained yet

        log.extend(rows(300, start=200))

        assert len(log.scan().read_all()) == 500, "the second scan is wrong"
        assert len(held.read_all()) == 200, (
            "the first reader saw the second query's relation"
        )


def test_a_seal_does_not_read_a_scan_s_pinned_snapshot(tmp_path: Path) -> None:
    """A stepping statement pins its CONNECTION's snapshot (#34).

    `Buffer._rows` steps a statement for the whole of its `fetchall`, and in
    WAL that pins the read snapshot of the connection it ran on. When the seal
    shared `_reader` with scans, a scan in flight handed the seal a stale view:
    it wrote its Parquet file from that view and `finish_seal` then deleted
    every buffered row through the group's end — including rows that never
    reached the file. Measured before the fix: 1,000 acknowledged offsets in
    neither tier.

    Staged rather than raced, so it is deterministic. An undrained cursor on
    `_reader` is exactly the state a concurrent `scan()` is in for the length
    of its fetch.

    Falsify by passing `self._reader` to `_rows` from `rows_between`: the seal
    reads 10 rows instead of 20 and the last 10 are lost.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA)
    with log:
        log.extend([{"event_ts": i, "key": "k"} for i in range(10)])

        buffer = log._buffer
        # Pin a snapshot on the shared read connection, as a scan's fetch does.
        pinned = buffer._reader.execute("SELECT * FROM buffer")
        pinned.fetchone()
        try:
            log.extend([{"event_ts": i, "key": "k"} for i in range(10, 20)])

            # The seal's input must include everything committed, not what the
            # pinned statement can see.
            assert buffer.rows_between(1, 21).num_rows == 20
        finally:
            pinned.fetchall()


def test_a_sealer_that_woke_to_a_finished_group_abandons_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The queue head is read before the claim, so it can go stale (#37).

    `_seal_queued` reads `pending_group()` and only then blocks in
    `acquire()`, which is a `BEGIN IMMEDIATE` and can wait the whole busy
    timeout. A sealer that wakes to find another has sealed that group finds
    its claim succeeds anyway — the range is free again — and proceeds on a
    group that no longer exists.

    `sealing` holds one row, so its `claim_seal` deletes the live sealer's, and
    that sealer's `finish_seal` then returns False: its Iceberg commit landed,
    but its `extent` row is never named and its buffer rows are never dropped.
    A wasted rewrite rather than loss, since reads are bounded by the table
    extent and the next attempt re-names the group.

    Staged on `pending_group` because that IS the race: the pre-claim read
    returns the group this sealer saw, the post-claim re-read returns what is
    actually queued. Falsify by removing the re-read — the stale sealer
    proceeds and overwrites `sealing` with a range nobody is working on.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, config=LogConfig(target_seal_size=1024))
    with log:
        log.extend([{"event_ts": i, "key": "k" * 200} for i in range(200)])

        stale = log._buffer.pending_group()

        assert stale is not None
        # Another sealer takes that group and finishes it.
        assert log._seal_queued() == stale[1]

        current = log._buffer.pending_group()

        assert current is not None
        assert current != stale

        # Now the blocked sealer wakes, still holding the group it read.
        truth = log._buffer.pending_group
        seen: list[int] = []

        def staged() -> tuple[int, int] | None:
            seen.append(1)

            return stale if len(seen) == 1 else truth()

        monkeypatch.setattr(log._buffer, "pending_group", staged)
        before = log._buffer.pending_seal()

        assert log._seal_queued() is None
        assert len(seen) >= 2, "the queue head was not re-read under the claim"

        monkeypatch.undo()

        assert log._buffer.pending_seal() == before
        assert log._buffer.pending_group() == current
