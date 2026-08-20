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
    config = LogConfig(target_size=8 * 1024, snapshot_retention=timedelta(days=1))
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
    config = LogConfig(target_size=8 * 1024, snapshot_retention=timedelta(days=1))
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
