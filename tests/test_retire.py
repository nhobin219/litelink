"""`retire()`: a log ended for good, and everything that must refuse after it.

Against a real archive, because retirement is defined by where the rows end
up — all in the archive, none local — and by what `restore` finds there.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import time
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

import litelink
from litelink import LogConfig, RetiredError, WriteHandle
from litelink._buffer import Buffer
from litelink._layout import Layout
from litelink._replication import control_socket, litestream_binary
from litelink._table import RETIRED_PROPERTY
from litelink.log import OFFSET
from tests.test_archive import ROWS, archived_log, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._s3 import S3Options

pytestmark = pytest.mark.s3


def written(
    tmp_path: Path, bucket: str, s3: S3Options, **overrides: object
) -> WriteHandle:
    """A log with rows in every tier: archived and evicted, local, buffered."""
    log = archived_log(
        tmp_path, bucket, s3, local_retention=timedelta(0), local_rows=1000, **overrides
    )
    log.extend(rows(ROWS))
    log.seal()
    log.sync(push_unsettled=True)
    log.maintain()
    log.extend({"event_ts": ROWS + i, "key": "t", "payload": "y"} for i in range(7))

    return log


def backup(source: Path, target: Path) -> None:
    """A copy of `buffer.db` as a replica would have shipped it."""
    target.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(source)
    dst = sqlite3.connect(target)
    src.backup(dst)
    src.close()
    dst.close()


def test_a_retired_log_is_all_archive_and_nothing_local(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Every row in the archive, none local, and both records say so.

    Falsify by removing the `evict(everything=True)` from `retire`: the local
    table keeps its files and `retire` refuses to mark the log retired.
    """
    with written(tmp_path, bucket, s3) as log:
        total = log.end_offset() - 1
        log.retire()

        assert log.table_rows() == 0
        assert log._buffer.extent() is None  # noqa: SLF001
        marker = log._buffer.retired()  # noqa: SLF001
        assert marker is not None
        assert (marker["state"], marker["through"]) == ("retired", total)

        archive = log._archive.require()  # noqa: SLF001
        archive.reload()
        recorded = json.loads(archive.properties[RETIRED_PROPERTY])
        assert recorded["through"] == total

    with litelink.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        offsets = reader.scan(columns=[OFFSET]).read_all().column(0).to_pylist()
        assert sorted(offsets) == list(range(1, total + 1))
        archived = reader.column_statistics(tier="archive")
        assert archived.record_count == total, "nothing local, so it is the log"


def test_a_retired_log_takes_no_rows_from_any_handle(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Refused by the buffer itself, so a writer opened BEFORE `retire()` — which
    never looks at the marker again — cannot add a row either.

    Falsify by dropping the `refuse_retired` trigger from `Buffer._create`: the
    earlier handle's append lands after retirement.
    """
    with written(tmp_path, bucket, s3) as log:
        earlier = litelink.open(tmp_path, "s", s3=s3)
        log.retire()

        for handle in (log, earlier):
            with pytest.raises(RetiredError, match=r"start_offset=\d+"):
                handle.append({"event_ts": 1, "key": "k", "payload": "p"})

        with pytest.raises(RetiredError):
            log.ingest(pa_table(1))

        earlier.close()


def pa_table(count: int):  # noqa: ANN201
    import pyarrow as pa

    from tests.test_archive import SCHEMA

    return pa.Table.from_pylist(rows(count), schema=SCHEMA)


def test_a_retired_log_opens_for_reading_only(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The refusal names when it retired and where the next log starts.

    Falsify by removing the retired check from `litelink.open`: the writer
    opens, and only its first append finds out.
    """
    with written(tmp_path, bucket, s3) as log:
        through = log.end_offset() - 1
        log.retire()

    with pytest.raises(RetiredError, match=f"start_offset={through + 1}"):
        litelink.open(tmp_path, "s", s3=s3)

    with litelink.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        assert reader.scan().read_all().num_rows == through


def test_restore_refuses_on_the_archive_alone(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A replica shipped BEFORE retirement carries no marker; the archive's
    `litelink.retired` property refuses it anyway.

    Falsify by removing the `archive_retired` check from `restore`: the old
    buffer brings the retired log back as a writer.
    """
    where = f"s3://{bucket}/prefix"
    primary = tmp_path / "primary"
    second = tmp_path / "second"
    with written(primary, bucket, s3) as log:
        backup(Layout(primary, "s").buffer_db, Layout(second, "s").buffer_db)
        log.retire()

    assert Buffer.peek_retired(Layout(second, "s").buffer_db) is None
    with pytest.raises(RetiredError, match="retired"):
        litelink.restore(second, "s", archive=where, s3=s3)


def test_restore_refuses_on_the_replica_marker_alone(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other guard on its own: the archive property hidden, the marker in
    the replica still refuses.

    Falsify by removing the `peek_retired` check from `restore`.
    """
    where = f"s3://{bucket}/prefix"
    primary = tmp_path / "primary"
    second = tmp_path / "second"
    with written(primary, bucket, s3) as log:
        log.retire()
        backup(Layout(primary, "s").buffer_db, Layout(second, "s").buffer_db)

    monkeypatch.setattr("litelink.log.archive_retired", lambda *_: None)
    with pytest.raises(RetiredError, match="retired"):
        litelink.restore(second, "s", archive=where, s3=s3)


def test_retire_resumes_after_a_crash(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash leaves the log retiring: no rows accepted, a writer can still
    open it, and `retire()` again finishes the job.

    Falsify by writing the `retiring` marker after the push rather than before
    it: the append between the crash and the resume lands.
    """
    with written(tmp_path, bucket, s3) as log:
        original = WriteHandle.sync

        def failing(self: WriteHandle, **_: object) -> None:
            msg = "the archive went away"
            raise OSError(msg)

        monkeypatch.setattr(WriteHandle, "sync", failing)
        with pytest.raises(OSError, match="went away"):
            log.retire()

        marker = log._buffer.retired()  # noqa: SLF001
        assert marker is not None and marker["state"] == "retiring"
        with pytest.raises(RetiredError, match="being retired"):
            log.append({"event_ts": 1, "key": "k", "payload": "p"})

    monkeypatch.setattr(WriteHandle, "sync", original)
    with litelink.open(tmp_path, "s", s3=s3) as again:
        again.retire()
        assert again._buffer.retired()["state"] == "retired"  # ty: ignore[not-subscriptable]  # noqa: SLF001


def test_retire_needs_an_archive(tmp_path: Path) -> None:
    """Emptying the local table of a local-only log would delete its rows."""
    import pyarrow as pa

    with litelink.new(tmp_path, "s", schema=pa.schema([("x", pa.int64())])) as log:
        log.append({"x": 1})
        with pytest.raises(ValueError, match="needs an archive"):
            log.retire()

        assert log._buffer.retired() is None  # noqa: SLF001


def test_hydrate_works_on_a_retired_log(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """It adds no rows and assigns no offsets, so retirement allows it."""
    with written(tmp_path, bucket, s3) as log:
        log.retire()
        assert log.table_rows() == 0

        log.hydrate(since=timedelta(hours=1))

        assert log.table_rows() > 0


# -- the WAL replica ------------------------------------------------------------


def sidecar_binary() -> str:
    binary = litestream_binary()
    if shutil.which(binary) is None and not os.access(binary, os.X_OK):
        pytest.skip("litestream is not provisioned — run `just litestream`")

    return binary


def test_retire_flushes_the_replica_through_the_running_sidecar(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """With the periodic sync an hour away, the replica still has the marker the
    moment `retire()` returns — because it asked the running sidecar to ship,
    through its control socket, and waited.

    Falsify by removing the final `_flush_replica()` from `retire`: the replica
    holds `retiring`, not `retired`.
    """
    binary = sidecar_binary()
    config = replace(LogConfig(), wal_replication=True)
    with written(tmp_path, bucket, s3, wal_replication=True) as log:
        assert log.config.wal_replication == config.wal_replication
        path = log.write_replication_config()
        text = path.read_text().replace(
            "      force-path-style: true\n",
            "      force-path-style: true\n      sync-interval: 1h\n",
        )
        path.write_text(text)
        environment = dict(os.environ)
        resolved = s3.resolved()
        environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key or ""
        environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key or ""
        sidecar = subprocess.Popen(  # noqa: S603
            [binary, "replicate", "-config", str(path)],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 15
            while not control_socket(log._layout).exists():  # noqa: SLF001
                assert time.monotonic() < deadline, "the sidecar never listened"
                time.sleep(0.1)

            log.retire()

            # Restored WHILE the sidecar runs: stopping it ships on the way
            # out, which would hide a missing flush.
            restored = tmp_path / "restored.db"
            subprocess.run(  # noqa: S603
                [
                    binary,
                    "restore",
                    "-config",
                    str(path),
                    "-o",
                    str(restored),
                    str(log._layout.buffer_db),  # noqa: SLF001
                ],
                env=environment,
                check=True,
                capture_output=True,
            )
        finally:
            sidecar.terminate()
            sidecar.wait(timeout=10)

    marker = Buffer.peek_retired(restored)
    assert marker is not None
    assert marker["state"] == "retired"


def test_retire_refuses_to_guess_when_no_sidecar_answers(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Replication on and nothing on the control socket: `retire()` raises
    rather than start a litestream of its own, which would be a second process
    replicating one database. The log stays retiring, taking no rows.

    Falsify by skipping the flush when the socket is absent: `retire()`
    completes with a replica that never saw the marker.
    """
    sidecar_binary()
    with written(tmp_path, bucket, s3, wal_replication=True) as log:
        with pytest.raises(RuntimeError, match="control socket"):
            log.retire()

        marker = log._buffer.retired()  # noqa: SLF001
        assert marker is not None and marker["state"] == "retiring"


def test_a_retired_log_never_reads_its_buffer(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retirement closes the buffer's range at the log's end — empty, since it
    never grows again — so no read converts its rows, whatever it asks.

    Falsify by skipping `empty_buffer_range` at the end of `retire()`: the
    range still spans the offsets the buffer held, so a read reaching into them
    reads the (empty) buffer.
    """
    with written(tmp_path, bucket, s3) as log:
        through = log.end_offset() - 1
        log.retire()
        _, rows = log._buffer.tiers()  # noqa: SLF001
        assert rows["buffer"][0] == (through + 1, through + 1)

    read: list[object] = []
    original = Buffer.rows_above

    def counted(buffer: Buffer, boundary: int | None):  # noqa: ANN202
        read.append(boundary)
        return original(buffer, boundary)

    monkeypatch.setattr(Buffer, "rows_above", counted)
    with litelink.open(tmp_path, "s", read_only=True, s3=s3) as reader:
        assert reader.scan().read_all().num_rows == through
        assert reader.scan(where="event_ts < 10").read_all().num_rows == 10
        assert reader.scan(start_offset=through - 3).read_all().num_rows == 4

    assert read == []
