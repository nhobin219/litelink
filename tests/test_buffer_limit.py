"""Committed occupancy, all-or-nothing admission and staging-first recovery."""

from __future__ import annotations

import multiprocessing
import os
import sqlite3
import threading
from contextlib import nullcontext
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink
from litelink import BufferUsage, LogConfig, LogFull, _buffer
from litelink._buffer import BUFFER_USAGE_VERSION_KEY, Buffer
from litelink._layout import Layout
from litelink._sqlite import SqliteBackend
from tests.conftest import sqlite_backend

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from pathlib import Path

    from litelink._backend import Cursor

SCHEMA = pa.schema(
    [pa.field("n", pa.int64(), nullable=False), pa.field("text", pa.string())]
)
ROW = {"n": 1, "text": "é"}
# Offset (8), int64 (8), string slot (4), validity bits rounded up (1),
# plus UTF-8 payload (2).
ROW_BYTES = 23


def create(root: Path, **kwargs: object) -> litelink.WriteHandle:
    return litelink.new(root, "s", schema=SCHEMA, config=replace(LogConfig(), **kwargs))


def legacy(root: Path) -> tuple[Path, BufferUsage]:
    with create(root) as log:
        log.extend([ROW, {"n": 2, "text": None}])
        expected = log.buffer_usage()

    path = Layout(root, "s").buffer_db
    with sqlite3.connect(path) as con:
        con.execute(
            "DELETE FROM meta WHERE k IN ('buffer_bytes', 'buffer_rows', 'buffer_usage_version')"
        )

    return path, expected


def crash_migration(path: Path, after_commit: bool) -> None:
    original = _buffer._write_meta

    def crash(con: Cursor, sql: Callable[[str], str], pairs: Mapping[str, str]) -> None:
        original(con, sql, pairs)
        if BUFFER_USAGE_VERSION_KEY in pairs:
            os._exit(79)

    if after_commit:
        Buffer._seed_group = lambda self: os._exit(79)
    else:
        pytest.MonkeyPatch().setattr(_buffer, "_write_meta", crash)

    Buffer.open(SqliteBackend.open(path), SCHEMA, name="s")


@pytest.mark.parametrize("cap", ["bytes", "rows"])
def test_refuses_whole_batch_at_either_cap_and_readmits(
    tmp_path: Path, cap: str
) -> None:
    limits = {"buffer_max_bytes": 2 * ROW_BYTES, "target_seal_size": 1}
    if cap == "rows":
        limits.update(buffer_max_bytes=1024, buffer_max_rows=2)

    with create(tmp_path, **limits) as log:
        assert log.append(ROW) == 1
        usage = log.buffer_usage()
        assert usage.bytes == ROW_BYTES
        assert not usage.refusing
        with pytest.raises(LogFull):
            log.extend(iter([ROW, ROW]))

        assert log.buffered_rows() == 1
        assert log.buffer_usage() == usage
        assert log.end_offset() == 2
        assert log.append(ROW) == 2  # Equality is admitted.
        assert log.buffer_usage().refusing
        with pytest.raises(LogFull):
            log.append(ROW)

        log.seal(flush=True)
        log.publish(flush=True)
        log.evict("buffer")
        assert log.buffer_usage().bytes == log.buffer_usage().rows == 0
        assert not log.buffer_usage().refusing
        assert log.append(ROW) == 3


@pytest.mark.parametrize("cap", ["bytes", "rows"])
@pytest.mark.parametrize("joined", [False, True])
def test_refused_stream_rolls_back_rows_groups_and_offsets(
    tmp_path: Path, cap: str, joined: bool
) -> None:
    limits = {
        "buffer_max_bytes": 2 * ROW_BYTES,
        "target_seal_size": 1,
        "target_seal_rows": 1,
    }
    if cap == "rows":
        limits.update(buffer_max_bytes=1024, buffer_max_rows=2)

    with create(tmp_path, **limits) as log:
        backend = sqlite_backend(log)
        assert log.append(ROW) == 1

        def state() -> dict[str, object]:
            tables = ("buffer", "extent", "meta", "sqlite_sequence")
            if tx is not None:
                return {
                    table: tx.execute(f"SELECT * FROM {table} ORDER BY rowid")
                    for table in tables
                }

            with backend.read() as cursor:
                return {
                    table: cursor.execute(
                        f"SELECT * FROM {table} ORDER BY rowid"
                    ).fetchall()
                    for table in tables
                }

        with log.transaction() if joined else nullcontext() as tx:
            if tx is not None:
                tx.execute("CREATE TABLE caller_state (n INTEGER)")
                tx.execute("INSERT INTO caller_state VALUES (1)")

            before = state()

            def rows() -> Iterator[Mapping[str, object]]:
                yield ROW
                with backend.read() as cursor:
                    assert (
                        cursor.execute("SELECT count(*) FROM buffer").fetchone()[0] == 2
                    )

                # Another connection sees only committed rows even while the
                # refused batch provisionally reaches beyond the cap.
                yield ROW
                with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as other:
                    assert (
                        other.execute("SELECT count(*) FROM buffer").fetchone()[0] == 1
                    )

            with pytest.raises(LogFull):
                log.extend(rows(), tx=tx)

            assert state() == before
            if tx is not None:
                assert tx.execute("SELECT n FROM caller_state") == [(1,)]
                tx.execute("INSERT INTO caller_state VALUES (2)")

            # The failed append neither consumes offsets nor poisons its caller.
            assert log.append(ROW, tx=tx) == 2

        assert log.buffer_usage().rows == 2
        assert log.buffer_usage().bytes == 2 * ROW_BYTES


def test_append_reads_and_updates_both_counters_once(tmp_path: Path) -> None:
    with create(tmp_path) as log:
        backend = sqlite_backend(log)
        statements: list[str] = []
        backend._writer.set_trace_callback(statements.append)
        try:
            assert log.extend(iter([ROW, ROW, ROW])) == [1, 2, 3]
        finally:
            backend._writer.set_trace_callback(None)

        counter_sql = [
            s for s in statements if "'buffer_bytes'" in s or "'buffer_rows'" in s
        ]
        assert len(counter_sql) == 2
        assert counter_sql[0].startswith("SELECT ")
        assert counter_sql[1].startswith("UPDATE meta ")
        assert all("'buffer_rows'" in s for s in counter_sql)
        assert log.buffer_usage().rows == 3
        assert log.buffer_usage().bytes == 3 * ROW_BYTES


@pytest.mark.parametrize("enabled", [False, True])
def test_preparation_streams_with_counters_enabled_or_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    monkeypatch.setattr(_buffer, "_BUFFER_USAGE_ENABLED", enabled)
    with create(tmp_path) as log:
        backend = sqlite_backend(log)
        statements: list[str] = []

        def rows() -> Iterator[Mapping[str, object]]:
            yield ROW
            # A streaming append inserts the first row before asking for the
            # next. Materializing preparation retains the whole batch instead.
            with backend.read() as cursor:
                assert cursor.execute("SELECT count(*) FROM buffer").fetchone()[0] == 1

            yield ROW

        backend._writer.set_trace_callback(statements.append)
        try:
            assert log.extend(rows()) == [1, 2]
        finally:
            backend._writer.set_trace_callback(None)

        counter_sql = [
            s for s in statements if "'buffer_bytes'" in s or "'buffer_rows'" in s
        ]
        assert len(counter_sql) == (2 if enabled else 0)
        assert log.buffered_rows() == 2


def test_savepoint_rollback_and_outer_rollback_include_counters(tmp_path: Path) -> None:
    with create(tmp_path, buffer_max_rows=2) as log:
        with log.transaction() as tx:
            log.append(ROW, tx=tx)
            with pytest.raises(LogFull):
                log.extend([ROW, ROW], tx=tx)

            # A refused savepoint leaves the caller's transaction usable.
            log.append(ROW, tx=tx)

        assert log.buffer_usage().rows == 2
        log.set_config(replace(log.config, buffer_max_rows=3))
        with pytest.raises(RuntimeError, match="abort"):
            with log.transaction() as tx:
                log.append(ROW, tx=tx)
                raise RuntimeError("abort")

        assert log.buffer_usage().rows == 2
        assert log.buffer_usage().bytes == 2 * ROW_BYTES
        log.set_config(replace(log.config, buffer_max_rows=None))
        with pytest.raises(ValueError):
            log.extend([ROW, {"n": "wrong"}])

        assert log.buffer_usage().rows == 2


def test_eviction_counts_actual_rows_and_is_idempotent(tmp_path: Path) -> None:
    with create(tmp_path) as log:
        log.extend([ROW] * 5)
        log.seal(flush=True)
        log.publish(flush=True)
        log.evict("buffer", start_offset=2, end_offset=4)
        assert log.buffer_usage().bytes == 3 * ROW_BYTES
        assert log.buffer_usage().rows == 3
        log.evict("buffer", start_offset=2, end_offset=4)
        assert log.buffer_usage().rows == 3
        log.evict("buffer")
        assert log.buffer_usage().bytes == log.buffer_usage().rows == 0


@pytest.mark.parametrize("after_commit", [False, True])
def test_migration_process_crash_and_no_repeat_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, after_commit: bool
) -> None:
    path, expected = legacy(tmp_path)
    child = multiprocessing.get_context("fork").Process(
        target=crash_migration, args=(path, after_commit)
    )
    child.start()
    child.join(timeout=10)
    assert child.exitcode == 79
    with sqlite3.connect(path) as con:
        marker = con.execute(
            "SELECT v FROM meta WHERE k = ?", (BUFFER_USAGE_VERSION_KEY,)
        ).fetchone()
        assert (marker is not None) == after_commit
        assert con.execute("SELECT count(*) FROM buffer").fetchone()[0] == 2

    with litelink.open(tmp_path, "s") as log:
        assert log.buffer_usage() == expected

    def forbidden(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        pytest.fail("migration scanned an already initialized log")

    monkeypatch.setattr(Buffer, "_count_rows", forbidden)
    with litelink.open(tmp_path, "s", read_only=True) as reader:
        assert reader.buffer_usage() == expected

    with litelink.open(tmp_path, "s") as log:
        assert log.buffer_usage() == expected


def test_two_openers_migrate_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, expected = legacy(tmp_path)
    backends = [SqliteBackend.open(path), SqliteBackend.open(path)]
    scanning, contender = threading.Event(), threading.Event()
    original = Buffer._count_rows
    calls = []
    errors = []

    def count(self, *args):  # noqa: ANN001, ANN002, ANN202
        calls.append(1)
        scanning.set()
        assert contender.wait(5)
        return original(self, *args)

    def trace(statement: str) -> None:
        if statement == "BEGIN IMMEDIATE":
            contender.set()

    backends[1]._writer.set_trace_callback(trace)
    monkeypatch.setattr(Buffer, "_count_rows", count)

    def first() -> None:
        try:
            buffer = Buffer.open(backends[0], SCHEMA, name="s")
            assert buffer.buffer_usage() == expected
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=first)
    thread.start()
    assert scanning.wait(5)
    try:
        buffer = Buffer.open(backends[1], SCHEMA, name="s")
        assert buffer.buffer_usage() == expected
    finally:
        thread.join(timeout=10)
        for backend in backends:
            backend.close()

    assert not thread.is_alive()
    assert errors == []
    assert len(calls) == 1


@pytest.mark.parametrize("everything", [False, True])
def test_staging_first_does_not_strand_a_full_buffer(
    tmp_path: Path, everything: bool
) -> None:
    with create(
        tmp_path,
        target_seal_size=4 * ROW_BYTES,
        target_seal_rows=2,
        target_compact_size=1,
        buffer_max_bytes=4 * ROW_BYTES,
        staging_retention=timedelta(0),
    ) as log:
        log.extend([ROW] * 4)
        log.seal(flush=True)
        log.publish(flush=True)
        assert log.buffer_usage().refusing
        files = log._table.data_files()
        assert len(files) == 2
        with pytest.raises(LogFull):
            log.append(ROW)

        log._maintenance.evict(everything=everything)
        assert log._table.data_files() == files
        log.evict("buffer", end_offset=2)
        log._maintenance.evict(everything=everything)
        assert (
            log._table.data_files() == files
        )  # The first file still straddles the point.
        log.evict("buffer", end_offset=3)
        log._maintenance.evict(everything=everything)
        assert min(f.start for f in log._table.data_files()) == 3
        assert log.buffer_usage().rows == 2
        log.evict("buffer")
        log._maintenance.evict(everything=everything)
        assert log._table.data_files() == []
        assert not log.buffer_usage().refusing
        assert log.append(ROW) == 5


def test_nested_and_null_measurement_survives_migration(tmp_path: Path) -> None:
    schema = pa.schema([pa.field("items", pa.list_(pa.string()))])
    with litelink.new(tmp_path, "s", schema=schema) as log:
        log.extend([{"items": ["é", None, "abc"]}, {"items": None}])
        expected = log.buffer_usage()

    with sqlite3.connect(Layout(tmp_path, "s").buffer_db) as con:
        con.execute("DELETE FROM meta WHERE k = 'buffer_usage_version'")

    with litelink.open(tmp_path, "s") as log:
        assert log.buffer_usage() == expected
        log._buffer.evict_rows(1, 2)
        remaining = log.buffer_usage()
        assert remaining.rows == 1
        assert 0 < remaining.bytes < expected.bytes


def test_configuration_and_live_usage(tmp_path: Path) -> None:
    assert LogConfig.from_json("{}").buffer_max_bytes == 8 * 1024**3
    assert LogConfig.from_json("{}").buffer_max_rows is None
    config = LogConfig(buffer_max_rows=17)
    assert LogConfig.from_json(config.to_json()) == config
    with (
        create(tmp_path) as log,
        litelink.open(tmp_path, "s", read_only=True) as reader,
    ):
        log.append(ROW)
        log.set_config(replace(log.config, buffer_max_rows=1))
        assert reader.buffer_usage().refusing
        assert reader.buffer_usage().buffer_max_rows == 1
        assert reader.buffer_usage().buffer_max_bytes == 8 * 1024**3
        log.set_config(replace(log.config, buffer_max_rows=None))
        assert not reader.buffer_usage().refusing
        with pytest.raises(ValueError, match="target_seal_size"):
            log.set_config(
                replace(log.config, buffer_max_bytes=log.config.target_seal_size - 1)
            )

        with pytest.raises(ValueError, match="buffer_max_rows"):
            log.set_config(replace(log.config, buffer_max_rows=0))


def test_benchmark_switch_forces_recount_on_normal_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with create(tmp_path, buffer_max_rows=1) as log:
        log.append(ROW)

    with monkeypatch.context() as patch:
        patch.setattr(_buffer, "_BUFFER_USAGE_ENABLED", False)
        with litelink.open(tmp_path, "s") as log:
            log.extend([ROW, ROW])
            with pytest.raises(RuntimeError, match="counters enabled"):
                log.buffer_usage()

    with litelink.open(tmp_path, "s") as log:
        assert log.buffer_usage().rows == 3
        assert log.buffer_usage().bytes == 3 * ROW_BYTES
        with pytest.raises(LogFull):
            log.append(ROW)


def crash_write(path: Path, operation: str) -> None:
    original = Buffer._change_usage

    def crash(self: Buffer, cursor: Cursor, size: int, rows: int) -> None:
        original(self, cursor, size, rows)
        os._exit(79)

    buffer = Buffer.open(SqliteBackend.open(path), SCHEMA, name="s")
    Buffer._change_usage = crash
    if operation == "append":
        buffer.append([ROW])
    else:
        buffer.evict_rows(1, 2)


@pytest.mark.parametrize("operation", ["append", "evict"])
def test_process_crash_rolls_back_rows_and_counters(
    tmp_path: Path, operation: str
) -> None:
    with create(tmp_path) as log:
        log.extend([ROW, ROW])
        expected = log.buffer_usage()

    child = multiprocessing.get_context("fork").Process(
        target=crash_write, args=(Layout(tmp_path, "s").buffer_db, operation)
    )
    child.start()
    child.join(timeout=10)
    assert child.exitcode == 79
    with litelink.open(tmp_path, "s") as log:
        assert log.buffer_usage() == expected
        assert log._buffer.count_from(0) == 2
        assert log.end_offset() == 3


def test_competing_appenders_cannot_both_admit_the_last_row(tmp_path: Path) -> None:
    with (
        create(tmp_path, buffer_max_rows=1) as first,
        litelink.open(tmp_path, "s") as second,
    ):
        barrier = threading.Barrier(2)
        results = []
        errors = []

        def append(log: litelink.WriteHandle) -> None:
            try:
                barrier.wait(timeout=5)
                try:
                    log.append(ROW)
                    results.append("admitted")
                except LogFull:
                    results.append("full")
            except BaseException as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=append, args=(log,)) for log in (first, second)
        ]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join(timeout=10)
            assert not thread.is_alive()

        assert errors == []
        assert sorted(results) == ["admitted", "full"]
        assert first.buffer_usage().rows == second.buffer_usage().rows == 1
        assert first.buffer_usage().bytes == ROW_BYTES


def test_readonly_legacy_usage_requires_writable_migration(tmp_path: Path) -> None:
    _, expected = legacy(tmp_path)
    with litelink.open(tmp_path, "s", read_only=True) as reader:
        with pytest.raises(RuntimeError, match="opening the log for writing"):
            reader.buffer_usage()

    with litelink.open(tmp_path, "s") as log:
        assert log.buffer_usage() == expected
