"""Per-database limits and scoped reads, without a service or object store."""

from __future__ import annotations

import contextlib
import pickle
import weakref
from dataclasses import FrozenInstanceError
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

import litelink
from litelink import DuckDBPolicy, Spill, _handle, _read

SCHEMA = pa.schema([pa.field("value", pa.int64(), nullable=False)])
# Sorting a wide generated relation exceeds 16 MiB but can spill. No large
# durable fixture or Python input allocation is needed to exercise that edge.
OVER_CAP = """
SELECT i, md5(i::VARCHAR) payload
FROM range(300000) t(i) ORDER BY payload
"""


def settings(connection: Any) -> tuple:
    return connection.execute(
        "SELECT current_setting('memory_limit'), current_setting('threads'), "
        "current_setting('temp_directory'), current_setting('max_temp_directory_size'), "
        "current_setting('enable_external_file_cache')"
    ).fetchone()


class TrackedRoot:
    def __init__(self, root: duckdb.DuckDBPyConnection) -> None:
        self.root = root
        self.closes = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.root, name)

    def close(self) -> None:
        self.closes += 1
        self.root.close()


def track_roots(monkeypatch: pytest.MonkeyPatch) -> list[TrackedRoot]:
    roots: list[TrackedRoot] = []
    factory = _handle.duckdb_connection

    def connect(**kwargs: Any) -> Any:
        tracked = TrackedRoot(factory(**kwargs))
        roots.append(tracked)
        return tracked

    monkeypatch.setattr(_handle, "duckdb_connection", connect)
    return roots


def assert_closed(roots: list[TrackedRoot]) -> None:
    assert len(roots) == 1
    assert roots[0].closes == 1
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        roots[0].execute("SELECT 1")


def test_policy_applied_at_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = DuckDBPolicy(64, 1, Spill(tmp_path / "spill", 128), memory_cache=False)
    expected = ("64.0 MiB", 1, str(tmp_path / "spill"), "128.0 MiB", False)
    load = _read.load_extension
    seen = []

    def load_checked(connection: Any, name: str, *, remote: bool) -> None:
        # In particular, do not SET memory_limit after LOAD iceberg.
        assert settings(connection) == expected
        seen.append(name)
        load(connection, name, remote=remote)

    monkeypatch.setattr(_read, "load_extension", load_checked)
    with contextlib.closing(litelink.duckdb_connection(duckdb=policy)) as root:
        assert settings(root) == expected
        with contextlib.closing(root.cursor()) as cursor:
            assert settings(cursor) == expected

    assert seen == ["avro", "iceberg"]
    assert not (tmp_path / "spill").exists()
    assert pickle.loads(pickle.dumps(policy)) == policy
    with pytest.raises(FrozenInstanceError):
        object.__getattribute__(policy, "__setattr__")("threads", 2)


def test_open_new_restore_readers_use_policy(tmp_path: Path) -> None:
    policy = DuckDBPolicy(64, 1, memory_cache=False)
    expected = ("64.0 MiB", 1, "", "0 bytes", False)
    prefix = (tmp_path / "published").as_uri()
    with litelink.new(
        tmp_path / "source", "s", schema=SCHEMA, published=prefix, duckdb=policy
    ) as log:
        assert log._reader._connection is None
        log.extend([{"value": 10}, {"value": 20}])
        assert log.sql("SELECT count(*) FROM log").read_all().column(0).to_pylist() == [
            2
        ]
        assert settings(log._reader._connection) == expected
        log.seal(flush=True)
        log.publish(flush=True)

    for read_only in (False, True):
        with litelink.open(
            tmp_path / "source", "s", read_only=read_only, duckdb=policy
        ) as log:
            assert log._reader._connection is None
            assert log.sql("SELECT count(*) FROM log").read_all().column(
                0
            ).to_pylist() == [2]
            assert settings(log._reader._connection) == expected

    with litelink.WriteHandle.open(tmp_path / "source", "s", duckdb=policy) as log:
        assert log.sql("SELECT count(*) FROM log").read_all().column(0).to_pylist() == [
            2
        ]
        assert settings(log._reader._connection) == expected

    # This covers the real published-only restore, including its cls.open hop.
    with litelink.restore(
        tmp_path / "restored", "s", published=prefix, duckdb=policy
    ) as log:
        assert log.sql("SELECT count(*) FROM log").read_all().column(0).to_pylist() == [
            2
        ]
        assert settings(log._reader._connection) == expected


def test_query_once_closes_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = DuckDBPolicy(64, 1)
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=litelink.LogConfig(
            staging_retention=timedelta(0), target_compact_size=1
        ),
    ) as log:
        log.extend([{"value": 10}, {"value": 20}])
        log.seal(flush=True)
        log.publish(flush=True)
        log.evict("buffer")
        log.evict("staging")
        log.append({"value": 30})
        assert log.staging_rows() == 0
        roots = track_roots(monkeypatch)
        result = log.query_once("SELECT sum(value) total FROM log", duckdb=policy)
        assert isinstance(result, pa.Table)
        assert result.to_pylist() == [{"total": 60}]
        assert_closed(roots)
        assert log._reader._connection is None
        roots.clear()
        local = log.query_once(
            "SELECT sum(value) total FROM log", duckdb=policy, published=False
        )
        assert local.to_pylist() == [{"total": 30}]
        assert_closed(roots)
        assert log._reader._connection is None
        roots.clear()
        with pytest.raises(duckdb.BinderException):
            log.query_once("SELECT missing FROM log", duckdb=policy)

        assert_closed(roots)


def test_query_once_over_cap_raises_and_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with litelink.new(tmp_path, "s", schema=SCHEMA) as log:
        roots = track_roots(monkeypatch)
        with pytest.raises(duckdb.OutOfMemoryException):
            log.query_once(OVER_CAP, duckdb=DuckDBPolicy(16, 1))

        assert_closed(roots)
        assert log._reader._connection is None


def test_spill_lets_over_cap_query_finish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = DuckDBPolicy(16, 1, Spill(tmp_path / "spill", 128), memory_cache=False)
    with litelink.new(tmp_path, "s", schema=SCHEMA) as log:
        roots = track_roots(monkeypatch)
        result = log.query_once(OVER_CAP, duckdb=policy)
        assert result.num_rows == 300000
        assert_closed(roots)


def test_spill_needs_existing_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "missing-parent" / "spill"
    policy = DuckDBPolicy(16, 1, Spill(directory, 128))
    with contextlib.closing(litelink.duckdb_connection(duckdb=policy)) as root:
        assert root.execute("SELECT 1").fetchone() == (1,)
        assert not directory.parent.exists()

    with litelink.new(tmp_path, "s", schema=SCHEMA) as log:
        roots = track_roots(monkeypatch)
        with pytest.raises(duckdb.IOException, match="Failed to create directory"):
            log.query_once(OVER_CAP, duckdb=policy)

        assert_closed(roots)
        assert not directory.parent.exists()
        assert log._reader._connection is None


def test_bound_default_connection() -> None:
    prior = duckdb.default_connection()
    try:
        litelink.bound_default_connection(DuckDBPolicy(16, 1, memory_cache=False))
        assert duckdb.sql(
            "SELECT current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('temp_directory'), current_setting('enable_external_file_cache')"
        ).fetchone() == ("16.0 MiB", 1, "", False)
    finally:
        replacement = duckdb.default_connection()
        duckdb.set_default_connection(prior)
        replacement.close()


def test_bound_default_connection_releases_old() -> None:
    prior = duckdb.default_connection()
    old = duckdb.connect()
    duckdb.set_default_connection(old)
    reference = weakref.ref(old)
    try:
        litelink.bound_default_connection(DuckDBPolicy(16, 1))
        assert duckdb.default_connection() is not old
        assert settings(duckdb.default_connection())[:2] == ("16.0 MiB", 1)
        # The helper releases ownership rather than closing caller aliases.
        assert old.execute("SELECT 42").fetchone() == (42,)
        del old
        assert reference() is None
    finally:
        replacement = duckdb.default_connection()
        duckdb.set_default_connection(prior)
        replacement.close()


def test_none_keeps_duckdb_defaults() -> None:
    with contextlib.closing(duckdb.connect()) as raw:
        expected = settings(raw)

    with contextlib.closing(litelink.duckdb_connection()) as root:
        assert settings(root) == expected

    with contextlib.closing(
        litelink.duckdb_connection(duckdb=None, memory_cache=False)
    ) as root:
        assert settings(root) == (*expected[:4], False)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"memory_limit_mib": 15},
        {"memory_limit_mib": True},
        {"memory_limit_mib": "64"},
        {"memory_limit_mib": 16.0},
        {"threads": 0},
        {"threads": True},
        {"threads": 1.0},
        {"spill": {}},
        {"memory_cache": 1},
    ],
)
def test_policy_rejects_bad_values(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        values: dict[str, Any] = {"memory_limit_mib": 16, "threads": 1}
        values.update(kwargs)
        DuckDBPolicy(**values)


@pytest.mark.parametrize(
    "directory,max_mib",
    [("spill", 1), (Path("spill"), 0), (Path("spill"), True), (Path("spill"), 1.5)],
)
def test_spill_rejects_bad_values(directory: Any, max_mib: Any) -> None:
    with pytest.raises(ValueError):
        Spill(directory, max_mib)


def test_extension_failure_closes_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened = []
    connect = duckdb.connect

    def tracked(**kwargs: Any) -> Any:
        root = connect(**kwargs)
        opened.append(root)
        return root

    def fail(connection: Any, name: str, *, remote: bool) -> None:
        raise RuntimeError("extension failure")

    monkeypatch.setattr(duckdb, "connect", tracked)
    monkeypatch.setattr(_read, "load_extension", fail)
    with pytest.raises(RuntimeError, match="extension failure"):
        litelink.duckdb_connection(duckdb=DuckDBPolicy(16, 1))

    assert len(opened) == 1
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        opened[0].execute("SELECT 1")
