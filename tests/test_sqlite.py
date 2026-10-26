"""SQLite's production write guard survives caller SQL and failed writes."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import litelink
from litelink._sqlite import SqliteBackend
from tests.conftest import sqlite_backend

if TYPE_CHECKING:
    from pathlib import Path


def test_writer_refuses_statements_outside_write(tmp_path: Path) -> None:
    """Falsify by dropping the factory's final query_only = ON."""
    backend = SqliteBackend.open(tmp_path / "buffer.db")
    try:
        with (
            backend.read() as cursor,
            pytest.raises(sqlite3.OperationalError, match="readonly"),
        ):
            cursor.execute("CREATE TABLE mylib_seen (n INTEGER)")
    finally:
        backend.close()


def test_caller_statement_does_not_remove_the_write_guard(tmp_path: Path) -> None:
    """Falsify with an authorizer guard: caller SQL removes that authorizer."""
    with litelink.new(tmp_path, "s", schema=pa.schema([("n", pa.int64())])) as log:
        with log.transaction() as tx:
            tx.execute("CREATE TABLE mylib_seen (n INTEGER)")

        with (
            sqlite_backend(log).read() as cursor,
            pytest.raises(sqlite3.OperationalError, match="readonly"),
        ):
            cursor.execute("INSERT INTO mylib_seen VALUES (1)")


def test_failed_write_restores_the_guard(tmp_path: Path) -> None:
    """Falsify by dropping query_only = ON on the exceptional exit."""
    backend = SqliteBackend.open(tmp_path / "buffer.db")
    try:
        with pytest.raises(ValueError, match="body failed"), backend.write() as cursor:
            cursor.execute("CREATE TABLE mylib_seen (n INTEGER)")
            raise ValueError("body failed")

        with (
            backend.read() as cursor,
            pytest.raises(sqlite3.OperationalError, match="readonly"),
        ):
            cursor.execute("CREATE TABLE mylib_seen (n INTEGER)")
    finally:
        backend.close()
