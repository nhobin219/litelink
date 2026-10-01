"""`litelink.duckdb_connection`: a connection provisioned to read a published
table from any machine (#108).

What it loads, and from where, is the contract: `avro` and `iceberg` always,
`httpfs` and the S3 secret only when asked, never fetched from the network,
and a missing extension raised as `ExtensionMissing` naming the fix.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pytest

import litelink

if TYPE_CHECKING:
    from pathlib import Path


def loaded(connection: duckdb.DuckDBPyConnection) -> set[str]:
    rows = connection.execute(
        "SELECT extension_name FROM duckdb_extensions() WHERE loaded"
    ).fetchall()

    return {str(name) for (name,) in rows}


def secrets(connection: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = connection.execute(
        "SELECT name, secret_string FROM duckdb_secrets()"
    ).fetchall()

    return {str(name): str(text) for name, text in rows}


def test_it_is_public_and_loads_the_iceberg_read_path() -> None:
    """`avro` and `iceberg`, and nothing a local read does not need.

    Falsify by loading `httpfs` unconditionally: a local read starts depending
    on an extension it never uses.
    """
    assert "duckdb_connection" in litelink.__all__
    assert "ExtensionMissing" in litelink.__all__

    connection = litelink.duckdb_connection()

    assert {"avro", "iceberg"} <= loaded(connection)
    assert "httpfs" not in loaded(connection)
    assert secrets(connection) == {}


def test_remote_loads_httpfs_and_the_secret_from_the_options() -> None:
    """`remote=True` with explicit options: the secret carries them, in
    DuckDB's spelling (host:port, with the scheme as `USE_SSL`).

    Falsify by building the secret from `S3Options()` instead of `s3`: the
    endpoint is the environment's, not the one passed.
    """
    options = litelink.S3Options(
        endpoint="http://127.0.0.1:9000",
        access_key="key",
        secret_key="secret",
        region="eu-west-2",
    )
    connection = litelink.duckdb_connection(options, remote=True)

    assert "httpfs" in loaded(connection)
    found = secrets(connection)
    assert set(found) == {"litelink_s3"}
    assert "127.0.0.1:9000" in found["litelink_s3"]
    assert "eu-west-2" in found["litelink_s3"]


def test_remote_without_options_reads_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no `s3`, credentials come from where every AWS tool looks — the
    environment, then the credential chain — as the log's own read path does.

    Falsify by requiring `s3` when `remote=True`: this raises.
    """
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9123")
    monkeypatch.setenv("AWS_REGION", "ap-south-1")
    connection = litelink.duckdb_connection(remote=True)

    found = secrets(connection)["litelink_s3"]
    assert "127.0.0.1:9123" in found
    assert "ap-south-1" in found


def test_options_without_remote_are_refused() -> None:
    """Credentials nobody installs read as anonymous, and that fails as a 403
    at the first query rather than at the call that dropped them.

    Falsify by ignoring `s3` when `remote` is False: no error, and no secret.
    """
    with pytest.raises(ValueError, match="remote=True"):
        litelink.duckdb_connection(litelink.S3Options(region="us-east-1"))


def test_an_unprovisioned_machine_gets_the_message_that_fixes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No bundle, an empty DuckDB home and autoinstall off — the offline
    machine §7 is about. The public factory raises the public error, naming
    the provisioning step rather than DuckDB's `INSTALL` advice.

    Falsify by catching `ExtensionMissing` around the `iceberg` load: the
    caller gets a connection that fails at its first `iceberg_scan` instead.
    """
    import litelink._read as read

    empty = tmp_path / "extensions"
    empty.mkdir()
    real = duckdb.connect

    def offline() -> duckdb.DuckDBPyConnection:
        return real(
            config={
                "extension_directory": str(empty),
                "autoinstall_known_extensions": False,
                "autoload_known_extensions": False,
            }
        )

    monkeypatch.setattr(read, "_bundled_extension", lambda *_: None)
    monkeypatch.setattr(read.duckdb, "connect", offline)

    with pytest.raises(litelink.ExtensionMissing) as caught:
        litelink.duckdb_connection()

    assert "`iceberg` extension is not installed" in str(caught.value)
    assert "just duckdb-extensions" in str(caught.value)
