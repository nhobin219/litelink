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
    assert "install_s3_secret" in litelink.__all__
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


def aws_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, credentials: bool
) -> None:
    """An AWS environment this test controls, whatever the machine has.

    The credential chain reads keys from the environment, a credentials file,
    a config file and instance metadata. Each is pinned here: no keys, files of
    our own, metadata off. With `credentials`, the credentials file holds a
    profile the chain finds; without, there is nothing anywhere — which is CI,
    and where the chain secret first failed (#109 review).
    """
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_ENDPOINT_URL",
        "AWS_REGION",
    ):
        monkeypatch.delenv(name, raising=False)

    empty = tmp_path / "aws-config"
    empty.write_text("")
    keys = tmp_path / "aws-credentials"
    keys.write_text(
        "[default]\naws_access_key_id = AKIDEXAMPLE\n"
        "aws_secret_access_key = SECRETEXAMPLE\n"
        if credentials
        else ""
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(empty))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(keys))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


def test_remote_without_options_reads_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """With no `s3`, the endpoint and region come from the environment and the
    keys from the credential chain — here a profile in a credentials file — as
    the log's own read path does. The chain secret refreshes itself.

    Falsify by requiring `s3` when `remote=True`, or by dropping `REFRESH auto`
    from `secret_sql`: this raises, or the refresh assertion fails.
    """
    aws_environment(monkeypatch, tmp_path, credentials=True)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9123")
    monkeypatch.setenv("AWS_REGION", "ap-south-1")
    connection = litelink.duckdb_connection(remote=True)

    found = secrets(connection)["litelink_s3"]
    assert "provider=credential_chain" in found
    assert "127.0.0.1:9123" in found
    assert "ap-south-1" in found
    assert "'refresh': auto" in found


def test_no_credentials_anywhere_names_the_fix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """DuckDB refuses a credential chain that finds nothing, with an error
    that names its chain and not the remedy. The caller gets the remedy.

    Falsify by letting the `duckdb.Error` from `create_secret` through: the
    raw "Secret Validation Failure" reaches the caller.
    """
    aws_environment(monkeypatch, tmp_path, credentials=False)

    with pytest.raises(RuntimeError, match="no S3 credentials were found") as caught:
        litelink.duckdb_connection(remote=True)

    assert "AWS_ACCESS_KEY_ID" in str(caught.value)
    assert isinstance(caught.value.__cause__, duckdb.Error)


def test_a_secret_installed_on_a_connection_reaches_its_cursors() -> None:
    """`install_s3_secret` on a connection litelink did not build — a shared
    database handing out cursors — replaces the secret for every cursor, as
    rotated keys need.

    Built with autoloading off, so `httpfs` arrives only through the explicit
    load — DuckDB would otherwise fetch it from its own home on first use, and
    not from litelink's bundle. Falsify by dropping the `httpfs` load from
    `install_s3_secret`: the first call raises.
    """
    connection = duckdb.connect(config={"autoload_known_extensions": False})
    first = litelink.S3Options(
        endpoint="http://127.0.0.1:9001", access_key="a", secret_key="a"
    )
    second = litelink.S3Options(
        endpoint="http://127.0.0.1:9002", access_key="b", secret_key="b"
    )
    litelink.install_s3_secret(connection, first)
    cursor = connection.cursor()
    assert "127.0.0.1:9001" in secrets(cursor)["litelink_s3"]

    litelink.install_s3_secret(connection, second)

    assert "127.0.0.1:9002" in secrets(cursor)["litelink_s3"]
    assert "httpfs" in loaded(connection)


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
