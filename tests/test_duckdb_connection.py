"""`litelink.duckdb_connection`: a connection provisioned to read a published
table from any machine (#108).

What it loads, and from where, is the contract: `avro` and `iceberg` always,
`httpfs` and the S3 secret only when asked, never fetched from the network,
and a missing extension raised as `ExtensionMissing` naming the fix.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


def test_s3_options_load_httpfs_and_the_secret_from_them() -> None:
    """Explicit `s3_options`: the secret carries them, in
    DuckDB's spelling (host:port, with the scheme as `USE_SSL`).

    Falsify by building the secret from `S3Options()` instead of `s3_options`: the
    endpoint is the environment's, not the one passed.
    """
    options = litelink.S3Options(
        endpoint="http://127.0.0.1:9000",
        access_key="key",
        secret_key="secret",
        region="eu-west-2",
    )
    connection = litelink.duckdb_connection(s3_options=options)

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


def test_empty_s3_options_read_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """With `S3Options()`, the endpoint and region come from the environment and the
    keys from the credential chain — here a profile in a credentials file — as
    the log's own read path does. The chain secret refreshes itself.

    Falsify by requiring explicit keys in `s3_options`, or by dropping `REFRESH auto`
    from `secret_sql`: this raises, or the refresh assertion fails.
    """
    aws_environment(monkeypatch, tmp_path, credentials=True)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9123")
    monkeypatch.setenv("AWS_REGION", "ap-south-1")
    connection = litelink.duckdb_connection(s3_options=litelink.S3Options())

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
        litelink.duckdb_connection(s3_options=litelink.S3Options())

    assert "AWS_ACCESS_KEY_ID" in str(caught.value)
    assert isinstance(caught.value.__cause__, duckdb.Error)


def test_current_metadata_refuses_a_location_with_no_hint(tmp_path: Path) -> None:
    """No hint means nothing published there, which is an answer, not a path
    to scan."""
    with pytest.raises(FileNotFoundError, match="version-hint.text"):
        litelink.current_metadata(f"file://{tmp_path}/nothing")


def test_a_credential_chain_loads_aws_itself_without_autoloading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A chain secret needs DuckDB's `aws` extension, and litelink loads it.

    Left to DuckDB, creating a `credential_chain` secret autoinstalls `aws`:
    online that is a silent download the bundling exists to prevent, and
    offline it fails, which `create_secret` then reported as missing
    credentials while the profile was right there. A checkout never showed it,
    because its DuckDB home already held `aws` and autoload found it.

    So DuckDB's autoinstall and autoload are switched off here, and the secret
    is created from a profile: it works only if litelink loads `aws` itself.

    Falsify by dropping the `aws` load from `load_s3_extensions`: the secret
    fails to create and this raises.
    """
    aws_environment(monkeypatch, tmp_path, credentials=True)
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    real_connect = duckdb.connect

    def connect(*args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        config = {
            **kwargs.pop("config", {}),
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        }
        return real_connect(*args, config=config, **kwargs)

    monkeypatch.setattr(duckdb, "connect", connect)
    connection = litelink.duckdb_connection(s3_options=litelink.S3Options())

    assert "aws" in loaded(connection)
    assert "provider=credential_chain" in secrets(connection)["litelink_s3"]

    # Explicit keys need only `httpfs`, so `aws` is not loaded for them.
    keyed = litelink.duckdb_connection(
        s3_options=litelink.S3Options(access_key="key", secret_key="secret")
    )
    assert "aws" not in loaded(keyed)


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


def settings(connection: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = connection.execute(
        "SELECT name, value FROM duckdb_settings() WHERE name IN ("
        "'enable_external_file_cache', 'cache_httpfs_type',"
        " 'cache_httpfs_cache_directory', 'cache_httpfs_min_disk_bytes_for_cache',"
        " 'cache_httpfs_disk_cache_reader_enable_memory_cache')"
    ).fetchall()

    return {str(name): str(value) for name, value in rows}


OPTIONS = litelink.S3Options(
    endpoint="http://127.0.0.1:9000", access_key="key", secret_key="secret"
)


def test_a_remote_connection_caches_in_memory_only_by_default() -> None:
    """Memory on, disk off (#118). A disk cache is for a reader on another
    machine, which asks for it; on a log's own host it would put back on disk
    what eviction removed.

    Falsify by defaulting `disk_cache` to True: `cache_httpfs` loads.
    """
    connection = litelink.duckdb_connection(s3_options=OPTIONS)

    assert "cache_httpfs" not in loaded(connection)
    assert settings(connection)["enable_external_file_cache"] == "true"


def test_a_disk_cache_is_layered_under_memory_in_the_keyed_directory(
    isolated_read_cache: Path,
) -> None:
    """With `disk_cache=True`: `cache_httpfs` on disk in the key's directory,
    DuckDB's memory cache switched back ON after the extension switches it
    off, and the volume floor set from the limit rather than the extension's
    5% (#118).

    Falsify by dropping the `enable_external_file_cache` line from
    `install_read_cache`: it reads false.
    """
    import shutil

    connection = litelink.duckdb_connection(
        s3_options=OPTIONS, disk_cache=True, cache_key="stream-1"
    )

    found = settings(connection)
    expected = isolated_read_cache / "litelink" / "stream-1"
    assert "cache_httpfs" in loaded(connection)
    assert found["cache_httpfs_type"] == "on_disk"
    assert found["cache_httpfs_cache_directory"] == str(expected)
    assert expected.is_dir()
    assert found["enable_external_file_cache"] == "true"
    assert found["cache_httpfs_disk_cache_reader_enable_memory_cache"] == "true"
    # The version hint is never cached on disk (#141), and is excluded once.
    exclusions = connection.execute(
        "SELECT * FROM cache_httpfs_list_exclusion_regex()"
    ).fetchall()
    assert exclusions == [(r".*/metadata/version-hint\.text$",)]
    floor = int(found["cache_httpfs_min_disk_bytes_for_cache"])
    assert floor == int(shutil.disk_usage(expected).total * (1 - 0.8))


def test_each_cache_layer_turns_off_on_its_own(tmp_path: Path) -> None:
    """`memory_cache=False` turns off every RAM layer, `cache_httpfs`'s own
    read-through cache included.

    Falsify by leaving `cache_httpfs`'s reader cache on when `memory_cache` is
    False: ~128 MB per process of RAM caching the flag claimed to turn off.
    """
    no_memory = litelink.duckdb_connection(
        s3_options=OPTIONS, memory_cache=False, disk_cache=True, cache_key=tmp_path
    )
    found = settings(no_memory)
    assert found["enable_external_file_cache"] == "false"
    assert found["cache_httpfs_disk_cache_reader_enable_memory_cache"] == "false"
    assert found["cache_httpfs_cache_directory"] == str(tmp_path)


@pytest.mark.parametrize("limit", [0.0, -0.1, 1.5])
def test_a_volume_limit_outside_zero_to_one_is_refused(limit: float) -> None:
    """How full the cache's volume may get is a share; 0 would evict
    everything at once and above 1 can never be reached."""
    with pytest.raises(ValueError, match="disk_cache_volume_limit"):
        litelink.duckdb_connection(
            s3_options=OPTIONS, disk_cache=True, disk_cache_volume_limit=limit
        )


def test_a_local_connection_caches_in_memory_and_honours_the_flag() -> None:
    """Only an S3 published table goes through httpfs, so a local connection
    loads no disk cache; the memory cache is on, and `memory_cache=False`
    turns it off.

    Falsify by applying `memory_cache` only to a remote connection: the second
    connection reads true.
    """
    connection = litelink.duckdb_connection()

    assert "cache_httpfs" not in loaded(connection)
    assert settings(connection)["enable_external_file_cache"] == "true"

    off = litelink.duckdb_connection(memory_cache=False)
    assert settings(off)["enable_external_file_cache"] == "false"


def test_a_disk_cache_without_s3_options_is_refused() -> None:
    """The disk cache wraps httpfs, which a local connection never loads, so
    asking for one there is refused rather than silently ignored.

    Falsify by dropping the check: the call returns a connection with no cache.
    """
    with pytest.raises(ValueError, match="s3_options"):
        litelink.duckdb_connection(disk_cache=True)


def test_the_cache_key_names_the_directory(
    tmp_path: Path, isolated_read_cache: Path
) -> None:
    """A relative key is a directory under the root, an absolute one is used
    as given, and none is `default` — a sibling of the keyed directories,
    never their parent, so no cache's eviction reaches another's (#118). A
    key that climbs out of the root is refused.

    Falsify by mapping None to the root itself: `default` is missing and every
    keyed directory sits inside the unkeyed cache.
    """
    from litelink._read import cache_directory

    root = isolated_read_cache / "litelink"
    assert cache_directory("stream-uuid") == root / "stream-uuid"
    assert cache_directory(tmp_path / "volume") == tmp_path / "volume"
    assert cache_directory() == root / "default"
    with pytest.raises(ValueError, match="outside"):
        cache_directory("../elsewhere")
