"""`litelink.replication` and `litelink.column_ddl`: a log's replication and
column checks, as public primitives over any `Replicated`.

The replica tests run the real sidecar against a local endpoint (`just
rustfs`), as the log's flush test does; everything else is local files.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pytest

import litelink
from litelink import S3Options, WriteHandle
from litelink import _replication as internals
from litelink.replication import (
    control_socket,
    flush,
    litestream_binary,
    litestream_config,
    restore_database,
)

# A syntactically valid location. Nothing in the local tests reaches it.
NOWHERE = "s3://bucket/prefix"


@dataclass(frozen=True)
class Databases:
    """A `Replicated` that is not a log's layout."""

    name: str
    directory: Path
    databases: tuple[Path, ...]


def layout_at(root: Path) -> Databases:
    directory = root / "app"
    return Databases("app", directory, (directory / "app.db",))


def write_rows(database: Path, upto: int, start: int = 1) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(database)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)")
        with con:
            con.executemany(
                "INSERT INTO items VALUES (?)", [(i,) for i in range(start, upto + 1)]
            )
    finally:
        con.close()


def count(database: Path) -> tuple[int, int]:
    con = sqlite3.connect(database)
    try:
        return con.execute("SELECT count(*), max(id) FROM items").fetchone()
    finally:
        con.close()


def sidecar_binary() -> str:
    binary = litestream_binary()
    if shutil.which(binary) is None and not os.access(binary, os.X_OK):
        pytest.skip("litestream is not provisioned — run `just litestream`")

    return binary


def recording(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Every process started from `_replication`, `subprocess.run` included."""
    ran: list[list[str]] = []
    popen = subprocess.Popen

    def recorded(command: list[str], *args: object, **kwargs: object) -> object:
        ran.append(list(command))
        return popen(command, *args, **kwargs)  # noqa: S603  # ty: ignore[no-matching-overload]

    monkeypatch.setattr(internals.subprocess, "Popen", recorded)
    return ran


# -- the config is the log's --------------------------------------------------


def _blocks(config: str) -> tuple[str, dict[str, str]]:
    """The config's header (the socket), and each database's block by file."""
    header, *databases = config.split("  - path: ")
    blocks = {}
    for block in databases:
        path, _, body = block.partition("\n")
        blocks[Path(path).name] = body

    return header, blocks


def test_a_replicated_config_is_the_logs_for_the_same_inputs(tmp_path: Path) -> None:
    """Same directory, name, published location, endpoint and retention: the
    socket is the same, and its database gets exactly the block the
    log's buffer gets, with the file name changed — the replica key, the
    snapshot window that prunes, and the endpoint lines.

    Falsify by dropping `snapshot_block` for a non-`Layout`, or keying the
    replica by anything but the path under the directory: the block differs.
    """
    s3 = S3Options(endpoint="http://127.0.0.1:9000", region="us-east-1")
    retention = timedelta(hours=6)
    log = WriteHandle.replication_config_for(tmp_path, "app", NOWHERE, s3, retention)
    its = litestream_config(layout_at(tmp_path), NOWHERE, s3, retention)

    log_header, log_blocks = _blocks(log)
    its_header, its_blocks = _blocks(its)
    assert its_header == log_header
    assert set(its_blocks) == {"app.db"}
    assert its_blocks["app.db"] == log_blocks["buffer.db"].replace(
        "buffer.db", "app.db"
    )
    # The pieces that block carries, so an empty match cannot pass.
    assert "path: prefix/app/_wal/app.db" in its
    assert "retention: 21600s" in its
    assert "interval: 10800s" in its
    assert "secret" not in its.lower()


@pytest.mark.parametrize(
    ("published", "match"),
    [
        ("file:///tmp/prefix", "remote published table"),
        ("s3://a:b/prefix", "in its bucket name"),
    ],
)
def test_the_config_refuses_a_location_it_cannot_ship_to(
    tmp_path: Path, published: str, match: str
) -> None:
    """A local location is not off the machine, and a malformed one would be
    written into the config (`bucket: a:b`) rather than fail.

    Falsify by removing `validate_published` from `litestream_config`: the
    malformed bucket returns a config.
    """
    with pytest.raises(ValueError, match=match):
        litestream_config(layout_at(tmp_path), published)


def test_the_config_refuses_a_database_outside_the_directory(tmp_path: Path) -> None:
    """Its replica key is its path under the directory, so outside it has none."""
    stray = Databases("app", tmp_path / "app", (tmp_path / "elsewhere.db",))
    with pytest.raises(ValueError, match="outside"):
        litestream_config(stray, NOWHERE)


# -- flush --------------------------------------------------------------------


def test_flush_refuses_a_database_it_does_not_replicate(tmp_path: Path) -> None:
    layout = layout_at(tmp_path)
    with pytest.raises(ValueError, match="not one of the databases"):
        flush(layout, layout.directory / "other.db")


def test_flush_without_a_sidecar_raises_and_starts_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """It asks the control socket and, finding nothing there, raises rather
    than start a litestream of its own. Every process it ran is a `sync`
    client.

    Falsify by having `flush` fall back to `litestream replicate` when the
    socket does not answer: a second command appears, or no error.
    """
    binary = sidecar_binary()
    ran = recording(monkeypatch)
    layout = layout_at(tmp_path)
    write_rows(layout.databases[0], 3)
    with pytest.raises(RuntimeError, match="control socket"):
        flush(layout, layout.databases[0], binary=binary, timeout=2)

    assert ran, "flush never asked the sidecar"
    assert all(command[1] == "sync" for command in ran), ran
    assert all("-socket" in command for command in ran), ran


def test_flush_asks_the_socket_it_is_given_and_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`flush(socket=X)` asks `X`, not the socket the config derives, and the
    error names `X` when nothing answers there.

    A sidecar that merges several configs listens on one socket of its own,
    so a flush that kept asking the derived one would never reach it.

    Falsify by ignoring `socket` in `_replication.flush`: the command carries
    the derived path, and the error names it.
    """
    binary = sidecar_binary()
    ran = recording(monkeypatch)
    layout = layout_at(tmp_path)
    write_rows(layout.databases[0], 1)
    given = tmp_path / "merged.sock"
    derived = control_socket(layout)
    assert given != derived
    with pytest.raises(RuntimeError, match="control socket") as raised:
        flush(layout, layout.databases[0], binary=binary, timeout=2, socket=given)

    assert str(given) in str(raised.value)
    assert str(derived) not in str(raised.value)
    assert len(ran) == 1, ran
    command = ran[0]
    assert command[command.index("-socket") + 1] == str(given)
    assert str(derived) not in command


# -- restore ------------------------------------------------------------------


@pytest.mark.parametrize("present", ["app.db", "app.db-wal"])
def test_restore_never_writes_over_a_local_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, present: str
) -> None:
    """A local database is the source of truth, and a stray `-wal` would be
    replayed into the restored file. Refused before anything runs.

    Falsify by removing the refusal: litestream is started.
    """
    ran = recording(monkeypatch)
    layout = layout_at(tmp_path)
    layout.directory.mkdir(parents=True)
    (layout.directory / present).write_bytes(b"local")
    config = layout.directory / "litestream.yml"
    config.write_text(litestream_config(layout, NOWHERE))
    with pytest.raises(FileExistsError, match="never restored over"):
        restore_database(config, layout.databases[0])

    assert ran == []
    assert (layout.directory / present).read_bytes() == b"local"


def _environment(s3: S3Options) -> dict[str, str]:
    environment = dict(os.environ)
    resolved = s3.resolved()
    environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key or ""
    environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key or ""
    return environment


@pytest.mark.s3
def test_flush_through_a_sidecar_on_its_own_socket_then_restore(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A sidecar listening only on a socket no config of this directory
    derives, as a merged process does: `flush(socket=)` ships through it,
    and `restore_database` on a machine with no database brings the rows
    back. Without `socket=` the flush raises.

    The sidecar's own sync is an hour away, so only the flush can have
    shipped the writes, and it still runs during the restore, because
    stopping it ships on the way out and would hide a missing flush.

    Falsify by removing the flush below: the restore has none of the rows.
    """
    binary = sidecar_binary()
    published = f"s3://{bucket}/prefix"
    layout = layout_at(tmp_path / "primary")
    database = layout.databases[0]
    write_rows(database, 1)
    derived = control_socket(layout)
    # In the temp directory, not under `tmp_path`, for the ~104-byte limit on
    # a Unix socket path.
    merged = (
        Path(tempfile.gettempdir()) / f"litelink-merged-{uuid.uuid4().hex[:12]}.sock"
    )
    text = litestream_config(layout, published, s3)
    assert f"  path: {derived}\n" in text
    config = layout.directory / "litestream.yml"
    config.write_text(
        text.replace(f"  path: {derived}\n", f"  path: {merged}\n").replace(
            "      force-path-style: true\n",
            "      force-path-style: true\n      sync-interval: 1h\n",
        )
    )
    sidecar = subprocess.Popen(  # noqa: S603
        [binary, "replicate", "-config", str(config)],
        env=_environment(s3),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 15
        while not merged.exists():
            assert time.monotonic() < deadline, "the sidecar never listened"
            time.sleep(0.1)

        assert not derived.exists()
        write_rows(database, 50, start=2)
        with pytest.raises(RuntimeError, match="control socket"):
            flush(layout, database, binary=binary, timeout=2)

        flush(layout, database, binary=binary, socket=merged)

        replacement = layout_at(tmp_path / "replacement")
        elsewhere = replacement.directory / "litestream.yml"
        replacement.directory.mkdir(parents=True)
        elsewhere.write_text(litestream_config(replacement, published, s3))
        assert restore_database(
            elsewhere, replacement.databases[0], s3_options=s3, binary=binary
        )
        assert count(replacement.databases[0]) == (50, 50)
    finally:
        sidecar.terminate()
        sidecar.wait(timeout=10)
        merged.unlink(missing_ok=True)


@pytest.mark.s3
def test_restore_without_a_replica_returns_false_and_writes_nothing(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    binary = sidecar_binary()
    layout = layout_at(tmp_path)
    layout.directory.mkdir(parents=True)
    config = layout.directory / "litestream.yml"
    config.write_text(litestream_config(layout, f"s3://{bucket}/prefix", s3))
    assert not restore_database(
        config, layout.databases[0], s3_options=s3, binary=binary
    )
    assert not layout.databases[0].exists()


# -- column_ddl ---------------------------------------------------------------


def test_column_ddl_refuses_what_an_append_refuses(tmp_path: Path) -> None:
    """A STRICT table built from `column_ddl` holds the log's I17
    types against raw SQL: no conversion of `'77'` into an integer, no
    infinity, no NULL in a non-nullable column, no third value in a bool.

    Falsify by declaring the column with its affinity (`INTEGER`) instead of
    `ANY`: `'77'` is stored as 77.
    """
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("price", pa.float64()),
            pa.field("ok", pa.bool_()),
        ]
    )
    columns = ", ".join(litelink.column_ddl(field) for field in schema)
    con = sqlite3.connect(tmp_path / "t.db")
    con.execute(f"CREATE TABLE t ({columns}) STRICT")
    con.execute("INSERT INTO t VALUES (1, 2.5, 1)")
    for row in [("77", 1.0, 0), (None, 1.0, 0), (2, float("inf"), 0), (3, 1.0, 7)]:
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("INSERT INTO t VALUES (?, ?, ?)", row)

    assert con.execute("SELECT * FROM t").fetchall() == [(1, 2.5, 1)]


def test_column_ddl_refuses_an_unsupported_type() -> None:
    with pytest.raises(TypeError):
        litelink.column_ddl(pa.field("d", pa.decimal128(10, 2)))
