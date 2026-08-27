"""Construction, validation, and what the injected collaborators buy.

`Log.__init__` takes built collaborators and does no I/O; `open` and
`open_readonly` are what construct and validate them. These tests exercise both
halves — the validation rules on the way in, and the substitutability that
having them as parameters is for.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from litelink import Log, LogConfig
from litelink._archive import Archive
from litelink._buffer import SORT_KEY, Buffer
from litelink._claim import EVERYTHING, Claim, new_owner
from litelink._layout import Layout
from litelink._maintenance import Maintenance
from litelink._read import Reader, duckdb_connection, secret_sql
from litelink._replication import WAL_PREFIX
from litelink._s3 import S3Options
from litelink._table import LogTable
from litelink.log import table_schema, validate

if TYPE_CHECKING:
    import json

from pathlib import Path

SCHEMA = pa.schema([pa.field("event_ts", pa.int64()), pa.field("key", pa.string())])


def test_offset_is_refused_in_the_schema() -> None:
    """I11, at the earliest point it can be caught."""
    with pytest.raises(ValueError, match="I11"):
        validate(table_schema(SCHEMA), (), LogConfig(), None)


def test_sort_by_must_name_real_columns() -> None:
    with pytest.raises(ValueError, match="not in the schema"):
        validate(SCHEMA, ("nonexistent",), LogConfig(), None)


def test_zero_retention_without_an_archive_is_refused() -> None:
    """§8: it means 'evict on upload', and there is nothing to upload to."""
    with pytest.raises(ValueError, match="archive"):
        validate(SCHEMA, (), LogConfig(local_retention=timedelta(0)), None)


def test_zero_retention_is_fine_with_an_archive() -> None:
    validate(SCHEMA, (), LogConfig(local_retention=timedelta(0)), "s3://bucket/x")


def test_table_schema_puts_offset_first() -> None:
    """§2: the library owns exactly one column, and it leads."""
    assert table_schema(SCHEMA).names == ["litelink_offset", "event_ts", "key"]
    assert not table_schema(SCHEMA).field("litelink_offset").nullable


def test_init_does_no_io(tmp_path: Path) -> None:
    """The initialiser assigns; `open` is what touches the disk.

    Constructing a Log against a root that does not exist must therefore
    succeed, because nothing in __init__ should be looking at it.
    """
    layout = Layout(tmp_path / "does-not-exist", "s")
    layout.create()
    table = LogTable.create(layout, table_schema(SCHEMA), ("event_ts",))
    buffer = Buffer.open(layout.buffer_db, SCHEMA)

    config = LogConfig()
    # Local-only, and still a real object: the reader, the maintainer and the
    # Log are handed the same one. It stores no location — it reads the log's,
    # so `set_archive` reaches all three by writing one row.
    buffer.set_meta(SORT_KEY, json.dumps(["event_ts"]))
    archive = Archive(layout, buffer, S3Options())
    log = Log(
        layout=layout,
        table=table,
        buffer=buffer,
        reader=Reader(layout, table, buffer, duckdb_connection, archive),
        maintenance=Maintenance(table, buffer, layout, archive),
        config=config,
        archive=archive,
    )

    assert log.name == "s"
    assert log.end_offset() == 1
    log.close()


def test_a_stub_buffer_can_be_injected(tmp_path: Path) -> None:
    """What the parameters are for: substituting a collaborator wholesale.

    Here a buffer that reports an implausible next offset, to show the value
    reaches `end_offset()` untouched rather than being recomputed from the
    catalog. Nothing had to be monkeypatched to do it.
    """

    class StubBuffer(Buffer):
        def next_offset(self) -> int:
            return 4_242

    layout = Layout(tmp_path, "s")
    layout.create()
    table = LogTable.create(layout, table_schema(SCHEMA), ("event_ts",))
    buffer = StubBuffer.open(layout.buffer_db, SCHEMA)
    config = LogConfig()
    buffer.set_meta(SORT_KEY, json.dumps(["event_ts"]))
    archive = Archive(layout, buffer, S3Options())

    log = Log(
        layout=layout,
        table=table,
        buffer=buffer,
        reader=Reader(layout, table, buffer, duckdb_connection, archive),
        maintenance=Maintenance(table, buffer, layout, archive),
        config=config,
        archive=archive,
    )

    assert log.end_offset() == 4_242
    log.close()


def test_layout_paths_are_derived_not_discovered(tmp_path: Path) -> None:
    """Every path a log writes is computable without touching the filesystem."""
    layout = Layout(tmp_path, "sensors")

    assert layout.buffer_db == tmp_path / "sensors" / "buffer.db"
    assert layout.table_id == "litelink.sensors"
    assert layout.seal_path(1, 51, "abc123") == "sensors/data/1-51-abc123.parquet"
    assert layout.compaction_path(1, 200, "abc123") == (
        "sensors/data/compacted/1-200-abc123.parquet"
    )
    assert (
        layout.relative(f"file://{tmp_path}/sensors/x.parquet") == "sensors/x.parquet"
    )


def test_open_readonly_refuses_a_missing_log(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no litelink log at"):
        Log.open(tmp_path / "nothing", "s", read_only=True)


def test_the_extent_cache_follows_the_metadata_pointer(tmp_path: Path) -> None:
    """A stale extent would double-count across the tier boundary (I3).

    The cache is keyed on `metadata_location`, so every commit must move it.
    This asserts the invalidation directly rather than trusting the read tests
    to notice: a cache that never invalidated would still pass most of them,
    because most do not commit between two reads.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    table = log._table

    assert table.extent() is None, "nothing sealed yet"

    log.extend([{"event_ts": 1, "key": "a"}, {"event_ts": 2, "key": "b"}])
    log.seal()
    table.reload()
    first = table.metadata_location
    assert table.extent() == (1, 2)

    log.extend([{"event_ts": 3, "key": "c"}])
    log.seal()
    table.reload()

    assert table.metadata_location != first, "a commit must move the pointer"
    assert table.extent() == (1, 3), "cache served a stale extent across a seal"
    log.close()


def test_the_extent_cache_is_reused_while_the_pointer_holds(tmp_path: Path) -> None:
    """The point of the cache: no manifest read when nothing has committed.

    Asserted by object identity — `_read_extent` builds a fresh tuple every
    time, so the same object coming back proves the manifests were not touched.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    log.extend([{"event_ts": 1, "key": "a"}])
    log.seal()

    table = log._table
    table.reload()
    first = table.extent()
    assert first == (1, 1)

    for _ in range(5):
        table.reload()
        assert table.extent() is first, "recomputed with the pointer unchanged"

    log.extend([{"event_ts": 2, "key": "b"}])
    log.seal()
    table.reload()

    assert table.extent() is not first, "a commit must force a re-read"
    log.close()


def test_new_refuses_to_clobber_an_existing_log(tmp_path: Path) -> None:
    Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)).close()

    with pytest.raises(FileExistsError, match="already exists"):
        Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))


def test_open_recovers_the_shape_from_the_log(tmp_path: Path) -> None:
    """`open` takes none of the shape, so all of it must be persisted.

    Schema comes from the Iceberg table, sort order from its declared sort
    order (§4), config and archive from the buffer's `meta` table (§2).
    """
    config = LogConfig(target_seal_size=4096, compact_min_files=7)
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("key", "event_ts"),
        config=config,
        archive="s3://bucket/prefix",
    ) as created:
        created.append({"event_ts": 1, "key": "a"})

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.sort_by == ("key", "event_ts")
        assert reopened._archive.uri == "s3://bucket/prefix"
        assert reopened.config == config
        # Logically the same schema, not byte-identical: Iceberg has one string
        # type, so `string` comes back as `large_string`.
        assert reopened._schema.names == SCHEMA.names
        assert reopened.end_offset() == 2


def test_a_log_with_no_stored_config_refuses_to_open(tmp_path: Path) -> None:
    """Same argument as the schema: new() always writes it.

    Substituting defaults would quietly change how a log seals and what it
    retains, which is worse than refusing to open it.
    """
    Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)).close()

    log = Log.open(tmp_path, "s")
    log._buffer._con.execute("DELETE FROM meta WHERE k = 'config'")
    log.close()

    with pytest.raises(ValueError, match="no stored config"):
        Log.open(tmp_path, "s")


def test_set_config_persists(tmp_path: Path) -> None:
    """Every knob in LogConfig governs future work, so no rewrite is needed."""
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.set_config(LogConfig(target_seal_size=1234, compact_min_files=9))

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.config.target_seal_size == 1234
        assert reopened.config.compact_min_files == 9


def test_set_config_validates(tmp_path: Path) -> None:
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        with pytest.raises(ValueError, match="archive"):
            log.set_config(LogConfig(local_retention=timedelta(0)))

        assert log.config == LogConfig(), "a rejected config must not be applied"


def test_set_archive_persists(tmp_path: Path) -> None:
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.set_archive("s3://bucket/x")

    with Log.open(tmp_path, "s") as reopened:
        assert reopened._archive.uri == "s3://bucket/x"
        reopened.set_archive(None)

    with Log.open(tmp_path, "s") as detached:
        assert detached._archive.uri is None


def test_sort_by_is_declared_on_the_table(tmp_path: Path) -> None:
    """§4: declared as table metadata AND applied at write time."""
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("key", "event_ts")) as log:
        assert log._table.sort_by() == ("key", "event_ts")


def test_changing_sort_by_requires_an_explicit_rewrite(tmp_path: Path) -> None:
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        with pytest.raises(ValueError, match="rewrite=True"):
            log.set_sort_by(("key",), rewrite=False)

        assert log.sort_by == ("event_ts",), "refused change must not apply"


def test_changing_sort_by_re_clusters_existing_files(tmp_path: Path) -> None:
    """The reason it cannot be a declaration alone (§7).

    Clustering is baked into each file when written, so a new order that only
    changed the metadata would leave every existing file sorted the old way —
    the same predicate fast on new data and slow on old, with nothing to say
    why.
    """
    import pyarrow.parquet as pq

    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.extend(
            [
                {"event_ts": 3, "key": "a"},
                {"event_ts": 1, "key": "c"},
                {"event_ts": 2, "key": "b"},
            ]
        )
        log.seal()

        written = next(tmp_path.rglob("*/data/*.parquet"))
        assert pq.read_table(written)["event_ts"].to_pylist() == [1, 2, 3]

        log.set_sort_by(("key",), rewrite=True)

        assert log.sort_by == ("key",)
        assert log._table.sort_by() == ("key",)
        merged = next(tmp_path.rglob("*compacted*/*.parquet"))
        assert pq.read_table(merged)["key"].to_pylist() == ["a", "b", "c"]
        # Reads are unaffected: order is by offset, and every row survives.
        rows = log.scan().read_all()
        assert rows["litelink_offset"].to_pylist() == [1, 2, 3]
        assert rows.num_rows == 3

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.sort_by == ("key",), "the new order must survive a reopen"


def test_the_reserved_column_is_refused_at_schema_change_time(tmp_path: Path) -> None:
    """I11 has two doors, not one.

    `validate` covers creation. A schema change is the other way a caller could
    introduce or retire the library's column, and monotonicity and non-reuse
    cannot be enforced on a column the application controls.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        with pytest.raises(ValueError, match="litelink_offset"):
            log.add_column("litelink_offset", pa.int64())

        with pytest.raises(ValueError, match="litelink_offset"):
            log.rename_column("event_ts", "litelink_offset", breaking_ok=True)

        with pytest.raises(ValueError, match="litelink_offset"):
            log.drop_column("litelink_offset", breaking_ok=True)

        # An ordinary column reaches the body — `add_column` is implemented,
        # so the refusal above is I11 and not the stub. The other two are
        # still unimplemented: both are BREAKING for consumers (I10).
        log.add_column("extra", pa.int64())

        assert "extra" in log._buffer.shape().columns

        with pytest.raises(NotImplementedError):
            log.rename_column("key", "renamed", breaking_ok=True)

        with pytest.raises(NotImplementedError):
            log.drop_column("key", breaking_ok=True)


def test_the_reserved_column_name_avoids_duckdbs_parser(tmp_path: Path) -> None:
    """Why it is not called `offset`.

    `SELECT offset` and `max(offset)` are DuckDB parser errors, so the old name
    forced every query — the library's and any reader's against the archive —
    to quote it forever, failing with a syntax error that says nothing about
    why.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.append({"event_ts": 1, "key": "a"})

        unquoted = log.sql("SELECT max(litelink_offset) FROM log").read_all()

        assert unquoted.column(0)[0].as_py() == 1


def test_a_relative_root_works(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`Log.new("litelink-data", …)` — what the demo scripts actually pass.

    A relative root produced `file://litelink-data/…`, which is not a relative
    file URI: it parses as host `litelink-data`, and DuckDB reports a missing
    file naming a path that plainly exists. It survived every test because they
    all pass tmp_path, which is absolute, and surfaced the first time someone
    ran `just demo-tail` with the default root.
    """
    monkeypatch.chdir(tmp_path)

    with Log.new("data", "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        assert log.root.is_absolute()
        assert log._layout.warehouse_uri.startswith("file:///")
        assert log._layout.catalog_uri.startswith("sqlite:////")

        log.extend([{"event_ts": 1, "key": "a"}, {"event_ts": 2, "key": "b"}])
        log.seal()
        log.append({"event_ts": 3, "key": "c"})

        # The seal is what breaks it: before one, the read never touches an
        # Iceberg metadata path at all.
        assert log.scan().read_all().num_rows == 3

    with Log.open("data", "s") as reopened:
        assert reopened.scan().read_all().num_rows == 3


def test_a_relative_root_is_resolved_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolved at construction, so a later chdir cannot move the log."""
    monkeypatch.chdir(tmp_path)
    log = Log.new("data", "s", schema=SCHEMA, sort_by=("event_ts",))
    root = log.root

    (tmp_path / "elsewhere").mkdir()
    monkeypatch.chdir(tmp_path / "elsewhere")

    log.append({"event_ts": 1, "key": "a"})
    log.seal()

    assert log.root == root
    assert log.scan().read_all().num_rows == 1
    log.close()


def test_a_log_buffer_fsyncs_on_every_commit(tmp_path: Path) -> None:
    """§3's durability claim, read back off the connection.

    `synchronous=FULL` is the whole product: WAL alone fsyncs at checkpoint
    rather than at commit, which puts committed rows back in the OS page cache
    — the exact loss this library exists to prevent. 2 is SQLite's code for
    FULL.
    """
    buffer = Buffer.open(tmp_path / "b.db", SCHEMA)
    try:
        assert buffer._con.execute("PRAGMA synchronous").fetchone()[0] == 2
    finally:
        buffer.close()


def test_a_derived_buffer_can_skip_the_fsync(tmp_path: Path) -> None:
    """For a buffer whose rows still exist somewhere else.

    The archive rewrite re-cuts through a scratch buffer whose every row came
    from the archive and is still in it until the rewrite's final commit, so a
    crash there costs a re-run rather than data. 0 is OFF. WAL stays either
    way: the read-only handle is a second connection to the same file, which is
    what WAL is for here — not durability.
    """
    buffer = Buffer.open(tmp_path / "scratch.db", SCHEMA, durable=False)
    try:
        assert buffer._con.execute("PRAGMA synchronous").fetchone()[0] == 0
        assert buffer._con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    finally:
        buffer.close()


def test_a_config_written_without_a_setting_still_opens() -> None:
    """Adding a setting must not make existing logs unopenable.

    `LogConfig` is policy, not data: a record written before a setting existed
    means the log was running that setting's default. Reading the record
    positionally turned every new field into a breaking change to `open`, over
    a value that was never load-bearing.
    """
    written = json.dumps({"target_seal_size": 4096, "compact_min_files": 2})

    recovered = LogConfig.from_json(written)

    assert recovered.target_seal_size == 4096
    assert recovered.compact_min_files == 2
    assert recovered.local_rows == LogConfig().local_rows
    assert recovered.snapshot_retention == LogConfig().snapshot_retention


def test_a_config_written_by_a_newer_version_still_opens() -> None:
    """The same tolerance from the other side: an unknown setting is one this
    version does not have, not a reason to refuse the log."""
    written = json.dumps({"target_seal_size": 4096, "a_setting_from_the_future": 7})

    assert LogConfig.from_json(written).target_seal_size == 4096


def test_every_database_a_restore_needs_is_listed(tmp_path: Path) -> None:
    """§3a: what a WAL-shipping sidecar has to replicate.

    All three, and the set is the library's to know rather than an operator's
    to guess. `buffer.db` holds rows no Parquet file has yet — the one everyone
    remembers. `catalog.db` says which files the local table is made of.
    `archive.db` says the same for the archive, so omitting it leaves the
    objects in S3 intact with nothing able to say what they are.

    The rewrite scratch is excluded: it is derived from the archive and deleted
    at the end of the operation that creates it, so replicating it would ship a
    temporary file to object storage to no purpose.
    """
    layout = Layout(tmp_path, "s")

    assert set(layout.databases) == {
        layout.buffer_db,
        layout.catalog_db,
        layout.archive_db,
    }
    assert layout.rewrite_db not in layout.databases
    assert all(path.suffix == ".db" for path in layout.databases)


def test_replication_config_names_every_database_and_the_wal_prefix(
    tmp_path: Path,
) -> None:
    """§3a, derived rather than restated.

    Everything in the config comes from the log: the file set, the destination
    beside the archived data, and the endpoint from the credentials it was
    opened with. A config written by hand knows what someone remembered.
    """
    s3 = S3Options(endpoint="http://127.0.0.1:9000", region="us-east-1")
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True),
        archive="s3://bucket/prefix",
        s3=s3,
    ) as log:
        rendered = log.replication_config()

        for database in log.databases:
            assert f"path: {database}" in rendered
            # Keyed by the LOG-RELATIVE path, not the bare filename: two logs
            # under one root sharing an archive prefix would otherwise ship
            # their distinct buffers to the same replica path, which is two
            # sidecars writing one replica.
            relative = database.relative_to(tmp_path).as_posix()
            assert f"path: prefix/{WAL_PREFIX}/{relative}" in rendered

        assert "bucket: bucket" in rendered
        # A non-AWS endpoint needs both, and neither can be left to the
        # environment: litestream resolves the region against real AWS
        # otherwise, and `bucket.host` is a DNS name only AWS serves.
        assert "endpoint: http://127.0.0.1:9000" in rendered
        assert "force-path-style: true" in rendered
        assert "secret" not in rendered.lower(), "credentials must stay in the env"


def test_the_config_uses_litestreams_current_single_replica_key(
    tmp_path: Path,
) -> None:
    """`replica`, not the deprecated `replicas` list.

    litestream v0.5.0 made it one replica per database and kept the list only
    for compatibility — `Replicas []*ReplicaConfig // Deprecated` in
    cmd/litestream/main.go as of v0.5.16, which is what `just litestream`
    pins. This never emitted more than one element, so the list bought nothing
    and dated the file.

    Checked as a shape, not a substring: `"replica:" in rendered` is also true
    of `replicas:`, so the assertion has to be that the plural is ABSENT, and
    that the fields sit under the singular at the indentation a mapping needs
    rather than the one a list item needs.
    """
    # An endpoint and a region, so every optional field is rendered — the
    # ones most likely to be left behind at the old indentation are exactly
    # the ones a bare `S3Options()` omits.
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True),
        archive="s3://bucket/prefix",
        s3=S3Options(endpoint="http://127.0.0.1:9000", region="us-east-1"),
    ) as log:
        rendered = log.replication_config()

    assert "replicas:" not in rendered
    assert "- type: s3" not in rendered, "a list item where a mapping belongs"
    assert "    replica:\n      type: s3\n" in rendered

    # Every replica field moved with it. Left at the list indentation they
    # parse as siblings of `replica` — which litestream ignores, so the
    # failure is a config that loads and replicates to the wrong place.
    for field in (
        "bucket:",
        "path: prefix/",
        "region:",
        "endpoint:",
        "force-path-style:",
    ):
        assert f"\n      {field}" in rendered, field


def test_wal_retention_writes_a_snapshot_window_the_sidecar_can_act_on(
    tmp_path: Path,
) -> None:
    """§3a. The un-archived window, stated as the only thing litestream takes.

    "Retain WAL above the archived offset" is the way to say what this is for
    and it is not expressible: litestream v0.5.16's knobs are all durations —
    `snapshot.interval`, `snapshot.retention`, `l0-retention` — and its CLI has
    no `snapshot` verb to force one after a sync and make a duration behave
    like an offset.

    **`interval` must come out SHORTER than `retention`.** litestream keeps
    snapshots and their LTX files for `retention`, and a restore needs one at
    or before the point it restores to; a longer interval leaves windows
    holding no snapshot and deletes the chain the restore needs.

    Verified against the real binary, which is the part a substring assertion
    cannot do: `litestream databases -config` accepts this file, and rejects it
    with "cannot unmarshal into time.Duration" when the retention is replaced
    with a non-duration — so the field is genuinely parsed, not ignored.
    """
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True, wal_retention=timedelta(hours=6)),
        archive="s3://bucket/prefix",
    ) as log:
        rendered = log.replication_config()
        databases = len(log.databases)

    assert (
        "    snapshot:\n      interval: 10800s\n      retention: 21600s\n" in rendered
    )
    # One block per database, not one for the file. A root holding several logs
    # gets a window each; the global `snapshot:` block would make the shortest
    # of them everyone's.
    assert rendered.count("    snapshot:") == databases


def test_wal_retention_is_refused_without_a_sidecar_to_read_it(tmp_path: Path) -> None:
    """It is a window written into the sidecar's config. With nothing shipping
    the WAL it is a setting nothing reads, which is the failure that looks
    exactly like a working one."""
    with pytest.raises(ValueError, match="wal_retention needs wal_replication"):
        validate(SCHEMA, (), LogConfig(wal_retention=timedelta(hours=6)), None)


def test_a_zero_wal_retention_is_refused(tmp_path: Path) -> None:
    """Zero is not "keep nothing", it is "expire every snapshot as you take
    it" — a replica that cannot restore to any point at all, reported by
    nothing. None is how you ask for litestream's own default."""
    config = LogConfig(wal_replication=True, wal_retention=timedelta(0))

    with pytest.raises(ValueError, match="wal_retention must be positive"):
        validate(SCHEMA, (), config, "s3://bucket/prefix")


def test_wal_retention_survives_the_round_trip_through_meta(tmp_path: Path) -> None:
    """Durations go to `meta` as seconds, like every other one."""
    config = LogConfig(wal_replication=True, wal_retention=timedelta(hours=6))

    assert LogConfig.from_json(config.to_json()).wal_retention == timedelta(hours=6)
    assert LogConfig.from_json(LogConfig().to_json()).wal_retention is None


def test_replication_needs_somewhere_to_ship_to(tmp_path: Path) -> None:
    """WAL segments go beside the archived data, so a local-only log has
    nowhere to put them — refused at construction rather than at the first
    attempt to write a config nothing could act on."""
    with pytest.raises(ValueError, match="wal_replication"):
        validate(SCHEMA, (), LogConfig(wal_replication=True), None)


def test_the_replication_config_is_written_beside_the_log(tmp_path: Path) -> None:
    """Derived like every other path: a setting for it would be one more thing
    to keep in step with the log it describes."""
    with Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True),
        archive="s3://bucket/prefix",
    ) as log:
        written = log.write_replication_config()

        assert written == tmp_path / "litestream.yml"
        assert written.read_text() == log.replication_config()


def test_the_archive_read_falls_back_to_the_aws_credential_chain() -> None:
    """The bug a local endpoint cannot catch.

    On an ordinary AWS host the credentials are in a profile, in instance
    metadata, or behind SSO — never in the arguments. pyiceberg and s3fs
    resolve those themselves, so writes worked; DuckDB got a secret with no
    keys, treated it as anonymous, and answered every `include_archive` read
    with 403. Against rustfs it never appeared, because a local endpoint always
    has explicit keys to pass.
    """
    rendered = secret_sql(S3Options(region="us-west-1"))

    assert "PROVIDER credential_chain" in rendered
    assert "KEY_ID" not in rendered
    assert "REGION 'us-west-1'" in rendered


def test_an_explicit_key_still_wins_over_the_chain() -> None:
    """Which is what makes "test locally, then against AWS" a change of
    environment rather than of code."""
    rendered = secret_sql(
        S3Options(
            endpoint="http://127.0.0.1:9000",
            access_key="litelink",
            secret_key="litelink-secret",
            region="us-east-1",
        )
    )

    assert "PROVIDER credential_chain" not in rendered
    assert "KEY_ID 'litelink'" in rendered
    # A non-AWS endpoint needs path-style addressing and the scheme split off:
    # DuckDB takes host:port with USE_SSL, where pyiceberg takes a URL.
    assert "ENDPOINT '127.0.0.1:9000'" in rendered
    assert "USE_SSL false" in rendered
    assert "URL_STYLE 'path'" in rendered


def test_two_logs_under_one_root_get_distinct_replica_paths(tmp_path: Path) -> None:
    """Two buffers must never ship to one replica path.

    `<root>/<log>/buffer.db` flattened to `buffer.db` would send both logs'
    buffers to the same object, which is two litestream instances writing one
    replica — the corruption litestream is explicit about — and a restore that
    hands back the other log's WAL.
    """
    shared = "s3://bucket/prefix"
    with (
        Log.new(tmp_path, "one", schema=SCHEMA, archive=shared) as first,
        Log.new(tmp_path, "two", schema=SCHEMA, archive=shared) as second,
    ):
        # The REPLICA path, not the database path — both spell themselves
        # `path:`. Told apart by indentation: the database's sits at the list
        # item (`  - path:`) and the replica's inside the `replica` mapping
        # under it. Tracks `_replication.litestream_config`, which is why the
        # prefix is written out rather than stripped.
        keys = {
            line.split("path: ", 1)[1]
            for rendered in (first.replication_config(), second.replication_config())
            for line in rendered.splitlines()
            if line.startswith("      path: ")
        }
        buffers = {key for key in keys if key.endswith("buffer.db")}

        assert len(buffers) == 2, f"buffers must not collide: {buffers}"


def test_compact_min_files_below_two_is_refused(tmp_path: Path) -> None:
    """A knob that can stall the log for ever should not accept the value.

    The floor is TWO, not one, and the difference is the whole defect: a run
    always holds at least one file, so at one every run looks mergeable,
    nothing is ever settled, and `stable_prefix` returns zero permanently.
    Sync pushes nothing, the watermark stands still, eviction pins on it and
    the local table grows without bound — while every pass rewrites every file
    to no purpose. Merging a run of one is a no-op rewrite in any case.
    """
    for value in (0, 1):
        with pytest.raises(ValueError, match="compact_min_files must be at least 2"):
            Log.new(
                tmp_path,
                f"s{value}",
                schema=SCHEMA,
                sort_by=("event_ts",),
                config=LogConfig(compact_min_files=value),
            )


def test_local_rows_zero_without_an_archive_is_refused(tmp_path: Path) -> None:
    """The same intent, refused on one knob and accepted on its twin.

    `local_retention=0` was refused on a local-only log as "would delete each
    file as it sealed", and `local_rows=0` means exactly that — keep the newest
    zero rows — and was accepted, evicting every sealed file as the only copy.
    """
    with pytest.raises(ValueError, match="evict on upload"):
        Log.new(
            tmp_path,
            "s",
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=LogConfig(local_rows=0),
        )


def test_a_log_from_the_lease_era_refuses_to_open(tmp_path: Path) -> None:
    """The rename is an offline upgrade, and silence would be the dangerous part.

    A buffer carrying the old `lease` table was last written by a build that
    coordinated through it, while this one coordinates through `claim`. Neither
    sees the other, so a rolling upgrade would put two sealers on the same
    queued group — the torn file the mechanism exists to prevent. Nothing can
    make an old binary respect the new table, so refuse rather than run beside
    one.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    with log:
        with log._buffer._lock:
            log._buffer._con.execute(
                "CREATE TABLE lease (role TEXT PRIMARY KEY, owner TEXT, expires_at INT)"
            )

    with pytest.raises(RuntimeError, match="coordinated through a `lease` table"):
        Log.open(tmp_path, "s")


def test_negative_local_retention_is_refused(tmp_path: Path) -> None:
    """The sign check its twin has always had.

    Eviction computes `now - local_retention`, so a negative one puts the
    cutoff in the FUTURE and every file in the log is stale. On a local-only
    log that is silent deletion of the only copy of everything, at every
    negative value, from one sign slip — and the zero rule never caught it
    because it tests equality.
    """
    with pytest.raises(ValueError, match="local_retention must not be negative"):
        Log.new(
            tmp_path,
            "s",
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=LogConfig(local_retention=timedelta(hours=-1)),
        )


def test_a_generous_floor_beside_an_evicting_one_is_allowed(tmp_path: Path) -> None:
    """Eviction takes the LOWER boundary, so the policy retaining more wins.

    A config is only "evict on upload" when every floor it states is one.
    Refusing `local_retention=0` beside `local_rows=1_000_000` would refuse a
    config that keeps a million rows regardless of age, with a message that is
    false for it.
    """
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=LogConfig(local_retention=timedelta(0), local_rows=1_000_000),
    )
    with log:
        log.extend([{"event_ts": i, "key": "k"} for i in range(8)])
        log.seal()
        log.maintain()

        assert log.scan().read_all().num_rows == 8, "evicted under a generous floor"


def test_two_processes_cannot_assemble_the_pair_validate_refuses(
    tmp_path: Path,
) -> None:
    """`validate` refuses a PAIR, so both halves must be read durably.

    Each setter checked its own new half against this process's memory of the
    other, so two handles could assemble the refused combination between them:
    one attaches an evict-on-upload policy while an archive is configured, the
    other detaches the archive against a policy it read before that. The next
    maintenance pass then executes it and deletes the only copy of everything.
    """
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with log, Log.open(tmp_path, "s") as other:
        # `other` opened while an archive was configured and a normal policy
        # was in force; it still remembers both.
        log.set_config(LogConfig(local_rows=0))

        with pytest.raises(ValueError, match="evict on upload"):
            other.set_archive(None)


def test_the_refused_pair_cannot_be_assembled_by_interleaving(tmp_path: Path) -> None:
    """Reading the other half durably is not enough; the check must be atomic.

    `validate` refuses a PAIR, and each setter reads the other half from
    `meta`. Read and write as two transactions with nothing between them and
    the check is only a statement about the past: each call passes against a
    state the other is about to change, and between them they assemble the very
    pair neither would accept. The next maintenance pass then executes it and
    deletes the only copy of everything sealed.

    Both setters take the same claim now, so the interleaving cannot happen —
    modelled here by holding that claim while the second call runs.
    """
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with log:
        log._settings_wait = 0.2  # ty: ignore[unresolved-attribute]
        held = log._lease("maintain")

        assert held.acquire()

        try:
            # Bounded: it waits for maintenance rather than refusing outright,
            # and reports rather than hanging when the wait runs out.
            with pytest.raises(RuntimeError, match="has held a claim"):
                log.set_config(LogConfig(local_rows=0))

        finally:
            held.release()


def test_negative_snapshot_retention_is_refused(tmp_path: Path) -> None:
    """The same sign slip, one field over.

    Expiry computes `now - snapshot_retention`, so a negative one puts the
    cutoff in the future: every superseded file is unlinked in the pass that
    supersedes it, and I6's promise — the grace must exceed the longest scan —
    is not shortened but inverted. Zero stays legal; it means "no grace", which
    tests and demos ask for on purpose.
    """
    with pytest.raises(ValueError, match="snapshot_retention must not be negative"):
        Log.new(
            tmp_path,
            "s",
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=LogConfig(snapshot_retention=timedelta(hours=-1)),
        )

    Log.new(
        tmp_path,
        "zero",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=LogConfig(snapshot_retention=timedelta(0)),
    ).close()


def test_a_configuration_change_waits_for_maintenance(tmp_path: Path) -> None:
    """It waits rather than refusing on the first try.

    The two setters share a claim because `validate` refuses a pair, but they
    also collide with ordinary maintenance — and the shipped writer calls both
    on every restart while a maintainer runs continuously. Measured before this
    wait existed: one startup in six failed, which turns a routine restart into
    a coin toss.
    """
    log = Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    with log:
        log._settings_wait = 5.0  # ty: ignore[unresolved-attribute]
        held = log._lease("maintain")

        assert held.acquire()

        released = threading.Event()

        def let_go() -> None:
            time.sleep(0.3)
            held.release()
            released.set()

        thread = threading.Thread(target=let_go)
        thread.start()
        try:
            log.set_config(LogConfig(local_rows=500))

            assert released.is_set(), "returned before the holder let go"
            assert log.config.local_rows == 500
        finally:
            thread.join(timeout=5)


def test_a_setter_that_lost_its_claim_does_not_write(tmp_path: Path) -> None:
    """The claim makes the read and the write one decision only while it is held.

    Both setters read the other half, validate the pair, then write. A stall
    past the TTL between the read and the write is the threat the TTL exists
    for: the other setter takes the lapsed claim lawfully, validates against
    the half this one has not written yet, writes its own — and between them
    they record the pair `validate` just refused, which the next maintenance
    pass carries out. Every data commit already asks the claim again at the
    write; the setters stopped one line short.
    """
    log = Log.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        archive="s3://bucket/prefix",
    )
    with log:
        before = log.config.local_rows
        original = log._buffer.get_meta
        rivals: list[Claim] = []

        def losing(key: str) -> str | None:
            # Between the read of the other half and the write: the claim
            # lapses and another owner takes it.
            if key == "archive" and not rivals:
                with log._buffer._lock:
                    log._buffer._con.execute("UPDATE claim SET expires_at = 1")

                rival = Claim(
                    log._buffer._con,
                    log._buffer._lock,
                    "maintain",
                    0,
                    EVERYTHING,
                    new_owner(),
                )
                assert rival.acquire()
                rivals.append(rival)

            return original(key)

        log._buffer.get_meta = losing  # ty: ignore[invalid-assignment]
        try:
            with pytest.raises(RuntimeError, match="lost the claim"):
                log.set_config(LogConfig(local_rows=7))

        finally:
            log._buffer.get_meta = original  # ty: ignore[invalid-assignment]
            for rival in rivals:
                rival.release()

        assert log.config.local_rows == before, "wrote without holding the claim"


def test_a_second_handle_sees_settings_changes_with_no_refresh(tmp_path: Path) -> None:
    """Neither the policy nor the archive location is copied into a process.

    This is the property that replaces twelve `refresh` calls. Each of them
    existed to drag a process-local copy back into agreement with the log, and
    every defect in this seam was a decision that read the copy where no such
    call had been placed — eight review rounds running, each in a place the
    previous round had not looked.

    There is one copy now, in `meta`. A handle that has never heard of a change
    cannot be wrong about it, because it holds nothing to be wrong with.
    """
    first = Log.new(
        tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), config=LogConfig()
    )
    with first, Log.open(tmp_path, "s") as second:
        assert second.config.local_rows is None
        assert not second._archive.configured()

        first.set_config(LogConfig(local_rows=4242))
        first.set_archive("s3://bucket/prefix")

        # `second` was never told, and never asked.
        assert second.config.local_rows == 4242
        assert second._archive.configured()
        assert second._archive.uri == "s3://bucket/prefix"
        assert second._maintenance.config.local_rows == 4242
        assert second._buffer.config().local_rows == 4242


def test_the_sort_order_is_recovered_from_meta_not_from_the_catalog(
    tmp_path: Path,
) -> None:
    """§4's clustering has to survive a machine, and the catalog does not.

    `sort_by` used to live only in the local Iceberg table, read back at open.
    `catalog.db` is replicated but records ABSOLUTE paths to local metadata no
    sidecar ships, so a failover rebuilds the local table rather than restoring
    it — and has to be told what order to declare. The archive could not answer
    either: `open_archive` never declared one.

    So `meta` carries it, and this proves `open` reads THAT rather than the
    table: the declaration is removed from under a closed log and the order
    still comes back.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        assert log._table.sort_by() == ("event_ts",)  # noqa: SLF001

    catalog = SqlCatalog(
        "local",
        uri=Layout(tmp_path, "s").catalog_uri,
        warehouse=Layout(tmp_path, "s").warehouse_uri,
    )
    table = catalog.load_table(Layout(tmp_path, "s").table_id)
    with table.update_sort_order() as update:
        update._apply()  # noqa: SLF001

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.sort_by == ("event_ts",), (  # noqa: SLF001
            "open read the table's declaration rather than meta"
        )


def test_a_log_with_no_stored_sort_order_is_refused(tmp_path: Path) -> None:
    """Absent means damaged, not "unsorted".

    `new` always writes it, so defaulting to `()` would silently de-cluster
    every file the next compaction rewrites while the table still declared a
    key. Same rule as the stored config, one line above it.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)):
        pass

    buffer = Buffer.open(Layout(tmp_path, "s").buffer_db, SCHEMA)
    try:
        buffer._con.execute("DELETE FROM meta WHERE k = 'sort_by'")  # noqa: SLF001
    finally:
        buffer.close()

    with pytest.raises(ValueError, match="no stored sort order"):
        Log.open(tmp_path, "s")


def test_clearing_the_sort_order_clears_both_records(tmp_path: Path) -> None:
    """An empty order is a value, not a no-op.

    `set_sort_order` used to return early on it, so `set_sort_by((),
    rewrite=True)` re-clustered every file and left the table declaring the old
    key. Harmless while `open` read that declaration and reverted; permanent
    once `meta` is the source of truth, because nothing would reconcile them.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.extend([{"event_ts": i, "key": f"k{i}"} for i in range(20)])
        log.set_sort_by((), rewrite=True)

        assert log._table.sort_by() == ()  # noqa: SLF001
        assert log._buffer.get_meta("sort_by") == "[]"  # noqa: SLF001

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.sort_by == ()  # noqa: SLF001


def test_a_rewrite_finishes_a_re_sort_that_died_after_the_meta_write(
    tmp_path: Path,
) -> None:
    """The one crash gap in `set_sort_by` that nothing used to heal.

    `set_sort_by` writes `meta` LAST, so a crash before it leaves the log
    deciding by the old key and a retry completes the operation. A crash AFTER
    it does not: the declarations say the new key, every existing file is still
    in the old one, and the natural retry used to find the orders equal and
    return without doing anything. Silent, permanent, and a §7 lie — a
    predicate on the declared leading column pruning nothing, on exactly the
    files the rewrite never reached.

    Reproduced by a review pass, so this asserts the ROWS rather than the
    declarations: both of those already said the right thing in the broken
    state, which is what made it silent.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        # `key` descending as `event_ts` ascends, so the two clusterings are
        # distinguishable and neither is the insertion order by accident.
        log.extend([{"event_ts": i, "key": f"k{20 - i:02d}"} for i in range(20)])
        while log.seal() is not None:
            pass

        # The state a crash after the `meta` write leaves: both declarations
        # carry the new key, the file carries the old clustering.
        log._table.set_sort_order(("key",))  # noqa: SLF001
        log._buffer.set_meta("sort_by", json.dumps(["key"]))  # noqa: SLF001

        written = pq.read_table(log._table.data_files()[0].path)  # noqa: SLF001
        keys = written.column("key").to_pylist()
        assert keys != sorted(keys), "the file was already re-clustered"
        assert log.sort_by == ("key",), "the declaration did not survive"

        # The retry. Same order the log already declares, which is precisely
        # the call that used to do nothing.
        log.set_sort_by(("key",), rewrite=True)

        rewritten = pq.read_table(log._table.data_files()[0].path)  # noqa: SLF001
        assert rewritten.column("key").to_pylist() == sorted(keys)
        assert rewritten.num_rows == 20, "the rewrite dropped or duplicated rows"


def test_seeding_the_sequence_forward_is_allowed_backward_is_not(
    tmp_path: Path,
) -> None:
    """The guard is about DIRECTION, not about the buffer being empty.

    It used to refuse any non-empty buffer, which blocked the one caller that
    needs it: a restore reserves an offset range (§3a) on a buffer holding the
    recovered tail — exactly a buffer with rows in it. Raising past those rows
    is safe, because SQLite assigns `max(max(rowid), seq) + 1` either way.

    Lowering is the unrecoverable one, and stays refused: the sequence is
    ignored, every following row lands on an offset belonging to different
    data, and nothing downstream can detect it.
    """
    buffer = Buffer.open(tmp_path / "b.db", SCHEMA)
    try:
        buffer.append([{"event_ts": i, "key": "k"} for i in range(2)])
        highest = buffer.next_offset() - 1

        buffer.seed_offsets(highest + (1 << 20))

        assert buffer.next_offset() == highest + (1 << 20)

        with pytest.raises(ValueError, match="holding rows up to"):
            buffer.seed_offsets(1)
    finally:
        buffer.close()


def test_an_unreadable_catalog_is_not_reported_as_an_absent_table(
    tmp_path: Path,
) -> None:
    """ "Cannot tell" and "no table" have opposite safe answers here.

    `Log.restore` reads this to decide whether it is resuming an interrupted
    restore. Answering False when the catalog merely could not be READ tells it
    to resume over a LIVE log — and the resume path reserves 2**20 offsets on
    it, deletes every `extent` row including queued cuts, wipes `sealing` and
    `claim`, drops the archive catalog row, and deletes buffered rows below the
    frontier.

    It is reachable without corruption: `catalog.db` runs in
    `journal_mode=delete` with no busy timeout on this connection, so a read
    landing in another process's commit window returns SQLITE_BUSY.
    `_recorded_location` refuses the same conflation, in the same words.
    """
    with Log.new(tmp_path, "s", schema=SCHEMA):
        pass

    layout = Layout(tmp_path, "s")

    assert LogTable.exists_for(layout) is True

    # Not a SQLite database at all, standing in for a read that cannot answer.
    layout.catalog_db.write_bytes(b"not a database, and not an absent one")

    with pytest.raises(LookupError):
        LogTable.exists_for(layout)

    # And the caller that matters treats it as "exists" rather than proceeding.
    with pytest.raises((LookupError, FileExistsError, ValueError, RuntimeError)):
        Log.restore(tmp_path, "s", archive="s3://bucket/prefix")
