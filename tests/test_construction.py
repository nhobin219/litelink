"""Construction, validation, and what the injected collaborators buy.

`WriteHandle.__init__` takes built collaborators and does no I/O; `open` and
`open_readonly` are what construct and validate them. These tests exercise both
halves — the validation rules on the way in, and the substitutability that
having them as parameters is for.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

import litelink
from litelink import LogConfig, WriteHandle
from litelink._buffer import SORT_KEY, Buffer
from litelink._handle import table_schema, validate
from litelink._layout import Layout, validate_published
from litelink._maintenance import Maintenance
from litelink._published import Published
from litelink._read import Reader, duckdb_connection, secret_sql
from litelink._replication import WAL_PREFIX
from litelink._s3 import S3Options
from litelink._table import LogTable

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


def test_zero_retention_is_fine_with_a_published_table() -> None:
    validate(SCHEMA, (), LogConfig(staging_retention=timedelta(0)), "s3://bucket/x")


def test_table_schema_puts_offset_first() -> None:
    """§2: the library owns exactly one column, and it leads."""
    assert table_schema(SCHEMA).names == ["litelink_offset", "event_ts", "key"]
    assert not table_schema(SCHEMA).field("litelink_offset").nullable


def test_init_does_no_io(tmp_path: Path) -> None:
    """The initialiser assigns; `open` is what touches the disk.

    Constructing a WriteHandle against a root that does not exist must therefore
    succeed, because nothing in __init__ should be looking at it.
    """
    layout = Layout(tmp_path / "does-not-exist", "s")
    layout.create()
    table = LogTable.create(layout, table_schema(SCHEMA), ("event_ts",))
    buffer = Buffer.open(layout.buffer_db, SCHEMA)

    config = LogConfig()
    # Local-only, and still a real object: the reader, the maintainer and the
    # WriteHandle are handed the same one. It stores no location — it reads the log's,
    # which is fixed when the log is created.
    buffer.set_meta(SORT_KEY, json.dumps(["event_ts"]))
    published = Published(layout, buffer, S3Options())
    log = WriteHandle(
        layout=layout,
        table=table,
        buffer=buffer,
        reader=Reader(layout, table, buffer, duckdb_connection, published),
        maintenance=Maintenance(table, buffer, layout, published),
        config=config,
        published=published,
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
    published = Published(layout, buffer, S3Options())

    log = WriteHandle(
        layout=layout,
        table=table,
        buffer=buffer,
        reader=Reader(layout, table, buffer, duckdb_connection, published),
        maintenance=Maintenance(table, buffer, layout, published),
        config=config,
        published=published,
    )

    assert log.end_offset() == 4_242
    log.close()


def test_layout_paths_are_derived_not_discovered(tmp_path: Path) -> None:
    """Every path a log writes is computable without touching the filesystem."""
    layout = Layout(tmp_path, "sensors")

    assert layout.buffer_db == tmp_path / "sensors" / "buffer.db"
    assert layout.table_id == "litelink.sensors"
    assert (
        layout.seal_path(1, 51, "abc123") == "sensors/data/sealed/1-51-abc123.parquet"
    )
    assert layout.compaction_path(1, "abc123") == (
        "sensors/data/compacted/1-abc123.parquet"
    )
    assert (
        layout.relative(f"file://{tmp_path}/sensors/x.parquet") == "sensors/x.parquet"
    )


def test_open_readonly_refuses_a_missing_log(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no litelink log at"):
        litelink.open(tmp_path / "nothing", "s", read_only=True)


def test_the_extent_cache_follows_the_metadata_pointer(tmp_path: Path) -> None:
    """A stale extent would double-count across the tier boundary (I3).

    The cache is keyed on `metadata_location`, so every commit must move it.
    This asserts the invalidation directly rather than trusting the read tests
    to notice: a cache that never invalidated would still pass most of them,
    because most do not commit between two reads.
    """
    log = litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    table = log._table

    assert table.span() is None, "nothing sealed yet"

    log.extend([{"event_ts": 1, "key": "a"}, {"event_ts": 2, "key": "b"}])
    log.seal(flush=True)
    table.reload()
    first = table.metadata_location
    assert table.span() == (1, 3)

    log.extend([{"event_ts": 3, "key": "c"}])
    log.seal(flush=True)
    table.reload()

    assert table.metadata_location != first, "a commit must move the pointer"
    assert table.span() == (1, 4), "cache served a stale extent across a seal"
    log.close()


def test_the_extent_cache_is_reused_while_the_pointer_holds(tmp_path: Path) -> None:
    """The point of the cache: no manifest read when nothing has committed.

    Asserted by object identity — `_read_extent` builds a fresh tuple every
    time, so the same object coming back proves the manifests were not touched.
    """
    log = litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    log.extend([{"event_ts": 1, "key": "a"}])
    log.seal(flush=True)

    table = log._table
    table.reload()
    first = table.span()
    assert first == (1, 2)

    for _ in range(5):
        table.reload()
        assert table.span() is first, "recomputed with the pointer unchanged"

    log.extend([{"event_ts": 2, "key": "b"}])
    log.seal(flush=True)
    table.reload()

    assert table.span() is not first, "a commit must force a re-read"
    log.close()


def test_new_refuses_to_clobber_an_existing_log(tmp_path: Path) -> None:
    litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)).close()

    with pytest.raises(FileExistsError, match="already exists"):
        litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))


def test_open_recovers_the_shape_from_the_log(tmp_path: Path) -> None:
    """`open` takes none of the shape, so all of it must be persisted.

    Schema comes from the Iceberg table, sort order from its declared sort
    order (§4), config and published table from the buffer's `meta` table (§2).
    """
    config = LogConfig(target_seal_size=4096, compact_min_files=7)
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        sort_by=("key", "event_ts"),
        config=config,
        published="s3://bucket/prefix",
    ) as created:
        created.append({"event_ts": 1, "key": "a"})

    with litelink.open(tmp_path, "s") as reopened:
        assert reopened.sort_by == ("key", "event_ts")
        assert reopened._published.uri == "s3://bucket/prefix"
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
    litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)).close()

    log = litelink.open(tmp_path, "s")
    log._buffer._con.execute("DELETE FROM meta WHERE k = 'config'")
    log.close()

    with pytest.raises(ValueError, match="no stored config"):
        litelink.open(tmp_path, "s")


def test_set_config_persists(tmp_path: Path) -> None:
    """Every knob in LogConfig governs future work, so no rewrite is needed."""
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.set_config(LogConfig(target_seal_size=1234, compact_min_files=9))

    with litelink.open(tmp_path, "s") as reopened:
        assert reopened.config.target_seal_size == 1234
        assert reopened.config.compact_min_files == 9


def test_set_config_validates(tmp_path: Path) -> None:
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        with pytest.raises(ValueError, match="remote published table"):
            log.set_config(LogConfig(wal_replication=True))

        assert log.config == LogConfig(), "a rejected config must not be applied"


def test_sort_by_is_declared_on_the_table(tmp_path: Path) -> None:
    """§4: declared as table metadata AND applied at write time."""
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("key", "event_ts")) as log:
        assert log._table.sort_by() == ("key", "event_ts")


def test_the_reserved_column_name_avoids_duckdbs_parser(tmp_path: Path) -> None:
    """Why it is not called `offset`.

    `SELECT offset` and `max(offset)` are DuckDB parser errors, so the old name
    forced every query — the library's and any reader's against the published table —
    to quote it forever, failing with a syntax error that says nothing about
    why.
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log.append({"event_ts": 1, "key": "a"})

        unquoted = log.sql("SELECT max(litelink_offset) FROM log").read_all()

        assert unquoted.column(0)[0].as_py() == 1


def test_a_relative_root_works(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`litelink.new("litelink-data", …)` — what the demo scripts actually pass.

    A relative root produced `file://litelink-data/…`, which is not a relative
    file URI: it parses as host `litelink-data`, and DuckDB reports a missing
    file naming a path that plainly exists. It survived every test because they
    all pass tmp_path, which is absolute, and surfaced the first time someone
    ran `just demo-tail` with the default root.
    """
    monkeypatch.chdir(tmp_path)

    with litelink.new("data", "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        assert log.root.is_absolute()
        assert log._layout.warehouse_uri.startswith("file:///")
        assert log._layout.catalog_uri.startswith("sqlite:////")

        log.extend([{"event_ts": 1, "key": "a"}, {"event_ts": 2, "key": "b"}])
        log.seal(flush=True)
        log.append({"event_ts": 3, "key": "c"})

        # The seal is what breaks it: before one, the read never touches an
        # Iceberg metadata path at all.
        assert log.scan().read_all().num_rows == 3

    with litelink.open("data", "s") as reopened:
        assert reopened.scan().read_all().num_rows == 3


def test_a_relative_root_is_resolved_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolved at construction, so a later chdir cannot move the log."""
    monkeypatch.chdir(tmp_path)
    log = litelink.new("data", "s", schema=SCHEMA, sort_by=("event_ts",))
    root = log.root

    (tmp_path / "elsewhere").mkdir()
    monkeypatch.chdir(tmp_path / "elsewhere")

    log.append({"event_ts": 1, "key": "a"})
    log.seal(flush=True)

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
    assert recovered.staging_max_bytes == LogConfig().staging_max_bytes
    assert recovered.staging_snapshot_retention == timedelta(minutes=15)
    assert recovered.published_snapshot_retention == timedelta(hours=1)


def test_a_config_from_before_the_split_retention_fills_both() -> None:
    """A record written before #113 holds one `snapshot_retention`, which then
    governed both tables. It fills both new settings, so a value raised for long
    scans keeps protecting the published table too.

    Falsify by reading the legacy key into staging alone: the published
    retention comes back as the 1 hour default.
    """
    written = json.dumps({"snapshot_retention": 86_400.0})

    recovered = LogConfig.from_json(written)

    assert recovered.staging_snapshot_retention == timedelta(days=1)
    assert recovered.published_snapshot_retention == timedelta(days=1)
    assert LogConfig.from_json(recovered.to_json()) == recovered


def test_a_config_written_by_a_newer_version_still_opens() -> None:
    """The same tolerance from the other side: an unknown setting is one this
    version does not have, not a reason to refuse the log."""
    written = json.dumps({"target_seal_size": 4096, "a_setting_from_the_future": 7})

    assert LogConfig.from_json(written).target_seal_size == 4096


def test_every_database_a_restore_needs_is_listed(tmp_path: Path) -> None:
    """§3a: what a WAL-shipping sidecar has to replicate.

    All three, and the set is the library's to know rather than an operator's
    to guess. `buffer.db` holds rows no Parquet file has yet — the one everyone
    remembers. `catalog.db` says which files the staging table is made of.
    `published.db` says the same for the published table, so omitting it leaves the
    objects in S3 intact with nothing able to say what they are.

    The rewrite scratch is excluded: it is derived from the published table and deleted
    at the end of the operation that creates it, so replicating it would ship a
    temporary file to object storage to no purpose.
    """
    layout = Layout(tmp_path, "s")

    assert set(layout.databases) == {
        layout.buffer_db,
        layout.catalog_db,
        layout.published_db,
    }
    assert all(path.suffix == ".db" for path in layout.databases)


def test_replication_config_names_every_database_and_the_wal_prefix(
    tmp_path: Path,
) -> None:
    """§3a, derived rather than restated.

    Everything in the config comes from the log: the file set, the destination
    beside the published data, and the endpoint from the credentials it was
    opened with. A config written by hand knows what someone remembered.
    """
    s3 = S3Options(endpoint="http://127.0.0.1:9000", region="us-east-1")
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True),
        published="s3://bucket/prefix",
        s3_options=s3,
    ) as log:
        rendered = log.replication_config()

        for database in log.databases:
            assert f"path: {database}" in rendered
            # The replica path carries the stream name once, in the prefix —
            # `prefix/<name>/_wal/<db>` — because all three databases now live
            # in the stream's own directory. Two logs sharing a published prefix
            # still cannot collide, which is the property that matters: the
            # name is in the destination rather than in the key.
            relative = database.relative_to(tmp_path / "s").as_posix()
            assert f"path: prefix/s/{WAL_PREFIX}/{relative}" in rendered

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
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True),
        published="s3://bucket/prefix",
        s3_options=S3Options(endpoint="http://127.0.0.1:9000", region="us-east-1"),
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
    """§3a. The un-published window, stated as the only thing litestream takes.

    "Retain WAL above the published offset" is the way to say what this is for
    and it is not expressible: litestream v0.5.16's knobs are all durations —
    `snapshot.interval`, `snapshot.retention`, `l0-retention` — and its CLI has
    no `snapshot` verb to force one after a publish and make a duration behave
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
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True, wal_retention=timedelta(hours=6)),
        published="s3://bucket/prefix",
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
    """WAL segments go beside the published data, so a local-only log has
    nowhere to put them — refused at construction rather than at the first
    attempt to write a config nothing could act on."""
    with pytest.raises(ValueError, match="wal_replication"):
        validate(SCHEMA, (), LogConfig(wal_replication=True), None)


def test_the_replication_config_is_written_beside_the_log(tmp_path: Path) -> None:
    """Derived like every other path: a setting for it would be one more thing
    to keep in step with the log it describes."""
    with litelink.new(
        tmp_path,
        "s",
        schema=SCHEMA,
        config=LogConfig(wal_replication=True),
        published="s3://bucket/prefix",
    ) as log:
        written = log.write_replication_config()

        # In the STREAM's directory, not the root: one sidecar per stream now,
        # so two logs under one root no longer write one file.
        assert written == tmp_path / "s" / "litestream.yml"
        assert written.read_text() == log.replication_config()


def test_the_published_read_falls_back_to_the_aws_credential_chain() -> None:
    """The bug a local endpoint cannot catch.

    On an ordinary AWS host the credentials are in a profile, in instance
    metadata, or behind SSO — never in the arguments. pyiceberg and s3fs
    resolve those themselves, so writes worked; DuckDB got a secret with no
    keys, treated it as anonymous, and answered every read of the published table
    with 403. Against rustfs it never appeared, because a local endpoint always
    has explicit keys to pass.
    """
    rendered = secret_sql(S3Options(region="us-west-1"))

    assert "PROVIDER credential_chain" in rendered
    # Resolved at creation, so a long-lived connection must refresh it (#108).
    assert "REFRESH auto" in rendered
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
        litelink.new(tmp_path, "one", schema=SCHEMA, published=shared) as first,
        litelink.new(tmp_path, "two", schema=SCHEMA, published=shared) as second,
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
    Publish pushes nothing, the watermark stands still, eviction pins on it and
    the staging table grows without bound — while every pass rewrites every file
    to no purpose. Merging a run of one is a no-op rewrite in any case.
    """
    for value in (0, 1):
        with pytest.raises(ValueError, match="compact_min_files must be at least 2"):
            litelink.new(
                tmp_path,
                f"s{value}",
                schema=SCHEMA,
                sort_by=("event_ts",),
                config=LogConfig(compact_min_files=value),
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
    log = litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",))
    with log:
        with log._buffer._lock:
            log._buffer._con.execute(
                "CREATE TABLE lease (role TEXT PRIMARY KEY, owner TEXT, expires_at INT)"
            )

    with pytest.raises(RuntimeError, match="coordinated through a `lease` table"):
        litelink.open(tmp_path, "s")


def test_negative_staging_retention_is_refused(tmp_path: Path) -> None:
    """The sign check its twin has always had.

    Eviction computes `now - staging_retention`, so a negative one puts the
    cutoff in the FUTURE and every file in the log is stale. On a local-only
    log that is silent deletion of the only copy of everything, at every
    negative value, from one sign slip — and the zero rule never caught it
    because it tests equality.
    """
    with pytest.raises(ValueError, match="staging_retention must not be negative"):
        litelink.new(
            tmp_path,
            "s",
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=LogConfig(staging_retention=timedelta(hours=-1)),
        )


@pytest.mark.parametrize(
    "name", ["staging_snapshot_retention", "published_snapshot_retention"]
)
def test_negative_snapshot_retention_is_refused(tmp_path: Path, name: str) -> None:
    """The same sign slip, one field over, for either table.

    Expiry computes `now - retention`, so a negative one puts the cutoff in the
    future: every superseded file is unlinked in the pass that supersedes it,
    and I6's promise — the grace must exceed the longest scan — is not
    shortened but inverted. Zero stays legal; it means "no grace", which tests
    and demos ask for on purpose.
    """
    with pytest.raises(ValueError, match=f"{name} must not be negative"):
        litelink.new(
            tmp_path,
            "s",
            schema=SCHEMA,
            sort_by=("event_ts",),
            config=replace(LogConfig(), **{name: timedelta(hours=-1)}),
        )

    litelink.new(
        tmp_path,
        "zero",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=replace(LogConfig(), **{name: timedelta(0)}),
    ).close()


def test_a_second_handle_sees_settings_changes_with_no_refresh(tmp_path: Path) -> None:
    """The policy is not copied into a process.

    This is the property that replaces twelve `refresh` calls. Each of them
    existed to drag a process-local copy back into agreement with the log, and
    every defect in this seam was a decision that read the copy where no such
    call had been placed — eight review rounds running, each in a place the
    previous round had not looked.

    There is one copy now, in `meta`. A handle that has never heard of a change
    cannot be wrong about it, because it holds nothing to be wrong with.
    """
    first = litelink.new(
        tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",), config=LogConfig()
    )
    with first, litelink.open(tmp_path, "s") as second:
        assert second.config.staging_max_bytes == LogConfig().staging_max_bytes

        first.set_config(LogConfig(staging_max_bytes=4242))

        # `second` was never told, and never asked.
        assert second.config.staging_max_bytes == 4242
        assert second._maintenance.config.staging_max_bytes == 4242
        assert second._buffer.config().staging_max_bytes == 4242


def test_the_sort_order_is_recovered_from_meta_not_from_the_catalog(
    tmp_path: Path,
) -> None:
    """§4's clustering has to survive a machine, and the catalog does not.

    `sort_by` used to live only in the staging table, read back at open.
    `catalog.db` is replicated but records ABSOLUTE paths to local metadata no
    sidecar ships, so a failover rebuilds the staging table rather than restoring
    it — and has to be told what order to declare. The published table could not answer
    either: `open_published` never declared one.

    So `meta` carries it, and this proves `open` reads THAT rather than the
    table: the declaration is removed from under a closed log and the order
    still comes back.
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        assert log._table.sort_by() == ("event_ts",)  # noqa: SLF001

    catalog = SqlCatalog(
        "staging",
        uri=Layout(tmp_path, "s").catalog_uri,
        warehouse=Layout(tmp_path, "s").warehouse_uri,
    )
    table = catalog.load_table(Layout(tmp_path, "s").table_id)
    with table.update_sort_order() as update:
        update._apply()  # noqa: SLF001

    with litelink.open(tmp_path, "s") as reopened:
        assert reopened.sort_by == ("event_ts",), (  # noqa: SLF001
            "open read the table's declaration rather than meta"
        )


def test_a_log_with_no_stored_sort_order_is_refused(tmp_path: Path) -> None:
    """Absent means damaged, not "unsorted".

    `new` always writes it, so defaulting to `()` would silently de-cluster
    every file the next compaction rewrites while the table still declared a
    key. Same rule as the stored config, one line above it.
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)):
        pass

    buffer = Buffer.open(Layout(tmp_path, "s").buffer_db, SCHEMA)
    try:
        buffer._con.execute("DELETE FROM meta WHERE k = 'sort_by'")  # noqa: SLF001
    finally:
        buffer.close()

    with pytest.raises(ValueError, match="no stored sort order"):
        litelink.open(tmp_path, "s")


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

    `WriteHandle.restore` reads this to decide whether it is resuming an interrupted
    restore. Answering False when the catalog merely could not be READ tells it
    to resume over a LIVE log — and the resume path reserves 2**20 offsets on
    it, deletes every `extent` row including queued cuts, wipes `sealing` and
    `claim`, drops the published catalog row, and deletes buffered rows below the
    frontier.

    It is reachable without corruption: `catalog.db` runs in
    `journal_mode=delete` with no busy timeout on this connection, so a read
    landing in another process's commit window returns SQLITE_BUSY.
    `_recorded_location` refuses the same conflation, in the same words.
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA):
        pass

    layout = Layout(tmp_path, "s")

    assert LogTable.exists_for(layout) is True

    # Not a SQLite database at all, standing in for a read that cannot answer.
    layout.catalog_db.write_bytes(b"not a database, and not an absent one")

    with pytest.raises(LookupError):
        LogTable.exists_for(layout)

    # And the caller that matters treats it as "exists" rather than proceeding.
    with pytest.raises((LookupError, FileExistsError, ValueError, RuntimeError)):
        litelink.restore(tmp_path, "s", published="s3://bucket/prefix")


@pytest.mark.parametrize(
    ("published", "expected"),
    [
        # The one that motivated the rule: a single missing slash. It reaches
        # `litestream_config` intact, splits at the first slash it finds, and
        # yields the bucket `s3:` — which litestream rejects as a YAML error
        # naming a generated file the caller never sees.
        ("s3:/bucket/prefix", "missing a slash"),
        ("s3:bucket/prefix", "missing a slash"),
        # No scheme at all: the same positional split, silently addressing a
        # bucket named after the first path segment.
        ("bucket/prefix", "must be an s3:// URI"),
        ("/local/path", "must be an s3:// URI"),
        ("https://bucket/prefix", "must be an s3:// URI"),
        # A scheme and nothing else.
        ("s3://", "names no bucket"),
        ("s3:///prefix", "names no bucket"),
        # Characters that break the config the bucket is written into. A `:`
        # in a bucket name emits `bucket: a:b`, a plain scalar ending in a
        # colon, which is the same YAML failure by another route.
        ("s3://a:b/prefix", "cannot appear in one"),
        ("s3://a b/prefix", "cannot appear in one"),
    ],
)
def test_a_malformed_published_uri_is_refused_with_its_own_shape(
    published: str, expected: str
) -> None:
    """Every consumer of `published` parses it POSITIONALLY.

    Which is why a malformed prefix cannot be left to fail downstream: it does
    not fail, it means something else. `s3:/bucket/prefix` is a valid string to
    every one of them, describing a bucket called `s3:`.

    Falsify by deleting the `validate_published` call in `validate`: every case
    here is accepted, and the first four reach litestream as
    `yaml: line 5: mapping values are not allowed in this context`.
    """
    with pytest.raises(ValueError, match=expected):
        validate_published(published)


def test_a_well_formed_published_uri_is_accepted() -> None:
    """The control for the rule above, and the reason it is not stricter.

    Bucket naming is the endpoint's rule, not litelink's — rustfs and MinIO
    accept names AWS would refuse, and this library is tested against both. So
    the check covers the shape the parsers depend on and the characters that
    break the generated config, and nothing else.

    Falsify by tightening `_BUCKET_CHARS` to AWS's own rule: the underscore
    and uppercase cases here start failing.
    """
    for published in (
        "s3://bucket",
        "s3://bucket/prefix",
        "s3://bucket/prefix/nested",
        "s3://bucket/prefix/",
        "s3://has_underscore/p",
        "s3://Has-Upper/p",
        "s3://has.dots/p",
    ):
        validate_published(published)


def test_every_entry_point_taking_a_published_table_checks_its_shape(
    tmp_path: Path,
) -> None:
    """`new` reaches it through `validate`; `restore` does not.

    `restore` takes no schema or config, so it never calls `validate` — and it
    hands the string straight to the litestream config writer. It needs the
    check of its own, and asserting both together makes a third entry point
    without one visible here.

    Falsify by removing `restore`'s explicit `validate_published` call: it raises
    RuntimeError from the subprocess instead, after creating a root.
    """
    bad = "s3:/bucket/prefix"

    with pytest.raises(ValueError, match="missing a slash"):
        litelink.new(tmp_path / "new", "s", schema=SCHEMA, published=bad)

    with pytest.raises(ValueError, match="missing a slash"):
        litelink.restore(tmp_path / "restore", "s", published=bad)

    # And nothing was created on the way to refusing. A shape error is decided
    # from the argument alone, so it must land before any directory does.
    assert not (tmp_path / "new").exists()
    assert not (tmp_path / "restore").exists()


def test_reclaiming_the_buffer_frees_pages_and_keeps_every_offset(
    tmp_path: Path,
) -> None:
    """Bounded free list, and I9 across the rewrite that bounds it.

    SQLite never shrinks a file on its own, so a buffer that publishes for months
    keeps every page it has ever needed. Invisible locally — the free list is
    reused — and paid off-box by every follower, because litestream replicates
    the FILE. Measured on a real 1-day-old capture: 457 MB holding 20,658 live
    rows, 92% of its pages free, restoring in 12.5 s against 0.8 s vacuumed.

    Driven through `Buffer` rather than a whole log on purpose. This is a
    property of the reclaim, and reaching it through seals would spend the
    test's time writing thousands of tiny Parquet files that have nothing to do
    with what is asserted.

    The second half matters more. `VACUUM` rebuilds the database, and this runs
    where the published table may have taken every row, so if the rewrite disturbed
    `litelink_offset` — its values, its gaps, or the AUTOINCREMENT counter
    behind them — the log would reissue offsets the published table already holds (I9).
    Offsets are compared exactly, gaps included.

    Falsify by making `reclaim_free_pages` return 0 without vacuuming: the
    ratio assertion fails with most of the file on the free list.
    """
    payload = "k" * 400
    buffer = Buffer.open(tmp_path / "buffer.db", SCHEMA)
    try:
        issued: list[int] = []
        for _ in range(12):
            issued += buffer.append(
                {"event_ts": i, "key": payload} for i in range(2000)
            )

        # The published table takes all but a tail, which is `evict("buffer")`'s shape.
        boundary = issued[-300]
        buffer.evict_rows(0, boundary + 1)
        # Then punch holes in what is left, so a renumbering rewrite would show
        # up as closed gaps rather than having to be inferred.
        survivors = [o for o in issued if o > boundary and o % 3 == 0]
        buffer._con.executemany(  # noqa: SLF001
            'DELETE FROM buffer WHERE "litelink_offset" = ?',
            [(o,) for o in issued if o > boundary and o % 3 != 0],
        )

        before = _page_stats(buffer)

        assert before[1] / before[0] >= 0.5, (
            "the fixture did not bloat the free list, so this proves nothing"
        )

        reclaimed = buffer.reclaim_free_pages()
        pages, free = _page_stats(buffer)

        assert reclaimed > 0
        assert free / pages < 0.5, (
            f"the buffer kept {free}/{pages} pages free after a reclaim"
        )

        live = [
            int(row[0])
            for row in buffer._con.execute(  # noqa: SLF001
                'SELECT "litelink_offset" FROM buffer ORDER BY "litelink_offset"'
            )
        ]

        assert live == survivors, "offsets were renumbered or gaps were closed"
        assert buffer.append([{"event_ts": 1, "key": "after"}]) == [max(issued) + 1], (
            "AUTOINCREMENT restarted inside the log"
        )
    finally:
        buffer.close()


def test_reclaiming_a_small_buffer_does_nothing(tmp_path: Path) -> None:
    """The floor, and why it is not a policy knob.

    A young log crosses any ratio on its first published pass — delete most of a
    few hundred KB and the free list is most of the file. Reclaiming there costs
    an exclusive lock, and stalls appends, to save a rounding error on the wire.

    Falsify by removing the `_VACUUM_FLOOR_BYTES` term: this reclaims, and every
    log pays a write stall from its first pass onward.
    """
    buffer = Buffer.open(tmp_path / "buffer.db", SCHEMA)
    try:
        # Enough to leave a free list that is most of the file, and far enough
        # under the floor that reclaiming it would be pure cost.
        issued = buffer.append({"event_ts": i, "key": "k" * 400} for i in range(4000))
        buffer.evict_rows(0, issued[-1] + 1)
        pages, free = _page_stats(buffer)
        page_size = int(buffer._con.execute("PRAGMA page_size").fetchone()[0])  # noqa: SLF001

        assert free / pages >= 0.5, "the premise is a mostly-empty file"
        assert free * page_size < 8 * 1024 * 1024, "the premise is that it is SMALL"
        assert buffer.reclaim_free_pages() == 0
    finally:
        buffer.close()


def _page_stats(buffer: Buffer) -> tuple[int, int]:
    """`(page_count, freelist_count)` for the buffer's own connection."""
    con = buffer._con  # noqa: SLF001

    return (
        int(con.execute("PRAGMA page_count").fetchone()[0]),
        int(con.execute("PRAGMA freelist_count").fetchone()[0]),
    )


def test_vacuum_free_ratio_survives_the_round_trip_and_is_bounded() -> None:
    """The setting is persisted like every other, and refused when it cannot mean
    what it says.

    A share of a file has no reading outside `[0, 1]`, and the mistake it invites
    is a percentage: `vacuum_free_ratio=50` would never fire, silently, for the
    life of the log — which is the same shape of failure as never setting it, and
    so invisible.

    Falsify by deleting the range rule in `validate`: `50` is accepted and the
    log quietly never reclaims.
    """
    config = replace(LogConfig(), vacuum_free_ratio=0.5)

    assert LogConfig.from_json(config.to_json()).vacuum_free_ratio == 0.5
    assert LogConfig.from_json(LogConfig().to_json()).vacuum_free_ratio is None
    # A log written before the setting existed reads as "never reclaim".
    assert (
        LogConfig.from_json(json.dumps({"target_seal_size": 4096})).vacuum_free_ratio
        is None
    )

    for bad in (50, -0.1, 1.5):
        with pytest.raises(ValueError, match="between 0 and 1"):
            validate(SCHEMA, (), replace(LogConfig(), vacuum_free_ratio=bad), None)


def test_maintain_reclaims_only_when_the_ratio_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`advance` is where a deployment opts into the pause, and the default is
    to decline.

    `VACUUM` blocks appends for as long as the live data takes to copy, so it is
    the one part of a maintenance pass that a caller must ask for. This asserts
    both directions, because a default that quietly reclaimed would put that
    pause on every existing deployment at upgrade.

    Falsify by calling `reclaim("buffer")` unconditionally in `advance`: the
    `None` case records a call.
    """
    calls: list[float] = []
    with litelink.new(tmp_path, "off", schema=SCHEMA) as log:
        monkeypatch.setattr(
            log._buffer,  # noqa: SLF001
            "reclaim_free_pages",
            lambda ratio=0.0: calls.append(ratio) or 0,
        )
        log.advance()

        assert calls == [], "reclaimed without being asked"

    with litelink.new(
        tmp_path / "on",
        "on",
        schema=SCHEMA,
        config=replace(LogConfig(), vacuum_free_ratio=0.25),
    ) as log:
        monkeypatch.setattr(
            log._buffer,  # noqa: SLF001
            "reclaim_free_pages",
            lambda ratio=0.0: calls.append(ratio) or 0,
        )
        log.advance()

        assert calls == [0.25], "the configured ratio did not reach the reclaim"


# -- immutable logs (#93) -------------------------------------------------------


def test_a_log_has_no_way_to_change_its_schema() -> None:
    """A log's schema is fixed at creation (§9): to change it, start a new log.

    Absent rather than refusing, so a caller finds out from a type checker.
    """
    for gone in ("add_column", "rename_column", "drop_column"):
        assert not hasattr(WriteHandle, gone), f"WriteHandle still has {gone}"


def test_a_log_an_older_release_left_mid_add_column_is_refused(tmp_path: Path) -> None:
    """The change cannot be finished here, so the refusal says where it can be.

    An `add_column` interrupted under 0.5 leaves its intent in `meta`; this
    release has no schema changes to finish it with, and opening the log
    anyway would serve it under a schema the table may not agree with.

    Falsify by removing the intent check from `_declared_schema`: the log
    opens as though nothing were outstanding.
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        log._buffer.set_meta(  # noqa: SLF001
            "schema_intent", json.dumps({"add": "late", "type": "00"})
        )

    with pytest.raises(ValueError, match=r"older release.*litelink 0\.5\.1"):
        litelink.open(tmp_path, "s")


def test_a_pre_02_log_names_the_release_that_can_migrate_it(tmp_path: Path) -> None:
    """The 0.1 migration is gone from this release; the refusal points at the
    last one that carries it, and at the command to run there.

    Falsify by restoring the old message, which named a `litelink.migrate`
    this release no longer has.
    """
    layout = Layout(tmp_path, "s")
    layout.legacy_catalog_db.parent.mkdir(parents=True, exist_ok=True)
    layout.legacy_catalog_db.touch()

    with pytest.raises(FileNotFoundError, match=r"(?s)litelink 0\.5\.1.*--apply"):
        litelink.open(tmp_path, "s")


def test_the_config_is_written_under_the_staging_names_and_reads_the_old_ones() -> None:
    """New logs store `staging_retention` (#98); one written before the
    rename stored `local_retention`, and still opens with its policy. Its
    `local_rows`, retired with `staging_rows`, is ignored.

    Falsify by dropping the fallback to `local_retention` in `from_json`: the
    old record reads back with no age limit.
    """
    config = LogConfig(staging_retention=timedelta(hours=2))
    written = json.loads(config.to_json())
    assert written["staging_retention"] == 7200
    assert "local_retention" not in written

    old = json.dumps({"local_retention": 7200, "local_rows": 500})
    recovered = LogConfig.from_json(old)
    assert recovered.staging_retention == timedelta(hours=2)


def test_set_config_takes_no_claim(tmp_path: Path) -> None:
    """The policy is one `meta` row that every decision re-reads, and the
    published location it is validated against is fixed at creation — so a
    configuration change neither waits for maintenance nor excludes it.

    Falsify by having `set_config` take the whole-log claim: it is refused
    while another owner holds the log.
    """
    with litelink.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        held = log._lease("maintain")
        assert held.acquire()
        try:
            log.set_config(LogConfig(staging_max_bytes=7))
        finally:
            held.release()

        assert log.config.staging_max_bytes == 7
