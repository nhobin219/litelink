"""Construction, validation, and what the injected collaborators buy.

`Log.__init__` takes built collaborators and does no I/O; `open` and
`open_readonly` are what construct and validate them. These tests exercise both
halves — the validation rules on the way in, and the substitutability that
having them as parameters is for.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from litelink import Log, LogConfig
from litelink._buffer import Buffer
from litelink._layout import Layout
from litelink._maintenance import Maintenance
from litelink._read import Reader
from litelink._table import LogTable
from litelink.log import table_schema, validate

if TYPE_CHECKING:
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
    assert table_schema(SCHEMA).names == ["offset", "event_ts", "key"]
    assert not table_schema(SCHEMA).field("offset").nullable


def test_init_does_no_io(tmp_path: Path) -> None:
    """The initialiser assigns; `open` is what touches the disk.

    Constructing a Log against a root that does not exist must therefore
    succeed, because nothing in __init__ should be looking at it.
    """
    layout = Layout(tmp_path / "does-not-exist", "s")
    layout.create()
    table = LogTable.create(layout, table_schema(SCHEMA), ("event_ts",))
    buffer = Buffer(layout.buffer_db, SCHEMA)

    config = LogConfig()
    log = Log(
        layout=layout,
        table=table,
        buffer=buffer,
        reader=Reader(layout, table, table_schema(SCHEMA)),
        maintenance=Maintenance(table, buffer, layout, config, ("event_ts",)),
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
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
    buffer = StubBuffer(layout.buffer_db, SCHEMA)
    config = LogConfig()

    log = Log(
        layout=layout,
        table=table,
        buffer=buffer,
        reader=Reader(layout, table, table_schema(SCHEMA)),
        maintenance=Maintenance(table, buffer, layout, config, ("event_ts",)),
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config,
    )

    assert log.end_offset() == 4_242
    log.close()


def test_layout_paths_are_derived_not_discovered(tmp_path: Path) -> None:
    """Every path a log writes is computable without touching the filesystem."""
    layout = Layout(tmp_path, "sensors")

    assert layout.buffer_db == tmp_path / "sensors" / "buffer.db"
    assert layout.table_id == "litelink.sensors"
    assert layout.seal_path(1, 51, date(2026, 8, 19)) == (
        "sensors/data/2026-08-19/1-51.parquet"
    )
    assert layout.compaction_path(1, 200) == "sensors/data/compacted/1-200.parquet"
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
    config = LogConfig(
        target_size=4096, compact_min_files=7, max_age=timedelta(seconds=90)
    )
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
        assert reopened._sort_by == ("key", "event_ts")
        assert reopened._archive == "s3://bucket/prefix"
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
        log.set_config(LogConfig(target_size=1234, compact_min_files=9))

    with Log.open(tmp_path, "s") as reopened:
        assert reopened.config.target_size == 1234
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
        assert reopened._archive == "s3://bucket/x"
        reopened.set_archive(None)

    with Log.open(tmp_path, "s") as detached:
        assert detached._archive is None


def test_sort_by_is_declared_on_the_table(tmp_path: Path) -> None:
    """§4: declared as table metadata AND applied at write time."""
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("key", "event_ts")) as log:
        assert log._table.sort_by() == ("key", "event_ts")


def test_changing_sort_by_requires_an_explicit_rewrite(tmp_path: Path) -> None:
    with Log.new(tmp_path, "s", schema=SCHEMA, sort_by=("event_ts",)) as log:
        with pytest.raises(ValueError, match="rewrite=True"):
            log.set_sort_by(("key",), rewrite=False)

        assert log._sort_by == ("event_ts",), "refused change must not apply"


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

        written = next(tmp_path.rglob("*/data/2*/*.parquet"))
        assert pq.read_table(written)["event_ts"].to_pylist() == [1, 2, 3]

        log.set_sort_by(("key",), rewrite=True)

        assert log._sort_by == ("key",)
        assert log._table.sort_by() == ("key",)
        merged = next(tmp_path.rglob("*compacted*/*.parquet"))
        assert pq.read_table(merged)["key"].to_pylist() == ["a", "b", "c"]
        # Reads are unaffected: order is by offset, and every row survives.
        rows = log.scan().read_all()
        assert rows["offset"].to_pylist() == [1, 2, 3]
        assert rows.num_rows == 3

    with Log.open(tmp_path, "s") as reopened:
        assert reopened._sort_by == ("key",), "the new order must survive a reopen"
