"""Adding a column: the non-breaking half of §9.

`rename_column` and `drop_column` stay unimplemented — both are breaking for
consumers (I10) and want a versioning conversation this does not have.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest

from litelink._s3 import S3Options
from litelink.log import Log, LogConfig

SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("key", pa.string()),
    ]
)


def open_log(root: Path) -> Log:
    if (root / "catalog.db").exists():
        return Log.open(root, "s")

    return Log.new(root, "s", schema=SCHEMA)


def rows(n: int, *, start: int = 0, **extra: object) -> list[dict[str, object]]:
    return [
        {"event_ts": 1000 + i, "key": f"k{i}", **extra} for i in range(start, start + n)
    ]


def test_a_column_added_to_a_log_with_sealed_files(tmp_path: Path) -> None:
    """Old files read null; nothing is rewritten to make that true.

    That is Iceberg's field-ID promise, and the reason this half is
    non-breaking: the new column gets a new ID, so a file written before it
    simply does not carry that ID and reads as null.
    """
    with open_log(tmp_path) as log:
        log.extend(rows(200))
        log.seal()
        before = {f.path for f in log._table.data_files()}

        log.add_column("region", pa.string())
        log.extend(rows(5, start=200, region="eu-west-1"))

        table = log.scan().read_all()

        assert table.num_rows == 205
        assert table.column("region").to_pylist()[:200] == [None] * 200
        assert set(table.column("region").to_pylist()[200:]) == {"eu-west-1"}
        assert {f.path for f in log._table.data_files()} == before, (
            "adding a column must not rewrite a single file"
        )


def test_a_sealer_that_never_appends_does_not_lose_the_new_column(
    tmp_path: Path,
) -> None:
    """The reproduction that killed the first design.

    A sealer never calls `append`, so a design that revalidated the schema per
    append would leave this process building its projection from the columns it
    was constructed with. `add_files` treats an optional field missing from a
    file as compatible and null-fills it, and `finish_seal` then deletes the
    buffer rows — so `region='eu-west-1'` would be acknowledged with an offset
    and silently gone.

    Falsify by making `Buffer.shape()` return `self._fallback` unconditionally:
    the sealed rows come back with `region` null.
    """
    with open_log(tmp_path) as writer:
        writer.extend(rows(10))

        # Opened BEFORE the change, and it never appends — only seals.
        with Log.open(tmp_path, "s") as sealer:
            writer.add_column("region", pa.string())
            writer.extend(rows(5, start=10, region="eu-west-1"))

            sealer.seal()

            sealed = sealer.scan().read_all()

        assert sealed.num_rows == 15
        assert set(sealed.column("region").to_pylist()[10:]) == {"eu-west-1"}, (
            "a sealer holding a stale schema drops the column it never saw"
        )


def test_a_reader_opened_before_the_change_serves_the_new_column(
    tmp_path: Path,
) -> None:
    """A reader must not serve the old columns for the rest of its life.

    Its schema used to be injected at construction, so a long-lived reader
    opened before a change would keep projecting the columns it started with.
    """
    with open_log(tmp_path) as writer, Log.open(tmp_path, "s") as reader:
        writer.extend(rows(3))
        assert reader.scan().read_all().num_rows == 3

        writer.add_column("region", pa.string())
        writer.extend(rows(2, start=3, region="eu"))

        after = reader.scan().read_all()

        assert "region" in after.column_names
        assert after.column("region").to_pylist()[-2:] == ["eu", "eu"]


def test_append_accepts_the_new_column_and_still_refuses_unknown_ones(
    tmp_path: Path,
) -> None:
    """I17 moves WITH the schema, rather than being frozen at open."""
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="does not have"):
            log.append({"event_ts": 1, "key": "a", "region": "eu"})

        log.add_column("region", pa.string())
        log.append({"event_ts": 1, "key": "a", "region": "eu"})

        with pytest.raises(ValueError, match="does not have: \\['zone'\\]"):
            log.append({"event_ts": 2, "key": "b", "zone": "z"})


def test_a_second_change_is_refused_while_one_is_outstanding(
    tmp_path: Path,
) -> None:
    """And the refusal NAMES the pending column.

    Allowing it is fatal, not merely untidy: step 7 would write
    `declared + zone`, dropping `region` from the declaration while
    `union_by_name` keeps it in both Iceberg tables, and clear the intent —
    after which `_declared_schema` refuses to open the log for every process,
    reader and writer, on a log with every row intact.
    """
    with open_log(tmp_path) as log:
        log._buffer.set_meta("schema_intent", '{"add": "region", "type": ""}')

        with pytest.raises(ValueError, match="adding 'region' has not finished"):
            log.add_column("zone", pa.string())


def test_add_column_refuses_what_could_never_have_been_declared(
    tmp_path: Path,
) -> None:
    """The reserved name (I11), a type the creation gate refuses, and a name
    already in use.

    Nullability needs no case: `type_` is a DataType and carries none, so a
    required column is unrepresentable rather than refused — which is the
    right constraint, since rows predating the column have no value for it.
    """
    with open_log(tmp_path) as log:
        with pytest.raises(ValueError, match="I11"):
            log.add_column("litelink_offset", pa.int64())

        with pytest.raises(TypeError):
            log.add_column("bad", pa.list_(pa.int64()))

        with pytest.raises(ValueError, match="already exists"):
            log.add_column("key", pa.string())


def test_the_change_survives_a_reopen(tmp_path: Path) -> None:
    """Step 7 is durable, and the intent is cleared by it."""
    with open_log(tmp_path) as log:
        log.add_column("region", pa.string())
        log.extend(rows(3, region="eu"))
        log.seal()

    with open_log(tmp_path) as reopened:
        assert "region" in reopened.scan().read_all().column_names
        assert not reopened._buffer.get_meta("schema_intent")


def archived_log(root: Path, bucket: str, s3: S3Options) -> Log:
    return Log.new(
        root,
        "s",
        schema=SCHEMA,
        config=LogConfig(target_seal_size=64 * 1024),
        archive=f"s3://{bucket}/prefix",
        s3=s3,
    )


def test_the_archive_gets_the_column_before_the_local_table(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§9: the local table is a window and can be rebuilt; the archive cannot.

    Asserted by failing the LOCAL commit and checking the archive already has
    the column. The order is not cosmetic: `add_files` refuses a file carrying
    a column the table lacks, so an archive left behind while the buffer moves
    ahead means every later push fails permanently, I4 pins eviction, and
    local disk grows without bound.

    Falsify by swapping steps 4 and 5 in `_apply_add_column`: the archive is
    then missing `region` when the local commit fails.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(20))
        log.seal()
        log.sync()

        boom = RuntimeError("local commit failed")

        def fail(_schema: pa.Schema) -> None:
            raise boom

        monkeypatch.setattr(log._table, "add_column", fail)

        with pytest.raises(RuntimeError):
            log.add_column("region", pa.string())

        remote = log._archive.table(repair=False)

        assert remote is not None
        assert "region" in remote.arrow_schema().names, (
            "the archive must be widened first"
        )
        assert "region" not in log._table.arrow_schema().names


def test_a_change_interrupted_before_step_seven_completes_on_reopen(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The intent stands, and the next open finishes the job.

    Every step is settled by PROBING, so a replay against a step that already
    landed is a no-op — `union_by_name` is idempotent where
    `update_schema().add_column` raises `name already exists`. Falsify by
    swapping it: recovery then raises inside `open`, which is unguarded, so
    every writer fails to open while `reader()` still works.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(20))
        log.seal()
        log.sync()

        # Interrupt between step 6 and step 7: both tables and the buffer DDL
        # have landed, the declaration has not.
        def stop(_pairs: dict[str, str]) -> None:
            msg = "crashed before step 7"
            raise RuntimeError(msg)

        monkeypatch.setattr(log._buffer, "set_meta_all", stop)

        with pytest.raises(RuntimeError):
            log.add_column("region", pa.string())

        monkeypatch.undo()

        assert log._buffer.get_meta("schema_intent"), "the intent must stand"
        assert "region" not in log._buffer.shape().columns

    with Log.open(tmp_path, "s", s3=s3) as reopened:
        assert "region" in reopened._buffer.shape().columns, (
            "recovery must finish the change"
        )
        assert not reopened._buffer.get_meta("schema_intent")

        reopened.extend(rows(3, start=20, region="eu"))

        assert reopened.scan().read_all().column("region").to_pylist()[-1] == "eu"


def test_add_column_with_the_archive_unreachable_leaves_the_log_writable(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """§11: a log works with no network, and a failed change must not undo that.

    The call fails — the archive has to be widened first, and it cannot be.
    What must NOT happen is the log becoming unopenable for writing until the
    bucket returns. Recovery leaves the intent standing and returns, so the
    log opens, appends, seals and reads; the change simply has not finished.

    Falsify by letting `_ArchiveDeferred` escape `_recover_schema_change`:
    every writer then fails to open while `reader()` still works.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(10))
        log.seal()
        log.sync()

    dead = S3Options(
        endpoint="http://127.0.0.1:1",
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
    )

    with Log.open(tmp_path, "s", s3=dead) as offline:
        with pytest.raises(Exception, match="unreachable|[Cc]ould not connect"):
            offline.add_column("region", pa.string())

        # The log still works, and no value of the new column can be stored —
        # the declaration is unchanged, so the INSERT column list is too.
        offline.extend(rows(5, start=10))

        assert offline.scan().read_all().num_rows == 15
        assert "region" not in offline._buffer.shape().columns

    # And it opens again, still offline, rather than wedging on the replay.
    with Log.open(tmp_path, "s", s3=dead) as still_offline:
        assert still_offline.scan().read_all().num_rows == 15
        assert still_offline._buffer.get_meta("schema_intent"), "the intent stands"

    # The archive returns; the next open finishes what was started.
    with Log.open(tmp_path, "s", s3=s3) as back:
        assert "region" in back._buffer.shape().columns
        assert not back._buffer.get_meta("schema_intent")

        back.extend(rows(2, start=15, region="eu"))

        assert back.scan().read_all().column("region").to_pylist()[-1] == "eu"


def test_recovery_defers_when_the_archive_commit_itself_fails(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The archive is REACHABLE and the widening commit still fails.

    Distinct from the unreachable case, and not covered by it: there
    `Archive.table()` raises and is converted to `_ArchiveDeferred` before any
    commit is attempted. Here the table opens fine and `union_by_name` fails
    part way — a full S3 metadata commit, the widest window in the sequence.

    Recovery must still defer rather than fail the open. Falsify by narrowing
    the catch in `_recover_schema_change` to `_ArchiveDeferred`: this open then
    raises, and because `recover()` is unguarded in `open`, every writer is
    locked out while `reader()` keeps working.
    """
    from litelink.log import _intent

    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(10))
        log.seal()
        log.sync()
        # An outstanding change, with both Iceberg tables untouched.
        log._buffer.set_meta("schema_intent", _intent("region", pa.string()))

    from litelink._table import LogTable

    def explode(self: LogTable, schema: pa.Schema) -> None:
        msg = "AWS Error NETWORK_CONNECTION during PutObject"
        raise OSError(msg)

    monkeypatch.setattr(LogTable, "add_column", explode)

    with Log.open(tmp_path, "s", s3=s3) as offline:
        assert offline._buffer.get_meta("schema_intent"), "the intent must stand"
        assert "region" not in offline._buffer.shape().columns

        offline.extend(rows(3, start=10))

        assert offline.scan().read_all().num_rows == 13

    monkeypatch.undo()

    with Log.open(tmp_path, "s", s3=s3) as healed:
        assert "region" in healed._buffer.shape().columns
        assert not healed._buffer.get_meta("schema_intent")


def test_reattaching_an_archive_behind_the_schema_is_refused(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """Detach, add a column, re-attach — the door that skips step 4.

    Attaching does no schema work: `open_archive` declares a schema only when
    it CREATES a table, and nothing re-declares an existing one. So the
    archive stays narrow and every later push fails permanently with
    `PyArrow table contains more columns`, `sync` never advances, and I4 pins
    eviction on the files it could not push — local disk grows without bound.

    Falsify by removing `_refuse_archive_behind`: the attach succeeds and
    `sync()` then raises on every call, with the watermark frozen.
    """
    with archived_log(tmp_path, bucket, s3) as log:
        log.extend(rows(20))
        log.seal()
        log.sync()

        uri = log.archive

        # Detaching needs the retention floors clear; see `_refuse_lossy_detach`.
        log.set_config(replace(log.config, local_retention=None, local_rows=None))
        log.set_archive(None)
        log.add_column("region", pa.string())

        with pytest.raises(ValueError, match="missing \\['region'\\]"):
            log.set_archive(uri)

        # And the log is still usable, having refused rather than half-attached.
        log.extend(rows(3, start=20, region="eu"))

        assert log.scan().read_all().num_rows == 23
