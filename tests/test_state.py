"""Keyed state committed with `extend`, recorded with each published snapshot (#202).

A caller that dedupes what it appends keeps facts about earlier appends — a
producer's last sequence number — and those facts are only as good as their
agreement with the rows. So they commit in the append's transaction, and a
published snapshot records them as of its own last offset, which is what a
restore from the published table alone brings back.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import textwrap
from datetime import timedelta
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
import pytest

import litelink
from litelink import LogConfig, RetiredError, WriteHandle
from litelink._buffer import Buffer
from litelink._layout import Layout
from litelink._state import decode, encode
from tests.test_log import SCHEMA, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._s3 import S3Options


def local(root: Path, published: Path, config: LogConfig | None = None) -> WriteHandle:
    """A log publishing to a local directory, so nothing here needs a network."""
    return litelink.new(
        root,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=config or LogConfig(),
        published=f"file://{published}",
    )


def versions(log: WriteHandle) -> list[tuple[str, int, bytes]]:
    """Every stored version, oldest first."""
    return [
        (str(key), int(through), bytes(value))
        for key, through, value in log._buffer._con.execute(  # noqa: SLF001
            "SELECT key, through, value FROM state ORDER BY version"
        )
    ]


def recorded(log: WriteHandle) -> tuple[int, dict[str, bytes]] | None:
    """What the published table's current snapshot records, read back."""
    published = log._published.require()  # noqa: SLF001
    published.reload()
    location = published.state_location
    if location is None:
        return None

    return decode(published.read_bytes(location))


# -- the write ----------------------------------------------------------------


def test_state_commits_with_the_rows_and_reads_back_newest(tmp_path: Path) -> None:
    with local(tmp_path / "a", tmp_path / "p") as log:
        assert log.state("producer:a") is None
        offsets = log.extend(rows(3), state={"producer:a": b"1", "producer:b": b"x"})
        assert log.state("producer:a") == b"1"

        log.extend(rows(2, start=3), state={"producer:a": b"2"})
        assert log.state("producer:a") == b"2"
        assert log.state("producer:b") == b"x", "a key not written keeps its value"
        assert offsets == [1, 2, 3]


def test_states_filters_by_a_literal_prefix(tmp_path: Path) -> None:
    """`%` and `_` in a prefix are characters, not patterns.

    Falsify by matching with `LIKE ? || '%'`: `p_` then matches `pq:1`.
    """
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend([], state={"p_:1": b"a", "pq:1": b"b", "p%:1": b"c", "o:1": b"d"})

        assert log.states("p_") == {"p_:1": b"a"}
        assert log.states("p%") == {"p%:1": b"c"}
        assert set(log.states()) == {"p_:1", "pq:1", "p%:1", "o:1"}


def test_state_with_no_rows_is_written_alone_at_the_last_offset(
    tmp_path: Path,
) -> None:
    """The seam write: no offset consumed, tagged with the last one issued."""
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend(rows(4))
        assert log.extend([], state={"k": b"v"}) == []
        assert log.end_offset() == 5, "no offset consumed"
        assert versions(log) == [("k", 4, b"v")]


def test_a_bad_row_rolls_the_state_back(tmp_path: Path) -> None:
    """One transaction: a row refused after the state was handed in leaves
    neither, and consumes no offset."""
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend(rows(1), state={"k": b"before"})
        with pytest.raises(ValueError, match="nope"):
            log.extend([*rows(2, start=1), {"nope": 1}], state={"k": b"after"})

        assert log.state("k") == b"before"
        assert log.end_offset() == 2
        assert log.buffered_rows() == 1


def test_a_failed_state_insert_rolls_the_rows_back(tmp_path: Path) -> None:
    """The other direction: the rows are inserted, then the state write fails.

    Falsify by committing the rows before `_put_state` (move the COMMIT up):
    the rows survive and the offsets are consumed.
    """
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend(rows(1), state={"k": b"before"})
        log._buffer._con.execute(  # noqa: SLF001
            "CREATE TEMP TRIGGER boom BEFORE INSERT ON main.state"
            " BEGIN SELECT RAISE(ABORT, 'boom'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="boom"):
            log.extend(rows(3, start=1), state={"k": b"after"})

        log._buffer._con.execute("DROP TRIGGER boom")  # noqa: SLF001
        assert log.state("k") == b"before"
        assert log.buffered_rows() == 1
        assert log.end_offset() == 2, "the offsets went back with the rows"


@pytest.mark.parametrize(
    ("state", "error"),
    [
        ({"": b"v"}, "non-empty str"),
        ({1: b"v"}, "non-empty str"),
        ({"k": "text"}, "is bytes, not str"),
        ({"k": None}, "is bytes, not NoneType"),
    ],
)
def test_a_bad_entry_refuses_the_whole_call(
    tmp_path: Path, state: dict[object, object], error: str
) -> None:
    with local(tmp_path / "a", tmp_path / "p") as log:
        with pytest.raises(TypeError, match=error):
            log.extend(rows(2), state=state)  # ty: ignore[invalid-argument-type]

        assert log.end_offset() == 1
        assert log.states() == {}


def test_state_survives_a_process_that_never_closed(tmp_path: Path) -> None:
    """Durable when `extend` returns, like the rows: a writer that exits without
    closing anything leaves both, and a reopen reads them.

    Falsify by writing state outside the append's transaction with no commit:
    the child's rows survive and the state does not.
    """
    root = tmp_path / "a"
    local(root, tmp_path / "p").close()
    child = textwrap.dedent(f"""
        import os, litelink
        from tests.test_log import rows
        log = litelink.open({str(root)!r}, "s")
        log.extend(rows(3), state={{"k": b"durable"}})
        os._exit(0)
    """)
    subprocess.run(  # noqa: S603
        [sys.executable, "-c", child],
        check=True,
        cwd=os.getcwd(),
        env={**os.environ, "PYTHONPATH": os.getcwd()},
    )

    with litelink.open(root, "s") as log:
        assert log.state("k") == b"durable"
        assert log.buffered_rows() == 3

    with litelink.open(root, "s", read_only=True) as reader:
        assert reader.state("k") == b"durable", "a reader reads it too"


def test_a_retired_log_refuses_state(tmp_path: Path) -> None:
    """Rows alone used to be what the retired trigger saw, and a state write
    with no rows inserts none. Falsify by dropping `refuse_retired_state`."""
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend(rows(2), state={"k": b"v"})
        log.retire()
        with pytest.raises(RetiredError):
            log.extend([], state={"k": b"w"})

        assert log.state("k") == b"v"


# -- the seal never splits a batch that carries state -----------------------


def test_a_batch_with_state_is_cut_at_its_end(tmp_path: Path) -> None:
    """A publish point inside such a batch would publish some of its rows and
    none of its state.

    Falsify by cutting on the crossing row whatever `entries` holds: the
    first group closes at offset 3, inside the batch.
    """
    config = LogConfig(target_seal_rows=3)
    with local(tmp_path / "a", tmp_path / "p", config) as log:
        log.extend(rows(7), state={"k": b"v"})
        log.extend(rows(7, start=7))
        groups = log._buffer._con.execute(  # noqa: SLF001
            "SELECT start_offset, end_offset FROM extent"
            " WHERE end_offset IS NOT NULL ORDER BY start_offset"
        ).fetchall()

    assert groups[0] == (1, 8), "the state batch's rows stay in one group"
    assert groups[1:] == [(8, 11), (11, 14)], "a plain batch is cut as before"


# -- versions, and what a publish records -----------------------------------


def test_versions_inside_the_open_group_collapse(tmp_path: Path) -> None:
    """No publish point lies between them, so only the newest can be asked for.

    Falsify by removing the DELETE in `_put_state`: four versions of `k`.
    """
    with local(tmp_path / "a", tmp_path / "p") as log:
        for i in range(4):
            log.extend(rows(1, start=i), state={"k": str(i).encode()})

        assert versions(log) == [("k", 4, b"3")]


def test_versions_across_a_cut_are_kept_until_published(tmp_path: Path) -> None:
    """A version below a closed group's end is what a publish through that end
    records, so it stays until the watermark passes it, and then only each
    key's newest at or below the watermark does.

    Falsify by removing the prune in `confirm_published`: `k`'s version at 2
    outlives the publish.
    """
    config = LogConfig(target_seal_rows=2)
    with local(tmp_path / "a", tmp_path / "p", config) as log:
        log.extend(rows(2), state={"k": b"a"})  # group [1, 3) closes
        log.extend(rows(2, start=2), state={"k": b"b"})  # group [3, 5) closes
        log.extend(rows(1, start=4), state={"k": b"c"})  # open group
        assert [v[1] for v in versions(log)] == [2, 4, 5]

        log.seal(flush=True)
        log.advance(flush=True)
        assert log.published_through() == 5
        assert versions(log) == [("k", 5, b"c")]


def test_a_publish_records_the_state_as_of_its_last_offset(tmp_path: Path) -> None:
    """Not the buffer's current state: what was committed after the published
    rows is not in the record.

    Falsify by recording `states()` instead of `state_as_of`: the record holds
    `later`.
    """
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend(rows(5), state={"p:1": b"one", "p:2": b"two"})
        log.seal(flush=True)
        log.extend(rows(3, start=5), state={"p:1": b"later"})
        # Publishes the sealed file alone: the later rows are not sealed.
        log.publish(flush=True)
        through = log.published_through()
        assert through == 5
        assert recorded(log) == (5, {"p:1": b"one", "p:2": b"two"})
        assert log.state("p:1") == b"later"


def test_a_log_without_state_publishes_no_state(tmp_path: Path) -> None:
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend(rows(5))
        log.advance(flush=True)
        published = log._published.require()  # noqa: SLF001
        assert "litelink.state" not in published.properties
        assert not (tmp_path / "p" / "s" / "state").exists()


def test_superseded_state_files_are_deleted_and_the_current_one_is_not(
    tmp_path: Path,
) -> None:
    """Each published commit names a new file; the old ones go through the
    deletion queue, and the drain's veto keeps the one the table names.

    Falsify by dropping the state file from `referenced_paths`: with a zero
    grace the drain deletes the current file too, which `supersede_state_files`
    never queued — so also queue it by hand, as an older build or a bug might.
    """
    config = LogConfig(published_snapshot_retention=timedelta(0))
    directory = tmp_path / "p" / "s" / "state"
    with local(tmp_path / "a", tmp_path / "p", config) as log:
        for i in range(3):
            log.extend(rows(2, start=2 * i), state={"k": str(i).encode()})
            log.advance(flush=True)

        published = log._published.require()  # noqa: SLF001
        published.reload()
        current = published.state_location
        assert current is not None
        log._buffer.enqueue_deletions([current], 0)  # noqa: SLF001
        log._maintenance.drain_published(timedelta(0))  # noqa: SLF001

        left = sorted(p.name for p in directory.iterdir())
        assert left == [current.rsplit("/", 1)[1]], "only the current file is left"
        assert recorded(log) == (6, {"k": b"2"})


def test_a_state_file_whose_register_did_not_land_is_queued(tmp_path: Path) -> None:
    """The name goes down before the object (I2); a commit that never names it
    leaves it in `state_published` until the next push queues it.

    Falsify by uploading before `intend_state_file`: the crashed upload's
    object is in no table, and survives the drain.
    """
    config = LogConfig(published_snapshot_retention=timedelta(0))
    directory = tmp_path / "p" / "s" / "state"
    with local(tmp_path / "a", tmp_path / "p", config) as log:
        log.extend(rows(2), state={"k": b"a"})
        published = log._published.require()  # noqa: SLF001
        # A push that uploaded and died before its register.
        assert log._write_state(published, 2)  # noqa: SLF001
        assert len(list(directory.iterdir())) == 1

        log.advance(flush=True)
        log._maintenance.drain_published(timedelta(0))  # noqa: SLF001
        published.reload()
        current = published.state_location
        assert current is not None
        assert [p.name for p in directory.iterdir()] == [current.rsplit("/", 1)[1]]


# -- restore --------------------------------------------------------------------


def test_a_restore_from_the_published_table_brings_the_state_back(
    tmp_path: Path,
) -> None:
    """As of the published end, not the dead buffer's newest: the rows that
    come back and the state that describes them agree.

    Falsify by removing `state=recorded_state` from `_restore_published`: the
    restored log has no state.
    """
    first = tmp_path / "a"
    with local(first, tmp_path / "p") as log:
        log.extend(rows(5), state={"p:1": b"one", "p:2": b"two"})
        log.advance(flush=True)
        log.extend(rows(3, start=5), state={"p:1": b"lost with the machine"})

    shutil.rmtree(first)
    with litelink.restore(
        tmp_path / "b", "s", published=f"file://{tmp_path / 'p'}"
    ) as restored:
        assert restored.states() == {"p:1": b"one", "p:2": b"two"}
        assert restored.scan().read_all().num_rows == 5
        # And it carries on: a new write is newer than the restored record.
        restored.extend(rows(1, start=5), state={"p:1": b"next"})
        assert restored.state("p:1") == b"next"


def test_a_restore_from_a_replica_takes_newer_published_state(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """A replica from before the last publish holds older state than the
    published record: per key, the newer wins.

    Falsify by removing the `adopt_state` from the replica path: `p:1` comes
    back as `old`.
    """
    where = f"s3://{bucket}/replica-state"
    first = tmp_path / "a"
    second = tmp_path / "b"
    log = litelink.new(
        first,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        published=where,
        s3_options=s3,
    )
    with log:
        log.extend(rows(2), state={"p:1": b"old", "p:2": b"two"})
        # The replica, taken here: before the next commit and publish.
        (second / "s").mkdir(parents=True)
        source = sqlite3.connect(Layout(first, "s").buffer_db)
        copy = sqlite3.connect(Layout(second, "s").buffer_db)
        source.backup(copy)
        source.close()
        copy.close()
        log.extend(rows(2, start=2), state={"p:1": b"new"})
        log.advance(flush=True)

    with litelink.restore(second, "s", published=where, s3_options=s3) as restored:
        assert restored.states() == {"p:1": b"new", "p:2": b"two"}


def test_adopt_state_keeps_a_newer_local_value(tmp_path: Path) -> None:
    """Per key, by the offset each record is as of."""
    buffer = Buffer.open(tmp_path / "buffer.db", SCHEMA)
    try:
        buffer.append(rows(5), {"newer": b"local", "older": b"local"})
        buffer.adopt_state({"newer": b"published", "older": b"published"}, 3)
        assert buffer.states() == {"newer": b"local", "older": b"local"}

        buffer.adopt_state({"older": b"published", "absent": b"published"}, 9)
        assert buffer.states() == {
            "newer": b"local",
            "older": b"published",
            "absent": b"published",
        }
    finally:
        buffer.close()


def test_the_record_round_trips_and_names_its_offset() -> None:
    payload = encode({"a": b"\x00\x01", "b": b""}, 41)
    assert decode(payload) == (41, {"a": b"\x00\x01", "b": b""})

    table = pq.read_table(__import__("pyarrow").BufferReader(payload))
    with pytest.raises(ValueError, match="without its offset"):
        sink = __import__("pyarrow").BufferOutputStream()
        pq.write_table(table.replace_schema_metadata({}), sink)
        decode(sink.getvalue().to_pybytes())


def test_a_restore_from_an_s3_published_table_brings_the_state_back(
    tmp_path: Path, bucket: str, s3: S3Options
) -> None:
    """The object-store tier: the state file is written and read through the
    published table's FileIO, with its credentials."""
    where = f"s3://{bucket}/state"
    first = tmp_path / "a"
    log = litelink.new(
        first,
        "s",
        schema=SCHEMA,
        sort_by=("event_ts",),
        published=where,
        s3_options=s3,
    )
    with log:
        log.extend(rows(4), state={"k": b"published"})
        log.advance(flush=True)
        log.extend(rows(1, start=4), state={"k": b"not yet"})

    shutil.rmtree(first)
    with litelink.restore(
        tmp_path / "b", "s", published=where, s3_options=s3
    ) as restored:
        assert restored.state("k") == b"published"


def test_a_restore_refuses_a_state_record_an_older_build_left_behind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A build without state support publishes without touching the property,
    so the record trails the rows. Restoring it would bring rows back with
    state that does not describe them.

    Falsify by removing the check in `_restore_published`: the restore returns
    `a` beside rows through 4.
    """
    first = tmp_path / "a"
    with local(first, tmp_path / "p") as log:
        log.extend(rows(2), state={"k": b"a"})
        log.advance(flush=True)
        # What a 0.13.1 maintainer's publish does: no state in its commit.
        monkeypatch.setattr(WriteHandle, "_write_state", lambda *_: {})
        log.extend(rows(2, start=2), state={"k": b"b"})
        log.advance(flush=True)

    shutil.rmtree(first)
    with pytest.raises(ValueError, match="state record is as of 2"):
        litelink.restore(tmp_path / "b", "s", published=f"file://{tmp_path / 'p'}")


def test_a_push_with_nothing_to_upload_queues_a_crashed_state_file(
    tmp_path: Path,
) -> None:
    """Falsify by removing the settle before the early return: the crashed
    upload's name stays in `state_published` and is never queued."""
    with local(tmp_path / "a", tmp_path / "p") as log:
        log.extend(rows(2), state={"k": b"a"})
        log.advance(flush=True)
        published = log._published.require()  # noqa: SLF001
        crashed = log._write_state(published, 2)["litelink.state"]  # noqa: SLF001
        log.publish(flush=True)
        queued = log._buffer.queued_deletions()  # noqa: SLF001
        assert published.uri(crashed) in queued
