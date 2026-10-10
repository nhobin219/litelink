"""Truncating and deleting a retired log, from its published table alone (#181).

A retired log cannot be opened for writing, and after a failover it may have
no local directory at all, so both operations take the published location and
a name. Most of these use a published table in a local directory of its own,
apart from the log's, so "no local directory" is one `rmtree` away.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import litelink
from litelink import LogConfig
from litelink._layout import Layout
from litelink._s3 import S3Options
from litelink._table import StaticTable, _published_location, shared_file_io
from tests.test_log import SCHEMA, rows

PER_SEAL = 4
SEALS = 5


def retired_log(
    root: Path, published: str, name: str = "s", s3: S3Options | None = None
) -> None:
    """Five published files of four rows, offsets 1-20, then retired."""
    log = litelink.new(
        root,
        name,
        schema=SCHEMA,
        sort_by=("event_ts", "key"),
        published=published,
        s3_options=s3,
        # Every file full, so `retire`'s last compaction merges none of them.
        config=LogConfig(target_seal_size=1 << 30, target_compact_size=1),
    )
    with log:
        for index in range(SEALS):
            log.extend(rows(PER_SEAL, start=index * PER_SEAL))
            log.seal(flush=True)

        log.publish(flush=True)
        log.retire()


def published_offsets(published: str, name: str = "s") -> list[int]:
    """What the published table holds, read through its version hint."""
    layout = Layout(Path("."), name)
    properties = S3Options().resolved().catalog_properties()
    io = shared_file_io(properties, published)
    location = _published_location(io, layout, published)
    assert location is not None
    table = StaticTable.from_metadata(location, properties)
    scanned = table.scan(selected_fields=("litelink_offset",)).to_arrow()

    return sorted(scanned.column("litelink_offset").to_pylist())


def parquet(directory: Path) -> set[Path]:
    return set(directory.rglob("*.parquet"))


@pytest.fixture
def where(tmp_path: Path) -> tuple[Path, str, Path]:
    """The log's root, its published URI and that directory, apart."""
    published = tmp_path / "published"
    published.mkdir()

    return tmp_path / "root", f"file://{published}", published


def test_truncate_drops_whole_files_from_a_retired_published_table(
    where: tuple[Path, str, Path],
) -> None:
    """Offset 11 lies inside the file starting at 9, which stays whole.

    Falsify by dropping the snap: the floor reads 11 and pyiceberg rewrites
    the straddling file.
    """
    root, published, _ = where
    retired_log(root, published)

    assert litelink.truncate(published, "s", below=11) == 9
    assert published_offsets(published) == list(range(9, 21))


def test_truncate_needs_no_local_directory(where: tuple[Path, str, Path]) -> None:
    """After a failover the published table is all there is."""
    root, published, _ = where
    retired_log(root, published)
    shutil.rmtree(root)

    assert litelink.truncate(published, "s", below=9) == 9
    assert published_offsets(published) == list(range(9, 21))


def test_truncate_refuses_a_live_log(where: tuple[Path, str, Path]) -> None:
    """A live log's truncation needs its claims and tier rows: its handle's."""
    root, published, _ = where
    with litelink.new(
        root, "s", schema=SCHEMA, sort_by=("event_ts",), published=published
    ) as log:
        log.extend(rows(8))
        log.seal(flush=True)
        log.publish(flush=True)

    with pytest.raises(ValueError, match="not a retired log"):
        litelink.truncate(published, "s", below=5)


def test_a_retired_log_read_locally_follows_the_truncate(
    where: tuple[Path, str, Path],
) -> None:
    """The local directory's catalog row and tier row were written at
    retirement, and a truncate from elsewhere moves the published table on
    without them. A read-only handle follows the version hint instead.

    Falsify by opening the published table through the catalog row for a
    retired log too: the scan still returns offsets 1-8, and `coverage`
    starts at 1.
    """
    root, published, _ = where
    retired_log(root, published)

    with litelink.open(root, "s", read_only=True) as before:
        assert before.coverage().published == (1, 21)

    litelink.truncate(published, "s", below=9)

    with litelink.open(root, "s", read_only=True) as log:
        scanned = log.scan(columns=["litelink_offset"]).read_all()
        assert sorted(scanned.column("litelink_offset").to_pylist()) == list(
            range(9, 21)
        )
        assert log.coverage().published == (9, 21)


def test_truncated_files_are_deleted_at_once(where: tuple[Path, str, Path]) -> None:
    """No maintainer runs on a retired log, so nothing would delete later what
    a truncate left for later: the files go in the call, with the snapshots
    and metadata that named them.

    Falsify by leaving the clean-up out: the two files stay.
    """
    root, published, directory = where
    retired_log(root, published)
    shutil.rmtree(root)
    before = parquet(directory)

    assert litelink.truncate(published, "s", below=9) == 9

    assert len(before - parquet(directory)) == 2
    assert published_offsets(published) == list(range(9, 21))


def test_a_clean_up_a_crash_skipped_is_done_by_the_next_call(
    where: tuple[Path, str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash after the truncating commit leaves the dropped files named by
    older snapshots; the next call cleans up whatever is unreferenced, even
    with nothing new to truncate.

    Falsify by cleaning up only what the call itself truncated: the files
    stay.
    """
    import litelink._retired as retired

    root, published, directory = where
    retired_log(root, published)
    before = parquet(directory)

    def crash(*_: object) -> None:
        raise RuntimeError("crashed")

    monkeypatch.setattr(retired, "_clean", crash)
    with pytest.raises(RuntimeError, match="crashed"):
        litelink.truncate(published, "s", below=9)

    assert parquet(directory) == before
    monkeypatch.undo()

    assert litelink.truncate(published, "s", below=9) == 9
    assert len(before - parquet(directory)) == 2
    assert published_offsets(published) == list(range(9, 21))


def test_truncate_refuses_when_another_call_moved_the_table(
    where: tuple[Path, str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two callers would each commit onto the metadata the hint named when
    they started. The hint is checked before each commit.

    Falsify by dropping the check: the truncate commits.
    """
    import litelink._retired as retired

    root, published, _ = where
    retired_log(root, published)
    real = retired._published_location
    reads: list[str] = []

    def moved_after_adoption(io, layout, prefix):  # noqa: ANN001, ANN202
        # The first read is the one the truncate adopts; every later one
        # finds another call's commit.
        reads.append(prefix)
        location = real(io, layout, prefix)
        return location if len(reads) == 1 else f"{location}.moved"

    monkeypatch.setattr(retired, "_published_location", moved_after_adoption)

    with pytest.raises(RuntimeError, match="moved"):
        litelink.truncate(published, "s", below=9)

    monkeypatch.setattr(retired, "_published_location", real)
    assert published_offsets(published) == list(range(1, 21))


def test_delete_removes_a_retired_log_and_nothing_beside_it(
    where: tuple[Path, str, Path],
) -> None:
    """Everything under `<published>/<name>/`, and the local directory given
    `root` — and nothing else: not a sibling log whose name extends this one.

    Falsify by deleting every object whose key starts with `<published>/s`:
    `s-v2` goes too.
    """
    root, published, directory = where
    retired_log(root, published, "s")
    retired_log(root, published, "s-v2")

    litelink.delete(published, "s", root=root)

    assert not (directory / "s").exists()
    assert not (root / "s").exists()
    assert published_offsets(published, "s-v2") == list(range(1, 21))
    assert (root / "s-v2").exists()

    # Again: nothing left, nothing refused.
    litelink.delete(published, "s", root=root)


def test_delete_without_root_leaves_the_local_directory(
    where: tuple[Path, str, Path],
) -> None:
    """After a failover there is no local directory to name, and without
    `root` nothing local is touched."""
    root, published, directory = where
    retired_log(root, published)

    litelink.delete(published, "s")

    assert not (directory / "s").exists()
    assert (root / "s").exists()


@pytest.mark.parametrize("left", ["hint and current", "current only"])
def test_delete_resumes_after_a_crash(where: tuple[Path, str, Path], left: str) -> None:
    """The files that say the log is retired go last, so a crash at either
    late step leaves enough for the next call to finish.

    Falsify by deleting the current `metadata.json` before the hint, or by
    refusing when the hint is gone: the second call raises and the files stay.
    """
    root, published, directory = where
    retired_log(root, published)
    metadata = directory / "s" / "metadata"
    hint = metadata / "version-hint.text"
    current = metadata / f"{hint.read_text().strip()}.metadata.json"
    keep = {current} if left == "current only" else {current, hint}
    for path in (directory / "s").rglob("*"):
        if path.is_file() and path not in keep:
            path.unlink()

    litelink.delete(published, "s", root=root)

    assert not (directory / "s").exists()
    assert not (root / "s").exists()


def test_truncate_never_creates_a_table_a_delete_removed(
    where: tuple[Path, str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A `delete` landing between the truncate's read of the hint and its
    adoption must not leave a fresh empty table behind, with no
    `litelink.retired`, that every later `delete` refuses.

    Falsify by adopting through `open_published(repair=True)`: it creates.
    """
    import litelink._retired as retired

    root, published, directory = where
    retired_log(root, published)
    real = retired._Catalog.register_table

    def racing(self, identifier, location):  # noqa: ANN001, ANN202
        litelink.delete(published, "s")
        return real(self, identifier, location)

    monkeypatch.setattr(retired._Catalog, "register_table", racing)
    with pytest.raises(FileNotFoundError):
        litelink.truncate(published, "s", below=9)

    assert not any((directory / "s").rglob("*.json"))


def test_delete_refuses_a_live_log(where: tuple[Path, str, Path]) -> None:
    root, published, directory = where
    with litelink.new(
        root, "s", schema=SCHEMA, sort_by=("event_ts",), published=published
    ) as log:
        log.extend(rows(8))
        log.seal(flush=True)
        log.publish(flush=True)

    with pytest.raises(ValueError, match="retired"):
        litelink.delete(published, "s", root=root)

    with pytest.raises(ValueError, match="retired"):
        litelink.delete(published, "s")

    assert parquet(directory)
    assert (root / "s" / "buffer.db").exists()


def test_truncate_never_deletes_the_log(where: tuple[Path, str, Path]) -> None:
    """Truncating everything leaves an empty table that still exists: reads
    find nothing, and removing the log is `delete`'s, at the caller's say."""
    root, published, _ = where
    retired_log(root, published)

    assert litelink.truncate(published, "s", below=21) == 21
    assert published_offsets(published) == []


@pytest.mark.s3
def test_truncate_and_delete_on_s3(tmp_path: Path, bucket: str, s3: S3Options) -> None:
    """The listing `delete` makes, and the commits `truncate` makes, against
    object storage."""
    published = f"s3://{bucket}/logs"
    retired_log(tmp_path, published, s3=s3)
    shutil.rmtree(tmp_path / "s")

    assert litelink.truncate(published, "s", below=9, s3_options=s3) == 9

    litelink.delete(published, "s", s3_options=s3)
    io = shared_file_io(s3.resolved().catalog_properties(), published)
    assert _published_location(io, Layout(tmp_path, "s"), published) is None
