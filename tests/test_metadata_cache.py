"""Immutable metadata read once per process, through a bounded cache (#184).

Every manifest, manifest list and `metadata.json` is written once under a
name no write reuses, so the shared FileIO serves a repeat read from memory.
"""

from __future__ import annotations

import collections
from typing import TYPE_CHECKING

import pytest
from pyiceberg.io.pyarrow import PyArrowFile

from litelink._table import _immutable, _ImmutableObjects
from tests.test_publish import published_log, rows

if TYPE_CHECKING:
    from pathlib import Path

    from litelink._s3 import S3Options


def test_the_cache_is_bounded_by_bytes_and_evicts_the_least_recent() -> None:
    """Falsify by never evicting: the oldest entry is still served."""
    cache = _ImmutableObjects(budget=100)
    cache.put("s3://b/a.avro", b"a" * 20)
    cache.put("s3://b/b.avro", b"b" * 20)
    cache.put("s3://b/c.avro", b"c" * 20)
    cache.put("s3://b/d.avro", b"d" * 20)
    assert cache.get("s3://b/a.avro") is not None  # now the most recent

    cache.put("s3://b/e.avro", b"e" * 20)
    cache.put("s3://b/f.avro", b"f" * 20)

    assert cache.get("s3://b/b.avro") is None
    assert cache.get("s3://b/a.avro") is not None
    assert cache.get("s3://b/f.avro") is not None


def test_an_object_above_a_quarter_of_the_budget_is_not_cached() -> None:
    """One large manifest must not evict everything else."""
    cache = _ImmutableObjects(budget=100)
    cache.put("s3://b/small.avro", b"s" * 10)
    cache.put("s3://b/large.avro", b"l" * 26)

    assert cache.get("s3://b/large.avro") is None
    assert cache.get("s3://b/small.avro") is not None


@pytest.mark.parametrize(
    ("location", "cached"),
    [
        ("s3://b/p/s/metadata/snap-1-0-abc.avro", True),
        ("s3://b/p/s/metadata/abc-m0.avro", True),
        ("s3://b/p/s/metadata/00003-abc.metadata.json", True),
        # Rewritten in place: caching it would pin a table at one version.
        ("s3://b/p/s/metadata/version-hint.text", False),
        ("s3://b/p/s/data/sealed/1-5-abc.parquet", False),
        ("file:///tmp/p/s/metadata/00003-abc.metadata.json", False),
        ("/tmp/s/metadata/snap-1-0-abc.avro", False),
    ],
)
def test_only_immutable_metadata_on_object_storage_is_cached(
    location: str, cached: bool
) -> None:
    assert _immutable(location) is cached


@pytest.mark.s3
def test_idle_maintenance_reads_no_metadata_twice(
    tmp_path: Path, bucket: str, s3: S3Options, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A maintainer tick with nothing new re-read every `metadata.json` and
    manifest it had already read; now each is fetched once, and a commit's
    new objects still are.

    Falsify by returning the plain `PyArrowFile` from `new_input`: idle ticks
    GET `metadata.json` again every time.
    """
    with published_log(tmp_path, bucket, s3) as log:
        log.extend(rows(40))
        log.advance(flush=True)

        gets: collections.Counter[str] = collections.Counter()
        real = PyArrowFile.open

        def counted(self: PyArrowFile, seekable: bool = True):  # noqa: ANN202
            if self.location.startswith("s3://"):
                gets[self.location] += 1

            return real(self, seekable)

        monkeypatch.setattr(PyArrowFile, "open", counted)
        for _ in range(3):
            log.advance()

        metadata = {k: v for k, v in gets.items() if k.endswith(".metadata.json")}
        assert all(v == 1 for v in metadata.values()), metadata
        repeats = {k: v for k, v in gets.items() if k.endswith(".avro") and v > 1}
        assert not repeats, repeats

        # A commit moves the table on, and its new metadata is read.
        before = log.published_through()
        log.extend(rows(40))
        log.advance(flush=True)
        assert log.published_through() > before
