"""Filesystem primitives with the durability ordering the spec requires."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

if TYPE_CHECKING:
    from pathlib import Path

    import pyarrow as pa


def fsync(path: Path) -> None:
    """Fsync a file AND the directory entry that reaches it (I1).

    On most filesystems the contents can be durable while the name that reaches
    them is not, so syncing only the file leaves a manifest entry pointing at a
    path that may not exist after a crash.
    """
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def write_parquet(table: pa.Table, path: Path, compression: str) -> None:
    """Write a data file and make it durable, in the one place that does it.

    Every data file this library creates goes through here — a seal, a
    compaction, an archive rewrite, a bulk ingest — because the pair of calls
    is the same pair every time and the codec is a setting that must not have
    four homes. It had none: all four sites called `pq.write_table` with no
    `compression`, taking pyarrow's Snappy default, and on a JSON payload
    column that measured 97 bytes/row against 51 for zstd. A fifth write site
    added later cannot silently take a different answer, because there is no
    version of this call that omits it.

    The fsync is not separable from the write (I1): a manifest entry for a file
    that did not survive the crash is the thing §4 orders these two against.
    """
    pq.write_table(table, path, compression=compression)
    fsync(path)
