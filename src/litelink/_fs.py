"""Filesystem primitives with the durability ordering the spec requires."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


def fsync(path: Path) -> None:
    """Fsync a file AND the directory entry that reaches it (I1).

    On most filesystems the contents can be durable while the name that reaches
    them is not, so publishing only the file leaves a manifest entry pointing at a
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
    """Write a data file and make it durable — this or `stream_parquet`, the
    only two places that do.

    A seal goes through here; a compaction and a bulk ingest go through
    `stream_parquet`. The pair keeps the codec a setting with one home: there
    is no version of either call that omits it, so a new write site cannot
    silently take pyarrow's Snappy default, which on a JSON payload column
    measured 97 bytes/row against 51 for zstd.

    The fsync is not separable from the write (I1): a manifest entry for a file
    that did not survive the crash is the thing §4 orders these two against.
    """
    pq.write_table(table, path, compression=compression)
    fsync(path)


@contextmanager
def stream_parquet(
    path: Path, schema: pa.Schema, compression: str
) -> Iterator[Callable[[pa.Table], int]]:
    """`write_parquet` for a file written a row group at a time.

    Yields a function that writes one table as ONE row group and returns the
    file's size on disk so far — exact, because each row group reaches the
    sink compressed and whole; only the footer is still to come. That is what
    lets a writer stop at a size on disk without estimating it.

    Durable on a clean exit, like `write_parquet`; on an exception the file is
    closed and left for whoever claimed its path to remove.
    """
    with pa.OSFile(str(path), "wb") as sink:
        writer = pq.ParquetWriter(sink, schema, compression=compression)
        try:

            def write(table: pa.Table) -> int:
                writer.write_table(table, row_group_size=max(1, table.num_rows))

                return sink.tell()

            yield write
        finally:
            writer.close()

    fsync(path)
