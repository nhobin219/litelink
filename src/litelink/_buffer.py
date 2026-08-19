"""The SQLite write buffer (SPEC §2, §3).

One database per stream. Durable on commit — `synchronous=FULL` with WAL — and
that is the whole durability story: there is no in-memory staging layer whose
loss a SIGKILL could expose.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

# Arrow type -> SQLite column affinity. Affinity is advisory in SQLite, but
# declaring it keeps the values round-tripping as the type the Iceberg schema
# will demand at seal, rather than as whatever Python handed in.
_AFFINITY = (
    (pa.types.is_boolean, "INTEGER"),
    (pa.types.is_integer, "INTEGER"),
    (pa.types.is_floating, "REAL"),
    (pa.types.is_temporal, "INTEGER"),
    (pa.types.is_string, "TEXT"),
    (pa.types.is_large_string, "TEXT"),
    (pa.types.is_binary, "BLOB"),
    (pa.types.is_large_binary, "BLOB"),
)


def _affinity(type_: pa.DataType) -> str:
    for predicate, affinity in _AFFINITY:
        if predicate(type_):
            return affinity

    msg = f"no SQLite affinity for Arrow type {type_}"
    raise TypeError(msg)


class Buffer:
    """The unsealed tail of a log."""

    def __init__(self, path: Path, schema: pa.Schema) -> None:
        """Open or create the buffer at `path`.

        `schema` is the application's columns, without `offset`.
        """
        self._schema = schema
        self._columns = tuple(schema.names)
        self._con = sqlite3.connect(path, isolation_level=None)
        self._con.execute("PRAGMA journal_mode=WAL")
        # §3's durability claim rests on this line. WAL alone fsyncs at
        # checkpoint, not at commit, which would put committed rows back in the
        # OS page cache — the exact loss this library exists to prevent.
        self._con.execute("PRAGMA synchronous=FULL")
        self._create()
        self._bytes = self._measure()

    def _create(self) -> None:
        columns = ",\n  ".join(
            f'"{name}" {_affinity(self._schema.field(name).type)}'
            for name in self._columns
        )
        # AUTOINCREMENT, not a bare INTEGER PRIMARY KEY: buffer rows are deleted
        # at every seal, and a rowid alias would reissue offsets already
        # committed to Iceberg once the table empties, silently corrupting every
        # tier boundary in §7 (I9, §2).
        self._con.execute(f"""
            CREATE TABLE IF NOT EXISTS buffer (
              "offset" INTEGER PRIMARY KEY AUTOINCREMENT,
              {columns}
            )
        """)
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS sealing (
              start_offset INTEGER, end_offset INTEGER, rel_path TEXT
            )
        """)
        # The same intent record as `sealing`, for the other operation that
        # creates a data file. Both exist so that no file is ever written whose
        # path this database does not already hold — see `pending_delete`.
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS compacting (
              lo INTEGER, hi INTEGER, rel_path TEXT
            )
        """)
        # The deletion queue. A file leaves the current snapshot long before it
        # may be deleted (I6), so the interval has to be remembered somewhere;
        # remembering it here is what makes reclamation a keyed read of this
        # table rather than a directory walk looking for things nobody claimed.
        #
        # `superseded_at`, not a precomputed deadline: the grace period is
        # `snapshot_retention`, and freezing it at enqueue time would mean a
        # lowered setting never applied to anything already queued.
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS pending_delete (
              rel_path TEXT PRIMARY KEY, superseded_at INTEGER NOT NULL
            )
        """)
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"
        )

    # -- size accounting --------------------------------------------------
    #
    # `target_size` is in bytes, so the seal trigger needs the buffered size on
    # every append — and a SUM() over the whole table per append is a full scan
    # of the thing being appended to. Tracked incrementally instead: measured
    # once at open, incremented per row, re-measured after a seal empties it.
    # Losing the counter to a restart costs nothing, since open re-measures.
    #
    # Approximate on purpose. SQLite stores an integer in 1-8 bytes and
    # `octet_length` reports its TEXT width, so neither side of this is exact.
    # It is a policy trigger, not an accounting record; being within a few
    # percent of the true size is what `target_size` actually needs.

    def byte_size(self) -> int:
        """Approximate bytes currently buffered."""
        return self._bytes

    def _measure(self) -> int:
        terms = ["8"]  # offset
        for name in self._columns:
            affinity = _affinity(self._schema.field(name).type)
            terms.append(
                f'coalesce(octet_length("{name}"), 0)'
                if affinity in ("TEXT", "BLOB")
                else "8"
            )

        row = self._con.execute(
            f"SELECT coalesce(sum({' + '.join(terms)}), 0) FROM buffer"
        ).fetchone()

        return int(row[0])

    def _row_bytes(self, row: Mapping[str, object]) -> int:
        total = 8
        for name in self._columns:
            value = row.get(name)
            if isinstance(value, bytes | bytearray | memoryview):
                total += len(value)
            elif isinstance(value, str):
                total += len(value.encode())
            elif value is not None:
                total += 8

        return total

    # -- write ------------------------------------------------------------

    def append(self, rows: Iterable[Mapping[str, object]]) -> list[int]:
        """Insert rows in one transaction. Returns the assigned offsets.

        One transaction means one fsync amortised across the batch, which is
        the whole of §3's throughput story.
        """
        placeholders = ", ".join("?" * len(self._columns))
        names = ", ".join(f'"{c}"' for c in self._columns)
        sql = f"INSERT INTO buffer ({names}) VALUES ({placeholders})"

        offsets: list[int] = []
        added = 0
        cursor = self._con.cursor()
        cursor.execute("BEGIN")
        try:
            for row in rows:
                self._reject_offset(row)
                cursor.execute(sql, tuple(row.get(c) for c in self._columns))
                # lastrowid is the assigned offset, available inside the open
                # transaction and before the row is visible to anyone else.
                offsets.append(int(cursor.lastrowid or 0))
                added += self._row_bytes(row)

            cursor.execute("COMMIT")
        except BaseException:
            cursor.execute("ROLLBACK")
            raise

        self._bytes += added

        return offsets

    @staticmethod
    def _reject_offset(row: Mapping[str, object]) -> None:
        """I11: `offset` is assigned by the library, never accepted."""
        if "offset" in row:
            msg = "`offset` is assigned by the library and cannot be supplied (I11)"
            raise ValueError(msg)

    # -- read -------------------------------------------------------------

    def next_offset(self) -> int:
        """The offset the next append will receive.

        Read from `sqlite_sequence`, which AUTOINCREMENT maintains as the
        highest value ever assigned and never lowers — not from `max(offset)`,
        which drops back to null every time a seal empties the table. No
        catalog resolve is needed: the sequence already outlives the rows.
        """
        row = self._con.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
        ).fetchone()

        return (row[0] if row else 0) + 1

    def extent(self) -> tuple[int, int] | None:
        """`(min, max)` offset currently buffered, or None if empty."""
        lo, hi = self._con.execute(
            'SELECT min("offset"), max("offset") FROM buffer'
        ).fetchone()

        return None if lo is None else (int(lo), int(hi))

    def rows_below(self, end: int) -> pa.Table:
        """Buffered rows with `offset < end`, as Arrow."""
        names = ", ".join(f'"{c}"' for c in ("offset", *self._columns))
        cursor = self._con.execute(
            f'SELECT {names} FROM buffer WHERE "offset" < ? ORDER BY "offset"', (end,)
        )
        columns = list(zip(*cursor.fetchall(), strict=True)) or [
            () for _ in range(len(self._columns) + 1)
        ]
        schema = pa.schema(
            [pa.field("offset", pa.int64(), nullable=False), *self._schema]
        )

        return pa.table(
            [
                pa.array(values, type=field.type)
                for values, field in zip(columns, schema, strict=True)
            ],
            schema=schema,
        )

    # -- seal bookkeeping -------------------------------------------------

    def claim_seal(self, start: int, end: int, rel_path: str) -> None:
        """Record the seal intent before the file exists (I2).

        The path is persisted, not recomputed: a retry that recomputed it could
        land on a different date directory and strand the first file.
        """
        self._con.execute("BEGIN")
        self._con.execute("DELETE FROM sealing")
        self._con.execute(
            "INSERT INTO sealing (start_offset, end_offset, rel_path) VALUES (?, ?, ?)",
            (start, end, rel_path),
        )
        self._con.execute("COMMIT")

    def pending_seal(self) -> tuple[int, int, str] | None:
        """The in-flight seal, if a crash left one."""
        row = self._con.execute(
            "SELECT start_offset, end_offset, rel_path FROM sealing"
        ).fetchone()

        return None if row is None else (int(row[0]), int(row[1]), str(row[2]))

    def finish_seal(self, end: int) -> None:
        """Delete sealed rows and clear the intent.

        Garbage collection, not correctness: the read boundary in §7 already
        excludes these rows the moment the Iceberg commit lands, so the window
        between that commit and this call is safe in both directions.
        """
        self._con.execute("BEGIN")
        self._con.execute('DELETE FROM buffer WHERE "offset" < ?', (end,))
        self._con.execute("DELETE FROM sealing")
        self._con.execute("COMMIT")
        self._bytes = self._measure()

    # -- compaction bookkeeping -------------------------------------------

    def claim_compaction(self, lo: int, hi: int, rel_path: str) -> None:
        """Record a compaction's output path before the file exists.

        The seal's I2 argument applied to the other writer: a compaction that
        crashes between writing and committing leaves a file on disk, and the
        only way to find it without a directory scan is to have written its
        name down first.
        """
        self._con.execute("BEGIN")
        self._con.execute("DELETE FROM compacting")
        self._con.execute(
            "INSERT INTO compacting (lo, hi, rel_path) VALUES (?, ?, ?)",
            (lo, hi, rel_path),
        )
        self._con.execute("COMMIT")

    def pending_compaction(self) -> tuple[int, int, str] | None:
        row = self._con.execute("SELECT lo, hi, rel_path FROM compacting").fetchone()

        return None if row is None else (int(row[0]), int(row[1]), str(row[2]))

    def clear_compaction(self) -> None:
        self._con.execute("DELETE FROM compacting")

    # -- deletion queue ---------------------------------------------------

    def enqueue_deletions(self, rel_paths: Iterable[str], superseded_at: int) -> None:
        """Queue superseded files, stamped with when they left the table.

        Enqueued in the same breath as the commit that superseded them, which
        is the only moment their paths are known without going to look.
        """
        self._con.execute("BEGIN")
        self._con.executemany(
            "INSERT OR IGNORE INTO pending_delete (rel_path, superseded_at) VALUES (?, ?)",
            [(p, superseded_at) for p in rel_paths],
        )
        self._con.execute("COMMIT")

    def due_deletions(self, cutoff: int) -> list[str]:
        """Files superseded at or before `cutoff` — now minus the grace period."""
        return [
            str(row[0])
            for row in self._con.execute(
                "SELECT rel_path FROM pending_delete WHERE superseded_at <= ?",
                (cutoff,),
            )
        ]

    def forget_deletion(self, rel_path: str) -> None:
        """Drop a queue entry, after its file is gone.

        Called after the unlink, never before: a crash in between leaves the
        row and the next drain retries an unlink that is already a no-op. The
        reverse order loses the path and leaks the file permanently.
        """
        self._con.execute("DELETE FROM pending_delete WHERE rel_path = ?", (rel_path,))

    def queued_deletions(self) -> list[str]:
        return [
            str(row[0])
            for row in self._con.execute("SELECT rel_path FROM pending_delete")
        ]

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        self._con.close()
