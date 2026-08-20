"""The SQLite write buffer (SPEC §2, §3).

One database per stream. Durable on commit — `synchronous=FULL` with WAL — and
that is the whole durability story: there is no in-memory staging layer whose
loss a SIGKILL could expose.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

from litelink._types import column_type


class Buffer:
    """The unsealed tail of a log."""

    def __init__(
        self, path: Path, schema: pa.Schema, *, readonly: bool = False
    ) -> None:
        """Open or create the buffer at `path`.

        `schema` is the application's columns, without `offset`.

        A readonly buffer opens the same file through SQLite's `mode=ro` URI so
        the handle cannot write even by mistake, and creates nothing. WAL allows
        any number of these alongside the single writer (§1).
        """
        self._schema = schema
        self._columns = tuple(schema.names)

        if readonly:
            self._con = sqlite3.connect(
                f"file:{path}?mode=ro",
                uri=True,
                isolation_level=None,
                check_same_thread=False,
            )
            self._reader = self._con
            self._bytes = 0
            return

        # check_same_thread=False because scheduling maintenance on a background
        # thread is the ordinary operational shape, and Python's guard would
        # otherwise forbid it. The C library is built serialized here
        # (`sqlite3.threadsafety == 3`), so the connection itself is safe; Log
        # holds a lock to serialise its own multi-statement sequences, which is
        # the part SQLite cannot know about.
        # The buffer serialises its own writes rather than leaving callers to
        # agree on a lock. One write connection is reached by several threads —
        # an append, a seal claiming and clearing its range, a maintenance pass
        # queuing deletions — and two BEGINs at once is "cannot start a
        # transaction within a transaction". Worse than the error: with
        # autocommit suspended by someone else's BEGIN, an unrelated INSERT
        # joins their transaction and commits or rolls back with it.
        self._lock = threading.RLock()

        self._con = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._con.execute("PRAGMA journal_mode=WAL")
        # §3's durability claim rests on this line. WAL alone fsyncs at
        # checkpoint, not at commit, which would put committed rows back in the
        # OS page cache — the exact loss this library exists to prevent.
        self._con.execute("PRAGMA synchronous=FULL")

        # A second connection for the rows a seal is about to write out. That
        # read is the expensive half of a seal, and on the write connection it
        # would serialise against the appends a background seal exists to stay
        # out of the way of. WAL allows it: one writer, any number of readers.
        # It deliberately does NOT take the lock above.
        self._reader = sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            isolation_level=None,
            check_same_thread=False,
        )
        self._create()
        self._bytes = self._measure()

    def _create(self) -> None:
        columns = ",\n  ".join(
            f'"{name}" {column_type(self._schema.field(name).type).sqlite}'
            for name in self._columns
        )
        # AUTOINCREMENT, not a bare INTEGER PRIMARY KEY: buffer rows are deleted
        # at every seal, and a rowid alias would reissue offsets already
        # committed to Iceberg once the table empties, silently corrupting every
        # tier boundary in §7 (I9, §2).
        self._con.execute(f"""
            CREATE TABLE IF NOT EXISTS buffer (
              "litelink_offset" INTEGER PRIMARY KEY AUTOINCREMENT,
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
            terms.append(
                f'coalesce(octet_length("{name}"), 0)'
                if column_type(self._schema.field(name).type).variable_length
                else "8"
            )

        row = self._con.execute(
            f"SELECT coalesce(sum({' + '.join(terms)}), 0) FROM buffer"
        ).fetchone()

        return int(row[0])

    def _row_bytes(self, row: Mapping[str, object]) -> int:
        """Approximate bytes for one row, and refuse what SQLite cannot store.

        The NaN check lives here because this loop already visits every value,
        so it costs a comparison rather than a pass. SQLite has no NaN — it
        stores one as NULL, verified — so a float column would take a NaN and
        return a null, silently, with no error anywhere. That is the same
        failure `_types` refuses whole types for, one level down: the library
        declines what it cannot carry faithfully rather than changing it.
        """
        total = 8
        for name in self._columns:
            value = row.get(name)
            if isinstance(value, bytes | bytearray | memoryview):
                total += len(value)
            elif isinstance(value, str):
                total += len(value.encode())
            elif value is not None:
                # `!=` on itself is the NaN test that needs no import and does
                # not trip over ints, bools or Decimals.
                if isinstance(value, float) and value != value:
                    msg = (
                        f"column {name!r}: SQLite stores NaN as NULL, so the value "
                        "would come back null rather than NaN. Use None if that is "
                        "what you mean."
                    )
                    raise ValueError(msg)

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

        with self._lock:
            return self._insert(rows, sql)

    def _insert(self, rows: Iterable[Mapping[str, object]], sql: str) -> list[int]:
        """The append's transaction, with the lock already held."""
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
            # Best-effort, and it must not raise over the original. An
            # interrupt landing inside COMMIT leaves no transaction to roll
            # back, and the bare version turned a Ctrl-C into
            # "cannot rollback - no transaction is active" with the real cause
            # buried underneath it.
            #
            # Note what that case means: the COMMIT had already succeeded, so
            # the rows ARE durable while this reports failure. That direction is
            # the safe one — the caller retrying would duplicate rows, whereas
            # believing a durable append failed costs only a redundant retry
            # that AUTOINCREMENT will assign fresh offsets to.
            with contextlib.suppress(sqlite3.OperationalError):
                cursor.execute("ROLLBACK")

            raise

        self._bytes += added

        return offsets

    @staticmethod
    def _reject_offset(row: Mapping[str, object]) -> None:
        """I11: `offset` is assigned by the library, never accepted."""
        if "litelink_offset" in row:
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
            'SELECT min("litelink_offset"), max("litelink_offset") FROM buffer'
        ).fetchone()

        return None if lo is None else (int(lo), int(hi))

    def count_above(self, boundary: int) -> int:
        """How many buffered rows sit above `boundary` — the unsealed tail.

        `litelink_offset` is the INTEGER PRIMARY KEY, so SQLite answers this
        with a rowid range rather than a scan, and rows already sealed but not
        yet deleted cost nothing.
        """
        row = self._con.execute(
            'SELECT count(*) FROM buffer WHERE "litelink_offset" > ?', (boundary,)
        ).fetchone()

        return int(row[0])

    def rows_below(self, end: int) -> pa.Table:
        """Buffered rows with `offset < end`, as Arrow."""
        names = ", ".join(f'"{c}"' for c in ("litelink_offset", *self._columns))
        cursor = self._reader.execute(
            f'SELECT {names} FROM buffer WHERE "litelink_offset" < ? ORDER BY "litelink_offset"',
            (end,),
        )
        columns = list(zip(*cursor.fetchall(), strict=True)) or [
            () for _ in range(len(self._columns) + 1)
        ]
        schema = pa.schema(
            [pa.field("litelink_offset", pa.int64(), nullable=False), *self._schema]
        )

        # Built loosely, then cast to the declared schema — the SQLite edge.
        # SQLite has no boolean and no distinction between string widths, so
        # its values come back as whatever storage class it chose; casting is
        # what turns them back into the types the caller declared. Constructing
        # directly with `type=` instead refuses rather than converting: a bool
        # column comes back as 1 and 0, and `pa.array([1], type=bool_())`
        # raises.
        loose = pa.table(
            [pa.array(values) for values in columns],
            names=list(schema.names),
        )

        return loose.cast(schema)

    # -- seal bookkeeping -------------------------------------------------

    def claim_seal(self, start: int, end: int, rel_path: str) -> None:
        """Record the seal intent before the file exists (I2).

        The path is persisted, not recomputed: a retry that recomputed it could
        land on a different date directory and strand the first file.
        """
        with self._lock:
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
        with self._lock:
            self._con.execute("BEGIN")
            self._con.execute('DELETE FROM buffer WHERE "litelink_offset" < ?', (end,))
            self._con.execute("DELETE FROM sealing")
            self._con.execute("COMMIT")
            self._bytes = self._measure()

    # -- meta ---------------------------------------------------------------
    #
    # §2's `meta` table. Holds the settings that cannot be recovered from the
    # Iceberg table — deployment policy rather than data shape — so that `open`
    # can reconstruct a log from what it actually is instead of asking the
    # caller to restate it and hoping they match.

    @staticmethod
    def peek_meta(path: Path, key: str) -> str | None:
        """Read one `meta` value without opening a full buffer.

        Opening a log has a chicken-and-egg shape: the declared Arrow schema
        lives in a table only an open buffer can read, and the buffer needs
        that schema to cast what it reads back. One read-only connection
        resolves it, which is cheaper than constructing a buffer twice and
        leaves the schema immutable for the buffer's whole life.
        """
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = connection.execute(
                "SELECT v FROM meta WHERE k = ?", (key,)
            ).fetchone()
        finally:
            connection.close()

        return None if row is None else str(row[0])

    def get_meta(self, key: str) -> str | None:
        row = self._con.execute("SELECT v FROM meta WHERE k = ?", (key,)).fetchone()

        return None if row is None else str(row[0])

    def set_meta(self, key: str, value: str) -> None:
        self._con.execute(
            "INSERT INTO meta (k, v) VALUES (?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (key, value),
        )

    # -- compaction bookkeeping -------------------------------------------

    def claim_compaction(self, lo: int, hi: int, rel_path: str) -> None:
        """Record a compaction's output path before the file exists.

        The seal's I2 argument applied to the other writer: a compaction that
        crashes between writing and committing leaves a file on disk, and the
        only way to find it without a directory scan is to have written its
        name down first.
        """
        with self._lock:
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
        with self._lock:
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
        if self._reader is not self._con:
            self._reader.close()
