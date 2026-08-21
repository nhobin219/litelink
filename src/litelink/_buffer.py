"""The SQLite write buffer (SPEC §2, §3).

One database per stream. Durable on commit — `synchronous=FULL` with WAL — and
that is the whole durability story: there is no in-memory staging layer whose
loss a SIGKILL could expose.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from pathlib import Path

from litelink._lease import DEFAULT_TTL_MS, Lease
from litelink._types import column_type

# The library-owned column (§2).
OFFSET = "litelink_offset"

# Stands in for "no row limit" so the per-row check stays one comparison. Far
# above any row count a buffer sized for read latency could reach.
_NO_ROW_LIMIT = 1 << 62

# How many appended-since-last-query slices the tail may accumulate before it
# is compacted back into one.
_MAX_TAIL_CHUNKS = 32

# IMMEDIATE, never a bare BEGIN. Every transaction here writes, and several read
# first — an append reads the open `extent` row before inserting anything.
# A deferred transaction that reads first takes a read snapshot, and if another
# process has written since, SQLite refuses to upgrade it to a writer and
# returns "database is locked" IMMEDIATELY: `busy_timeout` does not apply,
# because waiting could not help a snapshot that is already stale.
#
# Taking the write lock up front makes the wait a lock wait, which the timeout
# below does cover. Found by running the writer, the sealer and the maintainer
# as three processes: the writer died the moment the other two started.
_BEGIN = "BEGIN IMMEDIATE"

# Long enough to outlast a seal's brief write steps and a maintenance pass's
# commits, since those are what an append now queues behind across processes.
_BUSY_TIMEOUT_MS = 30_000


@dataclass(slots=True)
class _Group:
    """The open `extent` row, while an append transaction fills it.

    Read once per transaction and written back once, so the per-row accounting
    that decides the cut is arithmetic rather than a statement per row.
    """

    group_id: int
    start_offset: int | None
    bytes: int


class Buffer:
    """The unsealed tail of a log."""

    def __init__(
        self,
        writer: sqlite3.Connection,
        reader: sqlite3.Connection,
        schema: pa.Schema,
        columns: tuple[str, ...],
        *,
        target_size: int,
        target_rows: int | None = None,
    ) -> None:
        """Take built collaborators. `open` is what builds and validates them.

        `writer` is the connection every transaction here runs on; `reader` is
        a second, read-only handle for the rows a seal is about to write out.
        That read is the expensive half of a seal, and on the write connection
        it would serialise against the appends a seal exists to stay out of the
        way of. WAL allows it: one writer, any number of readers.

        `schema` is the application's columns, without `offset`; `columns` is
        their names, passed rather than derived so this assigns and nothing
        else. `target_size` is here rather than only on the seal because the
        cut it describes is made on the append path — see `extent`.
        """
        self._con = writer
        self._reader = reader
        self._schema = schema
        self._columns = columns
        self._target_size = target_size
        self._target_rows = target_rows
        # The buffer serialises its own writes rather than leaving callers to
        # agree on a lock. One write connection is reached by several threads —
        # an append, a seal claiming and clearing its range, a maintenance pass
        # queuing deletions — and two BEGINs at once is "cannot start a
        # transaction within a transaction". Worse than the error: with
        # autocommit suspended by someone else's BEGIN, an unrelated statement
        # joins their transaction and commits or rolls back with it, which is
        # how a lease once evaporated under its own holder.
        #
        # Assigned unconditionally, including for a readonly buffer. It used to
        # be skipped there, which left `lease()` raising AttributeError on a
        # handle that had every right to ask.
        #
        # The rule is every statement on `_con`, reads included — not just the
        # transactions. A bare SELECT issued while another thread has a
        # transaction open joins it and sees uncommitted rows, which a rollback
        # then unmakes; that is the same mechanism that once let a lease
        # evaporate under its holder. `_reader` needs none of this precisely
        # because nothing ever opens a transaction on it.
        self._lock = threading.RLock()
        # The read cache — see `rows_above`. Its own lock rather than the one
        # above, which appends hold: a read must not wait behind a write to
        # look at a table the write cannot invalidate.
        self._tail_lock = threading.Lock()
        self._tail: pa.Table | None = None
        self._tail_lo = 0
        self._tail_hi = 0

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[None]:
        """One write transaction, rolled back if the body raises.

        Every multi-statement write here goes through this. Four of them used
        to open a transaction and commit with no rollback, which is worse than
        it sounds: an error between BEGIN and COMMIT leaves the connection
        inside a transaction with the lock released, and the next statement any
        thread issues joins it — the mechanism that once erased a lease from
        under its holder.
        """
        with self._lock:
            self._con.execute(_BEGIN)
            try:
                yield
                # Inside the guard, not after it. A COMMIT can fail on its own
                # — a full disk, an I/O error — and leaving that unguarded put
                # the connection back in the state this helper exists to
                # prevent, with the lock released and the next statement any
                # thread issues joining a transaction nobody owns.
                self._con.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.OperationalError):
                    self._con.execute("ROLLBACK")

                raise

    def set_target_size(self, target_size: int, target_rows: int | None) -> None:
        """Adopt new cut limits in place, rather than being rebuilt around them.

        Policy can change under a running log (§12), and the cut is made here,
        so it has to arrive here. `Maintenance` takes the same shape.
        """
        with self._lock:
            self._target_size = target_size
            self._target_rows = target_rows

    @classmethod
    def open(
        cls,
        path: Path,
        schema: pa.Schema,
        *,
        target_size: int,
        target_rows: int | None = None,
        readonly: bool = False,
        durable: bool = True,
    ) -> Buffer:
        """Connect, configure, and create the tables. Then hand them to `cls`.

        The I/O half, kept out of `__init__` for the same reason `Log.open` is
        kept out of `Log.__init__`: a constructor that opens files cannot be
        handed a substitute, and a test that wants one should not have to
        monkeypatch its way in.

        A readonly buffer opens the same file through SQLite's `mode=ro` URI so
        the handle cannot write even by mistake, and creates nothing. WAL allows
        any number of these alongside the single writer (§1).

        `durable=False` is for a buffer whose contents are derived from
        something that still exists — the scratch buffer an archive rewrite
        re-cuts through, whose every row came from the archive and is still
        there until the rewrite's final commit. A crash costs a re-run rather
        than data, so the fsync per commit is paying for a guarantee nothing
        depends on. Never for a log's own buffer: there, the fsync IS the
        product (§3).
        """
        columns = tuple(schema.names)
        if readonly:
            con = cls._connect_readonly(path)

            return cls(
                con,
                con,
                schema,
                columns,
                target_size=target_size,
                target_rows=target_rows,
            )

        # check_same_thread=False because scheduling maintenance on a background
        # thread is the ordinary operational shape, and Python's guard would
        # otherwise forbid it. The C library is built serialized here
        # (`sqlite3.threadsafety == 3`), so the connection itself is safe; the
        # lock is for the multi-statement sequences SQLite cannot know about.
        writer = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        # WAL either way, and not for durability: the reader below is a second
        # connection to the same file, which is what WAL exists to allow.
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        # §3's durability claim rests on this line. WAL alone fsyncs at
        # checkpoint, not at commit, which would put committed rows back in the
        # OS page cache — the exact loss this library exists to prevent.
        writer.execute(f"PRAGMA synchronous={'FULL' if durable else 'OFF'}")

        buffer = cls(
            writer,
            cls._connect_readonly(path),
            schema,
            columns,
            target_size=target_size,
            target_rows=target_rows,
        )
        buffer._create()
        buffer._seed_group()

        return buffer

    @staticmethod
    def _connect_readonly(path: Path) -> sqlite3.Connection:
        return sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            isolation_level=None,
            check_same_thread=False,
        )

    def _create(self) -> None:
        """The schema. Unlocked, and the only place that is right.

        This and `_seed_group` run inside `open`, before the buffer has been
        handed to anyone, so there is no second thread to exclude. Every method
        reachable afterwards takes the lock.
        """
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
        # The seal queue, and the reason `target_size` means anything.
        #
        # A single running byte counter cannot keep that promise. It says a
        # threshold was crossed, never WHERE — so a sealer that polls one cuts
        # wherever the buffer has reached by the time it looks, swallowing
        # everything that arrived during the poll gap and during the seal
        # itself. File size would then track how far behind the sealer was.
        #
        # So the cut is made by the appender, in the transaction that crosses
        # it, and written down here. A closed row is a file that ought to
        # exist; the sealer reads one row to learn both that there is work and
        # exactly which offsets it covers. Falling behind costs latency rather
        # than file size, because every queued group is already the right size.
        #
        # The same shape as `pending_delete`: work that must not be rediscovered
        # by scanning is recorded when it is created.
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS extent (
              group_id     INTEGER PRIMARY KEY AUTOINCREMENT,
              start_offset INTEGER,
              end_offset   INTEGER,
              bytes        INTEGER NOT NULL DEFAULT 0,
              rel_path     TEXT UNIQUE,
              named_at     INTEGER
            )
        """)
        # The two states that are still work, which is what every hot query
        # wants and what stays small however many files the log accumulates.
        # Without it `_read_group` — once per append transaction — degrades
        # into a scan of one row per file ever written.
        self._con.execute("""
            CREATE INDEX IF NOT EXISTS extent_unsealed ON extent (group_id)
            WHERE rel_path IS NULL
        """)
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"
        )
        # Who owns which operation, across processes (§13.6). A Python lock
        # cannot say anything about a process that is no longer running, and
        # recovery has to know whether an interrupted operation was ours.
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS lease (
              role TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at INTEGER NOT NULL
            )
        """)

    # -- size accounting --------------------------------------------------
    #
    # The running total lives in the open `extent` row and is written in the
    # same transaction as the rows it accounts for. That is what lets any
    # process read it — a keyed read of one row, rather than a SUM() over the
    # table being appended to — and what makes it impossible for the count and
    # the rows to disagree after a crash.
    #
    # Approximate on purpose. SQLite stores an integer in 1-8 bytes and
    # `octet_length` reports its TEXT width, so neither side of this is exact.
    # It is a policy trigger, not an accounting record; being within a few
    # percent of the true size is what `target_size` actually needs.

    def _seed_group(self) -> None:
        """Ensure exactly one open group, seeded from whatever is buffered.

        The only SUM() left, and it runs once per open rather than per append.
        It fires for a log created before this table existed, and for one whose
        open group was closed by a sealer just before the process died.

        **The check and the insert are one transaction.** As two statements,
        two processes opening the same log at once — a writer and a maintainer
        starting together, which is the ordinary shape — both see no open group
        and both insert one. Two open rows then take the same `start_offset`,
        `close_open_group` closes BOTH at the same end, and `finish_seal` tries
        to give both the same `rel_path`: a UNIQUE violation that rolls back
        after the Iceberg commit has already landed, leaving the claim in place
        and every retry, recovery included, failing the same way. The seal
        queue wedges permanently and the buffer stops draining.
        """
        with self._transaction():
            if self._con.execute(
                "SELECT 1 FROM extent WHERE end_offset IS NULL AND rel_path IS NULL"
            ).fetchone():
                return

            covered = int(
                self._con.execute(
                    "SELECT coalesce(max(end_offset), 0) FROM extent"
                ).fetchone()[0]
            )
            start = self._con.execute(
                'SELECT min("litelink_offset") FROM buffer WHERE "litelink_offset" >= ?',
                (covered,),
            ).fetchone()[0]
            # Adopting whatever is buffered, rather than starting a fresh group
            # above it: those rows still have to become a file, and a group
            # that skipped them would leave them unsealed for ever.
            self._con.execute(
                "INSERT INTO extent (start_offset, bytes) VALUES (?, ?)",
                (start, self._measure_from(covered)),
            )

    def _measure_from(self, floor: int) -> int:
        terms = ["8"]  # offset
        for name in self._columns:
            terms.append(
                f'coalesce(octet_length("{name}"), 0)'
                if column_type(self._schema.field(name).type).variable_length
                else "8"
            )

        row = self._con.execute(
            f"SELECT coalesce(sum({' + '.join(terms)}), 0) FROM buffer"
            ' WHERE "litelink_offset" >= ?',
            (floor,),
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
        cursor = self._con.cursor()
        cursor.execute(_BEGIN)
        try:
            group = self._read_group(cursor)
            # Bound once, and the accounting inlined below, because this loop
            # runs per row: routing it through a method cost 19 points of
            # overhead against raw SQLite at 1,000-row batches.
            target = self._target_size
            # `_insert` runs per row, so both limits are bound once out here.
            # A row cap of None becomes one nothing reaches, which keeps the
            # inner test a comparison rather than a branch on None.
            target_rows = self._target_rows or _NO_ROW_LIMIT
            row_bytes = self._row_bytes
            columns = self._columns
            for row in rows:
                self._reject_offset(row)
                cursor.execute(sql, tuple(row.get(c) for c in columns))
                # lastrowid is the assigned offset, available inside the open
                # transaction and before the row is visible to anyone else.
                offset = int(cursor.lastrowid or 0)
                offsets.append(offset)

                if group.start_offset is None:
                    group.start_offset = offset

                group.bytes += row_bytes(row)
                # Whichever is reached FIRST. Both are ceilings on one file —
                # bytes bound memory, rows bound the read latency §7 sizes for
                # — so the tighter one wins, which is the opposite of how
                # `local_retention` and `local_rows` combine.
                if (
                    group.bytes >= target
                    or offset - (group.start_offset or offset) + 1 >= target_rows
                ):
                    group = self._cut(cursor, group, offset)

            self._write_group(cursor, group)
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

        return offsets

    def _read_group(self, cursor: sqlite3.Cursor) -> _Group:
        """The open group, once per transaction rather than once per row."""
        row = cursor.execute(
            "SELECT group_id, start_offset, bytes FROM extent"
            " WHERE end_offset IS NULL AND rel_path IS NULL"
        ).fetchone()

        return _Group(int(row[0]), row[1], int(row[2]))

    def _cut(self, cursor: sqlite3.Cursor, group: _Group, offset: int) -> _Group:
        """Close the group at `offset` and open the next. Once per FILE.

        The cut lands on the row that crossed, not at the end of the batch:
        `target_size` is the library's one promise about file size, and cutting
        on the batch boundary would make that promise depend on how the caller
        chose to batch — which §1 says carries no meaning of its own. A batch
        large enough crosses several times and comes through here each time.
        """
        self._write_group(cursor, group, end_offset=offset + 1)
        cursor.execute("INSERT INTO extent (bytes) VALUES (0)")

        return _Group(int(cursor.lastrowid or 0), None, 0)

    def _write_group(
        self, cursor: sqlite3.Cursor, group: _Group, end_offset: int | None = None
    ) -> None:
        """Write the accumulated group back. Once per cut, once per batch.

        `end_offset` closes it; without one the group stays open and this is
        just the running total being persisted for other processes to read.
        """
        cursor.execute(
            "UPDATE extent SET start_offset = ?, bytes = ?,"
            " end_offset = ? WHERE group_id = ?",
            (
                group.start_offset,
                group.bytes,
                end_offset,
                group.group_id,
            ),
        )

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
        with self._lock:
            row = self._con.execute(
                "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
            ).fetchone()

        return (row[0] if row else 0) + 1

    def extent(self) -> tuple[int, int] | None:
        """`(min, max)` offset currently buffered, or None if empty.

        Two statements, deliberately. SQLite rewrites `min(pk)` or `max(pk)`
        into a single B-tree edge seek ONLY when it is the sole aggregate in
        the select list; ask for both at once and the plan degrades from SEARCH
        to a full SCAN of the table. Measured on a 20,000-row buffer:
        2.465 ms together, 0.006 ms apart. Do not tidy these back into one.
        """
        with self._lock:
            lo = self._con.execute(
                'SELECT min("litelink_offset") FROM buffer'
            ).fetchone()[0]
            if lo is None:
                return None

            hi = self._con.execute(
                'SELECT max("litelink_offset") FROM buffer'
            ).fetchone()[0]

        return (int(lo), int(hi))

    def count_above(self, boundary: int) -> int:
        """How many buffered rows sit above `boundary` — the unsealed tail.

        `litelink_offset` is the INTEGER PRIMARY KEY, so SQLite answers this
        with a rowid range rather than a scan, and rows already sealed but not
        yet deleted cost nothing.
        """
        with self._lock:
            row = self._con.execute(
                'SELECT count(*) FROM buffer WHERE "litelink_offset" > ?', (boundary,)
            ).fetchone()

        return int(row[0])

    @property
    def schema(self) -> pa.Schema:
        """The declared Arrow schema, for building a second buffer like this
        one — an archive rewrite re-ingests through a scratch buffer and must
        cast the rows exactly as this one would."""
        return self._schema

    def seed_offsets(self, first: int) -> None:
        """Make the next appended row take offset `first`.

        Only for the scratch buffer an archive rewrite re-ingests through. Rows
        being re-cut keep the offsets they already have — they are the same
        rows, and §4's contiguous non-overlapping ranges are stated in them —
        so the sequence has to resume where the range starts rather than at 1.

        Seeded rather than supplied per row, so the rewrite goes through the
        ordinary append path and I11 still holds: nothing hands an offset to
        `extend`, the counter simply starts elsewhere.
        """
        # `sqlite_sequence` is SQLite's own, and documented as writable — but
        # it carries no unique constraint, so this is an UPDATE with an INSERT
        # behind it rather than an upsert. The row appears only after the first
        # AUTOINCREMENT insert, which for a buffer opened seconds ago has not
        # happened yet.
        with self._lock, self._con:
            # Loud, because the silent version is unrecoverable. SQLite assigns
            # `max(largest existing rowid, seq) + 1`, so seeding DOWN past
            # existing rows does nothing at all and every following row lands
            # at an offset belonging to different data — which a rewrite then
            # commits. Nothing downstream can detect it: the row counts match,
            # the ranges look contiguous, and the payloads are simply attached
            # to the wrong offsets.
            if self._con.execute("SELECT 1 FROM buffer LIMIT 1").fetchone():
                msg = (
                    "cannot seed offsets on a buffer that already holds rows: "
                    "SQLite ignores a sequence lowered past them"
                )
                raise ValueError(msg)

            updated = self._con.execute(
                "UPDATE sqlite_sequence SET seq = ? WHERE name = 'buffer'",
                (first - 1,),
            )
            if not updated.rowcount:
                self._con.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES ('buffer', ?)",
                    (first - 1,),
                )

    def group_bytes(self, end: int) -> int:
        """What the extent ending at `end` holds, before a file claims it.

        Read out so it can be recorded against the archive's copy: the scratch
        buffer measured these rows exactly as the appender would have, and that
        count is the whole reason the rewrite goes through a buffer at all.
        """
        with self._lock:
            row = self._con.execute(
                "SELECT bytes FROM extent WHERE end_offset = ? AND rel_path IS NULL",
                (end,),
            ).fetchone()

        return 0 if row is None else int(row[0])

    def rows_below(self, end: int) -> pa.Table:
        """Buffered rows with `offset < end`, as Arrow. The seal's input."""
        return self._rows("< ?", (end,))

    def rows_above(self, boundary: int | None) -> pa.Table:
        """Buffered rows with `offset > boundary`, as Arrow. The read's input.

        `boundary` is the local table's committed extent, so this is §7's
        unsealed tail — the rows the Iceberg leg does not already carry.

        Read here rather than by the query engine, and that is a correctness
        requirement rather than a preference. DuckDB's sqlite extension carries
        its OWN statically linked SQLite, so attaching this file put it under
        two independent SQLite libraries in one process. POSIX advisory locks
        are per process and per inode, and each library keeps its own table of
        open descriptors to work around that — so closing one library's handle
        drops the other library's locks, and the writer and reader stop being
        serialised at all. Measured: an in-process scan concurrent with appends
        corrupted the database on the FIRST scan ("database disk image is
        malformed", and a torn -shm mmap raises SIGBUS); the same workload with
        the reader in a separate process ran 327 scans clean.

        Converted incrementally, because a scan repeated over a buffer that
        gained 200 rows should not re-convert the other 20,000. Rows are
        immutable once committed, arrive only above the last one, and leave
        only as a prefix at a seal — so the previous answer stays valid in the
        middle, and a query pays for its own delta plus a zero-copy slice.
        Measured at a full 8 MiB buffer: 29.4 ms rebuilt, and two thirds of
        that is `fetchall` turning 120,000 values into Python objects.
        """
        floor = 0 if boundary is None else boundary
        with self._tail_lock:
            cached = self._reusable(floor)
            if cached is None:
                table = self._rows("> ?", (floor,))
            else:
                fresh = self._rows("> ?", (self._tail_hi,))
                table = (
                    cached if fresh.num_rows == 0 else pa.concat_tables([cached, fresh])
                )
                if table.column(0).num_chunks > _MAX_TAIL_CHUNKS:
                    # One chunk per query otherwise, forever. Combining is a
                    # copy, so it is amortised rather than paid every time.
                    table = table.combine_chunks()

            self._tail = table
            # Taken from the DATA, never from `floor`. They are not the same
            # number: `floor` is the table's boundary, and the first buffered
            # row above it can be higher still if a seal deleted the rows
            # between while this was being read. Recording `floor` as though it
            # were the row before the first made the slice arithmetic below
            # count from a row that no longer existed, and the miscount hid
            # buffered rows from every subsequent query — silently, because an
            # over-long slice comes back empty rather than raising.
            if table.num_rows:
                self._tail_lo = int(table.column(OFFSET)[0].as_py()) - 1
                self._tail_hi = int(table.column(OFFSET)[-1].as_py())
            else:
                self._tail_lo = self._tail_hi = floor

            return table

    def _reusable(self, floor: int) -> pa.Table | None:
        """The cached tail with everything through `floor` dropped, or None.

        The slice index is arithmetic — cached offsets are contiguous from
        `_tail_lo + 1`, so dropping through `floor` drops exactly
        `floor - _tail_lo` rows — and then checked, because that contiguity is
        a property of AUTOINCREMENT and prefix-only deletion rather than
        something enforced here. A failed check costs a rebuild, which is what
        the code did unconditionally before.

        Both directions are checked. A wrong non-empty slice starts at the
        wrong offset; a wrong EMPTY slice is the dangerous one, because it
        looks exactly like "nothing buffered above the boundary" and would be
        returned as an answer. `_tail_hi > floor` says the last cached row
        qualifies, so an empty result contradicts the cache itself.
        """
        if self._tail is None or not (self._tail_lo <= floor <= self._tail_hi):
            return None

        kept = self._tail.slice(floor - self._tail_lo)
        if kept.num_rows:
            if int(kept.column(OFFSET)[0].as_py()) != floor + 1:
                return None
        elif self._tail_hi > floor:
            return None

        return kept

    def _rows(self, predicate: str, params: tuple[object, ...]) -> pa.Table:
        """Buffered rows matching `offset <predicate>`, in offset order.

        The predicate is on the INTEGER PRIMARY KEY so SQLite answers it with
        `SEARCH buffer USING INTEGER PRIMARY KEY (rowid>?)` rather than reading
        rows the caller will discard. That is what keeps a deferred cleanup
        costing disk rather than query latency (§7).
        """
        names = ", ".join(f'"{c}"' for c in ("litelink_offset", *self._columns))
        cursor = self._reader.execute(
            f'SELECT {names} FROM buffer WHERE "litelink_offset" {predicate}'
            ' ORDER BY "litelink_offset"',
            params,
        )
        columns = list(zip(*cursor.fetchall(), strict=True)) or [
            () for _ in range(len(self._columns) + 1)
        ]
        schema = pa.schema(
            [pa.field("litelink_offset", pa.int64(), nullable=False), *self._schema]
        )

        return pa.table(
            [
                self._column(values, field.type)
                for values, field in zip(columns, schema, strict=True)
            ],
            schema=schema,
        )

    @staticmethod
    def _column(values: tuple[object, ...], declared: pa.DataType) -> pa.Array:
        """One column, at the SQLite edge, in the declared type.

        Typed construction first, and a cast only where that fails. SQLite has
        no boolean and no distinction between string widths, so its values come
        back as whatever storage class it chose — a bool column arrives as 1
        and 0, which `pa.array([1], type=bool_())` refuses and a cast converts.

        Casting the whole table unconditionally was the obvious version, and it
        cost roughly as much again as building it: every column paid the
        conversion pass, including the ones already in the right type.
        """
        try:
            return pa.array(values, type=declared)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            return pa.array(values).cast(declared)

    def lease(self, role: str, owner: str, ttl_ms: int = DEFAULT_TTL_MS) -> Lease:
        """A claim on `role`, backed by this database.

        Handed the connection AND the lock that guards it — see `Lease`.
        """
        return Lease(self._con, self._lock, role, owner, ttl_ms)

    # -- the seal queue ---------------------------------------------------

    def pending_group(self) -> tuple[int, int] | None:
        """The oldest group awaiting a file: `(start, end)`, end exclusive.

        The sealer's entire trigger, in any process. Closed groups sort below
        the open one, so this reads the first row of a table holding one entry
        per queued file plus the open one — never a scan of the buffer, and
        never a question about where to cut, because that was decided already.
        """
        with self._lock:
            row = self._con.execute(
                "SELECT start_offset, end_offset FROM extent"
                " WHERE end_offset IS NOT NULL AND rel_path IS NULL"
                " ORDER BY group_id LIMIT 1"
            ).fetchone()

        return None if row is None else (int(row[0]), int(row[1]))

    def last_queued_end(self) -> int | None:
        """The highest cut recorded but not yet sealed, or None if none is.

        What an explicit `seal()` must drain to. Taken under the same lock that
        made the cut, so it cannot miss one that call just recorded.
        """
        with self._lock:
            row = self._con.execute(
                "SELECT max(end_offset) FROM extent"
                " WHERE end_offset IS NOT NULL AND rel_path IS NULL"
            ).fetchone()

        return None if row[0] is None else int(row[0])

    def close_open_group(self) -> bool:
        """Cut the open group short so a sealer can pick it up.

        Only `seal()` calls this, and cutting short is exactly what "seal now"
        means — the resulting file is under `target_size` by definition. It is
        the one way this library writes an undersized file, and it takes a
        deliberate call to do it.

        With `cutoff`, only if the group's first row landed at or before then.
        That was `max_age`, which no longer exists; the parameter is kept
        because the offline archive rewrite needs the same conditional cut.

        An empty group is never closed either way; there would be no file.
        Harmless to race — the predicate matches nothing once another caller
        has closed it.
        """
        # Asked before it is written. The sealer calls this on every poll, and
        # the answer is almost always "nothing to close" — issuing a write
        # transaction to discover that would put a commit and an fsync on a
        # timer, for every log, forever. The read is a single row.
        with self._lock:
            if not self._con.execute(
                "SELECT 1 FROM extent WHERE end_offset IS NULL"
                " AND rel_path IS NULL AND start_offset IS NOT NULL",
                (),
            ).fetchone():
                return False

        with self._transaction():
            cursor = self._con.execute(
                "UPDATE extent SET end_offset ="
                ' (SELECT max("litelink_offset") + 1 FROM buffer)'
                " WHERE end_offset IS NULL AND rel_path IS NULL"
                " AND start_offset IS NOT NULL",
                (),
            )
            closed = bool(cursor.rowcount)
            if closed:
                self._con.execute("INSERT INTO extent (bytes) VALUES (0)")

        return closed

    # -- seal bookkeeping -------------------------------------------------

    def claim_seal(self, start: int, end: int, rel_path: str) -> None:
        """Record the seal intent before the file exists (I2).

        The path is persisted, not recomputed: a retry that recomputed it could
        land on a different date directory and strand the first file.
        """
        with self._transaction():
            self._con.execute("DELETE FROM sealing")
            self._con.execute(
                "INSERT INTO sealing (start_offset, end_offset, rel_path) VALUES (?, ?, ?)",
                (start, end, rel_path),
            )

    def pending_seal(self) -> tuple[int, int, str] | None:
        """The in-flight seal, if a crash left one."""
        with self._lock:
            row = self._con.execute(
                "SELECT start_offset, end_offset, rel_path FROM sealing"
            ).fetchone()

        return None if row is None else (int(row[0]), int(row[1]), str(row[2]))

    def finish_seal(self, end: int, rel_path: str) -> bool:
        """Delete sealed rows, retire the group, and clear the intent.

        Garbage collection, not correctness: the read boundary in §7 already
        excludes these rows the moment the Iceberg commit lands, so the window
        between that commit and this call is safe in both directions.

        Returns whether this caller's claim was the live one. False means it
        was superseded while it worked, and finishing belongs to whoever holds
        the claim now.

        The group is keyed by `end` rather than an id threaded through the
        seal. Groups are consecutive and non-overlapping, so an exclusive end
        identifies exactly one — which also means a recovered seal retires its
        group without `sealing` having had to remember which one it was.
        """
        with self._transaction():
            # Only OUR claim, and only if it is still the one recorded. A
            # writer stalled past its lease wakes up believing it owns this
            # seal; clearing unconditionally let it wipe the claim of the
            # owner that took over, stranding that owner's half-written file
            # under a name nothing recorded any more.
            cursor = self._con.execute(
                "DELETE FROM sealing WHERE rel_path = ?", (rel_path,)
            )
            if not cursor.rowcount:
                return False

            self._con.execute('DELETE FROM buffer WHERE "litelink_offset" < ?', (end,))
            # NAMED, not deleted. The row is the same fact before and after —
            # this range, these bytes — and sealing only settles where it
            # lives. Deleting it and writing the size to a second table was
            # half of this one reinvented, and left the two able to disagree.
            # Same transaction as the rows it retires, so a file can never be
            # committed with the count of what it holds lost.
            self._con.execute(
                "UPDATE extent SET rel_path = ?, named_at = unixepoch()"
                " WHERE end_offset = ? AND rel_path IS NULL",
                (rel_path, end),
            )

        return True

    # -- file sizes ---------------------------------------------------------

    def file_bytes(self) -> dict[str, int]:
        """What every known data file holds in memory, keyed by location.

        Root-relative for local files, so a log directory stays movable; the
        full URI for archived ones, which have no root to be relative to. A
        named extent is a file; an unnamed one is still buffered.

        All of it at once: the callers are compaction, sync and the archive
        rewrite, all of which walk the whole file list, and one indexed read
        beats a query per file.

        A file missing from this is not an error. It means the log has files
        this database never recorded — one written by a version that did not
        keep them, or an archive whose local extents were lost — and the
        callers treat an unknown size as "full", so an unmeasured file is never
        merged on a guess about what it holds.
        """
        with self._lock:
            rows = self._con.execute(
                "SELECT rel_path, bytes FROM extent WHERE rel_path IS NOT NULL"
            ).fetchall()

        return {str(row[0]): int(row[1]) for row in rows}

    def file_ages(self) -> dict[str, int]:
        """When each file was written, as a unix timestamp, keyed by location.

        A log's own record of its files' ages, because Iceberg's does not
        survive. A file's age used to be read off the snapshot that added it,
        and `expire` deletes that snapshot — after which the file appeared in
        no age map, `evict` could not call it stale, and `local_retention`
        silently stopped reclaiming anything.

        The two settings are sized by unrelated things: §6 wants
        `snapshot_retention` above the longest scan, §8 wants
        `local_retention` above the longest hot lookback. Any deployment where
        the second is longer than the first — which is the ordinary one — has
        every file losing its Iceberg age before it is old enough to evict.
        """
        with self._lock:
            rows = self._con.execute(
                "SELECT rel_path, named_at FROM extent"
                " WHERE rel_path IS NOT NULL AND named_at IS NOT NULL"
            ).fetchall()

        return {str(row[0]): int(row[1]) for row in rows}

    def record_file(self, rel_path: str, start: int, end: int, held: int) -> None:
        """Record a second file holding an extent the log already has.

        What `sync` calls when it pushes: the archive's copy covers the same
        offsets and holds the same bytes, so it gets its own row under its own
        URI rather than a measurement of its own. It could not be measured
        again anyway — nothing recoverable from a Parquet file is the
        appender's count of what those rows cost in memory, and the local row
        goes when the local file is unlinked.

        This is why the mapping lives here. Iceberg has no per-file field to
        hang it on: v2's data-file metadata is a fixed set — column sizes,
        value counts, encryption key metadata — with nothing user-extensible,
        and `add_files` offers no way to attach one. Table properties are per
        table. So the coordinator that already records every path before its
        file exists (I16) records this too, for both tiers, in one shape.
        """
        with self._lock:
            self._con.execute(
                "INSERT INTO extent"
                " (start_offset, end_offset, bytes, rel_path, named_at)"
                " VALUES (?, ?, ?, ?, unixepoch())"
                " ON CONFLICT(rel_path) DO UPDATE SET bytes = excluded.bytes",
                (start, end, held, rel_path),
            )

    def record_merge(self, rel_path: str, sources: Iterable[str]) -> None:
        """Replace the sources' extents with one covering all of them.

        Addition, not re-measurement: a merge writes exactly the rows it read,
        so the output holds what the inputs held and spans what they spanned.
        That keeps the number in the same currency as the seal that first
        measured it, however many rewrites later — which is the whole reason it
        is carried rather than derived from whatever the merged file compresses
        to. It is also what lets the archive rewrite build its extents with the
        same arithmetic a local compaction uses.
        """
        paths = list(sources)
        if not paths:
            return

        placeholders = ",".join("?" * len(paths))
        with self._transaction():
            summed = self._con.execute(
                "SELECT sum(bytes), count(*), min(start_offset), max(end_offset)"  # noqa: S608
                f" FROM extent WHERE rel_path IN ({placeholders})",
                paths,
            ).fetchone()
            # Only when every source was recorded. Summing a subset would
            # understate the output and invite a merge of something already
            # full; leaving it absent marks it unknown, which every caller
            # treats as "do not touch".
            if summed[1] == len(paths):
                self._con.execute(
                    "INSERT INTO extent"
                    " (start_offset, end_offset, bytes, rel_path, named_at)"
                    " VALUES (?, ?, ?, ?, unixepoch())"
                    " ON CONFLICT(rel_path) DO UPDATE SET bytes = excluded.bytes",
                    (summed[2], summed[3], int(summed[0]), rel_path),
                )

            self._con.execute(
                f"DELETE FROM extent WHERE rel_path IN ({placeholders})",  # noqa: S608
                paths,
            )

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
        with self._lock:
            row = self._con.execute("SELECT v FROM meta WHERE k = ?", (key,)).fetchone()

        return None if row is None else str(row[0])

    def set_meta_all(self, pairs: Mapping[str, str]) -> None:
        """Write several `meta` values in ONE transaction.

        For facts that are only true together. Re-pointing an archive writes
        where it is and resets the two watermarks that describe the previous
        one, and as separate autocommit statements a crash lands between them:
        either order leaves a log whose parts disagree, and both disagreements
        have cost a defect. One transaction has no between.
        """
        with self._transaction():
            self._con.executemany(
                "INSERT INTO meta (k, v) VALUES (?, ?) "
                "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                list(pairs.items()),
            )

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
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
        # Inserted, never replacing what is already there. Clearing first
        # destroyed the claims of an operation that had CRASHED — an archive
        # rewrite accumulates one per uploaded object, and recovery only runs
        # at `open`, so a long-lived maintainer starting its next merge wiped
        # them and left those objects referenced by nothing. Each operation
        # clears its own.
        with self._lock:
            self._con.execute(
                "INSERT INTO compacting (lo, hi, rel_path) VALUES (?, ?, ?)",
                (lo, hi, rel_path),
            )

    def claim_output(self, lo: int, hi: int, rel_path: str) -> None:
        """Record one more output path, without clearing the others.

        A compaction writes one file and `claim_compaction` says so by
        replacing whatever was there. An archive rewrite writes several before
        a single commit swaps them all in, and every one of them needs its name
        recorded before it exists (I2) — so they accumulate, and recovery
        removes each that the commit never claimed.
        """
        with self._lock:
            self._con.execute(
                "INSERT INTO compacting (lo, hi, rel_path) VALUES (?, ?, ?)",
                (lo, hi, rel_path),
            )

    def pending_compaction(self) -> tuple[int, int, str] | None:
        with self._lock:
            row = self._con.execute(
                "SELECT lo, hi, rel_path FROM compacting"
            ).fetchone()

        return None if row is None else (int(row[0]), int(row[1]), str(row[2]))

    def pending_outputs(self) -> list[tuple[int, int, str]]:
        """Every claimed output, for recovery. One row for a compaction,
        several for an interrupted archive rewrite."""
        with self._lock:
            rows = self._con.execute(
                "SELECT lo, hi, rel_path FROM compacting ORDER BY rowid"
            ).fetchall()

        return [(int(lo), int(hi), str(path)) for lo, hi, path in rows]

    def clear_compaction(self, rel_path: str | None = None) -> None:
        """Retire one claim, or every claim.

        One by default of the caller's choosing, because a claim belongs to an
        operation and clearing another's is how a crashed rewrite's uploads
        became unnameable. Recovery clears one at a time too, for the same
        reason: a rewrite whose lease lapsed mid-upload can be claiming its
        next segment while recovery is still resolving the last.

        The whole-table form is left for a caller that has genuinely resolved
        every row, and nothing does today.
        """
        with self._lock:
            if rel_path is None:
                self._con.execute("DELETE FROM compacting")
            else:
                self._con.execute(
                    "DELETE FROM compacting WHERE rel_path = ?", (rel_path,)
                )

    # -- deletion queue ---------------------------------------------------

    def enqueue_deletions(self, rel_paths: Iterable[str], superseded_at: int) -> None:
        """Queue superseded files, stamped with when they left the table.

        Enqueued in the same breath as the commit that superseded them, which
        is the only moment their paths are known without going to look.
        """
        with self._transaction():
            self._con.executemany(
                "INSERT OR IGNORE INTO pending_delete (rel_path, superseded_at) VALUES (?, ?)",
                [(p, superseded_at) for p in rel_paths],
            )

    def due_deletions(self, cutoff: int) -> list[str]:
        """Files superseded at or before `cutoff` — now minus the grace period."""
        with self._lock:
            return [
                str(row[0])
                for row in self._con.execute(
                    "SELECT rel_path FROM pending_delete WHERE superseded_at <= ?",
                    (cutoff,),
                ).fetchall()
            ]

    def forget_deletion(self, rel_path: str) -> None:
        """Drop a queue entry, after its file is gone.

        Called after the unlink, never before: a crash in between leaves the
        row and the next drain retries an unlink that is already a no-op. The
        reverse order loses the path and leaks the file permanently.
        """
        with self._lock:
            self._con.execute(
                "DELETE FROM pending_delete WHERE rel_path = ?", (rel_path,)
            )
            # The file is gone from disk, so what it held is no longer a fact
            # about anything. Dropped here rather than when the table stopped
            # referencing it: until the grace period passes an open scan may
            # still be reading it (I6).
            # The extent goes with the file. It described where those rows
            # live, and they no longer live anywhere by that name.
            self._con.execute("DELETE FROM extent WHERE rel_path = ?", (rel_path,))

    def queued_deletions(self) -> list[str]:
        with self._lock:
            return [
                str(row[0])
                for row in self._con.execute(
                    "SELECT rel_path FROM pending_delete"
                ).fetchall()
            ]

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        # The cache goes too. It is bounded by the unsealed tail and falls to
        # nothing once that is sealed, but a closed buffer holds no tail at
        # all, and a caller keeping the object alive should not keep the rows.
        with self._tail_lock:
            self._tail = None
            self._tail_lo = self._tail_hi = 0

        self._con.close()
        if self._reader is not self._con:
            self._reader.close()
