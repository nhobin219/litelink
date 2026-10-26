"""SQLite sessions, serialized transactions and storage-specific operations.

The coordinator never owns a connection. query_only is on except while this
backend owns a write, so a missed writer fails in production as well as tests.
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn, cast

import pyarrow as pa

from litelink._backend import BackendErrors, Cursor, PreparedRow
from litelink._buffer import (
    PUBLISHED_META_KEY,
    RETIRED_REFUSAL,
    Shape,
    _buffer_ddl,
    _meta_value,
    _retirement,
    owned,
)
from litelink._layout import Layout
from litelink._table import PublishedCatalog, SqlitePublishedCatalog
from litelink._transaction import CallerTransaction, SqliteTransaction, WriteLock

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence

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

# An append inside a caller's transaction (`Buffer.append` given `tx`).
_SAVEPOINT = "SAVEPOINT litelink_append"
_RELEASE = "RELEASE litelink_append"
_ROLLBACK_TO = "ROLLBACK TO litelink_append"

# Dead space below this is not worth an exclusive lock, whatever the ratio
# says: a young log crosses any ratio on its first publish pass, and reclaiming
# a few hundred KB there costs a write stall to save a rounding error on the
# wire. Not configurable, because it is not a policy — it is the point below
# which the policy cannot pay for itself. Sized to stay invisible against the
# ~21 MB an ordinary restore already fetches.
_VACUUM_FLOOR_BYTES = 8 * 1024 * 1024

# Long enough to outlast a seal's brief write steps and a maintenance pass's
# commits, since those are what an append now queues behind across processes.
_BUSY_TIMEOUT_MS = 30_000


class SqliteBackend:
    errors = BackendErrors(
        sqlite3.IntegrityError, sqlite3.OperationalError, sqlite3.Error
    )
    compacting_outputs = (
        "SELECT start_offset, end_offset, rel_path FROM compacting ORDER BY rowid"
    )

    def __init__(
        self,
        writer: sqlite3.Connection,
        tail: sqlite3.Connection,
        sealer: sqlite3.Connection,
        lock: WriteLock,
        *,
        readonly: bool = False,
        path: Path | None = None,
    ) -> None:
        self._path = path
        self._writer = writer
        self._tail = tail
        # A THIRD connection, for the seal alone, and it exists because
        # `_tail` cannot be shared with it.
        #
        # `_rows` steps a statement for the whole of its `fetchall`, and in WAL
        # a stepping statement pins that CONNECTION's read snapshot. Sharing
        # one between a scan and a seal therefore hands the seal whatever
        # snapshot the scan pinned — it writes its Parquet file from a stale
        # view, and coverage-based eviction can then delete buffered rows through
        # the group's end, including rows that never reached the file. Measured
        # with the former seal-time deletion:
        # 1,000 acknowledged offsets in neither tier, from a scan and a seal
        # running concurrently through the public API.
        #
        # Readers do not need this among themselves: `rows_from` holds
        # `_tail_lock` across its fetch, so only one steps at a time. A stale
        # view would be self-correcting there anyway, and structurally so —
        # offsets come from AUTOINCREMENT under one serialised writer and
        # deletions are prefix-only, so what a stale snapshot lacks is always a
        # SUFFIX, and `_rows(">= _tail_end")` cannot skip it.
        #
        # **This isolates the seal from READERS.** What keeps seals off each
        # other is the CLAIM: `Claim.acquire` filters on range overlap and
        # owner, not on kind, so it is a global range mutex. `_seal_queued`'s
        # re-read of the queue head closes the one hole in that — a sealer
        # whose claim succeeds because the range went free while it blocked,
        # and which would otherwise read a group that is gone. Measured across
        # 2.5M rows under four concurrent sealers: 0 overlapping reads here.
        #
        # Not an absolute, and the residual is the claim's TTL. Two overlapping
        # claims can coexist only if one expired, so a read would have to run
        # the full 30 s — about 15M rows at the measured 261 ms/150k, which
        # needs `target_seal_size` far above its 8 MiB default. Both would then
        # read the SAME range, whose rows were all committed before the group
        # closed, so the joiner still sees them.
        #
        # **That re-read is load-bearing, and the cost of removing it is
        # measured, not theoretical.** Without it a stale sealer reaches
        # `rows_between` and pins this connection for the length of a full
        # group read — 314 ms over 150k rows, against 8.5 ms for everything a
        # victim must do inside that window. Same probe without the re-read:
        # 49 stale pins and 48 overlapping reads over the same 2.5M rows, and
        # a gated run loses 100 acknowledged offsets, written into a file whose
        # recorded range is three times the rows it holds.
        #
        # An earlier version of this comment argued the overlap was harmless
        # because the fsyncs cost more than the pin lasts. That is backwards by
        # a factor of about 37, and it is recorded here so nobody restores the
        # shortcut on the strength of it.
        self._sealer = sealer
        # The backend serialises its own writes rather than leaving callers to
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
        # The rule is every statement on `_writer`, reads included — not just the
        # transactions. A bare SELECT issued while another thread has a
        # transaction open joins it and sees uncommitted rows, which a rollback
        # then unmakes; that is the same mechanism that once let a lease
        # evaporate under its holder.
        #
        # This used to say `_tail` needed none of it "precisely because
        # nothing ever opens a transaction on it". **A stepping statement IS an
        # open read transaction**, and `_rows` steps one for the whole of its
        # `fetchall` — so that premise was false and cost acknowledged rows.
        # What actually keeps `_tail` safe is that its one caller,
        # `rows_from`, holds `_tail_lock` across the fetch; the seal reads on
        # `_sealer` for the same reason. See `_sealer`.
        #
        # A caller's transaction (`transaction`) holds it for its whole block,
        # which is why it is a `WriteLock` rather than a bare `RLock`: the
        # thread holding the block may re-enter only through a call given its
        # `tx`, and anything else from that thread is refused rather than
        # joining the caller's transaction or rolling it back.
        self._lock = lock
        self.readonly = readonly

    @classmethod
    def open(cls, path: Path, *, readonly: bool = False) -> SqliteBackend:
        if readonly:
            writer = cls._connect_readonly(path)
            backend = cls(writer, writer, writer, WriteLock(), readonly=True, path=path)
        else:
            writer = sqlite3.connect(
                path, isolation_level=None, check_same_thread=False
            )
            cursor = writer.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
            cursor.execute("PRAGMA synchronous=FULL")
            cursor.close()
            backend = cls(
                writer,
                cls._connect_readonly(path),
                cls._connect_readonly(path),
                WriteLock(),
                path=path,
            )

        cursor = writer.cursor()
        cursor.execute("PRAGMA query_only=ON")
        cursor.close()
        return backend

    @staticmethod
    def _connect_readonly(path: Path) -> sqlite3.Connection:
        return sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            isolation_level=None,
            check_same_thread=False,
        )

    @contextlib.contextmanager
    def _writable(self) -> Iterator[Cursor]:
        with self._lock:
            cursor = self._writer.cursor()
            try:
                cursor.execute("PRAGMA query_only=OFF")
                yield cursor
            except BaseException:
                with contextlib.suppress(self.errors.any):
                    cursor.execute("PRAGMA query_only=ON")

                raise
            else:
                cursor.execute("PRAGMA query_only=ON")
            finally:
                cursor.close()

    @contextlib.contextmanager
    def write(self) -> Iterator[Cursor]:
        """One serialized write, rolled back if the body or COMMIT raises.

        Every multi-statement write goes through this. Four of them used to
        commit with no rollback: an error after BEGIN left the connection in a
        transaction with the lock released, and the next statement any thread
        issued joined it — the mechanism that erased a claim under its holder.
        """
        with self._writable() as cursor:
            cursor.execute(_BEGIN)
            try:
                yield cursor
                # Inside the guard: a full disk or I/O error during COMMIT must
                # not leave a transaction open after the lock is released.
                cursor.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(self.errors.any):
                    cursor.execute("ROLLBACK")

                raise

    @contextlib.contextmanager
    def savepoint(self, tx: CallerTransaction | None) -> Iterator[Cursor]:
        if tx is None:
            raise ValueError("no open caller transaction")

        tx.check()
        with self._lock.admit(), self._lock:
            cursor = self._writer.cursor()
            cursor.execute(_SAVEPOINT)
            try:
                yield cursor
                tx.check()
                cursor.execute(_RELEASE)
            except BaseException:
                with contextlib.suppress(self.errors.any):
                    cursor.execute(_ROLLBACK_TO)
                    cursor.execute(_RELEASE)

                raise
            finally:
                cursor.close()

    @contextlib.contextmanager
    def read(self) -> Iterator[Cursor]:
        with self._lock:
            cursor = self._writer.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def tail(self) -> Cursor:
        return self._tail.cursor()

    def sealer(self) -> Cursor:
        return self._sealer.cursor()

    @staticmethod
    def sql(text: str) -> str:
        return text

    @staticmethod
    def insert_id(cursor: Cursor, sql: str, params: Sequence[object]) -> int:
        return int(cast(sqlite3.Cursor, cursor.execute(sql, params)).lastrowid or 0)

    def insert_rows(
        self,
        cursor: Cursor,
        sql: str,
        rows: Iterable[PreparedRow],
        refuse: Callable[[PreparedRow, BaseException], NoReturn],
    ) -> Iterator[tuple[PreparedRow, int]]:
        # The native cursor executes once per row; no cursor proxy or per-row
        # insert_id call sits in this hot loop.
        native = cast(sqlite3.Cursor, cursor)
        for item in rows:
            try:
                native.execute(sql, item[1])
            except self.errors.integrity as exc:
                refuse(item, exc)

            yield item, int(native.lastrowid or 0)

    @staticmethod
    def next(cursor: Cursor) -> int:
        row = cursor.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
        ).fetchone()
        return (int(row[0]) if row else 0) + 1

    @staticmethod
    def floor(cursor: Cursor) -> int:
        row = cursor.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
        ).fetchone()
        highest = cursor.execute(
            'SELECT max("litelink_offset") FROM buffer'
        ).fetchone()[0]
        return max(int(row[0]) if row else 0, int(highest or 0))

    @staticmethod
    def seed(cursor: Cursor, first: int) -> None:
        highest = cursor.execute(
            'SELECT max("litelink_offset") FROM buffer'
        ).fetchone()[0]
        if highest is not None and first - 1 < highest:
            raise ValueError(
                f"cannot seed offsets to {first} on a buffer holding rows up to {highest}: SQLite ignores a sequence lowered past them"
            )

        updated = cursor.execute(
            "UPDATE sqlite_sequence SET seq = ? WHERE name = 'buffer'", (first - 1,)
        )
        if not updated.rowcount:
            cursor.execute(
                "INSERT INTO sqlite_sequence (name, seq) VALUES ('buffer', ?)",
                (first - 1,),
            )

    def advance(self, cursor: Cursor, n: int) -> tuple[int, int]:
        floor = self.floor(cursor)
        ceiling = floor + n
        self.seed(cursor, ceiling + 1)
        landed = self.next(cursor) - 1
        if landed != ceiling:
            raise RuntimeError(
                f"reserving {n} offsets from {floor} left the sequence at {landed} rather than {ceiling}; nothing has been issued"
            )

        return floor + 1, ceiling + 1

    def caller_transaction(self, cursor: Cursor) -> CallerTransaction:
        hold = self._lock.hold()
        hold.__enter__()

        def close() -> None:
            hold.__exit__(None, None, None)

        return SqliteTransaction(self._writer, owned, close=close)

    def published_catalog(self) -> PublishedCatalog:
        if self._path is None:
            raise ValueError("a file-backed catalog needs a backend opened from a path")

        return SqlitePublishedCatalog(
            Layout(self._path.parent.parent, self._path.parent.name)
        )

    def close(self) -> None:
        self._writer.close()
        for extra in (self._tail, self._sealer):
            if extra is not self._writer:
                extra.close()

    def _rename_range_columns(self, cursor: Cursor, table: str) -> None:
        """`lo`/`hi` → `start_offset`/`end_offset` on `table`, once."""
        columns = {str(row[1]) for row in cursor.execute(f"PRAGMA table_info({table})")}
        if "lo" in columns:
            cursor.execute(f"ALTER TABLE {table} RENAME COLUMN lo TO start_offset")

        if "hi" in columns:
            cursor.execute(f"ALTER TABLE {table} RENAME COLUMN hi TO end_offset")

    def create(self, schema: pa.Schema) -> None:
        """Create the schema and apply its legacy migrations under one write."""
        with self.write() as cursor:
            # `_fallback`, not `shape()`: this runs before the `meta` table it
            # would read exists, and the schema handed to the constructor is the
            # right source anyway — this call is what brings the table into being.
            cursor.execute(_buffer_ddl(Shape.of(schema)))
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sealing (
                  start_offset INTEGER, end_offset INTEGER, rel_path TEXT
                )
            """)
            # The same intent record as `sealing`, for the other operation that
            # creates a data file. Both exist so that no file is ever written whose
            # path this database does not already hold — see `pending_delete`.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS compacting (
                  start_offset INTEGER, end_offset INTEGER, rel_path TEXT
                )
            """)
            # The deletion queue. A file leaves the current snapshot long before it
            # may be deleted (I6), so the interval has to be remembered somewhere;
            # remembering it here is what makes reclamation a keyed read of this
            # table rather than a directory walk looking for things nobody claimed.
            #
            # `superseded_at`, not a precomputed deadline: the grace period is the
            # owning table's snapshot retention, and freezing it at enqueue time
            # would mean a lowered setting never applied to anything already queued.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending_delete (
                  rel_path TEXT PRIMARY KEY, superseded_at INTEGER NOT NULL
                )
            """)
            # The seal queue, and the reason `target_seal_size` means anything.
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
            cursor.execute("""
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
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS extent_unsealed ON extent (group_id)
                WHERE rel_path IS NULL
            """)
            # I4 per segment (§4a) reads the published copies covering what the
            # staging table still holds. Without this the lookup is a scan of one
            # row per file ever published, which grows without limit — the staging
            # file count does not.
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS extent_published ON extent (start_offset)
                WHERE rel_path IS NOT NULL
            """)
            # Its pre-#98 name, which an old log carries: one index, not two.
            cursor.execute("DROP INDEX IF EXISTS extent_archived")
            # A copy that was INTENDED, beside `extent`'s copies that exist. The two
            # are read by collaborators whose safe directions are opposite:
            # compaction must not merge across a range some published table may
            # hold, so it is safe when coverage is OVERSTATED; eviction must not
            # delete the only copy, so it is safe only when coverage is UNDERSTATED.
            # One record cannot be both, and collapsing them into one is what left a
            # crash between a register and the rows recording it able to wedge the
            # log.
            #
            # A separate table rather than a column on `extent`, because a build
            # that predates this has no idea it exists — so its eviction query is
            # unchanged and reads only landed copies, which is the safe polarity by
            # construction. A column would have read to it as coverage, and no
            # check in a new build can stop an old one.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS extent_intent (
                  rel_path     TEXT PRIMARY KEY,
                  start_offset INTEGER NOT NULL,
                  end_offset   INTEGER NOT NULL,
                  bytes        INTEGER NOT NULL
                )
            """)
            # Merged staging files that still have to replace published copies of
            # their inputs (#160). A merge whose inputs were published early — by
            # a flushed publish, before compaction was done with them — writes a
            # file the published table does not hold yet, under a watermark that
            # already covers it: `publish` swaps it in for those copies. Nothing
            # local could tell that file from one already swapped, so it is
            # recorded, from the merge's commit until the swap's.
            #
            # A table of its own, not a column on `extent`, for `extent_intent`'s
            # reason: a build that predates it ignores it, and evicts such a file
            # as published, which costs only the swap.
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS awaiting_swap (rel_path TEXT PRIMARY KEY)"
            )
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"
            )
            # A stored tier's offset range and its column statistics (#90), in two
            # tables rather than one: `litelink_offset` is the log's own sequence,
            # dense and monotonic across every tier, so a tier's `[start, end)` is
            # a fact about where it sits in the log rather than a statistic — and
            # it prunes on its own, as streamcast prunes whole logs by offset
            # (streamcast#32). Three tiers can have a row: the published table's,
            # which is what lets a read skip it (`_tiers`); the staging table's, a
            # cache of one version's rollup; and the buffer's, written only when
            # `retire()` closes it (see `close_buffer`).
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tier_offsets (
                  tier         TEXT PRIMARY KEY,
                  start_offset INTEGER NOT NULL,
                  end_offset   INTEGER NOT NULL
                )
            """)
            # `version` is which version of the table a row describes — the path of
            # its Iceberg metadata file — and only the STAGING row has one: it is a
            # cache of one version's rollup, valid for exactly that version. The
            # published table's row has none, because it is kept a superset of what
            # the published table holds below the staging table at every moment
            # (eviction widens it before its commit), whatever version the
            # published table is at.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tier_statistics (
                  tier       TEXT PRIMARY KEY,
                  statistics TEXT NOT NULL,
                  version    TEXT
                )
            """)
            # A retired log takes no rows, from any process — including a writer
            # that opened before `retire()` ran and never looks again. In SQLite
            # rather than in `append`, so it costs the write path nothing it would
            # notice and no handle can be stale about it. What it checks is the
            # buffer's closed range (see `close_buffer`): a buffer with an end is
            # a log that takes no more rows, one fact recorded once.
            cursor.execute(f"""
                CREATE TRIGGER IF NOT EXISTS refuse_retired BEFORE INSERT ON buffer
                WHEN EXISTS (SELECT 1 FROM tier_offsets WHERE tier = 'buffer')
                BEGIN SELECT RAISE(ABORT, '{RETIRED_REFUSAL}'); END
            """)
            # Who owns which operation, across processes (§13.6). A Python lock
            # cannot say anything about a process that is no longer running, and
            # recovery has to know whether an interrupted operation was ours.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS claim (
                  id         INTEGER PRIMARY KEY AUTOINCREMENT,
                  owner      TEXT NOT NULL,
                  expires_at INTEGER NOT NULL,
                  kind         TEXT NOT NULL,
                  start_offset INTEGER NOT NULL,
                  end_offset   INTEGER NOT NULL,
                  rel_path     TEXT
                )
            """)
            # Both tables named their range `lo`/`hi` before every range became
            # half-open, and those names read as inclusive. Renamed in place, as
            # every other table names a range; SQLite carries `claim_live` along.
            # The values need no conversion: a claim lives 30 s, and recovery
            # reads only the path from a `compacting` row.
            self._rename_range_columns(cursor, "claim")
            self._rename_range_columns(cursor, "compacting")
            # Every acquisition asks the same question — is a live claim covering
            # this range — and asks it inside a write transaction, so it is the one
            # query that must not degrade into a scan as claims accumulate.
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS claim_live
                ON claim (expires_at, start_offset, end_offset)
            """)
            # A buffer carrying the old `lease` table was last written by a build
            # that coordinated through it, and this one coordinates through
            # `claim`. Neither sees the other, so two sealers could claim the same
            # queued group — the torn file the claim mechanism exists to prevent.
            # Nothing here can make an OLD binary respect the new table, so the
            # rename is an offline upgrade: refuse rather than run alongside one.
            if cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lease'"
            ).fetchone():
                msg = (
                    "this log was last opened by a build that coordinated through a "
                    "`lease` table; ranged claims replaced it and the two do not "
                    "exclude each other. Stop every process using this log, then "
                    "run `DROP TABLE lease` on its buffer.db to complete the upgrade"
                )
                raise RuntimeError(msg)

    def reclaim(self, min_free_ratio: float = 0.0) -> int:
        """Return the file's dead space to the OS. Bytes reclaimed, or 0 (§3a).

        Reclaims when the free list is at least `min_free_ratio` of the file.
        The default reclaims whenever there is anything worth reclaiming, which
        is what an explicit `reclaim("buffer")` asks for; `advance` passes the
        log's `vacuum_free_ratio` instead.

        SQLite puts pages freed by a DELETE on a free list and never shrinks the
        file, so a buffer that seals and publishes for months keeps every page it
        has ever needed. That is invisible locally — the free list is reused —
        and expensive off-box, because litestream replicates the FILE: a
        `restore` downloads and applies the dead space. Measured
        on a 1-day-old capture: 457 MB holding 20,658 live rows, 92% of its pages
        free, restoring in 12.5 s against 0.8 s for the same content vacuumed.

        **The obvious objection is that `VACUUM` rewrites everything, so it must
        cost more in shipped WAL than it saves. Measured, it does not.** Two
        sidecars, same workload, six insert/delete cycles: 3.1 MB shipped
        without, 0.3 MB with. litestream ships LTX deltas of a SMALLER database,
        so the steady state is cheaper, and the one-time rewrite of an already
        bloated file measured 0.06 MB on the wire.

        **Not automatic, and not on the delete path.** `VACUUM` takes an
        exclusive lock and rebuilds the file, so its cost is the LIVE data and it
        stalls appends for that long — 0.3 s at 35 MB. Running it wherever rows
        happen to leave would put a background cost on the write path and take
        the decision away from the deployment that knows its append rate. It is
        a maintenance operation: `WriteHandle.reclaim("buffer")` calls it, and
        `advance` does too when `vacuum_free_ratio` is set.

        **Offsets are untouched, which is the property that matters (I9).**
        `litelink_offset` is an explicit `INTEGER PRIMARY KEY`, so it is column
        data rather than an implicit rowid SQLite may renumber: values keep
        their gaps and are never rewritten. `sqlite_sequence` survives too,
        including across a VACUUM of a FULLY DRAINED buffer — the ordinary state
        after `evict("buffer")` on a log the published table has caught up with,
        and the case that would matter: were that counter lost, AUTOINCREMENT
        would restart at 1 and reissue offsets the published table already
        holds. Verified both directly.

        Not in `write()`: SQLite refuses `VACUUM` inside a transaction. The lock is
        still held, so no append interleaves.
        """
        with self._writable() as cursor:
            page_size = int(cursor.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(cursor.execute("PRAGMA page_count").fetchone()[0])
            free = int(cursor.execute("PRAGMA freelist_count").fetchone()[0])
            if (
                not page_count
                or free * page_size < _VACUUM_FLOOR_BYTES
                or free < page_count * min_free_ratio
            ):
                return 0

            cursor.execute("VACUUM")

            return free * page_size

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
            return _meta_value(connection.cursor(), SqliteBackend.sql, key)
        finally:
            connection.close()

    @classmethod
    def peek_retired(cls, path: Path) -> dict[str, object] | None:
        """`retired()` for a database file no handle holds open — a replica."""
        con = cls._connect_readonly(path)
        try:
            row = con.execute(
                "SELECT o.end_offset, s.statistics"
                " FROM tier_offsets o JOIN tier_statistics s USING (tier)"
                " WHERE o.tier = 'buffer'"
            ).fetchone()
            published = _meta_value(con.cursor(), cls.sql, PUBLISHED_META_KEY)
        except sqlite3.OperationalError:
            return None
        finally:
            con.close()

        if row is None:
            return None

        return _retirement(int(row[0]), str(row[1]), published)
