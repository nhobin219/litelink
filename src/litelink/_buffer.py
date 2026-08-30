"""The SQLite write buffer (SPEC §2, §3).

One database per stream. Durable on commit — `synchronous=FULL` with WAL — and
that is the whole durability story: there is no in-memory staging layer whose
loss a SIGKILL could expose.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from litelink._config import LogConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping
    from pathlib import Path

from litelink._claim import DEFAULT_TTL_MS, Claim
from litelink._types import column_type

# Where the log records its settings. It lives here because this object owns
# `meta`, and `meta` is the one place the policy exists.
CONFIG_KEY = "config"

# §4's declared clustering. Here for the same reason `CONFIG_KEY` is: `meta` is
# the one place it exists, so this object owns the read.
SORT_KEY = "sort_by"

# The library-owned column (§2).
OFFSET = "litelink_offset"

# Where the log records its declared schema. Beside `CONFIG_KEY` and for the
# same reason: this object owns `meta`, and `meta` is the one place the schema
# exists. It used to live in `log.py`, which meant the module that OWNS the row
# could not read it without importing the module that names it.
SCHEMA_KEY = "arrow_schema"

# Where a schema change records what it set out to do, before it does any of
# it (I16). Cleared only when the change is complete — §9: a schema change is
# finished when SQLite says so, not when Iceberg does.
INTENT_KEY = "schema_intent"

# Where a log records the offset it was created to start at, or nothing if it
# started at 1. Durable because a future backfill needs to tell the RESERVE
# below it — deliberate, empty, safe to fill — from a `litelink.restore` fence,
# which is empty for the opposite reason and must never be filled.
#
# Nothing distinguishes them after the fact: `WriteHandle` says "the skipped range
# leaves no trace once the sequence has moved", and a restore whose replica was
# empty leaves the log high with nothing below it, positionally identical to a
# reserve. The recorded value is the only thing that separates the two.
START_OFFSET_KEY = "start_offset"

# Stands in for "no row limit" so the per-row check stays one comparison. Far
# above any row count a buffer sized for read latency could reach.
_NO_ROW_LIMIT = 1 << 62

# How many appended-since-last-query slices the tail may accumulate before it
# is compacted back into one.
_MAX_TAIL_CHUNKS = 32

# Bound once here rather than looked up per row: `frozenset.__contains__` is
# the unbound method, so `map(_CONTAINS, carriers, types)` drives the whole
# per-column scan in C.
_CONTAINS = frozenset.__contains__

# An explicitly infinite value is a legal float32 — what the range check exists
# to catch is a FINITE value that would silently become one.
_INFINITE = (float("inf"), float("-inf"))

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


def _column_ddl(name: str, field: pa.Field) -> str:
    """One column's DDL, carrying every part of I17 that SQLite can enforce.

    **Every column is `ANY`, and that is the whole design.** A STRICT column
    of a declared type does not refuse a wrong value, it CONVERTS one: an
    INTEGER column given `'77'` stores 77, `'007'` stores 7, and a REAL column
    given `'1e999'` stores `inf`. A TEXT column given `12345` stores
    `'12345'`. The conversion happens before any CHECK could see it, so the
    constraint would be asked about a value that had already been changed.

    `ANY` stores the value exactly as given, which is what lets `typeof` tell
    the truth about it. STRICT is still declared — it is what makes `ANY` mean
    "no conversion" rather than "no declared affinity".

    One CHECK per column rather than several: the type test and the range test
    are one question about one value, and SQLite evaluates a single expression
    more cheaply than two.

    The remaining leniency is `True` into an integer column, which stores 1.
    Python's driver converts a bool before SQLite sees the value, so no
    constraint can distinguish it from a plain int. It is lossless.
    """
    kind = column_type(field.type)
    q = f'"{name}"'

    if pa.types.is_boolean(field.type):
        # A bool column is an integer holding 0 or 1; anything else is a value
        # the read path would turn into `True` — `7` becomes true, silently.
        test = f"typeof({q}) = 'integer' AND {q} IN (0, 1)"
    elif pa.types.is_floating(field.type):
        # Two storage classes, tested separately, because what makes each of
        # them wrong is different.
        #
        # An integer is a legal float — `{"price": 5}` is too natural to
        # refuse — but only while the conversion is lossless, and `ANY` means
        # it stays an INTEGER in SQLite rather than being converted on the way
        # in. Past 2**53 (2**24 for float32) `pa.array(..., type=float64)`
        # then refuses to build the column AT ALL, so one such value makes
        # every scan and every seal raise for ever while appends keep
        # succeeding. Bounding it here is what keeps that unreachable.
        #
        # Testing the integer with BETWEEN rather than `abs` is deliberate:
        # `abs(-(2**63))` overflows in SQLite, which has no positive
        # counterpart for the most-negative int64, and raised
        # `OperationalError` out of the CHECK instead of a refusal.
        exact = kind.exact_int
        real = f"typeof({q}) = 'real'"
        if kind.bounds is not None:
            # `9e999` is how SQL spells infinity. An explicitly infinite value
            # is legal and stays legal — a float32 holds it exactly, so
            # passing one is a statement, not an overflow.
            real += f" AND (abs({q}) <= {kind.bounds[1]!r} OR abs({q}) = 9e999)"

        test = (
            real
            if exact is None
            else (
                f"(typeof({q}) = 'integer' AND {q} BETWEEN {-exact:d} AND {exact:d})"
                f" OR ({real})"
            )
        )
    else:
        test = (
            f"typeof({q}) = 'integer'"
            if kind.sqlite == "INTEGER"
            else (f"typeof({q}) = 'text'")
        )
        if kind.bounds is not None:
            lo, hi = kind.bounds
            test += f" AND {q} BETWEEN {lo:.0f} AND {hi:.0f}"

    parts = [f"{q} ANY"]
    if not field.nullable:
        # Absent and explicitly-None reach SQLite identically — the insert is
        # built as `row.get(c)` — so one constraint covers both.
        parts.append("NOT NULL")

    parts.append(f"CHECK ({q} IS NULL OR ({test}))")

    return " ".join(parts)


@dataclass(frozen=True, slots=True)
class Shape:
    """The declared schema, and everything the write path derives from it.

    One object because they are one fact, and separating them is a silent
    loss. A fresh `known` accepts a column a stale `columns` then drops in
    `tuple(row.get(c) for c in columns)` — the row is acknowledged with an
    offset and the value is gone. Deriving them together, in one read, is what
    makes that unrepresentable rather than merely avoided.
    """

    schema: pa.Schema
    columns: tuple[str, ...]
    known: frozenset[str]
    required: tuple[int, ...]
    carriers: tuple[frozenset[type], ...]
    accepts: tuple[Callable[[object], bool], ...]
    ranged: tuple[tuple[int, float, float], ...]
    exact_ints: tuple[tuple[int, int], ...]

    @property
    def table(self) -> pa.Schema:
        """The caller's columns with `offset` in front — the TABLE's schema.

        Here rather than in `log.py` so the archive can ask the buffer for it
        without importing the module that owns the log. It is the shape
        `create_table` is handed, and an archive born from a stale copy of it
        is the one holder that cannot be repaired afterwards: nothing in
        `src/` ever re-declares an existing table.
        """
        return pa.schema([pa.field(OFFSET, pa.int64(), nullable=False), *self.schema])

    @classmethod
    def of(cls, schema: pa.Schema) -> Shape:
        columns = tuple(schema.names)

        return cls(
            schema=schema,
            columns=columns,
            # `litelink_offset` is in the set so `_reject_offset` stays the
            # thing that refuses it, with its own message.
            known=frozenset((OFFSET, *columns)),
            # Positions, not names: the check reads the values tuple, which is
            # built from `columns` in this order, so a name-keyed set would
            # mean a second lookup per row on the write path.
            # `field(i)`, not `field(name)`: the index is what the check uses
            # to reach into the values tuple, so deriving it positionally makes
            # the two impossible to disagree. Name lookup was equivalent for
            # every schema that can exist — `_create` refuses a duplicate name
            # before a log gets built — but it read as if it might not be.
            required=tuple(
                i for i in range(len(columns)) if not schema.field(i).nullable
            ),
            # Aligned with `columns`, like `required`, and for the same reason.
            carriers=tuple(
                column_type(schema.field(i).type).carriers for i in range(len(columns))
            ),
            accepts=tuple(
                column_type(schema.field(i).type).accepts for i in range(len(columns))
            ),
            # Only the columns that CAN overflow, so a schema of int64s,
            # float64s and strings leaves this empty and pays one truthiness
            # test per row rather than a loop.
            # Float columns only, and used solely by the explainer — the
            # DDL is what enforces it.
            exact_ints=tuple(
                (i, exact)
                for i in range(len(columns))
                if (exact := column_type(schema.field(i).type).exact_int) is not None
            ),
            ranged=tuple(
                (i, *bounds)
                for i in range(len(columns))
                if (bounds := column_type(schema.field(i).type).bounds) is not None
            ),
        )


class Buffer:
    """The unsealed tail of a log."""

    def __init__(
        self,
        writer: sqlite3.Connection,
        reader: sqlite3.Connection,
        sealer: sqlite3.Connection,
        schema: pa.Schema,
    ) -> None:
        """Take built collaborators. `open` is what builds and validates them.

        `writer` is the connection every transaction here runs on; `reader` is
        a second, read-only handle for the rows a seal is about to write out.
        That read is the expensive half of a seal, and on the write connection
        it would serialise against the appends a seal exists to stay out of the
        way of. WAL allows it: one writer, any number of readers.

        `schema` is the application's columns, without `offset`. Its names
        used to be passed alongside it; they are derived in `Shape.of` now, so
        the two cannot be handed in disagreeing — the positions in
        `Shape.required` index the values tuple, and a caller that passed a
        different order would have aimed them at the wrong columns.
        `target_seal_size` is here rather than only on the seal because the cut
        it describes is made on the append path — see `extent`.
        """
        self._con = writer
        self._reader = reader
        # A THIRD connection, for the seal alone, and it exists because
        # `_reader` cannot be shared with it.
        #
        # `_rows` steps a statement for the whole of its `fetchall`, and in WAL
        # a stepping statement pins that CONNECTION's read snapshot. Sharing
        # one between a scan and a seal therefore hands the seal whatever
        # snapshot the scan pinned — it writes its Parquet file from a stale
        # view, and `finish_seal` then deletes every buffered row through the
        # group's end, including rows that never reached the file. Measured:
        # 1,000 acknowledged offsets in neither tier, from a scan and a seal
        # running concurrently through the public API.
        #
        # Readers do not need this among themselves: `rows_above` holds
        # `_tail_lock` across its fetch, so only one steps at a time. A stale
        # view would be self-correcting there anyway, and structurally so —
        # offsets come from AUTOINCREMENT under one serialised writer and
        # deletions are prefix-only, so what a stale snapshot lacks is always a
        # SUFFIX, and `_rows("> _tail_hi")` cannot skip it.
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
        # What `shape()` falls back to when `meta` has no schema row yet. Two
        # callers need that and both would otherwise fail at construction:
        # `_create` runs inside `Buffer.open` BEFORE `litelink.new` writes the row,
        # and the scratch buffer an archive rewrite cuts through is handed a
        # schema directly and only ever has `CONFIG_KEY` written into it.
        #
        # A fallback, not the value. Everything reads `shape()`, so a schema
        # change reaches every holder without anyone remembering to refresh —
        # which is the failure this replaces.
        self._fallback = Shape.of(schema)
        self._shape_cache: tuple[str, Shape] | None = None
        self._config_cache: tuple[str, LogConfig] | None = None
        self._sort_cache: tuple[str, tuple[str, ...]] | None = None
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
        # evaporate under its holder.
        #
        # This used to say `_reader` needed none of it "precisely because
        # nothing ever opens a transaction on it". **A stepping statement IS an
        # open read transaction**, and `_rows` steps one for the whole of its
        # `fetchall` — so that premise was false and cost acknowledged rows.
        # What actually keeps `_reader` safe is that its one caller,
        # `rows_above`, holds `_tail_lock` across the fetch; the seal reads on
        # `_sealer` for the same reason. See `_sealer`.
        self._lock = threading.RLock()
        # The read cache — see `rows_above`. Its own lock rather than the one
        # above, which appends hold: a read must not wait behind a write to
        # look at a table the write cannot invalidate.
        self._tail_lock = threading.Lock()
        self._tail: pa.Table | None = None
        # Which `Shape` the cached tail was built under, compared by IDENTITY:
        # `shape()` hands back the same object until the raw `meta` value
        # changes, so `is not` is exactly "the schema moved".
        #
        # Checked where the tail is used rather than pushed from `shape()`.
        # Pushing deadlocks: the refresh path holds `_tail_lock` and calls
        # `_rows`, which calls `shape()` — and `_tail_lock` is not reentrant,
        # so invalidating from inside `shape()` hangs the reader on itself.
        # Found by stack-dumping a suite that had sat idle for 29 minutes.
        self._tail_shape: Shape | None = None
        self._tail_lo = 0
        self._tail_hi = 0
        # The lowest boundary this cache is COMPLETE for — the floor it was
        # fetched above, not the offset it happens to start at.
        #
        # `_tail_lo` is `first_offset - 1`, which on a log whose offsets start
        # high is far above any boundary a reader asks for before the first
        # seal: `Reader.query` passes 0 while the local table has no extent, so
        # gating on `_tail_lo <= floor` missed on EVERY read and re-converted
        # the whole buffer per query. Measured at the default 8 MiB first-seal
        # window: 4.2 ms/read against 42.
        #
        # Two distinct facts, and conflating them is what cost that. Where the
        # cache STARTS bounds the slice arithmetic; what it is complete FOR
        # decides whether it can answer at all.
        self._tail_from = 0

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

    @classmethod
    def open(
        cls,
        path: Path,
        schema: pa.Schema,
        *,
        readonly: bool = False,
        durable: bool = True,
    ) -> Buffer:
        """Connect, configure, and create the tables. Then hand them to `cls`.

        The I/O half, kept out of `__init__` for the same reason `litelink.open` is
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
        if readonly:
            con = cls._connect_readonly(path)

            # A readonly buffer never seals, so it needs no third connection —
            # `rows_between` is unreachable without a claim, which needs a
            # writer.
            return cls(
                con,
                con,
                con,
                schema,
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
            cls._connect_readonly(path),
            schema,
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
        # `_fallback`, not `shape()`: this runs before the `meta` table it
        # would read exists, and the schema handed to the constructor is the
        # right source anyway — this call is what brings the table into being.
        shape = self._fallback
        columns = ",\n  ".join(
            _column_ddl(name, shape.schema.field(name)) for name in shape.columns
        )
        # AUTOINCREMENT, not a bare INTEGER PRIMARY KEY: buffer rows are deleted
        # at every seal, and a rowid alias would reissue offsets already
        # committed to Iceberg once the table empties, silently corrupting every
        # tier boundary in §7 (I9, §2).
        # STRICT is what makes `ANY` mean "store this value exactly as
        # given" rather than "no declared affinity". It is not itself the
        # gate — a STRICT column of a declared type CONVERTS a wrong value
        # rather than refusing it — so every column is `ANY` and carries its
        # own CHECK. See `_column_ddl`, which is where I17 actually lives.
        self._con.execute(f"""
            CREATE TABLE IF NOT EXISTS buffer (
              "litelink_offset" INTEGER PRIMARY KEY AUTOINCREMENT,
              {columns}
            ) STRICT
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
        # I4 per segment (§4a) reads the archive copies covering what the local
        # table still holds. Without this the lookup is a scan of one row per
        # file ever archived, which grows without limit — the local file count
        # does not.
        self._con.execute("""
            CREATE INDEX IF NOT EXISTS extent_archived ON extent (start_offset)
            WHERE rel_path IS NOT NULL
        """)
        # A copy that was INTENDED, beside `extent`'s copies that exist. The
        # two are read by collaborators whose safe directions are opposite:
        # compaction must not merge across a range some archive may hold, so it
        # is safe when coverage is OVERSTATED; eviction must not delete the only
        # copy, so it is safe only when coverage is UNDERSTATED. One record
        # cannot be both, and collapsing them into one is what left a crash
        # between a register and the rows recording it able to wedge the log.
        #
        # A separate table rather than a column on `extent`, because a build
        # that predates this has no idea it exists — so its eviction query is
        # unchanged and reads only landed copies, which is the safe polarity by
        # construction. A column would have read to it as coverage, and no
        # check in a new build can stop an old one.
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS extent_intent (
              rel_path     TEXT PRIMARY KEY,
              start_offset INTEGER NOT NULL,
              end_offset   INTEGER NOT NULL,
              bytes        INTEGER NOT NULL
            )
        """)
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"
        )
        # Who owns which operation, across processes (§13.6). A Python lock
        # cannot say anything about a process that is no longer running, and
        # recovery has to know whether an interrupted operation was ours.
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS claim (
              id         INTEGER PRIMARY KEY AUTOINCREMENT,
              owner      TEXT NOT NULL,
              expires_at INTEGER NOT NULL,
              kind       TEXT NOT NULL,
              lo         INTEGER NOT NULL,
              hi         INTEGER NOT NULL,
              rel_path   TEXT
            )
        """)
        # Every acquisition asks the same question — is a live claim covering
        # this range — and asks it inside a write transaction, so it is the one
        # query that must not degrade into a scan as claims accumulate.
        self._con.execute("""
            CREATE INDEX IF NOT EXISTS claim_live ON claim (expires_at, lo, hi)
        """)
        # A buffer carrying the old `lease` table was last written by a build
        # that coordinated through it, and this one coordinates through
        # `claim`. Neither sees the other, so two sealers could claim the same
        # queued group — the torn file the claim mechanism exists to prevent.
        # Nothing here can make an OLD binary respect the new table, so the
        # rename is an offline upgrade: refuse rather than run alongside one.
        if self._con.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lease'"
        ).fetchone():
            msg = (
                "this log was last opened by a build that coordinated through a "
                "`lease` table; ranged claims replaced it and the two do not "
                "exclude each other. Stop every process using this log, then "
                "run `DROP TABLE lease` on its buffer.db to complete the upgrade"
            )
            raise RuntimeError(msg)

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
    # percent of the true size is what `target_seal_size` actually needs.

    def table_columns(self) -> tuple[str, ...]:
        """The buffer table's ACTUAL columns, asked of SQLite.

        The probe recovery settles step 6 with. `_create` is
        `CREATE TABLE IF NOT EXISTS`, so a reopened buffer keeps whatever
        columns it already had — the declared schema saying otherwise proves
        nothing about this table.
        """
        with self._lock:
            rows = self._con.execute("PRAGMA table_info(buffer)").fetchall()

        return tuple(str(r[1]) for r in rows)

    def add_table_column(self, name: str, type_: pa.DataType) -> None:
        """Widen the buffer table. Idempotent, like every step of a change.

        SQLite cannot add a NOT NULL column without a default, which is not a
        limitation here but the same rule Iceberg enforces: rows that predate
        the column have no value for it, so it must be nullable. `add_column`
        refuses a non-nullable field before reaching this.
        """
        if name in self.table_columns():
            return

        # The SAME DDL a column created with the table gets. Building this
        # from the affinity alone left every column added by `add_column`
        # with no constraints at all — unvalidated for the life of the log,
        # since `_create` is `CREATE TABLE IF NOT EXISTS` and never revisits
        # it. `ALTER TABLE ADD COLUMN` accepts a CHECK, and it fires.
        ddl = _column_ddl(name, pa.field(name, type_))
        with self._lock:
            self._con.execute(f"ALTER TABLE buffer ADD COLUMN {ddl}")

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
        shape = self.shape()
        terms = ["8"]  # offset
        for name in shape.columns:
            terms.append(
                f'coalesce(octet_length("{name}"), 0)'
                if column_type(shape.schema.field(name).type).variable_length
                else "8"
            )

        row = self._con.execute(
            f"SELECT coalesce(sum({' + '.join(terms)}), 0) FROM buffer"
            ' WHERE "litelink_offset" >= ?',
            (floor,),
        ).fetchone()

        return int(row[0])

    def _row_bytes(self, row: Mapping[str, object], columns: tuple[str, ...]) -> int:
        """Approximate bytes for one row, and refuse what SQLite cannot store.

        `columns` is passed rather than read, because this runs once per row.
        Reading it here would mean a keyed `meta` read and a lock acquisition
        for every appended row — which is exactly what happened when this took
        it from a property, and it made the test suite eight times slower.

        The NaN check lives here because this loop already visits every value,
        so it costs a comparison rather than a pass. SQLite has no NaN — it
        stores one as NULL, verified — so a float column would take a NaN and
        return a null, silently, with no error anywhere. That is the same
        failure `_types` refuses whole types for, one level down: the library
        declines what it cannot carry faithfully rather than changing it.
        """
        total = 8
        for name in columns:
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
        with self._lock:
            # ONE read, inside the lock, and handed down. Read here and again
            # in `_insert` and the two can disagree: a schema change landing
            # between them builds the statement from one column list and the
            # value tuples from another, so the bindings do not match the
            # placeholders. Taking it under the lock is also what makes the
            # statement and the rows it carries describe the same schema.
            shape = self.shape()
            placeholders = ", ".join("?" * len(shape.columns))
            names = ", ".join(f'"{c}"' for c in shape.columns)
            sql = f"INSERT INTO buffer ({names}) VALUES ({placeholders})"

            return self._insert(rows, sql, shape)

    def _insert(
        self, rows: Iterable[Mapping[str, object]], sql: str, shape: Shape
    ) -> list[int]:
        """The append's transaction, with the lock already held."""
        offsets: list[int] = []
        cursor = self._con.cursor()
        cursor.execute(_BEGIN)
        try:
            group = self._read_group(cursor)
            # Bound once, and the accounting inlined below, because this loop
            # runs per row: routing it through a method cost 19 points of
            # overhead against raw SQLite at 1,000-row batches.
            config = self.config()
            target = config.target_seal_size
            # `_insert` runs per row, so both limits are bound once out here.
            # A row cap of None becomes one nothing reaches, which keeps the
            # inner test a comparison rather than a branch on None.
            target_rows = config.target_seal_rows or _NO_ROW_LIMIT
            row_bytes = self._row_bytes
            columns = shape.columns
            # Bound out here like `row_bytes` above, for the same reason: this
            # runs per row.
            # The ONE question SQLite cannot be asked. The insert names the
            # schema's columns, so a key the log does not have is dropped
            # before any SQL exists — no constraint can see what is not in the
            # statement. Everything else I17 promises is in the DDL now.
            declared = shape.known.issuperset
            width = len(columns)
            for row in rows:
                # I11 FIRST, so a row that is wrong twice gets the message
                # about the thing that is specifically forbidden rather than
                # the generic one. `litelink_offset` is in `_known`, so the
                # subset test passes it through to here either way; the order
                # is what decides which error the caller reads.
                self._reject_offset(row)

                values = tuple(row.get(c) for c in columns)
                # The unknown-column test, skipped when the row proves it
                # cannot have one. If every declared column came back
                # non-None then every declared column is PRESENT, so a row
                # of exactly `width` keys holds those columns and nothing
                # else — there is no room for an extra.
                #
                # Both halves are needed and neither alone is sound. A row
                # that omits a nullable column has the wrong width and is
                # perfectly legal, so width alone cannot refuse. And a row of
                # the right width can still hide an unknown key behind an
                # absent one — `ky` for `key` — which is why a single None
                # sends it to the full test. That pairing is the bug an
                # earlier draft shipped as `len(row) != width and not
                # declared(row)`: it short-circuited the wrong way round.
                #
                # `None in values` is one C-level pass, against a set test
                # that hashes every key in the row.
                if (len(row) != width or None in values) and not declared(row):
                    self._reject_unknown(row)

                try:
                    cursor.execute(sql, values)
                except sqlite3.IntegrityError as exc:
                    # SQLite is the gate; this only turns its answer into one
                    # a caller can act on. `CHECK constraint failed: key` does
                    # not say what was wrong with the value, or which value.
                    #
                    # Reached only on the way to raising, so it costs nothing
                    # in the ordinary case — and the `try` itself is free,
                    # CPython having zero-cost exceptions since 3.11.
                    self._explain(row, values, shape, exc)

                # lastrowid is the assigned offset, available inside the open
                # transaction and before the row is visible to anyone else.
                offset = int(cursor.lastrowid or 0)
                offsets.append(offset)

                if group.start_offset is None:
                    group.start_offset = offset

                group.bytes += row_bytes(row, columns)
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
        `target_seal_size` is the library's one promise about file size, and
        cutting on the batch boundary would make that promise depend on how the
        caller chose to batch — which §1 says carries no meaning of its own. A
        batch large enough crosses several times and comes through here each
        time.
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

    def _reject_unknown(self, row: Mapping[str, object]) -> None:
        """A row naming a column this log does not have (I17).

        Off the hot path: `_insert` calls this only once the subset test has
        already failed, so building the sorted difference costs nothing in the
        ordinary case.

        **Nothing below catches this.** The insert is built as
        `tuple(row.get(c) for c in columns)` — it enumerates the SCHEMA's
        columns, never the row's keys — so an unknown key is dropped before any
        SQL exists and neither SQLite nor pyarrow ever sees it. `append`
        returned an offset for a row it had silently truncated.

        The checks that DO exist fire at the wrong time. A value pyarrow cannot
        cast is stored, `append` succeeds, and then every scan and every seal
        raises on it for ever while appends keep working — measured. Refusing
        here is what keeps a rejectable row from wedging the log.
        """
        shape = self.shape()
        unknown = sorted(set(row) - shape.known)
        msg = (
            f"row names columns this log does not have: {unknown}. "
            f"Declared: {sorted(shape.columns)}"
        )
        raise ValueError(msg)

    def _reject_missing(
        self, row: Mapping[str, object], values: tuple[object, ...]
    ) -> None:
        """A row leaving a non-nullable column NULL (I17).

        Off the hot path, like `_reject_unknown`: reached only on the way to
        raising, so it can afford to name every offending column instead of
        the first one found.

        The sibling of the unknown-name case, and the same wedge from the
        other side. That one is a key the log does not have; this one is a key
        the log requires and did not get. Both end as a NULL nothing below
        catches — `add_files` null-fills an optional field missing from a
        file, and the scan cast is where it finally raises, long after the
        offset was handed out.

        Absent and explicitly-None are one refusal because they are one bug:
        `row.get` cannot tell them apart and neither can the scan that fails
        later. The message separates them because the fix differs — a missing
        key is usually a caller that forgot, an explicit None is usually a
        caller that meant it and needs the column declared nullable instead.
        """
        shape = self.shape()
        columns = shape.columns
        offending = sorted(columns[i] for i in shape.required if values[i] is None)
        absent = [c for c in offending if c not in row]
        supplied = [c for c in offending if c in row]
        detail = ""
        if absent:
            detail += f" Absent from the row: {absent}."

        if supplied:
            detail += f" Supplied as None: {supplied}."

        msg = (
            f"row leaves non-nullable columns NULL: {offending}.{detail} "
            "Declare the column nullable if None is a legal value for it."
        )
        raise ValueError(msg)

    def _explain(
        self,
        row: Mapping[str, object],
        values: tuple[object, ...],
        shape: Shape,
        exc: sqlite3.IntegrityError,
    ) -> None:
        """Turn a constraint failure into the refusal a caller can act on.

        The checks below used to run per row, ahead of the insert, and cost
        11% of the write path between them. They say exactly the same things;
        they just say them after SQLite has already decided, which is why they
        are now free. Each raises if it recognises the failure.

        Re-raises the original if none of them does, rather than inventing an
        explanation for a constraint this does not know about — a wrong
        diagnosis is worse than a terse one.
        """
        if any(values[i] is None for i in shape.required):
            self._reject_missing(row, values)

        self._check_types(row, values)

        for i, limit in shape.exact_ints:
            value = values[i]
            # `isinstance`, not `type(...) is`: an `IntEnum` member is an int
            # and reaches the same CHECK, and missing it here means the caller
            # gets a bare `CHECK constraint failed` instead of this message.
            # `bool` needs no exclusion — True and False are always in range.
            if isinstance(value, int) and not -limit <= value <= limit:
                name = shape.columns[i]
                # The bound is the range in which EVERY integer is exact, not
                # a claim about this one: 2**60 converts exactly and is still
                # refused, because a SQL CHECK cannot ask "is this particular
                # integer representable". So the message does not say the
                # conversion would be lossy — it says how to ask for it.
                msg = (
                    f"column {name!r} cannot hold the integer {value!r}: it is "
                    f"declared {shape.schema.field(name).type}, which holds every "
                    f"integer exactly only up to {limit}. Pass it as a float to "
                    "store it as one"
                )
                raise ValueError(msg)

        for i, lo, hi in shape.ranged:
            value = cast("float | None", values[i])
            if value is not None and not lo <= value <= hi and value not in _INFINITE:
                self._reject_range(row, i, value)

        raise exc

    def _check_types(
        self, row: Mapping[str, object], values: tuple[object, ...]
    ) -> None:
        """Decide a row the fast type gate could not pass (I17).

        Reached only when some value is not the exact type its column carries,
        which a correct row never is. So this can afford to ask the definitive
        question per column and to name every column that fails.

        **Nothing below catches these, and what does catches them too late.**
        SQLite has no column types, only affinities, so it stores whatever it
        is given. The declared schema is not consulted again until the value is
        read back — and by then `append` has returned an offset. Two outcomes,
        both measured: a value Arrow cannot parse (`"x"` into an int64) makes
        EVERY scan raise, including scans of rows written before it, while
        appends keep succeeding; and a value it can parse but not preserve
        (`1.5` into an int64, `12345` into a string) is silently rewritten, so
        what is read back is not what was appended and no error is raised
        anywhere.
        """
        shape = self.shape()
        columns, accepts = shape.columns, shape.accepts
        bad = [
            (columns[i], value)
            for i, value in enumerate(values)
            if value is not None and not accepts[i](value)
        ]
        if not bad:
            return

        detail = ", ".join(
            f"{name}={value!r} ({type(value).__name__}, declared "
            f"{shape.schema.field(name).type})"
            for name, value in bad
        )
        msg = (
            f"row has values of the wrong type: {detail}. SQLite would store "
            "them as given and the mismatch would not surface until a read"
        )
        raise ValueError(msg)

    def _reject_range(
        self, row: Mapping[str, object], index: int, value: object
    ) -> None:
        """A value of the right type whose MAGNITUDE the column cannot hold.

        Off the hot path, like the other refusals. The two cases it covers fail
        differently and neither says anything at append: an int32 given 2**40
        is stored by SQLite unchanged and then makes every scan raise, while a
        float32 given 1e300 reads back as `inf` with no error at all.

        An explicit infinity is allowed through — a float32 represents `inf`
        exactly, so passing one is a statement rather than an overflow. What is
        refused is a FINITE value that would silently become infinite.
        """
        shape = self.shape()
        name = shape.columns[index]
        msg = (
            f"column {name!r} cannot hold {value!r}: it is declared "
            f"{shape.schema.field(name).type}, whose range is "
            f"{shape.ranged[[i for i, *_ in shape.ranged].index(index)][1:]}"
        )
        raise ValueError(msg)

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
        return self.shape().schema

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
            # The hazard is DOWNWARD only, and this used to refuse any
            # non-empty buffer. Raising the sequence past the rows present is
            # safe — SQLite assigns `max(max(rowid), seq) + 1` either way — and
            # it is what a restore needs: reserving an offset range (§3a) has
            # to work on a buffer holding the recovered tail, which is exactly
            # a buffer with rows in it.
            #
            # Narrowed rather than bypassed. A second writer of this sequence
            # would be a second place to get the direction wrong, and the
            # direction is the whole of the danger.
            highest = self._con.execute(
                'SELECT max("litelink_offset") FROM buffer'
            ).fetchone()[0]
            if highest is not None and first - 1 < highest:
                msg = (
                    f"cannot seed offsets to {first} on a buffer holding rows up "
                    f"to {highest}: SQLite ignores a sequence lowered past them"
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

    def rows_between(self, start: int, end: int) -> pa.Table:
        """Buffered rows in `[start, end)`, as Arrow. The seal's input.

        **Bounded at BOTH ends, and the lower one is load-bearing.** A seal used
        to be able to take everything below its cut, because `finish_seal`
        deleted those rows immediately: the buffer's minimum was always the next
        group's start. Once the delete is deferred until the archive holds the
        range (§3a), that stops being true — and an unbounded read then writes
        every row from the archive frontier upward into the new file.

        Nothing catches that at seal time. The local `register` passes no `lo`,
        so `_refuse_straddle` returns early; what fails is later and elsewhere.
        The manifest's own ranges stop being non-overlapping (§4, §6), the local
        leg of a read is an unfiltered `iceberg_scan` so every overlapped row
        comes back twice, the next `sync` refuses the straddle for ever, and
        compaction's row-count verification fails.

        `start` costs nothing to supply: `pending_group` and `pending_seal` both
        already carry it, and both call sites already unpack it.
        """
        # On the seal's OWN connection. See `_sealer`: sharing `_reader` with a
        # concurrent scan means reading that scan's pinned snapshot and then
        # deleting rows this never saw.
        return self._rows("BETWEEN ? AND ?", (start, end - 1), self._sealer)

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
            # The tail is an Arrow table under the schema in force when it was
            # built, so a schema change has to discard it. Loud rather than
            # silent if it does not — `ArrowInvalid: Schema at index 1 was
            # different` out of the `concat_tables` below — but it would turn
            # every read in this process into that error.
            shape = self.shape()
            if self._tail_shape is not shape:
                self._tail = None
                self._tail_lo = self._tail_hi = self._tail_from = 0
                self._tail_shape = shape

            cached = self._reusable(floor)
            if cached is None:
                table = self._rows("> ?", (floor,))
                self._tail_from = floor
            else:
                fresh = self._rows("> ?", (self._tail_hi,))
                table = (
                    cached if fresh.num_rows == 0 else pa.concat_tables([cached, fresh])
                )
                if table.column(0).num_chunks > _MAX_TAIL_CHUNKS:
                    # One chunk per query otherwise, forever. Combining is a
                    # copy, so it is amortised rather than paid every time.
                    table = table.combine_chunks()

                # A hit implies `_tail_from <= floor`, so this only ever
                # raises — the guard above is what makes that true, and a
                # `max()` here would be dead.
                #
                # Conservative in one direction and never wrong in the other:
                # when `floor` is below where the cache STARTS the slice prunes
                # nothing, so the cache is still complete from where it was,
                # and raising loses a later hit rather than serving short. That
                # costs nothing in practice because the boundary is the local
                # table's extent, which only rises.
                self._tail_from = floor

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
        `_tail_lo + 1`, so dropping through the clamped `base` drops exactly
        `base - _tail_lo` rows — and then checked, because that contiguity is
        a property of AUTOINCREMENT and prefix-only deletion rather than
        something enforced here. A failed check costs a rebuild, which is what
        the code did unconditionally before.

        Both directions are checked. A wrong non-empty slice starts at the
        wrong offset; a wrong EMPTY slice is the dangerous one, because it
        looks exactly like "nothing buffered above the boundary" and would be
        returned as an answer. `_tail_hi > floor` says the last cached row
        qualifies, so an empty result contradicts the cache itself.
        """
        if self._tail is None or not (self._tail_from <= floor <= self._tail_hi):
            return None

        # Clamped, because a floor BELOW where the cache starts drops nothing
        # rather than a negative number of rows. That is the ordinary case on a
        # log whose offsets start high — every buffered row is already above
        # the boundary — and the unclamped subtraction is why the guard could
        # not simply be widened.
        base = max(floor, self._tail_lo)
        kept = self._tail.slice(base - self._tail_lo)
        if kept.num_rows:
            if int(kept.column(OFFSET)[0].as_py()) != base + 1:
                return None
        elif self._tail_hi > floor:
            return None

        return kept

    def _rows(
        self,
        predicate: str,
        params: tuple[object, ...],
        con: sqlite3.Connection | None = None,
    ) -> pa.Table:
        """Buffered rows matching `offset <predicate>`, in offset order.

        The predicate is on the INTEGER PRIMARY KEY so SQLite answers it with
        `SEARCH buffer USING INTEGER PRIMARY KEY (rowid>?)` rather than reading
        rows the caller will discard. That is what keeps a deferred cleanup
        costing disk rather than query latency (§7).
        """
        shape = self.shape()
        names = ", ".join(f'"{c}"' for c in (OFFSET, *shape.columns))
        cursor = (con or self._reader).execute(
            f'SELECT {names} FROM buffer WHERE "litelink_offset" {predicate}'
            ' ORDER BY "litelink_offset"',
            params,
        )
        columns = list(zip(*cursor.fetchall(), strict=True)) or [
            () for _ in range(len(shape.columns) + 1)
        ]
        schema = pa.schema(
            [pa.field(OFFSET, pa.int64(), nullable=False), *shape.schema]
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

    def claim(
        self,
        kind: str,
        lo: int,
        hi: int,
        owner: str,
        rel_path: str | None = None,
        ttl_ms: int = DEFAULT_TTL_MS,
    ) -> Claim:
        """A claim on the offset range `[lo, hi]`, backed by this database.

        Handed the connection AND the lock that guards it — see `Claim`.
        """
        return Claim(self._con, self._lock, kind, lo, hi, owner, rel_path, ttl_ms)

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
        means — the resulting file is under `target_seal_size` by definition.
        It is the one way this library writes an undersized file, and it takes
        a deliberate call to do it.

        An empty group is never closed either way; there would be no file.
        That is asked of the BUFFER, not of `start_offset`, and it used to be
        asked of neither — a group whose rows had gone set `end_offset` to the
        max of nothing, reported a rowcount of 1 anyway, and got a second open
        group inserted behind it. Two open rows is the permanent seal-queue
        wedge `_seed_group` documents: `close_open_group` closes both at the
        same end, and `finish_seal` then hits the `rel_path` UNIQUE after the
        Iceberg commit has already landed.

        Unreachable until `litelink.restore` began releasing archived rows out from
        under a knowingly-stale group (§3a), which is one transaction away from
        the reseed that fixes it.

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
                " AND rel_path IS NULL AND start_offset IS NOT NULL"
                " AND EXISTS (SELECT 1 FROM buffer"
                '             WHERE "litelink_offset" >= extent.start_offset)',
                (),
            ).fetchone():
                return False

        with self._transaction():
            cursor = self._con.execute(
                "UPDATE extent SET end_offset ="
                ' (SELECT max("litelink_offset") + 1 FROM buffer)'
                " WHERE end_offset IS NULL AND rel_path IS NULL"
                " AND start_offset IS NOT NULL"
                " AND EXISTS (SELECT 1 FROM buffer"
                '             WHERE "litelink_offset" >= extent.start_offset)',
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

    def finish_seal(self, end: int, rel_path: str, *, discard: bool = True) -> bool:
        """Retire the group, clear the intent, and drop the sealed rows.

        Garbage collection, not correctness: the read boundary in §7 already
        excludes these rows the moment the Iceberg commit lands, so the window
        between that commit and this call is safe in both directions.

        **`discard=False` keeps them, and that is I4 one tier up.** A seal moves
        rows from SQLite into a Parquet file that no sidecar replicates, so
        with WAL shipping on, deleting here removes the only off-box copy of a
        range the archive does not have yet — and the machine dying in that
        window loses them, silently, from the middle of the offset space
        (§3a). The caller passes False when replication is on and something is
        owed to an archive; `release_archived` is what removes them afterwards.

        Only the CALLER can decide that, which is why it is a parameter rather
        than a check here: this object knows nothing about archives.

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

            if discard:
                self._con.execute(
                    'DELETE FROM buffer WHERE "litelink_offset" < ?', (end,)
                )

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
        with self._transaction():
            # The upsert is untouched. What is new is the `forget_intent`
            # beside it: recording the copy and retiring the intent are one
            # fact, and a crash between two statements would leave the log
            # believing both. The upsert also has to be able to write a row
            # from nothing, because an owner that took over a lapsed claim may
            # have dropped this push's intents while its register was in flight.
            self._con.execute(
                "INSERT INTO extent"
                " (start_offset, end_offset, bytes, rel_path, named_at)"
                " VALUES (?, ?, ?, ?, unixepoch())"
                " ON CONFLICT(rel_path) DO UPDATE SET bytes = excluded.bytes",
                (start, end, held, rel_path),
            )
            self._con.execute(
                "DELETE FROM extent_intent WHERE rel_path = ?", (rel_path,)
            )

    def intend_file(self, rel_path: str, start: int, end: int, held: int) -> None:
        """Record a copy this log is ABOUT to write, before it writes it.

        The register that follows can land while the row recording it does not,
        and compaction decides what it may merge from those rows — so without
        this, a compaction-target change before the next sync regroups the
        pushed-but-unrecorded files and commits a local file straddling the
        archive's extent. Nothing re-cuts a local straddler, so every later
        push is refused and the log stops advancing.

        An UPSERT, and the difference is not cosmetic: a holder that stalled
        past its TTL and resumed can intend a path the owner that took over is
        also intending. A bare insert raises on the primary key, and the
        maintainer catches neither that nor anything like it — so the takeover
        race would kill the LAWFUL holder's pass rather than the stale one's.
        """
        with self._lock:
            self._con.execute(
                "INSERT INTO extent_intent"
                " (rel_path, start_offset, end_offset, bytes) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(rel_path) DO UPDATE SET"
                " start_offset = excluded.start_offset,"
                " end_offset = excluded.end_offset,"
                " bytes = excluded.bytes",
                (rel_path, start, end, held),
            )

    def intents(self, prefix: str) -> list[tuple[str, int, int, int]]:
        """Intended copies under `prefix`: `(rel_path, start, end, bytes)`.

        Unbounded by offset, deliberately. Reconciliation drops an intent the
        archive's manifest does not name, and one below the local window has to
        be reachable to be dropped — bounding this read would leave those rows
        beyond judgement for ever.
        """
        boundary = prefix.rstrip("/") + "/"
        with self._lock:
            rows = self._con.execute(
                "SELECT rel_path, start_offset, end_offset, bytes FROM extent_intent"
            ).fetchall()

        return [
            (str(r[0]), int(r[1]), int(r[2]), int(r[3]))
            for r in rows
            if str(r[0]).startswith(boundary)
        ]

    def archive_records(
        self, prefix: str, floor: int
    ) -> list[tuple[str, int, int, int]]:
        """Landed copies under `prefix`, keyed by PATH: `(rel_path, lo, hi, bytes)`.

        Reconciliation matches by path, and `archived_ranges` answers in bare
        offsets — so it cannot serve. Bounded by `floor` like the manifest walk
        beside it, or it grows with the archive and runs on every sync.
        """
        boundary = prefix.rstrip("/") + "/"
        with self._lock:
            rows = self._con.execute(
                "SELECT rel_path, start_offset, end_offset, bytes FROM extent"
                " WHERE rel_path IS NOT NULL AND end_offset > ?",
                (floor,),
            ).fetchall()

        return [
            (str(r[0]), int(r[1]), int(r[2]), int(r[3]))
            for r in rows
            if str(r[0]).startswith(boundary)
        ]

    def forget_intent(self, rel_path: str) -> None:
        """Drop an intent, whether it became a copy or never will."""
        with self._lock:
            self._con.execute(
                "DELETE FROM extent_intent WHERE rel_path = ?", (rel_path,)
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

    def archived_ranges(
        self, prefix: str | None, floor: int, *, include_intents: bool
    ) -> list[tuple[int, int]]:
        """Offset ranges an archive holds or is about to, under `prefix`.

        `include_intents` is keyword-only and has no default, so every caller
        states which question it is asking. Compaction asks whether ANY archive
        might hold a range, and is safe overstating it; eviction asks whether
        one DOES, and is safe only understating. Getting that backwards at one
        call site would be silent, which is the whole reason this parameter is
        awkward to pass.

        I4 asked of segments rather than of a watermark (§4a). `sync` records
        where each pushed file's copy went, so the archive's contents are
        already durable here per file — a watermark summarising them is a
        second copy of the same fact, and the only boundary in the log that can
        move backwards.

        Bounded by `floor` rather than by the prefix alone: the archive grows
        without limit and this only ever asks about ranges the local table
        still holds, which compaction bounds. The prefix match is applied in
        Python because SQLite's `LIKE` would not use the index that `floor`
        selects on.
        """
        # `None` means ANY archive, not the configured one. Two questions ask
        # this and they are not the same question. I4 asks whether THIS archive
        # holds a file, because it authorises a deletion. Compaction asks
        # whether ANY archive does, because it decides whether merging could
        # create a range no archive's cuts line up with — and detaching does
        # not make those copies stop existing.
        boundary = None if prefix is None else prefix.rstrip("/") + "/"
        # ONE statement, so one snapshot. Read as two, the tables are two
        # separate WAL reads and a `record_file` committing between them moves
        # a range out of the first and into the second AFTER the second was
        # taken — so it appears in neither, and compaction's read, which is
        # safe only when it OVERSTATES coverage, momentarily understates it.
        # That is the straddle this whole record exists to prevent, reopened by
        # the shape of the query rather than by the design.
        sql = (
            "SELECT start_offset, end_offset, rel_path FROM extent"
            " WHERE end_offset > ? AND rel_path IS NOT NULL"
        )
        args: tuple[int, ...] = (floor,)
        if include_intents:
            sql += (
                " UNION ALL"
                " SELECT start_offset, end_offset, rel_path FROM extent_intent"
                " WHERE end_offset > ?"
            )
            args = (floor, floor)

        with self._lock:
            rows = self._con.execute(sql, args).fetchall()

        def wanted(path: str) -> bool:
            return path.startswith(boundary) if boundary is not None else "://" in path

        return sorted((lo, hi) for lo, hi, path in rows if wanted(path))

    def set_meta_moved(self, key: str, value: str, reset: Mapping[str, str]) -> bool:
        """Record `value`, applying `reset` only if it is a MOVE.

        The question "is this a change?" is answered against the durable value,
        inside the transaction that acts on the answer. Asked of a process's
        memory instead, both answers are wrong in a two-process deployment,
        because nothing refreshes that memory except a sync:

        * memory stale, argument current — re-asserting the archive the log
          already has reads as a move and zeroes the watermarks of a bucket
          that genuinely holds the data;
        * memory stale, argument stale — re-pointing BACK to an archive reads
          as a restatement, and the log keeps the other archive's watermark
          over a bucket whose extent is lower. Eviction believes it (I4).

        Returns whether it was a move.
        """
        with self._transaction():
            row = self._con.execute("SELECT v FROM meta WHERE k = ?", (key,)).fetchone()
            current = (row[0] if row is not None else None) or None
            moved = current != (value or None)
            pairs = {key: value, **(dict(reset) if moved else {})}
            self._con.executemany(
                "INSERT INTO meta (k, v) VALUES (?, ?) "
                "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                list(pairs.items()),
            )
            return moved

    def set_meta_if(
        self, key: str, expected: str | None, pairs: Mapping[str, str]
    ) -> bool:
        """Write `pairs`, but only while `meta[key]` still reads `expected`.

        Compare-and-set, in ONE write transaction, for the guards that decide
        whether a fact still belongs to the log it was computed for. Read and
        write as separate statements, the check is only ever a statement about
        the past: `sync` re-reads which archive it is pushing to before
        recording a watermark, and a `set_archive` landing between the read and
        the write leaves the log pointed at the NEW archive holding the OLD
        one's extent — which eviction believes (I4) and nothing ever lowers.

        The lease does not close that window, because the window opens when the
        lease has already lapsed: a push that spent longer than the TTL in S3
        is exactly the case the guard exists for, and the re-point that races
        it took the lease lawfully. SPEC §4a states the rule — the read of a
        conflicting claim and the write that depends on it happen in one SQLite
        transaction, or they are not a guard.

        Returns whether the write happened, so callers can decline rather than
        record something they no longer have the right to record.
        """
        with self._transaction():
            row = self._con.execute("SELECT v FROM meta WHERE k = ?", (key,)).fetchone()
            current = (row[0] if row is not None else None) or None
            if current != (expected or None):
                return False

            self._con.executemany(
                "INSERT INTO meta (k, v) VALUES (?, ?) "
                "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                list(pairs.items()),
            )
            return True

    def shape(self) -> Shape:
        """The declared schema and its derivations, read from the log.

        The same shape as `config()`, for the same reason and against the same
        failure. There is exactly one copy of the schema and it is the `meta`
        row; everything that decides from it reads here, so nothing can hold a
        stale one.

        That matters more than it does for the policy, because the stale-copy
        failure here is SILENT. A sealer never calls `append`, so a design that
        revalidated per append would leave it building its projection from the
        columns it was constructed with — dropping a column a writer had
        already been given an offset for, null-filled by `add_files` and then
        deleted from the buffer by `finish_seal`. §4a's lesson: a design whose
        correctness needs N refresh calls is always one short, because nothing
        tells you what N is.

        The DECODE is cached, keyed on the raw value — reading the row costs
        about 1.8 us and `read_schema` costs far more. Keying on the durable
        value is what stops the cache being the stale copy it exists to
        prevent: when the row changes, the key changes.
        """
        raw = self.get_meta(SCHEMA_KEY)
        if raw is None:
            return self._fallback

        cached = self._shape_cache
        if cached is not None and cached[0] == raw:
            return cached[1]

        shape = Shape.of(pa.ipc.read_schema(pa.py_buffer(bytes.fromhex(raw))))
        self._shape_cache = (raw, shape)

        return shape

    def config(self) -> LogConfig:
        """The policy in force, read from the log rather than remembered.

        There is exactly one copy of this, and it is the `meta` row. Everything
        that decides from the policy reads it here, so nothing can hold a stale
        one — which is the failure this replaces: the policy used to live in
        `WriteHandle`, in `Maintenance` and, derived, in this object's seal target, all
        kept in step by `set_config` writing each. A refresh call had to sit
        wherever a decision was made, and a design needing N of those is always
        one short somewhere, because nothing says where N is.

        The PARSE is cached, keyed on the raw value. Reading the row costs
        1.8 us and decoding it costs far more, so the cache is what makes this
        affordable — and keying it on the durable value is what stops it being
        another stale copy: when the row changes, the key changes.
        """
        raw = self.get_meta(CONFIG_KEY)
        if raw is None:
            return LogConfig()

        cached = self._config_cache
        if cached is not None and cached[0] == raw:
            return cached[1]

        try:
            parsed = LogConfig.from_json(raw)
        except (ValueError, TypeError):
            # A value this build cannot read is not a reason to stop
            # maintaining the log; the last good one governs.
            return LogConfig() if cached is None else cached[1]

        self._config_cache = (raw, parsed)

        return parsed

    def sort_by(self) -> tuple[str, ...]:
        """The declared clustering, read from the log rather than remembered.

        The rule `config` follows, for the reason `config` follows it (§4a).
        This used to live in four places — `meta`, `WriteHandle`, `Maintenance` and
        `Archive` — kept in step by `set_sort_by` writing each. That is a
        fan-out, and a fan-out is only correct in the process that ran it: a
        maintainer already open elsewhere went on sorting by the key IT opened
        with while both tables declared the new one, and compaction, the pass
        that would have re-clustered them, read the same stale field.

        The PARSE is cached on the raw value, as `config`'s is. Keying it on
        the durable value is what keeps the cache from becoming the fifth home:
        when the row changes the key changes.

        A MISSING row is corruption, not "no order". `new` always writes it,
        and defaulting to unsorted here would silently de-cluster every file
        the next seal or compaction wrote while the tables went on declaring a
        key — which is the state `open` already refuses to start on.
        """
        raw = self.get_meta(SORT_KEY)
        if raw is None:
            msg = "the log has no stored sort order; it is corrupt"
            raise ValueError(msg)

        cached = self._sort_cache
        if cached is not None and cached[0] == raw:
            return cached[1]

        parsed = tuple(json.loads(raw))
        self._sort_cache = (raw, parsed)

        return parsed

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

    def restamp_deletions(self, rel_paths: Iterable[str], superseded_at: int) -> None:
        """Re-date queued files to when they ACTUALLY left the table.

        The queue is written before the commit that supersedes them, because a
        crash in between would otherwise lose the only record of those paths.
        But the grace period is about readers still holding them (I6), and a
        reader cannot be holding a file the commit has not yet superseded — so
        the clock has to start at the commit, not at the queueing.

        Stamped at the queueing, a rewrite slower than `snapshot_retention`
        burns the whole grace before it commits: the moment the originals stop
        being referenced they are already due, and drain takes them out from
        under any scan resolved a moment earlier. Measured at a 5 s retention:
        a reader 0.4 s old lost all fourteen files its snapshot named, and its
        scan failed mid-read with a 404. An attempt that ABORTS and is retried
        later is worse — it commits against a stamp burned days ago.

        `INSERT OR IGNORE` deliberately keeps the first stamp on re-enqueue, so
        this is an explicit update rather than a second insert.
        """
        with self._transaction():
            self._con.executemany(
                "UPDATE pending_delete SET superseded_at = ? WHERE rel_path = ?",
                [(superseded_at, p) for p in rel_paths],
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

    def strip_local_state(self, reserve: int) -> tuple[int, int]:
        """Drop everything that describes the machine this came FROM (§3a).

        A restored `buffer.db` is a faithful copy of a database that belonged
        to a box which no longer exists, and some of what it says is about that
        box rather than about the log. Returns `(released, next_offset)`.

        - **`extent` rows naming LOCAL files** go. They name Parquet on the
          machine that died, so `file_bytes` — and through it `memory`, which
          sizes merges — would describe files nothing can open. Rows naming
          ARCHIVE copies stay: that is the coverage I4 acts on, and it is still
          true.

          Narrower than it first looks, and worth saying so: compaction decides
          what to merge from the Iceberg table's `data_files`, not from these
          rows, and that table is rebuilt empty. So a stale row cannot make a
          merge reach for a missing file. What it can do is make this database
          describe a filesystem that does not exist, which is the thing every
          other path here is arranged to prevent.
        - **The open group** goes with them, and `_seed_group` reruns. Without
          this the recovered band is orphaned — its own rows have just been
          dropped as local, and `_seed_group` returns early whenever an open
          group exists, which a restored buffer always has because `_cut`
          inserts one after every cut. The surviving row starts at the dead
          box's UNSEALED floor, above the band, so the band would fall into no
          leg of a read and be lost at the first seal after recovery.
        - **`pending_delete` rows naming local files** go; REMOTE ones stay,
          and that half is required. `rewrite_archive` is the only thing that
          queues a remote entry, and this design refuses directory listing, so
          dropping them leaks archive objects nothing can ever find again.
        - **`claim` rows** go. They carry the dead box's owners and a future
          expiry, so keeping them makes this one wait out a TTL for processes
          that do not exist.
        - **`sealing` GOES**, and this was wrong in an earlier draft. The
          reasoning was that `_recover_seal` finds the rebuilt table empty and
          rewrites the interrupted file, recovering data. It does — and then
          duplicates it. The closed-but-unsealed `extent` row that seal
          belonged to is deleted above, so `finish_seal`'s naming UPDATE
          (keyed `end_offset = ? AND rel_path IS NULL`) matches nothing and
          returns True anyway, while the fresh open group still spans the
          range. With `discard=False` the rows are still buffered, so the next
          cut writes them a second time. Measured: 490 rows read where 440 are
          distinct, from two overlapping local files, with no error anywhere.

          Recovery is redundant here rather than protective. Every row that
          seal was writing is still in the buffer, and the open group
          `_seed_group` builds covers them — so dropping the claim re-seals
          them exactly once.

        - **`compacting` stays.** It only queues deletions, and its outputs
          are archive objects this machine never wrote.

        Finally the offset sequence is raised by `reserve`. See `litelink.restore`.
        """
        with self._transaction():
            self._con.execute(
                "DELETE FROM extent WHERE rel_path IS NULL OR rel_path NOT LIKE '%://%'"
            )
            self._con.execute(
                "DELETE FROM pending_delete WHERE rel_path NOT LIKE '%://%'"
            )
            self._con.execute("DELETE FROM claim")
            self._con.execute("DELETE FROM sealing")
            released = self._con.execute("SELECT count(*) FROM buffer").fetchone()[0]
            highest = self._con.execute(
                'SELECT max("litelink_offset") FROM buffer'
            ).fetchone()[0]
            seq = self._con.execute(
                "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
            ).fetchone()
            ceiling = max(int(seq[0]) if seq else 0, int(highest or 0)) + reserve
            self._con.execute(
                "UPDATE sqlite_sequence SET seq = ? WHERE name = 'buffer'",
                (ceiling,),
            )
            if not self._con.execute(
                "SELECT 1 FROM sqlite_sequence WHERE name = 'buffer'"
            ).fetchone():
                self._con.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES ('buffer', ?)",
                    (ceiling,),
                )

        self._seed_group()

        return int(released), ceiling + 1

    def reseed_group(self) -> None:
        """Rebuild the open group from what is buffered NOW.

        `_seed_group` returns early whenever an open group exists, which is
        what keeps it idempotent at open — so re-running it after rows have
        left the buffer changes nothing. This drops the stale row first.

        For the restore path, where the group was seeded from a replica's view
        of the archive and the archive has since moved on: see `litelink.restore`.
        """
        with self._transaction():
            self._con.execute(
                "DELETE FROM extent WHERE end_offset IS NULL AND rel_path IS NULL"
            )

        self._seed_group()

    def release_archived(self, boundary: int) -> int:
        """Drop buffer rows the archive now holds. Returns how many.

        The other half of `finish_seal(discard=False)`: those rows stayed
        because the archive did not have them yet, and this is what notices
        that it does.

        Bounded by the ARCHIVE's frontier, never by the seal's. That is the
        whole point — the seal moves rows to a file nothing replicates, and
        only the archive makes them safe off-box.

        Idempotent, and it has to be. Driven from the archive's own extent at
        the start of a pass rather than from the tail of a push, because a push
        has three early returns before its watermark: a crash between the
        register and this call would otherwise leave the rows held, and the
        next pass — finding nothing left to push — would return before reaching
        it. On a log that has gone quiet, for ever.
        """
        with self._lock, self._con:
            cursor = self._con.execute(
                'DELETE FROM buffer WHERE "litelink_offset" <= ?', (boundary,)
            )

        return cursor.rowcount

    def queued_deletions(self) -> list[str]:
        with self._lock:
            return [
                str(row[0])
                for row in self._con.execute(
                    "SELECT rel_path FROM pending_delete"
                ).fetchall()
            ]

    # -- lifecycle --------------------------------------------------------

    def _drop_tail(self) -> None:
        with self._tail_lock:
            self._tail = None
            self._tail_lo = self._tail_hi = self._tail_from = 0

    def close(self) -> None:
        # The cache goes too. It is bounded by the unsealed tail and falls to
        # nothing once that is sealed, but a closed buffer holds no tail at
        # all, and a caller keeping the object alive should not keep the rows.
        self._drop_tail()
        self._con.close()
        # Identity-checked, because a readonly buffer passes one connection
        # three times.
        for extra in (self._reader, self._sealer):
            if extra is not self._con:
                extra.close()
