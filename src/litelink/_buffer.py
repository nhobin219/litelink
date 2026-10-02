"""The SQLite write buffer (SPEC §2, §3).

One database per stream. Durable on commit — `synchronous=FULL` with WAL — and
that is the whole durability story: there is no in-memory staging layer whose
loss a SIGKILL could expose.
"""

from __future__ import annotations

import contextlib
import json
import math
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from litelink._config import LogConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping

from litelink._claim import DEFAULT_TTL_MS, Claim
from litelink._types import (
    NON_FINITE,
    Nested,
    NestedValueError,
    column_type,
    slot_bits,
)

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
# exists. It used to live in `_handle.py`, which meant the module that OWNS the
# row could not read it without importing the module that names it.
SCHEMA_KEY = "arrow_schema"

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

# Bumped at every change to a stored tier (#90), so a reader's decoded copy is
# never kept past a write. See `_tiers`.
TIER_GENERATION = "tier_generation"


# What the trigger raises with, so the append path can tell its refusal from a
# CHECK constraint's and explain it.
RETIRED_REFUSAL = "litelink: this log is retired"


def _metadata_sequence(version: str) -> int:
    """The sequence number at the front of an Iceberg metadata file's name.

    -1 for a name without one, which any real version supersedes.
    """
    prefix = version.rsplit("/", 1)[-1].split("-", 1)[0]

    return int(prefix) if prefix.isdigit() else -1


# The key the published table's location is recorded under —
# `_published.PUBLISHED_KEY`, spelled here because that module imports this
# one.
PUBLISHED_META_KEY = "published"

# Names a log written before #98 stored, under the name each now has. Every
# read falls back to the old name when the new one is absent, and every write
# stores the new one and deletes the old in the same transaction — so an old
# log opens unchanged, and each row takes the new name the first time it is
# written. Tier rows have to move rather than merely be read either way:
# eviction widens the published row by name, and an old-named row left beside
# a new one would be a stale, narrower range a reader could fall back to.
LEGACY_META = {"published": "archive", "published_through": "archive_through"}
LEGACY_TIERS = {"staging": "local", "published": "archive"}
_CURRENT_TIER = {old: new for new, old in LEGACY_TIERS.items()}


def _meta_value(con: sqlite3.Connection, key: str) -> str | None:
    """`meta[key]`, or the value under its pre-#98 name."""
    row = con.execute("SELECT v FROM meta WHERE k = ?", (key,)).fetchone()
    legacy = LEGACY_META.get(key)
    if row is None and legacy is not None:
        row = con.execute("SELECT v FROM meta WHERE k = ?", (legacy,)).fetchone()

    return None if row is None else str(row[0])


def _write_meta(con: sqlite3.Connection, pairs: Mapping[str, str]) -> None:
    """Upsert `pairs` under their current names, dropping any old-named row."""
    con.executemany(
        "INSERT INTO meta (k, v) VALUES (?, ?) "
        "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        list(pairs.items()),
    )
    con.executemany(
        "DELETE FROM meta WHERE k = ?",
        [(LEGACY_META[k],) for k in pairs if k in LEGACY_META],
    )


def _retirement(end: int, statistics: str, published: str | None) -> dict[str, object]:
    """What a closed buffer row says about the log, for `RetiredError`."""
    count = json.loads(statistics).get("record_count")
    return {
        "state": "retired" if count == 0 else "retiring",
        "through": end - 1,
        "published": published,
    }


class RetiredError(RuntimeError):
    """The log was retired (`WriteHandle.retire`), or is being retired.

    Raised by anything that would add rows to it or bring it back as a writer.
    Its rows are all in the published table, which reads still reach.
    """

    @classmethod
    def of(cls, marker: Mapping[str, object], name: str) -> RetiredError:
        if marker.get("state") == "retiring":
            return cls(
                f"log {name!r} is being retired, so it takes no more rows. Call "
                "retire() on a writer to finish it; a crash left it part-way"
            )

        through = marker.get("through")
        after = f"after offset {through}" if through is not None else "holding no rows"
        nxt = "" if through is None else f" at start_offset={int(str(through)) + 1}"
        return cls(
            f"log {name!r} was retired {after}; its rows "
            f"are in the published table at {marker.get('published', '?')}. Read it with "
            f"open(..., read_only=True) or any Iceberg engine, and write to a new "
            f"log{nxt}"
        )


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
        #
        # Finite only (#87): `9e999` is how SQL spells infinity, so `< 9e999`
        # refuses ±inf here, at the insert. NaN never reaches a CHECK — SQLite
        # stores it as NULL — and `_row_bytes` and `_explain` refuse it instead.
        exact = kind.exact_int
        limit = "9e999" if kind.bounds is None else f"{kind.bounds[1]!r}"
        comparison = "<" if kind.bounds is None else "<="
        real = f"typeof({q}) = 'real' AND abs({q}) {comparison} {limit}"

        test = (
            real
            if exact is None
            else (
                f"(typeof({q}) = 'integer' AND {q} BETWEEN {-exact:d} AND {exact:d})"
                f" OR ({real})"
            )
        )
    elif kind.sqlite == "BLOB":
        # `length` of a blob is its byte count, which is what makes the width
        # of a `fixed_size_binary` enforceable here: a 15-byte trace id is
        # refused at the insert rather than failing the seal's Parquet write.
        test = f"typeof({q}) = 'blob'"
        if kind.width is not None:
            test += f" AND length({q}) = {kind.width:d}"
    else:
        # A nested column is TEXT too — its JSON, which `Nested.encode` has
        # already checked against the declared type, since no CHECK can see
        # inside it.
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


def _buffer_ddl(shape: Shape) -> str:
    """The `buffer` table: where I17 is enforced, for a log and for `RowProbe`.

    One function because it is one set of rules. `validate_row` promises the
    answer `append` would give, and that holds by construction only while
    both tables come from here.
    """
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
    return f"""
        CREATE TABLE IF NOT EXISTS buffer (
          "litelink_offset" INTEGER PRIMARY KEY AUTOINCREMENT,
          {columns}
        ) STRICT
    """


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
    nested: tuple[tuple[int, str, Nested], ...]
    measure: Callable[[tuple[object, ...]], int]

    @property
    def table(self) -> pa.Schema:
        """The caller's columns with `offset` in front — the TABLE's schema.

        Here rather than in `_handle.py` so the published table can ask the
        buffer for it without importing the module that owns the log. It is the
        shape `create_table` is handed, and a published table born from a stale
        copy of it is the one holder that cannot be repaired afterwards: nothing
        in `src/` ever re-declares an existing table.
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
            # Like `ranged`: empty for a schema with no nested column, so the
            # write path pays one truthiness test per row for the feature.
            nested=tuple(
                (i, columns[i], nested)
                for i in range(len(columns))
                if (nested := column_type(schema.field(i).type).nested) is not None
            ),
            measure=_measurer(schema),
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
        # What `shape()` falls back to when `meta` has no schema row yet. Two
        # callers need that and both would otherwise fail at construction:
        # `_create` runs inside `Buffer.open` BEFORE `litelink.new` writes the
        # row, and the scratch buffer a published rewrite cuts through is handed
        # a schema directly and only ever has `CONFIG_KEY` written into it.
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
        # `rows_from`, holds `_tail_lock` across the fetch; the seal reads on
        # `_sealer` for the same reason. See `_sealer`.
        self._lock = threading.RLock()
        # The read cache — see `rows_from`. Its own lock rather than the one
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
        # The offsets the cached table holds, `[_tail_start, _tail_end)`, taken
        # from its rows. `_tail_end` is where the next fetch starts.
        self._tail_start = 0
        self._tail_end = 0
        # The lowest `start` this cache is COMPLETE for — the `start` it was
        # fetched from, not the offset its first row happens to have.
        #
        # On a log whose offsets start high, `_tail_start` is far above any
        # `start` a reader asks for before the first seal: `Reader.query`
        # passes None while the staging table has no span, so gating on
        # `_tail_start <= start` missed on EVERY read and re-converted the
        # whole buffer per query. Measured at the default 8 MiB first-seal
        # window: 4.2 ms/read against 42.
        #
        # Two distinct facts, and conflating them is what cost that. Where the
        # cache STARTS bounds the slice arithmetic; what it is complete FROM
        # decides whether it can answer at all.
        self._tail_complete_from = 0

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

        The I/O half, kept out of `__init__` for the same reason `litelink.open`
        is kept out of `WriteHandle.__init__`: a constructor that opens files
        cannot be handed a substitute, and a test that wants one should not have
        to monkeypatch its way in.

        A readonly buffer opens the same file through SQLite's `mode=ro` URI so
        the handle cannot write even by mistake, and creates nothing. WAL allows
        any number of these alongside the single writer (§1).

        `durable=False` is for a buffer whose contents are derived from
        something that still exists — the scratch buffer a published rewrite
        re-cuts through, whose every row came from the published table and is
        still there until the rewrite's final commit. A crash costs a re-run
        rather than data, so the fsync per commit is paying for a guarantee
        nothing depends on. Never for a log's own buffer: there, the fsync IS
        the product (§3).
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

    def _rename_range_columns(self, table: str) -> None:
        """`lo`/`hi` → `start_offset`/`end_offset` on `table`, once."""
        columns = {
            str(row[1]) for row in self._con.execute(f"PRAGMA table_info({table})")
        }
        if "lo" in columns:
            self._con.execute(f"ALTER TABLE {table} RENAME COLUMN lo TO start_offset")

        if "hi" in columns:
            self._con.execute(f"ALTER TABLE {table} RENAME COLUMN hi TO end_offset")

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
        self._con.execute(_buffer_ddl(self._fallback))
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
        # I4 per segment (§4a) reads the published copies covering what the
        # staging table still holds. Without this the lookup is a scan of one
        # row per file ever published, which grows without limit — the staging
        # file count does not.
        self._con.execute("""
            CREATE INDEX IF NOT EXISTS extent_published ON extent (start_offset)
            WHERE rel_path IS NOT NULL
        """)
        # Its pre-#98 name, which an old log carries: one index, not two.
        self._con.execute("DROP INDEX IF EXISTS extent_archived")
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
        # A stored tier's offset range and its column statistics (#90), in two
        # tables rather than one: `litelink_offset` is the log's own sequence,
        # dense and monotonic across every tier, so a tier's `[start, end)` is
        # a fact about where it sits in the log rather than a statistic — and
        # it prunes on its own, as streamcast prunes whole logs by offset
        # (streamcast#32). Three tiers can have a row: the published table's,
        # which is what lets a read skip it (`_tiers`); the staging table's, a
        # cache of one version's rollup; and the buffer's, written only when
        # `retire()` closes it (see `close_buffer`).
        self._con.execute("""
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
        self._con.execute("""
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
        self._con.execute(f"""
            CREATE TRIGGER IF NOT EXISTS refuse_retired BEFORE INSERT ON buffer
            WHEN EXISTS (SELECT 1 FROM tier_offsets WHERE tier = 'buffer')
            BEGIN SELECT RAISE(ABORT, '{RETIRED_REFUSAL}'); END
        """)
        # Who owns which operation, across processes (§13.6). A Python lock
        # cannot say anything about a process that is no longer running, and
        # recovery has to know whether an interrupted operation was ours.
        self._con.execute("""
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
        self._rename_range_columns("claim")
        self._rename_range_columns("compacting")
        # Every acquisition asks the same question — is a live claim covering
        # this range — and asks it inside a write transaction, so it is the one
        # query that must not degrade into a scan as claims accumulate.
        self._con.execute("""
            CREATE INDEX IF NOT EXISTS claim_live
            ON claim (expires_at, start_offset, end_offset)
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
    # Measured in the Arrow table's bytes, never below them and within 1.11x
    # across #84's grid of row shapes — see `_measurer`. It is a policy
    # trigger and compaction's sense of how full a file is, so an undercount
    # is the harmful direction: it seals files past the target.

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
        """The open group's bytes, recounted from the rows it holds.

        Through the appender's own `measure`, not a SQL sum, so a reopened log
        cannot disagree with the one that wrote it. Runs once per open and only
        when no open group exists, so reading the rows costs nothing that
        matters. A nested value is stored as JSON and measured as its decoded
        value, the same shape `append` measured.
        """
        shape = self.shape()
        names = ", ".join(f'"{c}"' for c in shape.columns)
        total = 0
        for values in self._con.execute(
            f'SELECT {names} FROM buffer WHERE "litelink_offset" >= ?', (floor,)
        ):
            total += shape.measure(values)
            bits = 0
            for i, _, codec in shape.nested:
                if values[i] is not None:
                    bits += codec.variable_bits(codec.decode(values[i]))

            total += -(-bits // 8)

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
            measure = shape.measure
            columns = shape.columns
            nested = shape.nested
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
                _reject_offset(row)

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
                    _reject_unknown(row, shape)

                extra = 0
                if nested:
                    values, extra = _encode_nested(values, nested)

                try:
                    cursor.execute(sql, values)
                except sqlite3.IntegrityError as exc:
                    if RETIRED_REFUSAL in str(exc):
                        raise self.retired_error() from None

                    # SQLite is the gate; this only turns its answer into one
                    # a caller can act on. `CHECK constraint failed: key` does
                    # not say what was wrong with the value, or which value.
                    #
                    # Reached only on the way to raising, so it costs nothing
                    # in the ordinary case — and the `try` itself is free,
                    # CPython having zero-cost exceptions since 3.11.
                    _explain(row, values, shape, exc)

                # lastrowid is the assigned offset, available inside the open
                # transaction and before the row is visible to anyone else.
                offset = int(cursor.lastrowid or 0)
                offsets.append(offset)

                if group.start_offset is None:
                    group.start_offset = offset

                group.bytes += measure(values) + extra
                # Whichever is reached FIRST. Both are ceilings on one file —
                # bytes bound memory, rows bound the read latency §7 sizes for
                # — so the tighter one wins, which is the opposite of how
                # `staging_retention` and `staging_rows` combine.
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

    def span(self) -> tuple[int, int] | None:
        """The buffered offsets as `[start, end)`, or None if empty.

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

        return (int(lo), int(hi) + 1)

    def count_from(self, start: int) -> int:
        """How many buffered rows sit at or above `start` — the unsealed tail.

        `litelink_offset` is the INTEGER PRIMARY KEY, so SQLite answers this
        with a rowid range rather than a scan, and rows already sealed but not
        yet deleted cost nothing.
        """
        with self._lock:
            row = self._con.execute(
                'SELECT count(*) FROM buffer WHERE "litelink_offset" >= ?', (start,)
            ).fetchone()

        return int(row[0])

    @property
    def schema(self) -> pa.Schema:
        """The declared Arrow schema, for building a second buffer like this
        one — a published rewrite re-ingests through a scratch buffer and must
        cast the rows exactly as this one would."""
        return self.shape().schema

    def seed_offsets(self, first: int) -> None:
        """Make the next appended row take offset `first`.

        Only for the scratch buffer a published rewrite re-ingests through. Rows
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

    def reserve(self, n: int) -> tuple[int, int]:
        """Take `n` offsets without writing a row. Returns `[start, end)`.

        What bulk ingest needs and nothing else provides (§13.4). A path that
        writes Parquet directly still has to make those offsets unissuable, or
        live capture hands the same ones to different rows and I9 is gone —
        and `sqlite_sequence` is the sole authority on which offsets have been
        issued, so the sequence is what has to move.

        Neither existing writer of it fits. `seed_offsets` sets an ABSOLUTE
        value, which is the direction that is dangerous: SQLite assigns
        `max(largest rowid, seq) + 1`, so a caller computing `first` from a
        stale read seeds below the rows present and every later row lands on an
        offset belonging to different data. `strip_local_state` advances by a
        reserve, but it is a restore path that also deletes extents, groups,
        claims and pending deletes. This one does the advance and nothing else,
        and it is RELATIVE — the caller says how many, never from where — so
        the hazardous direction is not expressible.

        **Keyed `WHERE name = 'buffer'`.** `extent.group_id` and the claim
        table are AUTOINCREMENT too, so an unkeyed `seq = seq + ?` would
        advance every sequence in the database, moving group ids and claim ids
        the same distance for no reason anyone would later be able to explain.

        The floor is `max(seq, max(offset))` rather than `seq` alone, as
        `strip_local_state` computes it. AUTOINCREMENT keeps the two in step,
        so they agree on every log this code has seen; taking the maximum costs
        one indexed edge seek and means a database where they have somehow
        parted still cannot hand back an offset a buffered row already holds.

        Read back before returning, because the failure is silent. A reservation
        that did not land looks exactly like one that did — the caller writes
        its file over offsets live capture is still free to issue, and nothing
        downstream can tell.
        """
        if n <= 0:
            msg = f"cannot reserve {n} offsets: a reservation is at least one"
            raise ValueError(msg)

        with self._transaction():
            seq = self._con.execute(
                "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
            ).fetchone()
            highest = self._con.execute(
                'SELECT max("litelink_offset") FROM buffer'
            ).fetchone()[0]
            floor = max(int(seq[0]) if seq else 0, int(highest or 0))
            ceiling = floor + n
            # An UPDATE with an INSERT behind it rather than an upsert:
            # `sqlite_sequence` carries no unique constraint, and its row
            # appears only after the first AUTOINCREMENT insert — which for a
            # log whose whole load arrives through this path never happens.
            updated = self._con.execute(
                "UPDATE sqlite_sequence SET seq = ? WHERE name = 'buffer'",
                (ceiling,),
            )
            if not updated.rowcount:
                self._con.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES ('buffer', ?)",
                    (ceiling,),
                )

            landed = int(
                self._con.execute(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
                ).fetchone()[0]
            )
            if landed != ceiling:
                msg = (
                    f"reserving {n} offsets from {floor} left the sequence at "
                    f"{landed} rather than {ceiling}; nothing has been issued"
                )
                raise RuntimeError(msg)

        return floor + 1, ceiling + 1

    def open_group_started(self) -> bool:
        """Whether the open group has taken rows — bulk ingest's third read.

        `ingest` refuses to run while any acknowledged row is still owed a
        file, and this is the read that says so for the rows no seal has been
        asked to cut yet. `pending_group` and `pending_seal` cover the two that
        have.

        True when there is NO open group either, which `_seed_group` makes
        unreachable while a log is open. It is refused rather than reasoned
        about: every unknown here has to point at declining the ingest, because
        the cost of being wrong the other way is a file registered over rows
        that are still in the buffer.
        """
        with self._lock:
            row = self._con.execute(
                "SELECT start_offset FROM extent"
                " WHERE end_offset IS NULL AND rel_path IS NULL"
            ).fetchone()

        return row is None or row[0] is not None

    def group_bytes(self, end: int) -> int:
        """What the extent ending at `end` holds, before a file claims it.

        Read out so it can be recorded against the published table's copy: the
        scratch buffer measured these rows exactly as the appender would have,
        and that count is the whole reason the rewrite goes through a buffer at
        all.
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
        group's start. Once the delete is deferred until the published table
        holds the range (§3a), that stops being true — and an unbounded read
        then writes every row from the published frontier upward into the new
        file.

        Nothing catches that at seal time. The staging `register` passes no
        `start`, so `_refuse_straddle` returns early; what fails is later and
        elsewhere. The manifest's own ranges stop being non-overlapping (§4,
        §6), the staging leg of a read is an unfiltered `iceberg_scan` so every
        overlapped row comes back twice, the next `publish` refuses the straddle
        for ever, and compaction's row-count verification fails.

        `start` costs nothing to supply: `pending_group` and `pending_seal` both
        already carry it, and both call sites already unpack it.
        """
        # On the seal's OWN connection. See `_sealer`: sharing `_reader` with a
        # concurrent scan means reading that scan's pinned snapshot and then
        # deleting rows this never saw.
        return self._rows("BETWEEN ? AND ?", (start, end - 1), self._sealer)

    def rows_from(self, start: int | None) -> pa.Table:
        """Buffered rows with `offset >= start`, as Arrow. The read's input.

        `start` is the end of the staging table's committed span, so this is
        §7's unsealed tail — the rows the Iceberg leg does not already carry.

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
        # Offsets start at 1, so "no boundary" is a `start` of 0.
        start = 0 if start is None else start
        with self._tail_lock:
            # The tail is an Arrow table under the schema in force when it was
            # built, so a schema change has to discard it. Loud rather than
            # silent if it does not — `ArrowInvalid: Schema at index 1 was
            # different` out of the `concat_tables` below — but it would turn
            # every read in this process into that error.
            shape = self.shape()
            if self._tail_shape is not shape:
                self._tail = None
                self._tail_start = self._tail_end = self._tail_complete_from = 0
                self._tail_shape = shape

            cached = self._reusable(start)
            if cached is None:
                table = self._rows(">= ?", (start,))
                self._tail_complete_from = start
            else:
                fresh = self._rows(">= ?", (self._tail_end,))
                table = (
                    cached if fresh.num_rows == 0 else pa.concat_tables([cached, fresh])
                )
                if table.column(0).num_chunks > _MAX_TAIL_CHUNKS:
                    # One chunk per query otherwise, forever. Combining is a
                    # copy, so it is amortised rather than paid every time.
                    table = table.combine_chunks()

                # A hit implies `_tail_complete_from <= start`, so this only
                # ever raises — the guard above is what makes that true, and a
                # `max()` here would be dead.
                #
                # Conservative in one direction and never wrong in the other:
                # when `start` is below where the cache STARTS the slice prunes
                # nothing, so the cache is still complete from where it was,
                # and raising loses a later hit rather than serving short. That
                # costs nothing in practice because the boundary is the staging
                # table's span end, which only rises.
                self._tail_complete_from = start

            self._tail = table
            # Taken from the DATA, never from `start`. They are not the same
            # number: `start` is the table's boundary, and the first buffered
            # row at or above it can be higher still if a seal deleted the rows
            # between while this was being read. Recording `start` as though it
            # were the first cached row made the slice arithmetic below count
            # from a row that no longer existed, and the miscount hid buffered
            # rows from every subsequent query — silently, because an over-long
            # slice comes back empty rather than raising.
            if table.num_rows:
                self._tail_start = int(table.column(OFFSET)[0].as_py())
                self._tail_end = int(table.column(OFFSET)[-1].as_py()) + 1
            else:
                self._tail_start = self._tail_end = start

            return table

    def _reusable(self, start: int) -> pa.Table | None:
        """The cached tail with everything below `start` dropped, or None.

        Usable when the cache is complete from at or below `start` and
        `start` is within what it has fetched: `_tail_complete_from <= start
        <= _tail_end`.

        The slice index is arithmetic — cached offsets are contiguous from
        `_tail_start`, so dropping everything below the clamped `base` drops
        exactly `base - _tail_start` rows — and then checked, because that
        contiguity is a property of AUTOINCREMENT and prefix-only deletion
        rather than something enforced here. A reserved range (`ingest`,
        `restore`) is a hole no buffered row fills, so a cache spanning one
        fails the check and is rebuilt; a failed check costs only that.

        Both directions are checked. A wrong non-empty slice starts at the
        wrong offset; a wrong EMPTY slice is the dangerous one, because it
        looks exactly like "nothing buffered from the boundary" and would be
        returned as an answer. `_tail_end > start` says the last cached row
        qualifies, so an empty result contradicts the cache itself.
        """
        if self._tail is None or not (
            self._tail_complete_from <= start <= self._tail_end
        ):
            return None

        # Clamped, because a `start` BELOW where the cache starts drops nothing
        # rather than a negative number of rows. That is the ordinary case on a
        # log whose offsets start high — every buffered row is already at or
        # above the boundary — and the unclamped subtraction is why the guard
        # could not simply be widened.
        base = max(start, self._tail_start)
        kept = self._tail.slice(base - self._tail_start)
        if kept.num_rows:
            if int(kept.column(OFFSET)[0].as_py()) != base:
                return None
        elif self._tail_end > start:
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
        nested = column_type(declared).nested
        if nested is not None:
            values = tuple(None if v is None else nested.decode(str(v)) for v in values)

        try:
            return pa.array(values, type=declared)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            return pa.array(values).cast(declared)

    def claim(
        self,
        kind: str,
        start: int,
        end: int,
        owner: str,
        rel_path: str | None = None,
        ttl_ms: int = DEFAULT_TTL_MS,
    ) -> Claim:
        """A claim on the offsets `[start, end)`, backed by this database.

        Handed the connection AND the lock that guards it — see `Claim`.
        """
        return Claim(self._con, self._lock, kind, start, end, owner, rel_path, ttl_ms)

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

        Unreachable until `litelink.restore` began releasing published rows out
        from under a knowingly-stale group (§3a), which is one transaction away
        from the reseed that fixes it.

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

    def finish_seal(self, end: int, rel_path: str) -> bool:
        """Retire the group and clear the intent. The sealed rows STAY.

        Dropping them is `evict("buffer")`'s, not the seal's (#122): a seal only
        moves data, and every deletion belongs to the cleanup half of the
        pipeline. Leaving them is safe in both directions — the read boundary
        in §7 excludes these rows the moment the Iceberg commit lands — and it
        is what `wal_replication` always needed anyway: the buffer is the
        off-box copy until the published table has the range (§3a).

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

        Root-relative for staging files, so a log directory stays movable; the
        full URI for published ones, which have no root to be relative to. A
        named extent is a file; an unnamed one is still buffered.

        All of it at once: the callers are compaction, publish and the published
        rewrite, all of which walk the whole file list, and one indexed read
        beats a query per file.

        A file missing from this is not an error. It means the log has files
        this database never recorded — one written by a version that did not
        keep them, or a published table whose `extent` rows were lost — and the
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
        no age map, `evict` could not call it stale, and `staging_retention`
        silently stopped reclaiming anything.

        The two settings are sized by unrelated things: §6 wants
        `staging_snapshot_retention` above the longest scan, §8 wants
        `staging_retention` above the longest hot lookback. Any deployment where
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

        What `publish` calls when it pushes: the published table's copy covers
        the same offsets and holds the same bytes, so it gets its own row under
        its own URI rather than a measurement of its own. It could not be
        measured again anyway — nothing recoverable from a Parquet file is the
        appender's count of what those rows cost in memory, and the staging row
        goes when the staging file is unlinked.

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
        this, a compaction-target change before the next publish regroups the
        pushed-but-unrecorded files and commits a staging file straddling the
        published table's span. Nothing re-cuts a staging straddler, so every
        later push is refused and the log stops advancing.

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
        published table's manifest does not name, and one below the staging
        window has to be reachable to be dropped — bounding this read would
        leave those rows beyond judgement for ever.
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

    def published_records(
        self, prefix: str, floor: int
    ) -> list[tuple[str, int, int, int]]:
        """Landed copies under `prefix`, keyed by PATH: `(rel_path, start, end,
        bytes)`.

        Reconciliation matches by path, and `published_ranges` answers in bare
        offsets — so it cannot serve. Bounded by `floor` like the manifest walk
        beside it, or it grows with the published table and runs on every
        publish pass.
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
        to. It is also what lets the published rewrite build its extents with
        the same arithmetic a staging compaction uses.
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
            return _meta_value(connection, key)
        finally:
            connection.close()

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            return _meta_value(self._con, key)

    def published_ranges(
        self, prefix: str | None, floor: int, *, include_intents: bool
    ) -> list[tuple[int, int]]:
        """Offset ranges a published table holds or is about to, under `prefix`.

        `include_intents` is keyword-only and has no default, so every caller
        states which question it is asking. Compaction asks whether ANY
        published table might hold a range, and is safe overstating it; eviction
        asks whether one DOES, and is safe only understating. Getting that
        backwards at one call site would be silent, which is the whole reason
        this parameter is awkward to pass.

        I4 asked of segments rather than of a watermark (§4a). `publish` records
        where each pushed file's copy went, so the published table's contents
        are already durable here per file — a watermark summarising them is a
        second copy of the same fact, and the only boundary in the log that can
        move backwards.

        Bounded by `floor` rather than by the prefix alone: the published table
        grows without limit and this only ever asks about ranges the staging
        table still holds, which compaction bounds. The prefix match is applied
        in Python because SQLite's `LIKE` would not use the index that `floor`
        selects on.
        """
        # `None` means ANY published table, not the configured one. Two
        # questions ask this and they are not the same question. I4 asks whether
        # THIS published table holds a file, because it authorises a deletion.
        # Compaction asks whether ANY published table does, because it decides
        # whether merging could create a range no published table's cuts line up
        # with — and pointing away does not make those copies stop existing.
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

        return sorted((start, end) for start, end, path in rows if wanted(path))

    def set_meta_moved(self, key: str, value: str, reset: Mapping[str, str]) -> bool:
        """Record `value`, applying `reset` only if it is a MOVE.

        The question "is this a change?" is answered against the durable value,
        inside the transaction that acts on the answer. Asked of a process's
        memory instead, both answers are wrong in a two-process deployment,
        because nothing refreshes that memory except a publish pass:

        * memory stale, argument current — re-asserting the published table the
          log already has reads as a move and zeroes the watermarks of a bucket
          that genuinely holds the data;
        * memory stale, argument stale — re-pointing BACK to a published table
          reads as a restatement, and the log keeps the other published table's
          watermark over a bucket whose span is lower. Eviction believes it
          (I4).

        Returns whether it was a move.
        """
        with self._transaction():
            current = _meta_value(self._con, key) or None
            moved = current != (value or None)
            _write_meta(self._con, {key: value, **(dict(reset) if moved else {})})
            return moved

    def raise_meta(self, values: Mapping[str, int]) -> None:
        """Write integer watermarks that only ever go up: a value already
        stored above the one given is kept.

        In ONE transaction, because two publishes on disjoint ranges can
        finish out of order (#118), and a read-then-write max would let the
        slower one lower what the faster recorded.
        """
        with self._transaction():
            _write_meta(
                self._con,
                {
                    name: str(max(value, int(_meta_value(self._con, name) or 0)))
                    for name, value in values.items()
                },
            )

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
        `WriteHandle`, in `Maintenance` and, derived, in this object's seal
        target, all kept in step by `set_config` writing each. A refresh call
        had to sit wherever a decision was made, and a design needing N of those
        is always one short somewhere, because nothing says where N is.

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

        The rule `config` follows, for the reason `config` follows it (§4a):
        one copy of a fact, in the log, so no process can hold a stale one.
        Fixed when the log is created; a different order is a new log.

        The PARSE is cached on the raw value, as `config`'s is, so the cache
        can never disagree with the row.

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

        For facts that are only true together. Re-pointing a published table
        writes where it is and resets the two watermarks that describe the
        previous one, and as separate autocommit statements a crash lands
        between them: either order leaves a log whose parts disagree, and both
        disagreements have cost a defect. One transaction has no between.
        """
        with self._transaction():
            _write_meta(self._con, pairs)

    def set_meta(self, key: str, value: str) -> None:
        with self._transaction():
            _write_meta(self._con, {key: value})

    # -- stored tiers (#90) ------------------------------------------------

    def tiers(
        self,
    ) -> tuple[str | None, dict[str, tuple[tuple[int, int], str, str | None]]]:
        """`(generation, {tier: ((start, end), statistics, version)})`, in ONE
        statement.

        What a read consults per query: every stored tier row and the counter
        that says whether any changed since, so a reader decodes only after a
        write. A buffer opened read-only before any writer created the tables
        has none.
        """
        try:
            with self._lock:
                rows = self._con.execute(
                    "SELECT (SELECT v FROM meta WHERE k = ?), s.tier,"
                    " o.start_offset, o.end_offset, s.statistics, s.version"
                    " FROM (SELECT 1) LEFT JOIN tier_statistics s"
                    " LEFT JOIN tier_offsets o ON o.tier = s.tier",
                    (TIER_GENERATION,),
                ).fetchall()
        except sqlite3.OperationalError:
            return None, {}

        generation = None if rows[0][0] is None else str(rows[0][0])
        found: dict[str, tuple[tuple[int, int], str, str | None]] = {}
        legacy: dict[str, tuple[tuple[int, int], str, str | None]] = {}
        for _, tier, start, end, statistics, version in rows:
            if tier is None or start is None:
                continue

            stored = ((int(start), int(end)), str(statistics), version)
            if str(tier) in _CURRENT_TIER:
                legacy[_CURRENT_TIER[str(tier)]] = stored
            else:
                found[str(tier)] = stored

        # A pre-#98 row stands in only where no current one exists.
        return generation, {**legacy, **found}

    def update_tier(
        self,
        tier: str,
        change: Callable[
            [tuple[int, int] | None, str | None], tuple[tuple[int, int], str] | None
        ],
    ) -> None:
        """Replace a tier's row with `change(offsets, statistics)`, in one
        transaction.

        The read and the write together, so two writers cannot each drop the
        other's change. None removes the row. The generation moves on every
        call.
        """
        with self._transaction():
            stored = self._tier_row(tier)
            updated = change(
                None if stored is None else stored[0],
                None if stored is None else stored[1],
            )
            self._drop_legacy_tier(tier)
            if updated is None:
                self._con.execute("DELETE FROM tier_offsets WHERE tier = ?", (tier,))
                self._con.execute("DELETE FROM tier_statistics WHERE tier = ?", (tier,))
            else:
                (start, end), encoded = updated
                self._con.execute(
                    "INSERT INTO tier_offsets (tier, start_offset, end_offset)"
                    " VALUES (?, ?, ?) ON CONFLICT(tier) DO UPDATE SET"
                    " start_offset = excluded.start_offset,"
                    " end_offset = excluded.end_offset",
                    (tier, start, end),
                )
                self._con.execute(
                    "INSERT INTO tier_statistics (tier, statistics) VALUES (?, ?)"
                    " ON CONFLICT(tier) DO UPDATE SET statistics = excluded.statistics",
                    (tier, encoded),
                )

            self._bump_tiers()

    def store_staging_statistics(
        self, version: str, offsets: tuple[int, int], statistics: str
    ) -> bool:
        """Store the staging tier's rollup for `version`, unless a newer one is
        stored.

        Written by whichever process committed `version`, after the commit — the
        staging row is a cache, stamped with the version it describes, so a
        reader uses it only for exactly that version and a late or missing write
        can cost a recompute but never a wrong answer.

        **Forward only**, compared in the same transaction as the write. Two
        commits' stores can land in either order, and an older one arriving last
        would leave every process recomputing until the next commit. Versions
        compare by the sequence number Iceberg puts at the front of each
        metadata file's name (`00012-<uuid>.metadata.json`).

        **Only the latest version is kept**, deliberately. A reader misses the
        stored row in two ways: its version is newer (the commit landed, its
        store has not yet — a few milliseconds), or older (a newer commit stored
        its row between this reader resolving its version and looking it up —
        well under a millisecond). History would help only the second, commits
        come seconds apart, and a miss costs one 2–3 ms rollup that the
        process then caches for itself. An expiry pass kept in step with
        snapshot retention is not worth that.

        Returns whether it wrote.
        """
        with self._transaction():
            stored = self._tier_row("staging")
            if (
                stored is not None
                and stored[2] is not None
                and _metadata_sequence(stored[2]) >= _metadata_sequence(version)
            ):
                return False

            self._drop_legacy_tier("staging")
            self._con.execute(
                "INSERT INTO tier_offsets (tier, start_offset, end_offset)"
                " VALUES ('staging', ?, ?) ON CONFLICT(tier) DO UPDATE SET"
                " start_offset = excluded.start_offset,"
                " end_offset = excluded.end_offset",
                offsets,
            )
            self._con.execute(
                "INSERT INTO tier_statistics (tier, statistics, version)"
                " VALUES ('staging', ?, ?) ON CONFLICT(tier) DO UPDATE SET"
                " statistics = excluded.statistics, version = excluded.version",
                (statistics, version),
            )
            self._bump_tiers()

            return True

    def adopt_current_names(self) -> None:
        """Move every pre-#98 `meta` key and tier row to its current name.

        For a writer's `open`, so an old log stores the new names from then
        on rather than one row at a time as each is written. One transaction,
        and the values are untouched: the fences compare values, so a process
        that read a location before this sees the same one after.
        """
        with self._transaction():
            for new, old in LEGACY_META.items():
                value = self._con.execute(
                    "SELECT v FROM meta WHERE k = ?", (old,)
                ).fetchone()
                if value is not None:
                    self._con.execute(
                        "INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO NOTHING",
                        (new, value[0]),
                    )
                    self._con.execute("DELETE FROM meta WHERE k = ?", (old,))

            moved = False
            for new, old in LEGACY_TIERS.items():
                for table in ("tier_offsets", "tier_statistics"):
                    held = self._con.execute(
                        f"SELECT 1 FROM {table} WHERE tier = ?",  # noqa: S608
                        (new,),
                    ).fetchone()
                    if held is None:
                        cursor = self._con.execute(
                            f"UPDATE {table} SET tier = ? WHERE tier = ?",  # noqa: S608
                            (new, old),
                        )
                    else:
                        cursor = self._con.execute(
                            f"DELETE FROM {table} WHERE tier = ?",  # noqa: S608
                            (old,),
                        )

                    moved = moved or cursor.rowcount > 0

            if moved:
                self._bump_tiers()

    def _tier_row(self, tier: str) -> tuple[tuple[int, int], str, str | None] | None:
        """Inside the caller's transaction: `tier`'s row, or its pre-#98 one."""
        for name in (tier, LEGACY_TIERS.get(tier)):
            if name is None:
                continue

            row = self._con.execute(
                "SELECT o.start_offset, o.end_offset, s.statistics, s.version"
                " FROM tier_offsets o JOIN tier_statistics s USING (tier)"
                " WHERE o.tier = ?",
                (name,),
            ).fetchone()
            if row is not None:
                version = None if row[3] is None else str(row[3])
                return (int(row[0]), int(row[1])), str(row[2]), version

        return None

    def _drop_legacy_tier(self, tier: str) -> None:
        """Inside the caller's transaction: remove `tier`'s pre-#98 row."""
        legacy = LEGACY_TIERS.get(tier)
        if legacy is not None:
            self._con.execute("DELETE FROM tier_offsets WHERE tier = ?", (legacy,))
            self._con.execute("DELETE FROM tier_statistics WHERE tier = ?", (legacy,))

    def _bump_tiers(self) -> None:
        """Inside the caller's transaction: a new generation for readers' caches."""
        self._con.execute(
            "INSERT INTO meta (k, v) VALUES (?, '1')"
            " ON CONFLICT(k) DO UPDATE SET v = CAST(v AS INTEGER) + 1",
            (TIER_GENERATION,),
        )

    def lowest_offset(self) -> int | None:
        """The lowest offset buffered, or None when the buffer is empty.

        Only ever rises — rows arrive above it and leave as a prefix — so a
        value read a moment early is still a lower bound on every row a later
        read can find here. One B-tree edge seek; see `span`.
        """
        with self._lock:
            row = self._con.execute(
                'SELECT min("litelink_offset") FROM buffer'
            ).fetchone()

        return None if row is None or row[0] is None else int(row[0])

    def no_rows(self) -> pa.Table:
        """A buffer tail with no rows, shaped as `rows_from` shapes one.

        For a read that has ruled the buffer out: its leg stays in the union,
        empty, so nothing about the query's shape changes. Read on the reader
        connection like any tail, without touching the tail cache.
        """
        with self._tail_lock:
            return self._rows("> ?", (_NO_ROW_LIMIT,))

    # -- retirement -------------------------------------------------------

    def close_buffer(self, statistics: str) -> None:
        """Give the buffer an end: the log takes no more rows (`retire()`, step 1).

        The buffer's `end_offset` is otherwise unknown — it grows. Closed, it
        is the offset the next append would have taken, read INSIDE this write
        transaction so no append can land between reading it and closing: the
        trigger refuses every append from the commit on, so the end is final.
        Its start is the lowest offset still buffered; rows only leave from
        here, so the range stays a superset of what the buffer holds.

        **This row is authoritative, unlike the other tier rows**, which are
        caches routing can rebuild. Only `retire()` writes it and nothing drops
        or recomputes it. A no-op when the buffer is already closed.
        """
        with self._transaction():
            if self._con.execute(
                "SELECT 1 FROM tier_offsets WHERE tier = 'buffer'"
            ).fetchone():
                return

            seq = self._con.execute(
                "SELECT seq FROM sqlite_sequence WHERE name = 'buffer'"
            ).fetchone()
            end = (seq[0] if seq else 0) + 1
            lowest = self._con.execute(
                'SELECT min("litelink_offset") FROM buffer'
            ).fetchone()[0]
            start = end if lowest is None else int(lowest)
            self._write_buffer_row(start, end, statistics)

    def empty_buffer_range(self, statistics: str) -> None:
        """Narrow the closed range to `[end, end)`, empty: `retire()` is done.

        Only once the buffer holds nothing; the record count in `statistics`
        (0) is what tells a retired log from one still retiring.
        """
        with self._transaction():
            row = self._con.execute(
                "SELECT end_offset FROM tier_offsets WHERE tier = 'buffer'"
            ).fetchone()
            if row is None:
                msg = "the buffer has no end to narrow to; close it first"
                raise RuntimeError(msg)

            end = int(row[0])
            self._write_buffer_row(end, end, statistics)

    def _write_buffer_row(self, start: int, end: int, statistics: str) -> None:
        """Inside the caller's transaction."""
        self._con.execute(
            "INSERT INTO tier_offsets (tier, start_offset, end_offset)"
            " VALUES ('buffer', ?, ?) ON CONFLICT(tier) DO UPDATE SET"
            " start_offset = excluded.start_offset,"
            " end_offset = excluded.end_offset",
            (start, end),
        )
        self._con.execute(
            "INSERT INTO tier_statistics (tier, statistics) VALUES ('buffer', ?)"
            " ON CONFLICT(tier) DO UPDATE SET statistics = excluded.statistics",
            (statistics,),
        )
        self._bump_tiers()

    def retired(self) -> dict[str, object] | None:
        """The log's retirement, read from the buffer's closed range, or None.

        `retiring` while the range can still hold rows (its record count is
        not known to be 0), `retired` once `retire()` has emptied it.
        """
        try:
            with self._lock:
                row = self._con.execute(
                    "SELECT o.start_offset, o.end_offset, s.statistics"
                    " FROM tier_offsets o JOIN tier_statistics s USING (tier)"
                    " WHERE o.tier = 'buffer'"
                ).fetchone()
        except sqlite3.OperationalError:
            return None

        if row is None:
            return None

        return _retirement(int(row[1]), str(row[2]), self.get_meta(PUBLISHED_META_KEY))

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
            published = _meta_value(con, PUBLISHED_META_KEY)
        except sqlite3.OperationalError:
            return None
        finally:
            con.close()

        if row is None:
            return None

        return _retirement(int(row[0]), str(row[1]), published)

    def retired_error(self) -> RetiredError:
        """The refusal for anything that would add rows to a retired log."""
        return RetiredError.of(self.retired() or {"state": "retired"}, self._name())

    def _name(self) -> str:
        """The log's name, for messages: the directory `buffer.db` sits in."""
        row = self._con.execute("PRAGMA database_list").fetchone()

        return Path(str(row[2])).parent.name if row is not None else "this log"

    # -- compaction bookkeeping -------------------------------------------

    def claim_compaction(self, start: int, end: int, rel_path: str) -> None:
        """Record a compaction's output path before the file exists.

        The seal's I2 argument applied to the other writer: a compaction that
        crashes between writing and committing leaves a file on disk, and the
        only way to find it without a directory scan is to have written its
        name down first.
        """
        # Inserted, never replacing what is already there. Clearing first
        # destroyed the claims of an operation that had CRASHED — a published
        # rewrite accumulates one per uploaded object, and recovery only runs at
        # `open`, so a long-lived maintainer starting its next merge wiped them
        # and left those objects referenced by nothing. Each operation clears
        # its own.
        with self._lock:
            self._con.execute(
                "INSERT INTO compacting (start_offset, end_offset, rel_path)"
                " VALUES (?, ?, ?)",
                (start, end, rel_path),
            )

    def claim_output(self, start: int, end: int, rel_path: str) -> None:
        """Record one more output path, without clearing the others.

        A compaction writes one file and `claim_compaction` says so by
        replacing whatever was there. A published rewrite writes several before
        a single commit swaps them all in, and every one of them needs its name
        recorded before it exists (I2) — so they accumulate, and recovery
        removes each that the commit never claimed.
        """
        with self._lock:
            self._con.execute(
                "INSERT INTO compacting (start_offset, end_offset, rel_path)"
                " VALUES (?, ?, ?)",
                (start, end, rel_path),
            )

    def pending_compaction(self) -> tuple[int, int, str] | None:
        with self._lock:
            row = self._con.execute(
                "SELECT start_offset, end_offset, rel_path FROM compacting"
            ).fetchone()

        return None if row is None else (int(row[0]), int(row[1]), str(row[2]))

    def pending_outputs(self) -> list[tuple[int, int, str]]:
        """Every claimed output, for recovery. One row for a compaction,
        several for an interrupted published rewrite."""
        with self._lock:
            rows = self._con.execute(
                "SELECT start_offset, end_offset, rel_path FROM compacting"
                " ORDER BY rowid"
            ).fetchall()

        return [(int(start), int(end), str(path)) for start, end, path in rows]

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

        Stamped at the queueing, a rewrite slower than its table's snapshot
        retention burns the whole grace before it commits: the moment the originals stop
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
          PUBLISHED copies stay: that is the coverage I4 acts on, and it is
          still true.

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
          and that half is required. `compact("published")` is the only thing that
          queues a remote entry, and this design refuses directory listing, so
          dropping them leaks published objects nothing can ever find again.
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
          range. A seal never deletes its rows (that is `evict("buffer")`'s),
          so they are still buffered and the next cut writes them a second
          time. Measured: 490 rows read where 440 are
          distinct, from two overlapping local files, with no error anywhere.

          Recovery is redundant here rather than protective. Every row that
          seal was writing is still in the buffer, and the open group
          `_seed_group` builds covers them — so dropping the claim re-seals
          them exactly once.

        - **`compacting` stays.** It only queues deletions, and its outputs
          are published objects this machine never wrote.

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
        of the published table and the published table has since moved on: see
        `litelink.restore`.
        """
        with self._transaction():
            self._con.execute(
                "DELETE FROM extent WHERE end_offset IS NULL AND rel_path IS NULL"
            )

        self._seed_group()

    def evict_rows(self, start: int, end: int) -> int:
        """Drop buffer rows in `[start, end)`. Returns how many.

        `evict("buffer")`'s delete. The caller decides `end` from what the next
        durable copy holds; this only deletes. One statement under the write
        lock, so a wide range stalls appends for its duration — bounding the
        range is how a caller chunks it.

        Idempotent: rows already gone are simply not there to delete.
        """
        with self._lock, self._con:
            cursor = self._con.execute(
                'DELETE FROM buffer WHERE "litelink_offset" >= ?'
                ' AND "litelink_offset" < ?',
                (start, end),
            )

        return cursor.rowcount

    def reclaim_free_pages(self, min_free_ratio: float = 0.0) -> int:
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

        Not in `_transaction`: SQLite refuses `VACUUM` inside one. The lock is
        still held, so no append interleaves.
        """
        with self._lock:
            page_size = int(self._con.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(self._con.execute("PRAGMA page_count").fetchone()[0])
            free = int(self._con.execute("PRAGMA freelist_count").fetchone()[0])
            if (
                not page_count
                or free * page_size < _VACUUM_FLOOR_BYTES
                or free < page_count * min_free_ratio
            ):
                return 0

            self._con.execute("VACUUM")

            return free * page_size

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
            self._tail_start = self._tail_end = self._tail_complete_from = 0

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


def _measurer(schema: pa.Schema) -> Callable[[tuple[object, ...]], int]:
    """One row's size in the Arrow table a seal builds, compiled per schema (#84).

    That is the currency `target_seal_size` and every `extent.bytes` are
    stated in, and compaction sizes merges by it, so it must not undercount.
    It used to count 8 for any non-string value and 0 for a null, which is
    right only for wide 64-bit rows: a sparse schema — where Arrow still gives
    each null its slot — measured a third of its real size, so seals came out
    three times the target.

    Most of it depends only on the schema, so it is computed here once: every
    column's slot and validity bit, `slot_bits`, whether or not the value is
    null. Per row, only a string's UTF-8 length and a blob's length are added —
    fewer columns visited than before, not more. A nested value's variable
    part is measured where it is encoded, see `_encode_nested`.

    The NaN test rides here because this runs for every row after the insert:
    a NaN is stored by SQLite as NULL, so no CHECK sees it in a nullable
    column, and only float columns are visited for it.
    """
    fixed_bits = 64 + sum(slot_bits(field.type) for field in schema)
    fixed = -(-fixed_bits // 8)
    names = schema.names
    text = tuple(
        i
        for i, field in enumerate(schema)
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
    )
    blobs = tuple(i for i, field in enumerate(schema) if pa.types.is_binary(field.type))
    floats = tuple(
        (i, names[i])
        for i, field in enumerate(schema)
        if pa.types.is_floating(field.type)
    )

    def measure(values: tuple[object, ...]) -> int:
        total = fixed
        for i in text:
            value = values[i]
            if value is not None:
                total += len(value.encode())  # ty: ignore[unresolved-attribute]

        for i in blobs:
            value = values[i]
            if value is not None:
                total += len(value)  # ty: ignore[invalid-argument-type]

        for i, name in floats:
            value = values[i]
            # `!=` on itself is the NaN test, false for None and for an int.
            if value != value:
                _reject_non_finite(name, value)  # ty: ignore[invalid-argument-type]

        return total

    return measure


def _encode_nested(
    values: tuple[object, ...], nested: tuple[tuple[int, str, Nested], ...]
) -> tuple[tuple[object, ...], int]:
    """Check each nested value against its declared type, and store its JSON.

    Here rather than in a CHECK because SQLite cannot see inside one; see
    `Nested`. None passes through untouched, so `NOT NULL` and
    `_reject_missing` answer for a nested column exactly as for any other.

    Also returns the nested values' variable Arrow bytes, measured on the
    values rather than the JSON they become; the fixed slots are already in
    the schema's `measure`.
    """
    stored = list(values)
    bits = 0
    for i, name, codec in nested:
        value = stored[i]
        if value is not None:
            try:
                stored[i] = codec.encode(value)
            except NestedValueError as exc:
                msg = f"column {name!r} cannot hold this value {exc}"
                raise ValueError(msg) from None

            bits += codec.variable_bits(value)

    return tuple(stored), -(-bits // 8)


def _reject_unknown(row: Mapping[str, object], shape: Shape) -> None:
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
    unknown = sorted(set(row) - shape.known)
    msg = (
        f"row names columns this log does not have: {unknown}. "
        f"Declared: {sorted(shape.columns)}"
    )
    raise ValueError(msg)


def _reject_missing(
    row: Mapping[str, object], values: tuple[object, ...], shape: Shape
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
    # First, and by value rather than by column type: SQLite stores a NaN as
    # NULL, so in a non-nullable column it fails NOT NULL and would otherwise
    # surface as a bare `NOT NULL constraint failed` that names no value.
    for name, value in zip(shape.columns, values, strict=True):
        if isinstance(value, float) and not math.isfinite(value):
            _reject_non_finite(name, value)

    if any(values[i] is None for i in shape.required):
        _reject_missing(row, values, shape)

    _check_types(row, values, shape)

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
        if value is not None and not lo <= value <= hi:
            _reject_range(row, i, value, shape)

    raise exc


def _check_types(
    row: Mapping[str, object], values: tuple[object, ...], shape: Shape
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


def _reject_non_finite(name: str, value: float) -> None:
    """NaN or ±inf, refused on every write path (#87); see `NON_FINITE`."""
    msg = f"column {name!r} cannot hold {value!r}: {NON_FINITE}"
    raise ValueError(msg)


def _reject_range(
    row: Mapping[str, object], index: int, value: object, shape: Shape
) -> None:
    """A value of the right type whose MAGNITUDE the column cannot hold.

    Off the hot path, like the other refusals. The two cases it covers fail
    differently and neither says anything at append: an int32 given 2**40
    is stored by SQLite unchanged and then makes every scan raise, while a
    float32 given 1e300 reads back as `inf` with no error at all. An infinity
    itself never reaches here; `_explain` refuses it first.
    """
    name = shape.columns[index]
    msg = (
        f"column {name!r} cannot hold {value!r}: it is declared "
        f"{shape.schema.field(name).type}, whose range is "
        f"{shape.ranged[[i for i, *_ in shape.ranged].index(index)][1:]}"
    )
    raise ValueError(msg)


def _reject_offset(row: Mapping[str, object]) -> None:
    """I11: `offset` is assigned by the library, never accepted."""
    if "litelink_offset" in row:
        msg = "`offset` is assigned by the library and cannot be supplied (I11)"
        raise ValueError(msg)


class RowProbe:
    """The buffer's acceptance rules, with no log behind them (#77).

    A private in-memory `buffer` table built by `_buffer_ddl`, so its CHECKs
    are the log's own rather than a Python restatement of them, and a check
    that inserts and rolls back. Everything SQLite cannot be asked is asked in
    the same order `_insert` asks it, through the same helpers — so the answer
    and the message are what `append` would give.

    That order is repeated rather than shared: `_insert`'s loop is inlined for
    throughput, and routing it through a helper cost 19 points against raw
    SQLite. `test_validate_row_answers_exactly_as_append_does` is what holds
    the two together.
    """

    def __init__(self, schema: pa.Schema) -> None:
        self._shape = Shape.of(schema)
        # Nothing touches disk, so nothing here needs `synchronous` or WAL.
        # One connection shared across threads, serialised by the lock below:
        # a probe is cached per schema and reached from whichever thread asks.
        self._con = sqlite3.connect(
            ":memory:", isolation_level=None, check_same_thread=False
        )
        self._con.execute(_buffer_ddl(self._shape))
        names = ", ".join(f'"{c}"' for c in self._shape.columns)
        placeholders = ", ".join("?" * len(self._shape.columns))
        self._sql = f"INSERT INTO buffer ({names}) VALUES ({placeholders})"
        self._lock = threading.Lock()

    def check(self, row: Mapping[str, object]) -> None:
        """Raise what `append([row])` would raise, or return if it would pass."""
        shape = self._shape
        _reject_offset(row)
        values = tuple(row.get(c) for c in shape.columns)
        if (
            len(row) != len(shape.columns) or None in values
        ) and not shape.known.issuperset(row):
            _reject_unknown(row, shape)

        if shape.nested:
            values, _ = _encode_nested(values, shape.nested)

        with self._lock:
            self._con.execute(_BEGIN)
            try:
                try:
                    self._con.execute(self._sql, values)
                except sqlite3.IntegrityError as exc:
                    _explain(row, values, shape, exc)

                shape.measure(values)
            finally:
                self._con.execute("ROLLBACK")
