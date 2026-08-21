"""Per-operation ownership of an offset RANGE (SPEC §4a, §13.6).

Two questions have to be answered before a maintenance pass may start work, and
only one of them is answerable from the data. **May these two run together** is
interval arithmetic: offsets are immutable and files cover contiguous
non-overlapping ranges (§4), so two operations on disjoint ranges commute, and
whether they are disjoint is a comparison of two integers. **Is this in-flight
record live work, or did the process holding it die** is not derivable from
anything; only a deadline answers it.

So a claim carries both: a range, and an owner with an expiry. One row per
OPERATION rather than one per role, which is what lets several passes run at
once without excluding each other by kind — a compaction merging one run and a
sync pushing another have nothing to say to each other.

**The check and the insert are one transaction.** Reading the live claims,
deciding, and then inserting leaves exactly the window the decision was meant to
close: eviction sees no claim and decides to drop everything below 500, a
compaction claims [400, 600] and starts merging, eviction commits its removal,
and the merge commits rows 400-500 back. Both sides declaring in a transaction
that saw the other's absence is what makes the ordering total, which is why
eviction claims a range rather than merely consulting the claims of others.

It lives in the buffer database because that is already the coordinator (I16),
and because a claim has to outlive the process holding it — a Python lock cannot
say anything about a process that is no longer running.

**Expiry rather than release** is what makes a crash survivable. A holder that
exits cleanly releases; one that is killed leaves a claim that lapses, after
which another process may take it and replay whatever the first left behind.
Recovery ownership follows operation ownership: §11 records the hazard, where
opening a log replayed another process's in-flight work — a maintenance process
redoing the writer's seal, a writer deleting a half-written compaction. The
claim's `rel_path` is what says which file that work was writing.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

# Long enough that a slow seal does not lose its own lease mid-operation — a
# compaction over a large table is seconds — and short enough that a killed
# process does not strand its work for long.
DEFAULT_TTL_MS = 30_000


def new_owner() -> str:
    """A fresh identity for one attempt to hold a role.

    A UUID, because being unique is the whole job and nothing derived does it
    safely: process ids are reused once a process exits, thread idents once a
    thread does. An owner built from either could be inherited by something new,
    which would let a stranger re-enter a lease as though it were its own.

    Being unique per attempt is also what lets one mechanism cover both cases.
    Callers mint an owner per acquisition rather than per Log, so two threads
    sharing a Log are two owners, and the row that refuses a second holder in
    another process refuses one in another thread on the same terms.

    The pid and thread are for whoever reads the table wanting to know who is
    holding it. They are diagnostic, not identity: nothing compares them, and
    two owners differing only there are still two owners.
    """
    return f"{uuid.uuid4()}:pid={os.getpid()}:thread={threading.get_ident()}"


@dataclass(slots=True)
class Claim:
    """One operation's claim on one offset range, held in SQLite.

    `lock` is the mutex guarding `connection`, and holding it is what makes
    these statements atomic rather than merely single. Without it a claim write
    can land inside a transaction another thread has open on the same
    connection — an append's `BEGIN IMMEDIATE` — and then it commits or rolls
    back with that transaction rather than on its own. A rolled-back append took
    the claim row with it, leaving its holder believing it held work the table
    no longer recorded: observed as two sealers writing the same file, and
    pyiceberg refusing the second with "already referenced by table".
    """

    connection: sqlite3.Connection
    lock: threading.RLock
    kind: str
    lo: int
    hi: int
    owner: str
    rel_path: str | None = None
    ttl_ms: int = DEFAULT_TTL_MS
    row_id: int | None = None

    def acquire(self) -> bool:
        """Take the range, if no live claim of another owner overlaps it.

        Check and insert in ONE write transaction, which is the whole point —
        see the module docstring. `BEGIN IMMEDIATE` takes the write lock up
        front, so whichever transaction commits second saw the first, and there
        is no interval in which both believe they own the range.
        """
        now = _now_ms()
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                # Ranges are inclusive on both ends, so [a, b] and [c, d]
                # overlap exactly when a <= d and b >= c.
                clash = self.connection.execute(
                    "SELECT 1 FROM claim WHERE expires_at > ? AND owner <> ? "
                    "AND lo <= ? AND hi >= ? LIMIT 1",
                    (now, self.owner, self.hi, self.lo),
                ).fetchone()
                if clash is not None:
                    self.connection.execute("ROLLBACK")

                    return False

                # Expired overlapping rows go, in the same transaction. One
                # row per operation means nothing overwrites them the way a
                # per-role row did, so without this the table grows for ever —
                # and a lapsed holder would still find its own row and renew
                # itself back to life over the range someone else now owns.
                self.connection.execute(
                    "DELETE FROM claim WHERE expires_at <= ? AND lo <= ? AND hi >= ?",
                    (now, self.hi, self.lo),
                )
                cursor = self.connection.execute(
                    "INSERT INTO claim (owner, expires_at, kind, lo, hi, rel_path) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        self.owner,
                        now + self.ttl_ms,
                        self.kind,
                        self.lo,
                        self.hi,
                        self.rel_path,
                    ),
                )
                self.row_id = cursor.lastrowid
                self.connection.execute("COMMIT")
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

            return True

    def renew(self) -> bool:
        """Extend the claim. False means we no longer hold it.

        A holder that stalls past its TTL can find its claim reclaimed, and must
        find that out rather than carry on: the new owner may already be
        replaying the work it was doing.

        So an EXPIRED claim cannot renew, even though its row may still be
        there — one row per operation means nobody overwrote it, and letting it
        extend itself would hand the range back to a holder the log has already
        moved past.
        """
        if self.row_id is None:
            return False

        now = _now_ms()
        with self.lock:
            cursor = self.connection.execute(
                "UPDATE claim SET expires_at = ? "
                "WHERE id = ? AND owner = ? AND expires_at > ?",
                (now + self.ttl_ms, self.row_id, self.owner, now),
            )

            return cursor.rowcount == 1

    def held(self) -> bool:
        """Whether we hold it right now, unexpired."""
        if self.row_id is None:
            return False

        with self.lock:
            row = self.connection.execute(
                "SELECT owner, expires_at FROM claim WHERE id = ?", (self.row_id,)
            ).fetchone()

        return row is not None and row[0] == self.owner and row[1] > _now_ms()

    def release(self) -> None:
        """Give it up, so the next operation need not wait out the TTL."""
        if self.row_id is None:
            return

        with self.lock:
            self.connection.execute(
                "DELETE FROM claim WHERE id = ? AND owner = ?",
                (self.row_id, self.owner),
            )

        self.row_id = None


def _now_ms() -> int:
    return int(time.time() * 1000)
