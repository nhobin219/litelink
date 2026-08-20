"""Cross-process ownership of an operation (SPEC §13.6).

§11 records the hazard: opening a log runs recovery, and recovery claims every
interrupted operation including another process's. Verified in both directions —
a maintenance process redoes the writer's in-flight seal, and a writer deletes a
maintenance process's half-written compaction while it is still being written.

The fix is that **recovery ownership follows operation ownership**, and a lease
is what says who owns what. `sealing` belongs to whoever holds the `seal` lease,
`compacting` to whoever holds `maintain`; each recovers its own and leaves the
other alone.

It lives in the buffer database because that is already the coordinator (I16),
and because a lease has to outlive the process holding it — a Python lock cannot
say anything about a process that is no longer running.

**Expiry rather than release** is what makes a crash survivable. A holder that
exits cleanly releases; one that is killed leaves a lease that lapses, after
which another process may take it and replay whatever the first left behind.
That is the same argument as the intent records themselves: the durable state
says what was being attempted, and someone has to be entitled to finish it.
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


@dataclass(frozen=True, slots=True)
class Lease:
    """One process's claim on one role, held in SQLite."""

    connection: sqlite3.Connection
    role: str
    owner: str
    ttl_ms: int = DEFAULT_TTL_MS

    def acquire(self) -> bool:
        """Take the lease if it is free, expired, or already ours.

        One statement, so two processes racing cannot both win: SQLite
        serialises the upsert and the `WHERE` decides. The loser sees
        `rowcount == 0` and knows it does not hold the role.
        """
        now = _now_ms()
        cursor = self.connection.execute(
            "INSERT INTO lease (role, owner, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT(role) DO UPDATE SET owner = excluded.owner, "
            "expires_at = excluded.expires_at "
            "WHERE lease.owner = excluded.owner OR lease.expires_at <= ?",
            (self.role, self.owner, now + self.ttl_ms, now),
        )

        return cursor.rowcount == 1

    def renew(self) -> bool:
        """Extend our claim. False means we no longer hold it.

        A holder that stalls past its TTL can find the lease taken, and must
        find that out rather than carry on: the new holder may already be
        replaying the work it was doing.
        """
        cursor = self.connection.execute(
            "UPDATE lease SET expires_at = ? WHERE role = ? AND owner = ?",
            (_now_ms() + self.ttl_ms, self.role, self.owner),
        )

        return cursor.rowcount == 1

    def held(self) -> bool:
        """Whether we hold it right now, unexpired."""
        row = self.connection.execute(
            "SELECT owner, expires_at FROM lease WHERE role = ?", (self.role,)
        ).fetchone()

        return row is not None and row[0] == self.owner and row[1] > _now_ms()

    def release(self) -> None:
        """Give it up, so the next process need not wait out the TTL."""
        self.connection.execute(
            "DELETE FROM lease WHERE role = ? AND owner = ?", (self.role, self.owner)
        )


def _now_ms() -> int:
    return int(time.time() * 1000)
