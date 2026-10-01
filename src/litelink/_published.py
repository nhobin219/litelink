"""Where the published table is, and the one handle to it (SPEC §5).

Its own object because three collaborators need the same answer and none of them
owns it. The handle decides where the published table is, `Maintenance` asks
whether I4 is owed anything before it evicts, and `Reader` needs a table handle
when a query needs the published table. Held on the handle and reached through
it, that last one is a problem: a reader is constructed by `litelink.new` and
`litelink.open` and injected into the handle, so at the moment the reader is
built there is no handle to ask. The previous shape resolved it by mutating the
reader after construction with a callback bound to a half-built Log — which
works, and which no reader test can set up without building a Log first.

One injected object instead. `new`/`open` construct it and pass it to all
three, so each is given its published table at construction like every other
collaborator, and `set_published` has one place to write instead of a fan-out
to keep in step. That the fan-out is gone is not only tidiness: the cached
handle lives here too, so re-pointing the published table drops it, where
before the Log changed the URI and went on serving reads from the table it had
already opened.

Every log has a published table (#98): a remote one on S3, or a local directory
— by default under the log's own. `remote()` is the one question about which,
and it decides only what object storage needs: httpfs and credentials on a read,
and a target for the WAL replica.
"""

from __future__ import annotations

import contextlib
import threading
from typing import TYPE_CHECKING

from litelink._layout import is_remote
from litelink._s3 import S3Options
from litelink._table import LogTable, PublishedAbsent

if TYPE_CHECKING:
    from collections.abc import Sequence

    from litelink._buffer import Buffer
    from litelink._layout import Layout


# Where the published table's location is recorded. It lives here rather than
# in `_handle` because it is not `WriteHandle`'s private business: `evict` acts
# on I4 and so has to be able to ask the buffer, not this object's memory,
# whether the published table is owed anything.
PUBLISHED_KEY = "published"


class Published:
    """Where the published table is, and the handle onto it.

    **It holds no copy of the location.** That is the whole design of this
    object now, and it is the answer to a defect found in eight review rounds
    running: the location lived here as a field, kept in step with the log by a
    `refresh` call at every decision that depended on it, and each round found
    a decision that had no such call — a pass reading "no published table"
    from memory while the log had one, a repairing open pointed at a bucket
    the log had left, a fence comparing a value against itself because a
    re-point moved both sides of the comparison together.

    A design whose correctness needs N refresh calls is always one short
    somewhere, because nothing tells you what N is. So there is one copy, in
    `meta`, and this reads it: 1.8 us against a 5.4 ms query. A stale location
    is no longer a bug to guard against; it is not a thing that can exist.
    """

    def __init__(
        self,
        layout: Layout,
        buffer: Buffer,
        s3: S3Options | None = None,
    ) -> None:
        self._layout = layout
        self._buffer = buffer
        self._s3 = s3 or S3Options()
        # The shape this would create a table with is NOT held here. It is
        # read from the buffer at use, because it is the one stale copy that
        # cannot be repaired afterwards: `open_published` hands it to
        # `create_table`, so a published table attached after a schema change
        # would be born with the OLD columns, and nothing in `src/` re-declares
        # an existing table. Every later push then fails, I4 pins eviction, and
        # local disk grows without bound. The clustering goes the same way,
        # which `main` arrived at independently for `sort_by`.
        self._handle: LogTable | None = None
        # What the cached handle was opened FOR. Keying the cache on the
        # durable value is what keeps it from becoming the stale copy this
        # class was rewritten to remove: when the log is re-pointed, the key
        # changes and the handle is retired rather than surviving the move.
        self._handle_uri: str | None = None
        # Guards the handle and its key together, because they are one fact.
        # The reader resolves the published table on a query thread while a
        # maintainer publishes on another and `set_published` re-points it from
        # a third.
        self._lock = threading.RLock()

    def redeclare_sort_order(self, sort_by: Sequence[str]) -> None:
        """Push a clustering onto an already-open handle.

        TAKES the order rather than reading it, and that is what lets
        `set_sort_by` call this BEFORE it writes `meta` — the ordering the
        staging half already depends on. Reading it here would force the call
        after the row, and `open_published` declares an order only on the table
        it CREATES, so a crash in that gap would leave an existing published
        table declaring the old key for ever while every file pushed into it
        was clustered by the new one. Nothing re-declares a published table
        that already exists.

        An argument is not the second home this class shed. The field was: it
        stayed at whatever the process opened with, so a published table created
        after a re-sort was born declaring the old key. `table` reads `meta`
        when it opens a handle, so creation is correct by construction and this
        covers the table that already exists.

        OPENS one rather than settling for a handle this process happens to
        hold. An earlier version read `self._handle`, and `set_sort_by` never
        opens the published table itself — `validate` reads only the URI — so a
        re-sort from a process that had not touched the published table left its
        declaration stale for ever, successfully and silently. Review caught the
        gap and the docstring that admitted it.

        `repair=False` never creates: a published table that does not exist
        yet is left alone, and the one created later is created from `meta`,
        which by then holds the new order.

        Best effort against the published half. The published table may be
        unreachable, and a re-sort is a local operation that has already
        rewritten every staging file by the time this runs; failing it here
        would report a failure that did not happen.
        """
        with contextlib.suppress(Exception):
            handle = self.table()
            if handle is not None:
                handle.set_sort_order(sort_by)

    def location(self) -> str:
        """Where the published table is, according to the log.

        Every log has one (#98). An empty or missing row — what `set_published
        (None)` writes, and what a log created without one holds — means the
        local default under the log's directory.
        """
        return self._buffer.get_meta(PUBLISHED_KEY) or self._layout.default_published

    @property
    def uri(self) -> str:
        """The location, for callers that read it as an attribute."""
        return self.location()

    def remote(self) -> bool:
        """Whether the published table is object storage — what httpfs,
        credentials and the WAL replica need — rather than a local directory."""
        return is_remote(self.location())

    @property
    def s3(self) -> S3Options:
        """Credentials and endpoint, from the environment at the point of use.

        Never persisted: a log directory gets copied and attached elsewhere,
        and must not carry a key with it.
        """
        return self._s3

    def table(self, *, repair: bool = False) -> LogTable | None:
        """The published table, or None when none exists at its location yet —
        nothing has published there, and `repair` is off (`PublishedAbsent`).

        `repair` lets `open_published` drop a catalog entry naming another
        prefix and create a fresh table at this one. The maintenance claim is
        what entitles a caller to that; the location the LOG records is what
        says which published table to do it to — and reading that location
        here, rather than trusting one handed in at construction, is what
        makes the two inseparable.
        """
        with self._lock:
            uri = self.location()
            if self._handle is None or self._handle_uri != uri:
                try:
                    self._handle = LogTable.open_published(
                        self._layout,
                        uri,
                        self._s3,
                        self._buffer.shape().table,
                        self._buffer.sort_by(),
                        repair=repair,
                    )
                except PublishedAbsent:
                    return None

                self._handle_uri = uri

            return self._handle

    def adopt(self, uri: str) -> LogTable:
        """Open the table at `uri` with repair on — creating it, or adopting
        it through its `version-hint.text` — and point the catalog at it.

        For `set_published`, BEFORE the log records `uri`: a move that cannot
        reach its new table fails with nothing written, rather than leaving
        the catalog naming the old one. The handle is cached against `uri`, so
        once the location is recorded every caller gets it.
        """
        with self._lock:
            self._handle = LogTable.open_published(
                self._layout,
                uri,
                self._s3,
                self._buffer.shape().table,
                self._buffer.sort_by(),
                repair=True,
            )
            self._handle_uri = uri

            return self._handle

    def require(self, *, repair: bool = True) -> LogTable:
        """The published table, insisting there is one.

        For the write side of §5, where "no published table" is a caller error
        rather than a leg of a union to leave out.
        """
        table = self.table(repair=repair)
        if table is None:
            msg = "this log has no published table configured"
            raise ValueError(msg)

        return table
