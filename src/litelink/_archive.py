"""Where the archive is, and the one handle to it (SPEC §5).

Its own object because three collaborators need the same answer and none of
them owns it. The Log decides whether an archive is attached, `Maintenance`
asks whether I4 is owed anything before it evicts, and `Reader` needs a table
handle when a query passes `include_archive`. Held on the Log and reached
through it, that last one is a problem: a reader is constructed by `Log.new`
and `Log.open` and injected into the Log, so at the moment the reader is built
there is no Log to ask. The previous shape resolved it by mutating the reader
after construction with a callback bound to a half-built Log — which works, and
which no reader test can set up without building a Log first.

One injected object instead. `new`/`open` construct it and pass it to all
three, so each is given its archive at construction like every other
collaborator, and `set_archive` has one place to write instead of a fan-out to
keep in step. That the fan-out is gone is not only tidiness: the cached handle
lives here too, so re-pointing the archive drops it, where before the Log
changed the URI and went on serving reads from the table it had already opened.

A local-only log gets one of these with `uri=None`, rather than the three
holders each taking `Archive | None`. It is the difference between "there is no
archive" and "there is nowhere to look yet", and only the second survives
`set_archive`: attaching one to a log that started local has to reach the
reader and the maintainer, and with an optional they would each be holding a
None that nothing can update. This object is the slot, and the slot always
exists — `configured()` is the question about what is in it.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from litelink._s3 import S3Options
from litelink._table import ArchiveAbsent, LogTable

if TYPE_CHECKING:
    import pyarrow as pa

    from litelink._layout import Layout


# Where the archive's location is recorded. It lives here rather than in `log`
# because it is not `Log`'s private business: `evict` acts on I4 and so has to
# be able to ask the buffer, not this object's memory, whether an archive is
# owed anything.
ARCHIVE_KEY = "archive"


class Archive:
    """The remote tier: a URI, credentials, and the table they open."""

    def __init__(
        self,
        layout: Layout,
        uri: str | None = None,
        s3: S3Options | None = None,
        schema: pa.Schema | None = None,
    ) -> None:
        self._layout = layout
        # `or None` throughout: detaching writes an empty string to `meta`
        # rather than deleting the row, and an empty archive is no archive.
        self._uri = uri or None
        self._s3 = s3 or S3Options()
        self._schema = schema
        self._handle: LogTable | None = None
        # Guards all three fields together, because they are one fact. The
        # reader resolves the archive on a query thread while a maintainer
        # syncs on another and `set_archive` can re-point it from a third, and
        # without this the interleaving that matters is: `set_uri` clears the
        # handle, then an open already in flight for the OLD uri stores its
        # result, and every read after that is served from an archive the log
        # has been told to stop using. Two threads opening at once would also
        # each pay the round trip and one would use a handle the other has
        # discarded.
        #
        # Cheap by construction: taken when an archive is opened or
        # re-pointed, never per row and never per query once the handle
        # exists. A leaf in the lock order — nothing here calls back into the
        # Log, the reader or the maintainer — so it can be taken under any of
        # their locks and adds no cycle.
        self._lock = threading.Lock()

    @property
    def uri(self) -> str | None:
        """Where the archive is, or None when there is none."""
        with self._lock:
            return self._uri

    @property
    def s3(self) -> S3Options:
        """Credentials, for callers that configure their own client — DuckDB's
        S3 secret on the read path. Never persisted; see `_s3`."""
        with self._lock:
            return self._s3

    def configured(self) -> bool:
        """Whether there is an archive at all. Cheap, and opens nothing."""
        with self._lock:
            return self._uri is not None

    def refresh(self, recorded: str | None) -> None:
        """Adopt the location the log durably records.

        This object is a process's MEMORY of where the archive is, and another
        process can change where it is. Attaching one to a log a maintainer
        already has open is a supported operation (§13.0), and until the
        maintainer hears about it every reader of this object answers for the
        configuration the log had at open — including `evict`, which asks
        whether I4 owes the archive anything before deleting the only local
        copy of a row. Answered from stale memory, the answer is no.

        The detach direction heals on its own, because a push to an archive
        that is gone fails. The attach direction has nothing that fails.
        """
        self.set_uri(recorded)

    def set_uri(self, uri: str | None) -> None:
        """Attach, re-point, or detach.

        Drops the open handle. A cached table from the previous URI would
        otherwise keep answering reads and taking pushes after the log has been
        told to use a different one.
        """
        uri = uri or None
        # Under the lock, like every other access. Without it, an open already
        # in flight inside `table()` for the PREVIOUS uri stores its result
        # after this has cleared the handle — and every read afterwards is
        # served from an archive the log has been told to stop using, which is
        # the exact interleaving the lock was added for.
        with self._lock:
            if uri != self._uri:
                # The cached handle only. The catalog entry is checked against
                # the prefix when the archive is next opened — `open_archive`
                # explains why a repair that runs every time beats a step here
                # that a crash can skip, and why detaching and reattaching the
                # same archive has to keep working.
                self._handle = None

            self._uri = uri

    def table(self, *, repair: bool = False) -> LogTable | None:
        """The remote table, or None when unconfigured. Opened on first use.

        Lazy because opening it costs a round trip to object storage, which a
        log that never syncs should not pay and a hot read must not pay at all
        (I5): `include_archive=False` is the default, and a reader that never
        opts in never reaches this.
        """
        with self._lock:
            if self._uri is None:
                return None

            if self._handle is None:
                if self._schema is None:
                    msg = "archive was constructed without a schema"
                    raise ValueError(msg)

                # Held across the open, which is a round trip. Deliberate: a
                # second caller arriving mid-open should wait for that handle
                # rather than start a second one, and `set_uri` should wait
                # rather than clear a field the open is about to write.
                try:
                    self._handle = LogTable.open_archive(
                        self._layout, self._uri, self._s3, self._schema, repair=repair
                    )
                except ArchiveAbsent:
                    # Nothing pushed yet. No leg, no error — and nothing
                    # created, because creating a table is the write side's
                    # business and a read must not have side effects.
                    return None

            return self._handle

    def require(self, *, repair: bool = True) -> LogTable:
        """The remote table, insisting there is one.

        For the write side of §5, where "no archive" is a caller error rather
        than a leg of a union to leave out — and where the caller holds the
        maintenance lease, which is what makes it the one allowed to create the
        table or replace an entry naming somewhere else.
        """
        table = self.table(repair=repair)
        if table is None:
            msg = "this log is local-only; no archive is configured"
            raise ValueError(msg)

        return table
