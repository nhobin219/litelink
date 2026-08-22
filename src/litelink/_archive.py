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

A local-only log gets one of these too, rather than the three
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

    from litelink._buffer import Buffer
    from litelink._layout import Layout


# Where the archive's location is recorded. It lives here rather than in `log`
# because it is not `Log`'s private business: `evict` acts on I4 and so has to
# be able to ask the buffer, not this object's memory, whether an archive is
# owed anything.
ARCHIVE_KEY = "archive"

# Distinguishes "no location given" from "the log records no archive", which is
# a real value meaning detached.
_UNSET = "\x00unset"


class Archive:
    """Where the archive is, and the handle onto it.

    **It holds no copy of the location.** That is the whole design of this
    object now, and it is the answer to a defect found in eight review rounds
    running: the location lived here as a field, kept in step with the log by a
    `refresh` call at every decision that depended on it, and each round found
    a decision that had no such call — a pass reading "no archive" from memory
    while the log had one, a repairing open pointed at a bucket the log had
    left, a fence comparing a value against itself because a re-point moved
    both sides of the comparison together.

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
        schema: pa.Schema | None = None,
    ) -> None:
        self._layout = layout
        self._buffer = buffer
        self._s3 = s3 or S3Options()
        self._schema = schema
        self._handle: LogTable | None = None
        # What the cached handle was opened FOR. Keying the cache on the
        # durable value is what keeps it from becoming the stale copy this
        # class was rewritten to remove: when the log is re-pointed, the key
        # changes and the handle is retired rather than surviving the move.
        self._handle_uri: str | None = None
        # Guards the handle and its key together, because they are one fact.
        # The reader resolves the archive on a query thread while a maintainer
        # syncs on another and `set_archive` re-points it from a third.
        self._lock = threading.RLock()

    def location(self) -> str | None:
        """Where the archive is, according to the log.

        `or None` because detaching writes an empty string to `meta` rather
        than deleting the row, and an empty archive is no archive.
        """
        return self._buffer.get_meta(ARCHIVE_KEY) or None

    @property
    def uri(self) -> str | None:
        """The location, for callers that read it as an attribute."""
        return self.location()

    @property
    def s3(self) -> S3Options:
        """Credentials and endpoint, from the environment at the point of use.

        Never persisted: a log directory gets copied and attached elsewhere,
        and must not carry a key with it.
        """
        return self._s3

    def configured(self) -> bool:
        """Whether the log has an archive at all. Opens nothing."""
        return self.location() is not None

    def table(self, *, repair: bool = False) -> LogTable | None:
        """The remote table, or None when the log has no archive.

        `repair` lets `open_archive` drop a catalog entry naming another prefix
        and create a fresh table at this one. The maintenance claim is what
        entitles a caller to that; the location the LOG records is what says
        which archive to do it to — and reading that location here, rather than
        trusting one handed in at construction, is what makes the two
        inseparable.
        """
        with self._lock:
            uri = self.location()
            if uri is None:
                self._handle = None
                self._handle_uri = None

                return None

            if self._handle is None or self._handle_uri != uri:
                try:
                    self._handle = LogTable.open_archive(
                        self._layout, uri, self._s3, self._schema, repair=repair
                    )
                except ArchiveAbsent:
                    return None

                self._handle_uri = uri

            return self._handle

    def require(self, *, repair: bool = True) -> LogTable:
        """The remote table, insisting there is one.

        For the write side of §5, where "no archive" is a caller error rather
        than a leg of a union to leave out.
        """
        table = self.table(repair=repair)
        if table is None:
            msg = "this log has no archive configured"
            raise ValueError(msg)

        return table
