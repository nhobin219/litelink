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
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from litelink._s3 import S3Options
from litelink._table import LogTable

if TYPE_CHECKING:
    import pyarrow as pa

    from litelink._layout import Layout


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

    @property
    def uri(self) -> str | None:
        """Where the archive is, or None when there is none."""
        return self._uri

    @property
    def s3(self) -> S3Options:
        """Credentials, for callers that configure their own client — DuckDB's
        S3 secret on the read path. Never persisted; see `_s3`."""
        return self._s3

    def configured(self) -> bool:
        """Whether there is an archive at all. Cheap, and opens nothing."""
        return self._uri is not None

    def set_uri(self, uri: str | None) -> None:
        """Attach, re-point, or detach.

        Drops the open handle. A cached table from the previous URI would
        otherwise keep answering reads and taking pushes after the log has been
        told to use a different one.
        """
        uri = uri or None
        if uri != self._uri:
            self._handle = None

        self._uri = uri

    def table(self) -> LogTable | None:
        """The remote table, or None when unconfigured. Opened on first use.

        Lazy because opening it costs a round trip to object storage, which a
        log that never syncs should not pay and a hot read must not pay at all
        (I5): `include_archive=False` is the default, and a reader that never
        opts in never reaches this.
        """
        if not self.configured():
            return None

        if self._handle is None:
            if self._schema is None:
                msg = "archive was constructed without a schema"
                raise ValueError(msg)

            assert self._uri is not None
            self._handle = LogTable.open_archive(
                self._layout, self._uri, self._s3, self._schema
            )

        return self._handle

    def require(self) -> LogTable:
        """The remote table, insisting there is one.

        For the write side of §5, where "no archive" is a caller error rather
        than a leg of a union to leave out.
        """
        table = self.table()
        if table is None:
            msg = "this log is local-only; no archive is configured"
            raise ValueError(msg)

        return table
