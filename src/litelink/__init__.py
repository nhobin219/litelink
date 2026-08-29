"""Durable append-only capture into Iceberg tables.

See ``docs/SPEC.md``. All three tiers are implemented — the SQLite buffer, the
local Iceberg table, and the archive on object storage — and a log survives
losing its machine (``restore``). Schema evolution (§9) is implemented for
``add_column``; blob fields (§15) are specified and are not.

**Two objects, and the constructors say which one you get.** ``Log`` is the
writer: it appends, seals, maintains and syncs. ``LogReader`` is everything you
can ask a log without writing to it, and it is what ``reader`` and ``follow``
return. There is no mode flag — ``Log.open(read_only=True)`` used to return a
``Log`` whose thirteen write methods raised, and both it and the flag are gone.

**The local/remote boundary is the constructor you call**, not an argument you
pass:

- ``open`` and ``reader`` want a *root on this machine*. They read the log's
  own directory, see the writer's commits as they land, and take no ``archive``
  argument because the log already records where its archive is.
- ``follow`` wants an *archive URI* and no root. It restores the writer's
  buffer from object storage into scratch space and merges it with the archive,
  so it reads a log running on another machine, as of a point in time.

``Row`` and ``S3Options`` are exported because both appear in public
signatures, and a type a caller has to name has to be importable. ``S3Options``
is deliberately not part of ``LogConfig`` — see ``litelink._s3``, which is
where the reasoning lives.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink._assembly import follow, new, open, reader, restore  # noqa: A004
from litelink._s3 import S3Options
from litelink.log import Coverage, Log, LogConfig, LogReader, Row

try:
    __version__ = version("litelink")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "Coverage",
    "Log",
    "LogConfig",
    "LogReader",
    "Row",
    "S3Options",
    "__version__",
    "follow",
    "new",
    "open",
    "reader",
    "restore",
]
