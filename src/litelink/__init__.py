"""Durable append-only capture into Iceberg tables.

See ``docs/SPEC.md``. All three tiers are implemented — the SQLite buffer, the
local Iceberg table, and the archive on object storage — and a log survives
losing its machine (``restore``). Schema evolution (§9) is implemented for
``add_column``; blob fields (§15) are specified and are not.

**The log is the directory and the objects in the bucket; these classes are
handles to it.** That is why none of them is called ``Log`` — a class named
after the data invites the question of why a read-only one is a lesser version
of it. Every handle can read, and each subclass only adds:

.. code-block:: text

    LogHandle                    identity, read, observe, close
    ├── LocalReadHandle          + the replication config surface
    │   └── WriteHandle          + append, seal, maintain, sync, ...
    └── RemoteReadHandle         + owns the scratch root it was built in

Nothing inherits a method it has to refuse. Annotate ``LogHandle`` when you do
not care which you were given.

**The local/remote boundary is the constructor you call**, not an argument you
pass:

- ``open`` wants a *root on this machine*, and ``read_only=`` picks the type it
  returns. It reads the log's own directory, sees the writer's commits as they
  land, and takes no ``archive`` argument because the log already records where
  its archive is.
- ``follow`` wants an *archive URI* and no root. It restores the writer's
  buffer from object storage into scratch space and merges it with the archive,
  so it reads a log running on another machine, as of a point in time.

``Row`` and ``S3Options`` are exported because both appear in public
signatures, and a type a caller has to name has to be importable. ``S3Options``
is deliberately not part of ``LogConfig`` — see ``litelink._s3``, which is
where the reasoning lives.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink._assembly import follow, new, open, restore, snapshot  # noqa: A004
from litelink._preflight import Check, Report, preflight
from litelink._s3 import S3Options
from litelink.log import (
    Coverage,
    LocalReadHandle,
    LogConfig,
    LogHandle,
    RemoteReadHandle,
    Row,
    WriteHandle,
)

try:
    __version__ = version("litelink")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "Check",
    "Coverage",
    "LocalReadHandle",
    "LogConfig",
    "LogHandle",
    "RemoteReadHandle",
    "Report",
    "Row",
    "S3Options",
    "WriteHandle",
    "__version__",
    "follow",
    "new",
    "open",
    "preflight",
    "restore",
    "snapshot",
]
