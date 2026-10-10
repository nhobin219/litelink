"""Durable append-only capture into Iceberg tables.

See ``docs/SPEC.md``. All three tiers are implemented — the SQLite buffer, the
staging table, and the published table on object storage — and a log survives
losing its machine (``restore``). A log's schema is fixed when it is created;
changing it means starting a new log (§9). Blob fields (§15) are specified and
are not implemented.

**The log is the directory and the objects in the bucket; these classes are
handles to it.** That is why none of them is called ``Log`` — a class named
after the data invites the question of why a read-only one is a lesser version
of it. Every handle can read, and each subclass only adds:

.. code-block:: text

    LogHandle                    identity, read, observe, close
    └── LocalReadHandle          + the replication config surface
        └── WriteHandle          + append, seal, maintain, publish, ...

Nothing inherits a method it has to refuse. Annotate ``LogHandle`` when you do
not care which you were given.

**Every handle is on the primary**, the host that holds the log's directory.
``open`` wants a *root on this machine*, and ``read_only=`` picks the type it
returns; it reads the buffer, the staging table and the published table, and sees the
writer's commits as they land. Another machine reads the published table with any
Iceberg engine — it is plain Iceberg, published through ``version-hint.text``
— rather than through litelink (#90).

``Row`` and ``S3Options`` are exported because both appear in public
signatures, and a type a caller has to name has to be importable. ``S3Options``
is deliberately not part of ``LogConfig`` — see ``litelink._s3``, which is
where the reasoning lives.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink import manifest
from litelink._assembly import new, open, restore  # noqa: A004
from litelink._buffer import RetiredError
from litelink._handle import (
    OFFSET,
    Coverage,
    LocalReadHandle,
    LogConfig,
    LogHandle,
    Row,
    WriteHandle,
    validate_row,
)
from litelink._preflight import Check, Report, preflight
from litelink._read import (
    ExtensionMissing,
    current_metadata,
    duckdb_connection,
    install_s3_secret,
)
from litelink._retired import delete, truncate
from litelink._s3 import S3Options
from litelink._statistics import ColumnStatistics, Tier, TierStatistics

try:
    __version__ = version("litelink")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = [
    "OFFSET",
    "Check",
    "ColumnStatistics",
    "Coverage",
    "ExtensionMissing",
    "LocalReadHandle",
    "LogConfig",
    "LogHandle",
    "Report",
    "RetiredError",
    "Row",
    "S3Options",
    "Tier",
    "TierStatistics",
    "WriteHandle",
    "__version__",
    "current_metadata",
    "delete",
    "duckdb_connection",
    "install_s3_secret",
    "manifest",
    "new",
    "open",
    "preflight",
    "restore",
    "truncate",
    "validate_row",
]
