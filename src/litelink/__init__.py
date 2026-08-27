"""Durable append-only capture into Iceberg tables.

See ``docs/SPEC.md``. All three tiers are implemented — the SQLite buffer, the
local Iceberg table, and the archive on object storage — and a log survives
losing its machine (``Log.restore``). Schema evolution (§9) and blob fields
(§15) are specified and are not.

``Row`` and ``S3Options`` are exported for the same reason: both appear in
public signatures, and a type a caller has to name has to be importable.

``S3Options`` appears in the signatures of ``new``,
``open``, ``restore`` and ``replication_config_for``: a caller pointing at
anything that is not AWS has to construct one, and a caller annotating against
those signatures has to name it. It is deliberately not part of ``LogConfig`` —
see ``litelink._s3``, which is where the reasoning lives.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink._s3 import S3Options
from litelink.log import Log, LogConfig, Row

try:
    __version__ = version("litelink")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = ["Log", "LogConfig", "Row", "S3Options", "__version__"]
