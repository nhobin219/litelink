"""Durable append-only capture into Iceberg tables.

See ``docs/SPEC.md``. All three tiers are implemented — the SQLite buffer, the
local Iceberg table, and the archive on object storage — and a log survives
losing its machine (``Log.restore``). Schema evolution (§9) and blob fields
(§15) are specified and are not.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink.log import Log, LogConfig

try:
    __version__ = version("litelink")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = ["Log", "LogConfig", "__version__"]
