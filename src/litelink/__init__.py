"""Durable append-only capture into Iceberg tables.

See ``docs/SPEC.md``. The local capture loop is implemented; the archive tier
(``sync``) is not — see the README for what runs today.
"""

from importlib.metadata import PackageNotFoundError, version

from litelink.log import Log, LogConfig

try:
    __version__ = version("litelink")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = ["Log", "LogConfig", "__version__"]
