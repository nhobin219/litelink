"""Durable append-only capture into Iceberg tables.

Specification only — see ``docs/SPEC.md``. Nothing here is implemented yet;
this module exists so the package is importable and the tooling has a target.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("litelink")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "0.0.0"

__all__ = ["__version__"]
