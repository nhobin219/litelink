"""The committed buffer occupancy and its write limits."""

from dataclasses import dataclass


class LogFull(RuntimeError):
    """The entire append would exceed a buffer cap; retry after eviction."""


@dataclass(frozen=True, slots=True)
class BufferUsage:
    """A single committed metadata snapshot, in uncompressed row bytes.

    ``refusing`` means an enabled cap is reached. A larger batch can be
    refused below the cap; refusal never latches after occupancy falls.
    """

    bytes: int
    rows: int
    buffer_max_bytes: int
    buffer_max_rows: int | None
    refusing: bool
