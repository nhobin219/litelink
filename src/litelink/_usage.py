"""The committed buffer occupancy and its write limits."""

from dataclasses import dataclass


class LogFull(RuntimeError):
    """The whole append exceeds a cap; eviction helps only if ``fits_empty``."""

    def __init__(
        self,
        *,
        batch_rows: int,
        batch_bytes: int,
        buffer_max_rows: int | None,
        buffer_max_bytes: int,
    ) -> None:
        self.batch_rows = batch_rows
        self.batch_bytes = batch_bytes
        self.buffer_max_rows = buffer_max_rows
        self.buffer_max_bytes = buffer_max_bytes
        self.fits_empty = batch_bytes <= buffer_max_bytes and (
            buffer_max_rows is None or batch_rows <= buffer_max_rows
        )
        super().__init__(
            "append would exceed the buffer write limit: "
            f"batch_rows={batch_rows}, batch_bytes={batch_bytes}, "
            f"buffer_max_rows={buffer_max_rows}, buffer_max_bytes={buffer_max_bytes}, "
            f"fits_empty={self.fits_empty}; "
            + (
                "retry after eviction"
                if self.fits_empty
                else "shrink the batch or raise the cap before retrying"
            )
        )


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
