"""Filesystem primitives with the durability ordering the spec requires."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def fsync(path: Path) -> None:
    """Fsync a file AND the directory entry that reaches it (I1).

    On most filesystems the contents can be durable while the name that reaches
    them is not, so syncing only the file leaves a manifest entry pointing at a
    path that may not exist after a crash.
    """
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
