"""Linux allocated-disk accounting outside append-cap measurement windows."""

from __future__ import annotations

import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path


def trial_bytes(trial: Path) -> int:
    """Allocated bytes (GNU du), including directories, SQLite and WAL files."""
    result = subprocess.run(
        ["du", "--summarize", "--block-size=1", "--", str(trial)],
        capture_output=True,
        text=True,
    )
    if result.returncode and not trial.exists():
        # The worker may have removed the trial while the supervisor sampled it.
        return 0

    result.check_returncode()
    return int(result.stdout.split()[0])


def remove_trial(trial: Path, result: dict) -> None:
    """Retain the final footprint before reclaiming a completed/failed trial."""
    try:
        result["trial_du_bytes"] = trial_bytes(trial)
    finally:
        shutil.rmtree(trial, ignore_errors=True)


def disk_snapshot(output: Path) -> dict:
    # Sample du first: a subsequent cleanup can over-credit one sample, rather
    # than mistake the worker's deleted bytes for external pressure. These are
    # diagnostic samples, not an atomic filesystem snapshot or a disk quota.
    du_error = None
    try:
        own_bytes = trial_bytes(output / "trial")
    except subprocess.CalledProcessError as error:
        # A live directory can change during du. Retain df and the error rather
        # than silently charge an unknown footprint to external pressure.
        own_bytes = None
        du_error = error.stderr

    filesystem = (
        subprocess.check_output(
            [
                "df",
                "--block-size=1",
                "--output=size,used,avail,pcent",
                "--",
                str(output.parent),
            ],
            text=True,
        )
        .splitlines()[-1]
        .split()
    )
    total, used, free = map(int, filesystem[:3])
    return {
        "at": datetime.now(UTC).isoformat(),
        "total_bytes": total,
        "used_bytes": used,
        "free_bytes": free,
        "percent_used": int(filesystem[3].rstrip("%")),
        "trial_du_bytes": own_bytes,
        "trial_du_error": du_error,
        # Subtract the reclaimable trial from used space, equivalently add it
        # back to free space. Comparing free - trial would double-charge it.
        "free_excluding_trial_bytes": free + own_bytes
        if own_bytes is not None
        else None,
    }


def below_reserve(snapshot: dict, reserve: int) -> bool:
    available = snapshot["free_excluding_trial_bytes"]
    return available is None or available < reserve
