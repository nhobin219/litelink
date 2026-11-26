"""Supervise append_cap.py without changing its workload or statistical method."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from _append_cap_disk import below_reserve, disk_snapshot


def write_status(path: Path, status: dict) -> None:
    status["updated_at"] = datetime.now(UTC).isoformat()
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(json.dumps(status, indent=2))
    temporary.replace(path)


def stop(process: subprocess.Popen) -> None:
    # Only this controller and its workers belong to the new process group.
    if process.poll() is not None:
        return

    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=20)


def supervise(
    command: list[str],
    output: Path,
    status_path: Path,
    interval: float,
    reserve: int,
    deadline: float,
) -> int:
    snapshot = disk_snapshot(output)
    status = {
        "state": "running",
        "command": command,
        "reserve_bytes": reserve,
        "interval_seconds": interval,
        "deadline_seconds": deadline,
        "disk_at_launch": snapshot,
        "disk": snapshot,
    }
    write_status(status_path, status)
    if below_reserve(snapshot, reserve):
        status.update(
            state="aborted",
            abort_reason="trial disk accounting failed"
            if snapshot.get("trial_du_error")
            else "external disk reserve",
            exit_code=1,
        )
        status["disk_at_abort"] = snapshot
        write_status(status_path, status)
        return 1

    started = time.monotonic()
    with subprocess.Popen(command, start_new_session=True) as process:
        try:
            while True:
                try:
                    code = process.wait(timeout=interval)
                    break
                except subprocess.TimeoutExpired:
                    snapshot = disk_snapshot(output)
                    status["disk"] = snapshot
                    if below_reserve(snapshot, reserve):
                        status["abort_reason"] = (
                            "trial disk accounting failed"
                            if snapshot.get("trial_du_error")
                            else "external disk reserve"
                        )
                    elif time.monotonic() - started >= deadline:
                        status["abort_reason"] = "internal deadline"

                    if "abort_reason" in status:
                        status["disk_at_abort"] = snapshot
                        status["state"] = "aborting"
                        write_status(status_path, status)
                        stop(process)
                        code = process.returncode
                        break

                    write_status(status_path, status)
        finally:
            stop(process)

    snapshot = disk_snapshot(output)
    if code != 0:
        status.setdefault("disk_at_abort", snapshot)

    status.update(
        state="completed" if code == 0 else "aborted",
        exit_code=code,
        disk=snapshot,
    )
    write_status(status_path, status)
    return 0 if code == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--reserve-bytes", type=int, default=3 * 1024**3)
    parser.add_argument("--deadline-seconds", type=float, default=46 * 3600)
    parser.add_argument("controller_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.status = args.status.resolve()
    controller_args = args.controller_args
    if controller_args[:1] == ["--"]:
        controller_args = controller_args[1:]

    if any(a == "--output" or a.startswith("--output=") for a in controller_args):
        parser.error("supply --output only to the supervisor")

    if args.interval <= 0 or args.reserve_bytes < 0 or args.deadline_seconds <= 0:
        parser.error("interval/deadline must be positive and reserve nonnegative")

    if args.output.exists():
        parser.error("output must be a fresh path")

    if args.status.exists() or args.status.is_relative_to(args.output):
        parser.error("status must be a fresh path outside output")

    command = [
        sys.executable,
        "-u",
        str(Path(__file__).with_name("append_cap.py")),
        "--output",
        str(args.output),
        *controller_args,
    ]
    return supervise(
        command,
        args.output,
        args.status,
        args.interval,
        args.reserve_bytes,
        args.deadline_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
