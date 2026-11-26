"""Offline checks for the matrix, schedule and statistical decision."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import _append_cap_disk as disk
import append_cap as controller
import append_cap_supervise as supervisor
import pytest
from append_cap import abba, bootstrap, cell_name, gate, matrix


def test_cap_cli_reaches_workers_and_all_result_metadata(tmp_path, monkeypatch):
    cap = 17179869184
    requests = []

    class Worker:
        def __init__(self, *args, **kwargs):
            self.stdin = io.StringIO()
            self.stdout = self

        def readline(self):
            request = json.loads(self.stdin.getvalue().splitlines()[-1])
            requests.append(request)
            return json.dumps(
                {
                    "buffer_max_bytes": request.get("buffer_max_bytes"),
                    "rows_s": 1,
                    "p99_us": 1,
                    "window_complete": True,
                }
            )

        def wait(self, timeout):
            return 0

    output = tmp_path / "results"
    monkeypatch.setattr(
        controller, "source", lambda *args: (tmp_path, {"source_sha256": "same"})
    )
    monkeypatch.setattr(
        controller,
        "subprocess",
        SimpleNamespace(
            Popen=Worker, PIPE=subprocess.PIPE, TimeoutExpired=subprocess.TimeoutExpired
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "append_cap.py",
            "--off",
            "OFF",
            "--on",
            "ON",
            "--consumer-repo",
            str(tmp_path),
            "--output",
            str(output),
            "--buffer-max-bytes",
            str(cap),
            "--smoke",
            "--repetitions",
            "1",
            "--bootstrap",
            "10",
            "--cells",
            "direct/0/16/0",
        ],
    )
    controller.main()
    assert len(requests) == 8
    assert all(request["buffer_max_bytes"] == cap for request in requests)
    environment = json.loads((output / "environment.json").read_text())
    assert environment["options"]["buffer_max_bytes"] == cap
    rows = [
        json.loads(line) for line in (output / "runs.jsonl").read_text().splitlines()
    ]
    assert len(rows) == 8
    assert all(row["buffer_max_bytes"] == cap for row in rows)
    assert json.loads((output / "summary.json").read_text())["buffer_max_bytes"] == cap


@pytest.mark.parametrize("cap", [0, (8 << 30) - 1])
def test_cap_below_seal_target_is_rejected_before_setup(
    tmp_path, monkeypatch, capsys, cap
):
    output = tmp_path / "results"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "append_cap.py",
            "--off",
            "OFF",
            "--on",
            "ON",
            "--consumer-repo",
            str(tmp_path),
            "--output",
            str(output),
            "--buffer-max-bytes",
            str(cap),
        ],
    )
    with pytest.raises(SystemExit) as error:
        controller.main()

    assert error.value.code == 2
    assert "--buffer-max-bytes must be >=8589934592" in capsys.readouterr().err
    assert not output.exists()


@pytest.fixture
def worker_module(monkeypatch):
    # These tests exercise harness construction/protocol without installing the
    # optional broker stack or writing gigabytes to reach the real cap.
    class LogFull(Exception):
        pass

    library = SimpleNamespace(
        LogConfig=lambda **kwargs: SimpleNamespace(**kwargs),
        LogFull=LogFull,
        __file__="library.py",
    )
    modules = {
        "pyarrow": MagicMock(),
        "streamcast": MagicMock(__file__="consumer.py"),
        "uvicorn": MagicMock(),
        "litelink": library,
        "litelink._buffer": SimpleNamespace(_BUFFER_USAGE_ENABLED=False),
    }
    original = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: modules[name] if name in modules else original(name),
    )
    spec = importlib.util.spec_from_file_location(
        "cap_worker", Path(__file__).with_name("_append_cap_worker.py")
    )
    assert spec is not None and spec.loader is not None
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    return worker


@pytest.mark.parametrize("variant", ["BASE", "OFF", "ON"])
@pytest.mark.parametrize("kind", ["direct", "e2e"])
def test_worker_configures_cap_in_both_paths(
    worker_module, monkeypatch, tmp_path, variant, kind
):
    worker = worker_module
    worker.VARIANT = variant
    cap = 17179869184
    configs = []

    def created(*args, **kwargs):
        configs.append(kwargs["config"] if kind == "direct" else args[0].storage.config)
        raise RuntimeError("construction observed")

    monkeypatch.setattr(worker.litelink, "new", created, raising=False)
    worker.streamcast.Stream = lambda **kwargs: SimpleNamespace(**kwargs)
    worker.streamcast.Storage = lambda **kwargs: SimpleNamespace(**kwargs)
    worker.streamcast.StreamHandle.open = created
    request = {
        "root": str(tmp_path / "trial"),
        "cell": {"kind": kind, "payload": 0, "batch": 16, "group": False},
        "buffer_max_bytes": cap,
    }
    with pytest.raises(RuntimeError, match="construction observed"):
        if kind == "direct":
            worker.direct(request)
        else:
            asyncio.run(worker.e2e(request))

    assert len(configs) == 1
    assert configs[0].target_seal_size == 8 << 30
    if variant == "BASE":
        assert not hasattr(configs[0], "buffer_max_bytes")
    else:
        assert configs[0].buffer_max_bytes == cap


def test_worker_cap_refusal_is_actionable_and_retained(
    worker_module, monkeypatch, tmp_path
):
    worker = worker_module
    worker.VARIANT = "ON"
    request = {
        "root": str(tmp_path / "trial"),
        "cell": {"kind": "direct", "payload": 2000, "batch": 4096, "group": False},
        "buffer_max_bytes": 8 << 30,
    }

    def full(request):
        raise worker.litelink.LogFull("append would exceed the buffer write limit")

    monkeypatch.setattr(worker, "direct", full)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(request) + "\n"))
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    worker.main()
    result = json.loads(output.getvalue())
    assert result["error_type"] == "harness_config"
    assert result["buffer_max_bytes"] == 8589934592
    assert "--buffer-max-bytes" in result["error"]
    assert "fresh output" in result["error"]
    assert "LogFull" in result["error"]


def test_controller_saves_worker_failure_before_nonzero_exit(tmp_path, monkeypatch):
    cap = 8 << 30
    output = tmp_path / "results"

    class Worker:
        def __init__(self, *args, **kwargs):
            self.stdin = io.StringIO()
            self.stdout = io.StringIO(
                json.dumps(
                    {
                        "error": "buffer cap exhausted; use --buffer-max-bytes",
                        "error_type": "harness_config",
                        "buffer_max_bytes": cap,
                    }
                )
                + "\n"
            )

        def wait(self, timeout):
            return 0

    monkeypatch.setattr(
        controller, "source", lambda *args: (tmp_path, {"source_sha256": "same"})
    )
    monkeypatch.setattr(
        controller,
        "subprocess",
        SimpleNamespace(
            Popen=Worker, PIPE=subprocess.PIPE, TimeoutExpired=subprocess.TimeoutExpired
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "append_cap.py",
            "--off",
            "OFF",
            "--on",
            "ON",
            "--consumer-repo",
            str(tmp_path),
            "--output",
            str(output),
            "--smoke",
            "--repetitions",
            "1",
            "--bootstrap",
            "10",
            "--cells",
            "direct/2000/4096/0",
        ],
    )
    with pytest.raises(
        RuntimeError, match="OFF direct/2000/4096/0.*buffer cap exhausted"
    ):
        controller.main()

    rows = [
        json.loads(line) for line in (output / "runs.jsonl").read_text().splitlines()
    ]
    assert len(rows) == 1
    assert rows[0]["error_type"] == "harness_config"
    assert rows[0]["buffer_max_bytes"] == cap
    assert rows[0]["cell"]["batch"] == 4096
    assert not (output / "summary.json").exists()


def observations(rate_ratio: float, tail_ratio: float) -> list[dict]:
    rows = []
    for rep in range(15):
        # Shared drift differs by orders of magnitude: independent resampling
        # would lose pairing and turn these exact ratios into broad intervals.
        drift = 2.0**rep
        for reference in ("OFF", "BASE"):
            for variant in abba(reference, bool(rep % 2)):
                rows.append(
                    {
                        "rep": rep,
                        "reference": reference,
                        "variant": variant,
                        "rows_s": drift * (rate_ratio if variant == "ON" else 1),
                        "p99_us": drift * (tail_ratio if variant == "ON" else 1),
                    }
                )

    return rows


def test_matrix_and_balanced_schedule():
    cells = matrix([1, 16, 256, 4096])
    assert len(cells) == len({cell_name(c) for c in cells}) == 18
    assert sum(c["kind"] == "e2e" or c["batch"] >= 16 for c in cells) == 14
    assert {c["group"] for c in cells if c["kind"] == "e2e"} == {False, True}
    for reference in ("OFF", "BASE"):
        assert abba(reference, False) == (reference, "ON", "ON", reference)
        assert abba(reference, True) == ("ON", reference, reference, "ON")


def test_bootstrap_preserves_pairs_and_reports_absolute_intervals():
    report = bootstrap(observations(1, 1), 100, 447221)
    assert report["estimates"]["BASE"]["rows_s"]["median"] == 128
    assert report["estimates"]["BASE"]["rows_s"]["ci95"][0] < 128
    assert report["estimates"]["BASE"]["rows_s"]["ci95"][1] > 128
    for comparison in report["comparisons"].values():
        assert comparison["rows_s"] == {"ratio": 1, "ci95": [1, 1]}
        assert comparison["p99_us"] == {"ratio": 1, "ci95": [1, 1]}
        assert comparison["pass"]


@pytest.mark.parametrize("rate,tail", [(0.97, 1), (1, 1.06)])
def test_each_metric_can_fail_the_gate(rate, tail):
    report = bootstrap(observations(rate, tail), 100, 447221)
    assert all(not c["pass"] for c in report["comparisons"].values())


def test_both_ci_bounds_must_clear_thresholds():
    assert gate({"rows_s": {"ci95": [0.98, 1.1]}, "p99_us": {"ci95": [0.9, 1.05]}})
    assert not gate({"rows_s": {"ci95": [0.979, 1.1]}, "p99_us": {"ci95": [0.9, 1]}})
    assert not gate({"rows_s": {"ci95": [1, 1.1]}, "p99_us": {"ci95": [0.9, 1.051]}})


def test_trial_du_is_recorded_before_removal(tmp_path):
    trial = tmp_path / "trial"
    trial.mkdir()
    (trial / "buffer.db").write_bytes(b"x" * 16384)
    (trial / "buffer.db-wal").write_bytes(b"x" * 8192)
    expected = int(subprocess.check_output(["du", "-s", "-B1", str(trial)]).split()[0])
    assert expected >= 24576
    result = {"rows": 1}
    disk.remove_trial(trial, result)
    assert result == {"rows": 1, "trial_du_bytes": expected}
    assert not trial.exists()
    assert disk.trial_bytes(trial) == 0


def test_disk_snapshot_separates_trial_from_external_pressure(tmp_path, monkeypatch):
    output = tmp_path / "matrix"
    trial_sizes = iter([4500000000, 1000000000, 0])

    def du(trial):
        assert trial == output / "trial"
        return next(trial_sizes)

    def df(command, **kwargs):
        assert command == [
            "df",
            "--block-size=1",
            "--output=size,used,avail,pcent",
            "--",
            str(tmp_path),
        ]
        return "1B-blocks Used Avail Use%\n10000000000 8000000000 2000000000 80%\n"

    monkeypatch.setattr(disk, "trial_bytes", du)
    monkeypatch.setattr(disk.subprocess, "check_output", df)
    own_pressure = disk.disk_snapshot(output)
    assert own_pressure["free_bytes"] == 2000000000
    assert own_pressure["percent_used"] == 80
    assert own_pressure["trial_du_bytes"] == 4500000000
    assert own_pressure["free_excluding_trial_bytes"] == 6500000000
    assert own_pressure["at"]
    assert not disk.below_reserve(own_pressure, 3 * 1024**3)
    external_pressure = disk.disk_snapshot(output)
    assert disk.below_reserve(external_pressure, 3 * 1024**3)
    no_trial = disk.disk_snapshot(output)
    assert no_trial["free_excluding_trial_bytes"] == no_trial["free_bytes"]
    assert not disk.below_reserve(no_trial, 2000000000)
    assert disk.below_reserve(no_trial, 2000000001)


def test_supervisor_retains_launch_periodic_and_abort_disk(tmp_path, monkeypatch):
    output = tmp_path / "matrix"
    status_path = tmp_path / "status.json"
    samples = iter(
        [
            {"free_excluding_trial_bytes": 10, "free_bytes": 10, "percent_used": 20},
            {"free_excluding_trial_bytes": 10, "free_bytes": 2, "percent_used": 90},
            {"free_excluding_trial_bytes": 2, "free_bytes": 1, "percent_used": 95},
            {"free_excluding_trial_bytes": 2, "free_bytes": 2, "percent_used": 90},
        ]
    )
    saved = []
    original_write = supervisor.write_status

    def save(path, status):
        original_write(path, status)
        saved.append(json.loads(path.read_text()))

    class Process:
        pid = 12345
        returncode = None

        def __init__(self, command, **kwargs):
            assert command == ["controller"]
            assert kwargs == {"start_new_session": True}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            assert self.returncode == -15

        def wait(self, timeout):
            if self.returncode is None:
                raise subprocess.TimeoutExpired("controller", timeout)

            return self.returncode

    def stop(process):
        before_stop = json.loads(status_path.read_text())
        assert before_stop["state"] == "aborting"
        assert before_stop["disk_at_abort"]["free_bytes"] == 1
        process.returncode = -15

    monkeypatch.setattr(supervisor, "disk_snapshot", lambda path: next(samples))
    monkeypatch.setattr(supervisor, "write_status", save)
    monkeypatch.setattr(supervisor.subprocess, "Popen", Process)
    monkeypatch.setattr(supervisor, "stop", stop)
    assert supervisor.supervise(["controller"], output, status_path, 30, 3, 3600) == 1
    assert len(saved) == 4
    assert saved[0]["disk_at_launch"]["free_bytes"] == 10
    assert saved[1]["state"] == "running"
    assert saved[1]["disk"]["free_bytes"] == 2
    assert saved[1]["disk"]["percent_used"] == 90
    assert saved[2]["abort_reason"] == "external disk reserve"
    assert saved[2]["disk_at_abort"]["free_bytes"] == 1
    assert saved[2]["disk_at_abort"]["percent_used"] == 95
    assert saved[2]["disk_at_launch"] == saved[0]["disk_at_launch"]
    assert saved[2]["state"] == "aborting"
    assert saved[3]["state"] == "aborted"


def test_supervisor_launch_abort_and_completed_child(tmp_path):
    output = tmp_path / "matrix"
    rejected = tmp_path / "rejected.json"
    # An impossible reserve refuses to start even a command that does not exist.
    assert (
        supervisor.supervise(["missing-controller"], output, rejected, 1, 10**30, 1)
        == 1
    )
    aborted = json.loads(rejected.read_text())
    assert aborted["disk_at_abort"] == aborted["disk_at_launch"]
    assert aborted["disk_at_abort"]["free_bytes"] > 0
    assert 0 <= aborted["disk_at_abort"]["percent_used"] <= 100
    completed = tmp_path / "completed.json"
    command = [sys.executable, "-c", "pass"]
    assert supervisor.supervise(command, output, completed, 30, 0, 60) == 0
    status = json.loads(completed.read_text())
    assert status["state"] == "completed"
    assert status["exit_code"] == 0


def test_failed_du_keeps_df_and_aborts_with_accounting_reason(tmp_path, monkeypatch):
    def failed_du(trial):
        raise subprocess.CalledProcessError(1, ["du"], stderr="unreadable trial")

    monkeypatch.setattr(disk, "trial_bytes", failed_du)
    status_path = tmp_path / "status.json"
    assert (
        supervisor.supervise(
            ["missing-controller"], tmp_path / "matrix", status_path, 30, 0, 60
        )
        == 1
    )
    status = json.loads(status_path.read_text())
    assert status["abort_reason"] == "trial disk accounting failed"
    snapshot = status["disk_at_abort"]
    assert snapshot["trial_du_bytes"] is None
    assert snapshot["free_excluding_trial_bytes"] is None
    assert snapshot["trial_du_error"] == "unreadable trial"
    assert snapshot["free_bytes"] > 0
    assert 0 <= snapshot["percent_used"] <= 100
