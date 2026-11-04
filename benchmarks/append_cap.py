"""Rebuilt append-cap matrix controller; see append_cap/README.md for provenance."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import tarfile
from contextlib import ExitStack
from pathlib import Path
from typing import Any

BASE = "b1b4e3506e157507f1fc3a72e586451a954fcaed"
CONSUMER = "84e86d533be43945673aa8e374c7cb5cf8931ff0"
VARIANTS = ("BASE", "OFF", "ON")


def matrix(batches: list[int]) -> list[dict[str, Any]]:
    cells = [
        {"kind": "direct", "batch": b, "payload": p, "group": g}
        for p, b, g in itertools.product((0, 2000), batches, (False, True))
    ]
    return cells + [
        {"kind": "e2e", "batch": 256, "payload": 0, "group": g} for g in (False, True)
    ]


def abba(reference: str, reverse: bool) -> tuple[str, ...]:
    a, b = ("ON", reference) if reverse else (reference, "ON")
    return a, b, b, a


def percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def interval(values: list[float]) -> list[float]:
    return [percentile(values, 0.025), percentile(values, 0.975)]


def bootstrap(rows: list[dict[str, Any]], samples: int, seed: int) -> dict[str, Any]:
    """Resample whole repetitions, keeping ABBA observations paired."""
    reps = sorted({r["rep"] for r in rows})
    by_variant = {
        v: {
            metric: [
                statistics.median(
                    r[metric] for r in rows if r["variant"] == v and r["rep"] == rep
                )
                for rep in reps
            ]
            for metric in ("rows_s", "p99_us")
        }
        for v in VARIANTS
    }
    paired = {
        ref: {
            v: {
                metric: [
                    statistics.median(
                        r[metric]
                        for r in rows
                        if r["variant"] == v
                        and r["rep"] == rep
                        and r["reference"] == ref
                    )
                    for rep in reps
                ]
                for metric in ("rows_s", "p99_us")
            }
            for v in (ref, "ON")
        }
        for ref in ("OFF", "BASE")
    }
    rng = random.Random(seed)
    draws = [[rng.randrange(len(reps)) for _ in reps] for _ in range(samples)]
    estimates = {}
    for v in VARIANTS:
        estimates[v] = {}
        for metric, values in by_variant[v].items():
            boot = [statistics.median(values[i] for i in draw) for draw in draws]
            estimates[v][metric] = {
                "median": statistics.median(values),
                "ci95": interval(boot),
            }

    comparisons = {}
    for ref, variants in paired.items():
        comparisons[ref] = {}
        for metric in ("rows_s", "p99_us"):
            on, control = variants["ON"][metric], variants[ref][metric]
            ratios = [
                statistics.median(on[i] for i in draw)
                / statistics.median(control[i] for i in draw)
                for draw in draws
            ]
            comparisons[ref][metric] = {
                "ratio": statistics.median(on) / statistics.median(control),
                "ci95": interval(ratios),
            }

        comparisons[ref]["pass"] = gate(comparisons[ref])

    return {"estimates": estimates, "comparisons": comparisons}


def gate(comparison: dict[str, Any]) -> bool:
    return (
        comparison["rows_s"]["ci95"][0] >= 0.98
        and comparison["p99_us"]["ci95"][1] <= 1.05
    )


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def source(repo: Path, spec: str, destination: Path) -> tuple[Path, dict[str, Any]]:
    checkout = Path(spec).expanduser()
    if checkout.is_dir():
        root = checkout.resolve()
        sha = git(root, "rev-parse", "HEAD")
        if git(
            root,
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            "src",
            "pyproject.toml",
        ):
            raise ValueError(f"source checkout must be clean: {spec}")

    else:
        sha = git(repo, "rev-parse", f"{spec}^{{commit}}")
        destination.mkdir()
        archive = destination.parent / f"{destination.name}.tar"
        subprocess.run(
            ["git", "-C", str(repo), "archive", "-o", str(archive), sha], check=True
        )
        with tarfile.open(archive) as bundle:
            bundle.extractall(destination, filter="data")

        archive.unlink()
        root = destination

    digest = hashlib.sha256()
    for path in sorted((root / "src").rglob("*.py")):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())

    return root, {"commit": sha, "source_sha256": digest.hexdigest()}


def run(args: argparse.Namespace) -> None:
    # Exclusive output creation prevents pooling or overwriting earlier trials.
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    sources = {}
    identities = {}
    for variant in VARIANTS:
        sources[variant], identities[variant] = source(
            args.repo, getattr(args, variant.lower()), out / variant
        )

    if identities["OFF"]["source_sha256"] != identities["ON"]["source_sha256"]:
        raise ValueError("OFF and ON must use identical source")

    consumer, consumer_identity = source(
        args.consumer_repo, args.consumer, out / "consumer"
    )
    environment = {
        "sources": identities,
        "consumer": consumer_identity,
        "options": vars(args),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "affinity": sorted(os.sched_getaffinity(0)),
        "dependencies": subprocess.check_output(
            [sys.executable, "-m", "pip", "freeze"], text=True
        )
        if args.pip_freeze
        else "see worker package versions",
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "worker_sha256": hashlib.sha256(
            Path(__file__).with_name("_append_cap_worker.py").read_bytes()
        ).hexdigest(),
    }
    (out / "environment.json").write_text(
        json.dumps(environment, indent=2, default=str)
    )
    cells = matrix(args.batches)
    if args.cells:
        cells = [cell for cell in cells if cell_name(cell) in args.cells]
        if not cells:
            raise ValueError("no cells selected")

    print(
        f"{len(cells)} cells, {args.repetitions} paired ABBA repetitions; smoke={args.smoke}",
        flush=True,
    )
    all_rows = []
    with ExitStack() as stack:
        workers = {}
        for variant in VARIANTS:
            env = os.environ | {
                "PYTHONPATH": os.pathsep.join(
                    (str(sources[variant] / "src"), str(consumer / "src"))
                ),
                "PYTHONNOUSERSITE": "1",
            }
            error_log = stack.enter_context((out / f"{variant}.stderr").open("w"))
            worker = subprocess.Popen(
                [
                    sys.executable,
                    "-u",
                    str(Path(__file__).with_name("_append_cap_worker.py")),
                    variant,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=error_log,
                text=True,
                env=env,
            )
            stack.callback(stop, worker)
            workers[variant] = worker

        raw = stack.enter_context((out / "runs.jsonl").open("w"))
        for rep in range(args.repetitions):
            ordered = cells[rep % len(cells) :] + cells[: rep % len(cells)]
            if rep % 2:
                ordered = list(reversed(ordered))

            for cell in ordered:
                refs = ("OFF", "BASE") if rep % 2 == 0 else ("BASE", "OFF")
                for ref in refs:
                    for position, variant in enumerate(abba(ref, bool(rep % 2))):
                        request = {
                            "cell": cell,
                            "seconds": args.seconds,
                            "warmup": args.warmup,
                            "max_calls": args.max_calls,
                            "root": str(out / "trial"),
                        }
                        worker = workers[variant]
                        assert worker.stdin is not None and worker.stdout is not None
                        worker.stdin.write(json.dumps(request) + "\n")
                        worker.stdin.flush()
                        line = worker.stdout.readline()
                        if not line:
                            raise RuntimeError(
                                f"{variant} worker exited; inspect its stderr"
                            )

                        result = json.loads(line)
                        if "error" in result:
                            raise RuntimeError(f"{variant}: {result['error']}")

                        result.update(
                            cell=cell,
                            variant=variant,
                            rep=rep,
                            reference=ref,
                            position=position,
                        )
                        raw.write(json.dumps(result) + "\n")
                        raw.flush()
                        all_rows.append(
                            {k: v for k, v in result.items() if k != "latencies_ns"}
                        )

                print(f"completed rep={rep + 1} {cell_name(cell)}", flush=True)

    report = {}
    for cell in cells:
        rows = [r for r in all_rows if r["cell"] == cell]
        summary = bootstrap(rows, args.bootstrap, args.seed)
        applicable = cell["kind"] == "e2e" or cell["batch"] >= 16
        summary["applicable"] = applicable
        summary["windows_complete"] = all(r["window_complete"] for r in rows)
        passed = summary["windows_complete"] and all(
            c["pass"] for c in summary["comparisons"].values()
        )
        summary["verdict"] = (
            "SMOKE"
            if args.smoke
            else ("PASS" if passed else "FAIL")
            if applicable
            else "REPORT ONLY"
        )
        report[cell_name(cell)] = summary
        print(f"{cell_name(cell)}: {summary['verdict']}")
        for v, metrics in summary["estimates"].items():
            print(f"  {v} rows/s {metrics['rows_s']} p99 us {metrics['p99_us']}")

        for ref, comparison in summary["comparisons"].items():
            verdict = (
                "SMOKE"
                if args.smoke
                else "PASS"
                if comparison["pass"] and summary["windows_complete"]
                else "FAIL"
            )
            print(f"  ON/{ref}: {verdict} {comparison}")

    full = len(cells) == 18 and args.batches == [1, 16, 256, 4096]
    overall = (
        "SMOKE"
        if args.smoke
        else "PASS"
        if full
        and all(s["verdict"] in ("PASS", "REPORT ONLY") for s in report.values())
        else "FAIL"
    )
    (out / "summary.json").write_text(
        json.dumps({"overall": overall, "full_matrix": full, "cells": report}, indent=2)
    )
    print(f"OVERALL {overall}" + (" (partial matrix)" if not full else ""))


def stop(worker: subprocess.Popen) -> None:
    if worker.stdin:
        worker.stdin.close()

    try:
        worker.wait(timeout=10)
    except subprocess.TimeoutExpired:
        worker.terminate()
        worker.wait(timeout=10)


def cell_name(cell: dict[str, Any]) -> str:
    return f"{cell['kind']}/{cell['payload']}/{cell['batch']}/{int(cell['group'])}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--off", required=True)
    parser.add_argument("--on", required=True)
    parser.add_argument("--consumer-repo", type=Path, required=True)
    parser.add_argument("--consumer", default=CONSUMER)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batches", type=int, nargs="+", default=[1, 16, 256, 4096])
    parser.add_argument(
        "--cells", nargs="+", help="select names such as direct/0/16/0 e2e/0/256/1"
    )
    parser.add_argument("--repetitions", type=int, default=15)
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument(
        "--warmup",
        type=int,
        default=16,
        help="physical calls or requests, outside timing",
    )
    parser.add_argument(
        "--max-calls",
        type=int,
        default=100000,
        help="hard bound on retained latency samples/run",
    )
    parser.add_argument("--bootstrap", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=447221)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="allow tiny runs; never report an overall gate pass",
    )
    parser.add_argument(
        "--pip-freeze",
        action="store_true",
        help="also record pip freeze when pip is installed",
    )
    args = parser.parse_args()
    if (
        not math.isfinite(args.seconds)
        or args.seconds <= 0
        or min(args.repetitions, args.bootstrap, args.max_calls, *args.batches) < 1
        or args.warmup < 0
    ):
        parser.error("positive finite windows/counts and nonnegative warmup required")

    if len(set(args.batches)) != len(args.batches):
        parser.error("batch sizes must be unique")

    if not args.smoke and (args.repetitions < 15 or args.bootstrap < 10000):
        parser.error(
            "measurements require >=15 repetitions and >=10000 bootstrap samples; use --smoke for tiny checks"
        )

    run(args)


if __name__ == "__main__":
    main()
