"""Write and read throughput, on this machine.

    uv run python benchmarks/throughput.py [--payload BYTES] [--quick]

SPEC §3 and §7 carry numbers from a 2 vCPU box with ~1068 us fsyncs; every one
of them says to re-measure on target hardware. This is that measurement.

What to look for:

  - **Write scales with batch size, not with work done.** One fsync per
    transaction dominates, so 1 row per commit and 1000 rows per commit differ
    by orders of magnitude while doing the same amount of real work (§3).
  - **The buffer is the variable cost of a read.** SQLite is row-oriented, so
    its leg costs per row what Parquet costs per 40 rows. That is why §7 calls
    the seal threshold a read-latency knob (§7).
"""

from __future__ import annotations

import argparse
import itertools
import tempfile
import time
from pathlib import Path

from _bench import best_of, fresh_log, observations


def bench_writes(root: Path, payload: int, batches: list[int], rows_each: int) -> None:
    print(f"\nwrite — {rows_each:,} rows per run, {payload} B wide, synchronous=FULL")
    print(f"  {'batch':>7} {'rows/s':>12} {'us/row':>10} {'commits':>9}")

    for batch in batches:
        log = fresh_log(root)
        source = observations(payload, seed=batch)
        commits = rows_each // batch
        try:
            started = time.perf_counter()
            for _ in range(commits):
                log.extend(itertools.islice(source, batch))

            elapsed = time.perf_counter() - started
        finally:
            log.close()

        rows = commits * batch
        print(
            f"  {batch:>7,} {rows / elapsed:>12,.0f} {elapsed / rows * 1e6:>10.1f}"
            f" {commits:>9,}"
        )


def bench_reads(
    root: Path, payload: int, buffer_sizes: list[int], sealed_rows: int
) -> None:
    log = fresh_log(root)
    source = observations(payload, seed=7)
    try:
        log.extend(itertools.islice(source, sealed_rows))
        log.seal()

        # §7 calls this the architecture overhead: resolve the catalog, read the
        # tier boundary from manifest statistics. Fixed, not proportional, and
        # paid by every read — so it is worth knowing before reading the rest.
        overhead = best_of(5, log.table_extent) * 1000
        print(f"\nfixed overhead — resolve catalog + boundary: {overhead:.2f} ms")

        print(f"\nread — {sealed_rows:,} rows sealed into the table, then N buffered")
        print(f"  {'buffered':>9} {'total rows':>11} {'union ms':>10} {'rows/s':>12}")

        buffered = 0
        for target in buffer_sizes:
            log.extend(itertools.islice(source, target - buffered))
            buffered = target

            elapsed = best_of(3, lambda: log.scan().read_all())
            total = sealed_rows + buffered

            print(
                f"  {buffered:>9,} {total:>11,} {elapsed * 1000:>10.1f}"
                f" {total / elapsed:>12,.0f}"
            )

        # The projection lever from §7: excluding the wide column roughly halves
        # the buffer leg, and is the only lever that does not require sealing
        # more often. Both timings carry the fixed overhead above, so compare
        # the difference, not the ratio.
        wide = best_of(3, lambda: log.scan().read_all()) * 1000
        narrow = (
            best_of(
                3, lambda: log.scan(columns=["litelink_offset", "event_ts"]).read_all()
            )
            * 1000
        )
        print(
            f"\nprojection — all columns {wide:.1f} ms vs two columns {narrow:.1f} ms"
            f"  ({wide - narrow:+.1f} ms)"
        )
    finally:
        log.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--payload", type=int, default=400, help="payload bytes per row"
    )
    parser.add_argument("--quick", action="store_true", help="smaller runs")
    args = parser.parse_args()

    batches = [1, 10, 50, 200] if args.quick else [1, 10, 50, 200, 1000]
    rows_each = 2_000 if args.quick else 20_000
    sealed = 20_000 if args.quick else 200_000
    buffer_sizes = [1_000, 5_000] if args.quick else [1_000, 5_000, 20_000, 60_000]

    with tempfile.TemporaryDirectory(prefix="litelink-bench-") as tmp:
        root = Path(tmp) / "data"
        bench_writes(root, args.payload, batches, rows_each)
        bench_reads(root, args.payload, buffer_sizes, sealed)


if __name__ == "__main__":
    main()
