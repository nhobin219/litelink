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

Both points are printed as a takeaway line under their table, computed from the
run rather than asserted: a reader who knows what the numbers mean does not
need it, and one who does not should not have to infer it from a grid.
"""

from __future__ import annotations

import argparse
import itertools
import tempfile
import time
from pathlib import Path

from _bench import best_of, fresh_log, observations


def bench_writes(root: Path, payload: int, batches: list[int], rows_each: int) -> None:
    print("\nWRITE  how fast rows land durably, by how many rows one extend()")
    print("       call carries. extend() is ONE transaction, so its size is one")
    print("       fsync amortised — the whole of §3's throughput story. this is a")
    print("       call-site choice, not a setting: nothing in LogConfig tunes it.")
    print(f"       {rows_each:,} rows per run, {payload} B wide, synchronous=FULL\n")
    print(
        f"  {'rows per extend()':>17} {'rows/s':>12} {'per row':>12}"
        f" {'transactions':>13}"
    )

    measured: list[tuple[int, float, int]] = []
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
        measured.append((batch, rows / elapsed, commits))
        print(
            f"  {batch:>17,} {rows / elapsed:>12,.0f}"
            f" {elapsed / rows * 1e6:>9,.0f} us {commits:>13,}"
        )

    slowest, fastest = measured[0], measured[-1]
    print(
        f"\n  -> one extend() of {fastest[0]:,} rows is"
        f" {fastest[1] / slowest[1]:,.0f}x faster than {fastest[0]:,} append()"
        " calls."
    )
    print(
        f"     the same rows and the same work: {slowest[2]:,} fsyncs against"
        f" {fastest[2]:,}."
    )
    print(
        f"     hand extend() whole batches where the feed allows it."
        f" {slowest[1]:,.0f} rows/s"
    )
    print("     is the ceiling where it does not.")


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

        print("\nREAD   every read merges two places: Parquet files, already sealed,")
        print("       and the SQLite buffer, not yet. the buffer is the variable cost.")
        print(f"       {sealed_rows:,} rows sealed, then a buffer that grows\n")
        print(f"  {'buffered':>9} {'rows read':>11} {'time':>11} {'rows/s':>12}")

        measured: list[tuple[int, float]] = []
        buffered = 0
        for target in buffer_sizes:
            log.extend(itertools.islice(source, target - buffered))
            buffered = target

            elapsed = best_of(3, lambda: log.scan().read_all())
            total = sealed_rows + buffered
            measured.append((buffered, elapsed * 1000))
            print(
                f"  {buffered:>9,} {total:>11,} {elapsed * 1000:>8.1f} ms"
                f" {total / elapsed:>12,.0f}"
            )

        print(
            f"\n  -> before a single row is touched, any read costs {overhead:.2f} ms:"
            "\n     resolving the catalog and finding the sealed/buffered boundary."
        )

        # Only claimed when the run actually shows it. A quick run buffers too
        # few rows for the buffer's slope to clear the noise, and printing a
        # trend the numbers above contradict is worse than printing none.
        first, last = measured[0], measured[-1]
        # A bare "it went up" is not enough: two timings 0.7 ms apart on a
        # 19 ms read is scheduling noise, and reporting a slope from it invents
        # a trend. The full sweep clears both floors by an order of magnitude.
        grew = (
            last[0] > first[0]
            and last[1] - first[1] > 1.0
            and last[1] > first[1] * 1.05
        )
        if grew:
            per_thousand = (last[1] - first[1]) / ((last[0] - first[0]) / 1_000)
            print(
                f"  -> each 1,000 rows left in the buffer cost {per_thousand:.1f} ms"
                " here.\n     sealing more often is the knob that bounds it (§7)."
            )

        else:
            print(
                "  -> this run is too small to separate the buffer's cost from"
                " noise.\n     re-run without --quick for the full sweep."
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
            f"  -> asking for two columns instead of all of them: {narrow:.1f} ms"
            f" against {wide:.1f} ms,\n     {wide - narrow:.1f} ms less. the one"
            " lever that does not need sealing more often (§7)."
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

    run = "quick" if args.quick else "full"
    print(f"litelink throughput on this machine — {run} run, {args.payload} B rows.")
    print("every number below is measured here and now; nothing is a published figure.")

    with tempfile.TemporaryDirectory(prefix="litelink-bench-") as tmp:
        root = Path(tmp) / "data"
        bench_writes(root, args.payload, batches, rows_each)
        bench_reads(root, args.payload, buffer_sizes, sealed)


if __name__ == "__main__":
    main()
