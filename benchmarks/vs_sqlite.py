"""What litelink costs on top of the SQLite write it is built on.

    uv run python benchmarks/vs_sqlite.py [--payload BYTES] [--rows N]

The write path is `INSERT` into SQLite at `synchronous=FULL`, so throughput is
SQLite's throughput and the fsync dominates everything (SPEC §3). That makes
raw SQLite the floor, and the only interesting number the distance from it.

The two sides are matched deliberately: same columns, same WAL and synchronous
settings, same AUTOINCREMENT primary key, same rows per transaction. What
differs is only what litelink adds — rejecting a caller-supplied `offset` (I11),
collecting assigned offsets, size accounting for the seal trigger, and the seal
check itself.

**Read the last column, not the percentage.** Overhead is roughly flat per row
— the bookkeeping is rejecting a caller-supplied `offset`, collecting assigned
offsets, and size accounting — while the fsync's share of each row shrinks as
the batch grows. So the percentage rises with batch size while the actual cost
does not move:

```
  batch      raw sqlite        litelink   overhead   us/row
      1         1,552/s         1,498/s       3.7%     23.6
     10         8,403/s         8,979/s      -6.4%     -7.6
     50        37,111/s        34,302/s       8.2%      2.2
    200        92,742/s        76,520/s      21.2%      2.3
   1000       184,831/s       143,192/s      29.1%      1.6
```

Two microseconds a row against a fsync near a millisecond is not a design
problem, and §7 wants the buffer under ~20k rows anyway, so real capture
commits in tens rather than thousands.

Two regressions this has already caught, both worth not reintroducing:

  - a `min/max` query on every append, to decide whether the buffer was empty —
    13-62% against the floor for a fact an O(1) counter already had
  - constructing the Log inside the timed region, which charges an Iceberg
    catalog and table creation to write throughput and reports ~100% overhead
    that is not real
"""

from __future__ import annotations

import argparse
import itertools
import sqlite3
import tempfile
from pathlib import Path

from _bench import COLUMNS, best_of_setup, fresh_log, observations

from litelink import Log

RUNS = 3


INSERT = (
    f"INSERT INTO buffer ({', '.join(COLUMNS)}) "
    f"VALUES ({', '.join('?' * len(COLUMNS))})"
)


def open_raw(db: Path) -> sqlite3.Connection:
    """A bare SQLite buffer with litelink's durability settings. Setup, untimed."""
    db.unlink(missing_ok=True)
    connection = sqlite3.connect(db, isolation_level=None)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        'CREATE TABLE buffer ("litelink_offset" INTEGER PRIMARY KEY AUTOINCREMENT,'
        " event_ts INTEGER, ingest_ts INTEGER, icao24 TEXT, altitude_ft INTEGER,"
        " speed_kt REAL, note TEXT)"
    )

    return connection


def write_raw(
    connection: sqlite3.Connection, batch: int, rows: int, payload: int
) -> None:
    source = observations(payload)
    for _ in range(rows // batch):
        connection.execute("BEGIN")
        for row in itertools.islice(source, batch):
            connection.execute(INSERT, tuple(row[c] for c in COLUMNS))

        connection.execute("COMMIT")


def write_litelink(log: Log, batch: int, rows: int, payload: int) -> None:
    source = observations(payload)
    for _ in range(rows // batch):
        log.extend(itertools.islice(source, batch))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=int, default=400)
    parser.add_argument("--rows", type=int, default=4_000)
    parser.add_argument(
        "--batches", type=int, nargs="+", default=[1, 10, 50, 200, 1000]
    )
    args = parser.parse_args()

    print(
        f"{args.rows:,} rows per run, {args.payload} B payload, "
        f"synchronous=FULL, best of {RUNS}"
    )
    print(
        f"\n  {'batch':>6} {'raw sqlite':>13} {'litelink':>13} {'overhead':>10} {'us/row':>9}"
    )

    with tempfile.TemporaryDirectory(prefix="litelink-floor-") as tmp:
        root = Path(tmp)
        for batch in args.batches:
            floor = best_of_setup(
                RUNS,
                lambda: open_raw(root / "raw.db"),
                lambda con, batch=batch: write_raw(con, batch, args.rows, args.payload),
            )
            actual = best_of_setup(
                RUNS,
                lambda: fresh_log(root / "log"),
                lambda log, batch=batch: write_litelink(
                    log, batch, args.rows, args.payload
                ),
            )
            print(
                f"  {batch:>6} {args.rows / floor:>11,.0f}/s {args.rows / actual:>11,.0f}/s"
                f" {(actual - floor) / floor * 100:>9.1f}%"
                f" {(actual - floor) / args.rows * 1e6:>8.1f}"
            )


if __name__ == "__main__":
    main()
