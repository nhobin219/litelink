"""Write the litestream config for a log, for continuous RPO (§3a).

    uv run python examples/replicate.py [--root DIR]

Everything in it is derived: which SQLite files carry the log's state, where
they go (`_wal` beside the archived data), and the endpoint they go through.
That is the point of asking the log rather than writing the file by hand — the
set is not obvious and getting it wrong is silent. `buffer.db` holds rows no
Parquet file has yet, the one everybody remembers. `catalog.db` says which
files the local table is made of. `archive.db` says the same for the archive,
so omitting it leaves the objects in S3 intact and nothing able to say what
they are.

You do not need to run this to get replication: with `wal_replication=True` the
maintainer writes the config and runs the sidecar itself. This is for running
litestream as an independent process instead —

    uv run python examples/replicate.py --root DIR
    litestream replicate -config DIR/litestream.yml

To restore, per database:

    litestream restore -config DIR/litestream.yml -o RESTORED/buffer.db DIR/positions/buffer.db

A restored buffer holding rows already sealed into the table needs no
reconciliation: the read boundary (§7) comes from the table's committed extent,
so they fall outside the buffer's contribution automatically.

Restoring onto ANOTHER machine is a different operation and this is not it.
`catalog.db` records absolute paths to local Iceberg metadata that no sidecar
replicates, so the restore above works back onto the same paths and fails
elsewhere. Use `Log.restore` for that.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from _stream import NAME

from litelink import Log
from litelink._s3 import S3Options


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    args = parser.parse_args()

    # Readonly: this reads the log's shape and writes nothing to it. A config
    # generator that took the write lock would be a strange thing to run
    # alongside a live writer, which is exactly when you want it.
    try:
        log = Log.open(args.root, NAME, read_only=True, s3=S3Options())
    except FileNotFoundError as exc:
        raise SystemExit(f"{exc}\nstart `just demo-capture` first") from exc

    try:
        if log.config.wal_replication:
            # The maintainer already runs one against these databases, and two
            # litestream instances on one database is the thing litestream is
            # explicit about. Refused rather than warned: the recipe's own
            # restore walkthrough invites running this next to a live demo.
            raise SystemExit(
                "wal_replication is on, so `just demo-maintain` is already "
                "replicating these databases. Run this only to replicate "
                "independently — start the capture without --replicate first."
            )

        written = log.write_replication_config()
    except ValueError as exc:
        raise SystemExit(f"{exc}\nrun the capture with --archive") from exc
    finally:
        log.close()

    print(f"wrote {written}")
    print(f"  litestream replicate -config {written}")


if __name__ == "__main__":
    main()
