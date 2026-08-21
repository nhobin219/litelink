"""Generate a litestream config for a log, for continuous RPO (§3a).

    uv run python examples/replicate.py [--root DIR] --to s3://bucket/prefix

Prints a `litestream.yml` naming every SQLite file the log's state lives in.
Run the sidecar with it and the loss window on machine failure falls from
"whatever has not sealed yet" to the replication lag.

**Why this exists rather than a hand-written config.** The set of files is not
obvious and getting it wrong is silent. `buffer.db` holds rows no Parquet file
has yet — the ones everybody remembers. `catalog.db` says which files the local
table is made of. `archive.db` says the same for the archive, so omitting it
leaves the objects in S3 intact and nothing able to say what they are. The log
knows the set; a config written by hand knows what someone remembered.

**litelink does not run litestream.** It is a separate process reading the WAL,
which is precisely why replication does not put the network in the write path:
if it dies you lose replication, not data. A library that supervised it would
give that up. It is also why nothing here is a `LogConfig` setting — whether a
sidecar is running is a fact about the deployment, and a boolean in the log
claiming it would be a setting nothing reads.

To restore, per database, using the same config:

    litestream restore -config litestream.yml -o RESTORED/buffer.db ORIGINAL/buffer.db

Restoring is correct by construction: a restored buffer may hold rows already
sealed into the table, and the read boundary (§7) comes from the table's
committed extent, so those rows fall outside the buffer's contribution
automatically. No reconciliation, no dedup pass.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from _stream import NAME

from litelink import Log
from litelink._s3 import S3Options


def config(log: Log, destination: str, s3: S3Options) -> str:
    """A litestream config replicating every database the log needs.

    Written by hand rather than through a YAML library: it is a fixed shape,
    and an example that made the reader install a dependency to see it would be
    hiding the thing it is meant to show.

    The endpoint is written explicitly rather than left to `AWS_ENDPOINT_URL`.
    litestream reads credentials from the environment but resolves the bucket's
    region against real AWS unless the replica says otherwise — so against
    rustfs or MinIO it fails with "cannot lookup bucket region" while the
    credentials it needs are sitting right there in the environment.

    **No credentials in the file.** litestream takes them from
    LITESTREAM_ACCESS_KEY_ID / LITESTREAM_SECRET_ACCESS_KEY or the AWS ones, so
    the generated config is safe to commit, copy and hand around — the same
    reason `S3Options` is not part of `LogConfig`.
    """
    bucket, _, prefix = destination.removeprefix("s3://").rstrip("/").partition("/")
    resolved = s3.resolved()

    lines = ["dbs:"]
    for database in log.databases:
        key = f"{prefix}/{database.name}" if prefix else database.name
        lines += [
            f"  - path: {database}",
            "    replicas:",
            "      - type: s3",
            f"        bucket: {bucket}",
            f"        path: {key}",
        ]
        if resolved.region:
            lines.append(f"        region: {resolved.region}")

        if resolved.endpoint:
            # Anything that is not AWS also needs path-style addressing:
            # `bucket.host` is a DNS name only AWS actually serves.
            lines += [
                f"        endpoint: {resolved.endpoint}",
                "        force-path-style: true",
            ]

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument(
        "--to", required=True, help="replica prefix, e.g. s3://bucket/wal"
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="write here instead of stdout"
    )
    args = parser.parse_args()

    # Readonly: this reads the log's shape and writes nothing to it. A config
    # generator that took the write lock would be a strange thing to run
    # alongside a live writer, which is exactly when you want it.
    try:
        log = Log.open(args.root, NAME, read_only=True)
    except FileNotFoundError as exc:
        raise SystemExit(f"{exc}\nstart `just demo-capture` first") from exc

    try:
        rendered = config(log, args.to, S3Options())
    finally:
        log.close()

    if args.out is None:
        print(rendered, end="")
    else:
        args.out.write_text(rendered)
        print(f"wrote {args.out}")
        print(f"  litestream replicate -config {args.out}")


if __name__ == "__main__":
    main()
