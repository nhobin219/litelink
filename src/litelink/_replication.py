"""Continuous WAL shipping, as a config a sidecar can run (§3a).

litelink does not run the sidecar. It is a separate process reading the WAL,
which is exactly why replication does not put the network in the write path: if
it dies you lose replication, not data. A library that supervised it would be
taking on process lifecycle — restart backoff, orphan reaping, shutdown
ordering — and would risk the one thing litestream is explicit about, which is
that two instances must never replicate the same database. A maintainer killed
with SIGKILL holds its lease for the full TTL and orphans its child, so the next
one to take the lease would start a second.

What the library owns is what the config has to SAY: which files carry the
log's state, and where they go. Both are things only the log knows, and both
are silently wrong when written by hand — a config that omits `archive.db`
leaves the objects in S3 intact with nothing able to say what they are.

**One sidecar per ROOT, not per log.** `catalog.db` and `archive.db` live at the
root and are shared by every log under it, so two logs each running their own
sidecar would have two litestream instances replicating those two files — the
one thing litestream says never to do — and, under a shared archive prefix,
shipping them to one replica path. Only the buffer is per log. A root holding
several logs therefore wants one config naming every buffer under it, which
this does not generate: it describes the log it was asked about. Until it does,
put each log in its own root, or write that config by hand.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import timedelta
    from pathlib import Path

    from litelink._s3 import S3Options

# Where the WAL goes inside the archive prefix. Beside the log's own directory
# rather than inside it: the warehouse holds `<name>/data` and `<name>/metadata`
# per log, and WAL segments among them would be swept up by anything walking a
# log's prefix.
WAL_PREFIX = "_wal"


def destination(archive: str) -> str:
    """Where WAL segments go, given the archive prefix.

    Derived rather than configured. A second setting for it would be a second
    bucket to provision, and the failure it invites is replicating the WAL
    somewhere nobody backs up — which looks fine until the restore.
    """
    return f"{archive.rstrip('/')}/{WAL_PREFIX}"


def snapshot_block(retention: timedelta) -> list[str]:
    """The per-database `snapshot:` lines for a retention window.

    **`interval` is derived, and it has to be shorter than `retention`.**
    litestream keeps snapshots and the LTX files belonging to them for
    `retention`, and a restore needs a snapshot at or before the point it is
    restoring to. An interval longer than the retention leaves windows with no
    snapshot in them at all, which deletes the chain a restore needs. Half is
    the simplest value that always leaves one inside.

    Seconds, spelled as a Go duration. litestream parses these with
    `time.ParseDuration`, which takes `21600s` as readily as `6h`, and seconds
    are the one encoding that cannot drift the way a hand-rounded `6h30m` can.
    """
    seconds = retention.total_seconds()

    return [
        "    snapshot:",
        f"      interval: {seconds / 2:g}s",
        f"      retention: {seconds:g}s",
    ]


def litestream_config(
    databases: Sequence[Path],
    root: Path,
    archive: str,
    s3: S3Options,
    retention: timedelta | None = None,
) -> str:
    """A litestream config replicating every database the log needs.

    Written as text rather than through a YAML library: it is a fixed shape,
    and a dependency to emit nine lines is a dependency to audit.

    The endpoint is written explicitly. litestream reads credentials from the
    environment but resolves the bucket's region against real AWS unless the
    replica says otherwise — so against rustfs or MinIO it fails with "cannot
    lookup bucket region" while the credentials it needs sit unused in the
    environment.

    **No credentials in the file.** litestream takes them from
    LITESTREAM_ACCESS_KEY_ID / LITESTREAM_SECRET_ACCESS_KEY or the AWS ones, so
    the generated config is safe to commit, copy and hand around — the same
    reason `S3Options` is not part of `LogConfig`.
    """
    target = destination(archive)
    bucket, _, prefix = target.removeprefix("s3://").rstrip("/").partition("/")
    resolved = s3.resolved()

    lines = ["dbs:"]
    for database in databases:
        # Keyed by the path RELATIVE TO THE ROOT, not the bare filename. A
        # buffer lives at `<root>/<log>/buffer.db`, so two logs under one root
        # sharing an archive prefix would flatten to the same replica path —
        # two sidecars writing one replica, which is the corruption litestream
        # is explicit about, and a restore that hands back the other log's WAL.
        name = database.relative_to(root).as_posix()
        key = f"{prefix}/{name}" if prefix else name
        # `replica`, singular. litestream v0.5.0 made it one replica per
        # database and carries the `replicas` list as deprecated
        # (cmd/litestream/main.go: `Replicas []*ReplicaConfig // Deprecated`).
        # This never emitted more than one element, so the list bought nothing
        # and dated the file.
        lines.append(f"  - path: {database}")
        if retention is not None:
            # Per database rather than in the global `snapshot:` block, so a
            # root holding several logs can carry one config with a window each
            # — the global one would make the shortest of them everyone's.
            lines += snapshot_block(retention)

        lines += [
            "    replica:",
            "      type: s3",
            f"      bucket: {bucket}",
            f"      path: {key}",
        ]
        if resolved.region:
            lines.append(f"      region: {resolved.region}")

        if resolved.endpoint:
            # Anything that is not AWS also needs path-style addressing:
            # `bucket.host` is a DNS name only AWS actually serves.
            lines += [
                f"      endpoint: {resolved.endpoint}",
                "      force-path-style: true",
            ]

    return "\n".join(lines) + "\n"
