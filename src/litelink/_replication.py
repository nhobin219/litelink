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
are silently wrong when written by hand.

`published.db` is in the set, though the published table can now name its own metadata
(`version-hint.text`) and a FAILOVER deliberately does not restore it — a
stale copy wins over the bucket's own pointer. It is replicated for the
same-machine case, where it saves a round trip. See `litelink.restore`.

**One sidecar per ROOT, not per log.** `catalog.db` and `archive.db` live at the
root and are shared by every log under it, so two logs each running their own
sidecar would have two litestream instances replicating those two files — the
one thing litestream says never to do — and, under a shared published prefix,
shipping them to one replica path. Only the buffer is per log. A root holding
several logs therefore wants one config naming every buffer under it, which
this does not generate: it describes the log it was asked about. Until it does,
put each log in its own root, or write that config by hand.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import timedelta

    from litelink._layout import Layout
    from litelink._s3 import S3Options

# Where the WAL goes inside the published prefix. INSIDE the log's own directory,
# beside `data/` and `metadata/`, so a stream is one self-contained prefix that
# can be replicated, restored or deleted whole.
#
# It used to sit at the prefix root, one `_wal/` for every log, on the reasoning
# that WAL segments among the table's own directories "would be swept up by
# anything walking a log's prefix". Nothing here walks — that refusal is why
# `pending_delete` exists — and the underscore says what the directory is to
# anything that does, which is the same convention Iceberg's own `metadata/`
# relies on. What kept it at the prefix root was the CATALOGS: `catalog.db` and
# `archive.db` were shared per root — `buffer.db` never was — so a sidecar per
# log would have run two litestream instances against one database, which
# litestream forbids. Per-stream catalogs removed that, and the replica
# followed them into the stream's own prefix.
WAL_PREFIX = "_wal"


def destination(published: str, name: str) -> str:
    """Where a stream's WAL segments go, given the published prefix.

    Derived rather than configured. A second setting for it would be a second
    bucket to provision, and the failure it invites is replicating the WAL
    somewhere nobody backs up — which looks fine until the restore.
    """
    return f"{published.rstrip('/')}/{name}/{WAL_PREFIX}"


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

    **Rendered as an integer, never `%g`.** `%g` switches to exponent notation
    at a million, so a 30-day window emitted `2.592e+06s` and litestream
    refused the whole file: "cannot unmarshal into time.Duration". The
    threshold is 11 days 13 hours, which is an ordinary WAL retention, and the
    failure lands at sidecar start — so the maintainer's restart loop just
    fails forever with replication off. `%g` bit at the other end too, turning
    a sub-second retention into `5e-07s`.
    """
    seconds = retention.total_seconds()

    # Floored at a second, so a very short retention cannot render `0s` — an
    # interval of zero is not "snapshot constantly", it is a value litestream
    # has no sensible reading of.
    return [
        "    snapshot:",
        f"      interval: {max(1, round(seconds / 2))}s",
        f"      retention: {max(1, round(seconds))}s",
    ]


def litestream_config(
    layout: Layout,
    published: str,
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
    if not published.startswith("s3://"):
        msg = (
            f"replication needs a remote published table (s3://), not {published!r}: the "
            f"WAL replica exists to get unsealed rows off this machine"
        )
        raise ValueError(msg)

    target = destination(published, layout.name)
    bucket, _, prefix = target.removeprefix("s3://").rstrip("/").partition("/")
    resolved = s3.resolved()

    # The sidecar's control socket, which is how `retire()` asks it to ship
    # NOW and waits until it has (`flush`). Asking the running sidecar is the
    # only way: a second litestream process replicating the same database is
    # the corruption litestream warns about.
    lines = [
        "socket:",
        "  enabled: true",
        f"  path: {control_socket(layout)}",
        "dbs:",
    ]
    for database in layout.databases:
        # Keyed by the path relative to the STREAM's directory, which is where
        # all three databases now live, so the replica path is
        # `<prefix>/<name>/_wal/buffer.db`. The stream name is already in
        # `target`; repeating it here would nest it twice.
        #
        # It has to stay a derived name rather than `database.name`: two logs
        # sharing a published prefix must not flatten to one replica path — two
        # sidecars writing one replica is the corruption litestream is explicit
        # about, and a restore that hands back the other log's WAL.
        name = database.relative_to(layout.directory).as_posix()
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


def control_socket(layout: Layout) -> Path:
    """Where a log's sidecar listens for control commands.

    In the temp directory, keyed by a hash of the log's directory, rather than
    inside it: a Unix socket path is limited to about 104 bytes, and a log's
    own directory is routinely longer than that. Derived from the layout alone,
    so the config and `flush` agree without either recording it.
    """
    digest = hashlib.sha256(str(layout.directory).encode()).hexdigest()[:16]

    return Path(tempfile.gettempdir()) / f"litelink-{digest}.sock"


def flush(layout: Layout, binary: str | None = None, timeout: int = 60) -> None:
    """Ship `buffer.db` to its replica now, and return once the replica has it.

    `litestream sync -wait` through the running sidecar's control socket:
    measured on 0.5.16 against rustfs with the periodic publish at 1 h, a restore
    before it had none of 5 new rows, the command returned in 29 ms, and a
    restore after it had all 5. It is a CLIENT of the sidecar — it opens no
    database and writes no replica — so it never makes a second writer.

    Raises when the sidecar does not answer, rather than starting a litestream
    of its own: this cannot tell "no sidecar" from "a sidecar started from a
    config without the socket", and guessing wrong in the second case is two
    processes replicating one database.
    """
    socket = control_socket(layout)
    command = [
        litestream_binary(binary),
        "sync",
        "-wait",
        "-json",
        "-timeout",
        str(timeout),
        "-socket",
        str(socket),
        str(layout.buffer_db),
    ]
    try:
        done = subprocess.run(  # noqa: S603
            command, check=True, capture_output=True, timeout=timeout + 10
        )
    except FileNotFoundError:
        msg = "litestream was not found, and flushing the WAL replica needs it"
        raise RuntimeError(msg) from None
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        detail = getattr(exc, "stderr", b"") or b""
        msg = (
            f"could not flush {layout.buffer_db.name} to its WAL replica through "
            f"the sidecar's control socket at {socket}: "
            f"{detail.decode(errors='replace').strip() or exc}. Regenerate the "
            "litestream config with write_replication_config() and restart the "
            "sidecar — a config from before litelink 0.6 has no control socket"
        )
        raise RuntimeError(msg) from exc

    try:
        report = json.loads(done.stdout)
    except ValueError as exc:
        msg = f"litestream sync returned something other than JSON: {done.stdout!r}"
        raise RuntimeError(msg) from exc

    # Integers in 0.5.16's `-json` output, checked rather than assumed.
    shipped, current = report.get("replica_txid"), report.get("txid")
    if (
        not isinstance(shipped, int)
        or not isinstance(current, int)
        or shipped < current
    ):
        msg = (
            f"the WAL replica of {layout.buffer_db.name} is at transaction "
            f"{shipped} after a flush, behind the database's {current}"
        )
        raise RuntimeError(msg)


def litestream_binary(override: str | None = None) -> str:
    """The sidecar binary to run, pinned build first.

    `just litestream` puts a checksum-verified release in `.bin/`, and a
    checkout should restore with the version it pinned rather than whatever
    the machine carries: v0.5.0 changed the config format, and this module
    writes one shape.

    Resolved against the REPO rather than the cwd, so running from a
    subdirectory finds it. Falls through to PATH, which is what an installed
    package uses.
    """
    if override is not None:
        return override

    # A checkout's binary first, so a contributor tests what `just litestream`
    # pinned rather than whatever the machine carries.
    pinned = Path(__file__).resolve().parents[2] / ".bin" / "litestream"
    if os.access(pinned, os.X_OK):
        return str(pinned)

    # Then the one shipped inside the wheel. This is the case that makes an
    # installed package self-sufficient: it needs no PATH, which matters
    # because systemd user units do not inherit a login shell's, so
    # `which litestream` succeeding proves nothing about the unit that will
    # run the restore.
    bundled = Path(__file__).resolve().parent / ".bin" / "litestream"
    if os.access(bundled, os.X_OK):
        return str(bundled)

    # And finally PATH, for someone who installed it themselves.
    return "litestream"


def restore_buffer(
    config: Path, destination: Path, options: S3Options, binary: str | None = None
) -> None:
    """Restore one database from its replica, into `destination`.

    **`-if-replica-exists`, so an ABSENT replica is not an error.** It exits 0
    and writes nothing, which leaves the caller's `destination.exists()` check
    to say what happened — and that check is the whole point, because "there is
    no replica here" is the answer a restore needs to explain in its own
    words. Without the flag litestream exits 1 on absence
    and the RuntimeError below fires first, so both callers' explanatory
    messages were unreachable and a mistyped log name surfaced as

        litestream restore failed: Error: no matching backup files available

    which names neither the prefix searched nor the log.

    This module used to refuse the flag, on the reasoning that exiting 0 made a
    missing replica "indistinguishable from a restored one". It does not: the
    two differ by whether the file is there, which is exactly what the caller
    tests. Nor does the flag swallow real failures — measured against
    litestream 0.5.16, a bucket that does not exist still exits 1
    (`NoSuchBucket`, 404) and a bad key still exits 1 (`InvalidAccessKeyId`,
    403). Only absence is quiet.

    Credentials go to the child through the ENVIRONMENT, never into the config
    file, which is why that file is safe to commit and hand around. litestream
    reads its own names first and falls back to the AWS ones.
    """
    environment = dict(os.environ)
    resolved = options.resolved()
    if resolved.access_key and resolved.secret_key:
        environment["LITESTREAM_ACCESS_KEY_ID"] = resolved.access_key
        environment["LITESTREAM_SECRET_ACCESS_KEY"] = resolved.secret_key

    destination.parent.mkdir(parents=True, exist_ok=True)
    command = [
        litestream_binary(binary),
        "restore",
        "-if-replica-exists",
        "-config",
        str(config),
        "-o",
        str(destination),
        str(destination),
    ]
    try:
        subprocess.run(command, check=True, env=environment, capture_output=True)  # noqa: S603
    except FileNotFoundError:
        msg = (
            "litestream was not found, and this log needs it to restore.\n"
            "\n"
            "litelink's platform wheels ship it, so this is either a "
            "pure-Python wheel — the fallback for a platform with no build — "
            "or an install from source. Put litestream on PATH "
            "(https://litestream.io/install), or pass `binary=` explicitly.\n"
            "\n"
            "PATH is worth checking rather than assuming: systemd user units do "
            "not inherit a login shell's PATH, so `which litestream` succeeding "
            "in a terminal says nothing about the unit that will actually run "
            "the restore."
        )
        raise RuntimeError(msg) from None
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.decode(errors="replace").strip()
        msg = f"litestream restore failed: {detail or exc}"
        raise RuntimeError(msg) from exc
