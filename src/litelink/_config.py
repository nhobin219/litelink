"""The operational policy, and nothing else (SPEC §12).

Its own module because `Buffer` needs it and `_handle` imports `Buffer`. Kept
in `_handle`, that is a cycle — which the buffer worked around with a deferred import
inside the one method that reads it, twice. A deferred import is a cycle you
have decided to live with; this is the cycle not existing.

Nothing here imports from the package, which is what keeps it that way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta

# The size compaction writes files to ON DISK, when nothing says: Iceberg's
# own `write.target-file-size-bytes`.
#
# Larger files cost readers nothing measurable and save the wide ones a great
# deal. Measured on a gigabyte of synthetic ticks and order-book snapshots
# (`benchmarks/compaction.py`, #158): against files of 64 MiB of Arrow — 95
# and 205 of them — 512 MB files cut a `count(*)` over S3 from 190 and 410
# requests to 4, and a full scan from 570-1,230 to 209-414, while every query
# measured locally was within noise of the small files.
#
# Files land AT it: compaction grows one in-progress file a step at a time and
# cuts it on the writer's own `tell()` (#162), so a finished file overshoots by
# at most one row group, however much better the merge compressed.
#
# What it costs is time in staging. `publish` takes only files compaction has
# finished with, so a stream publishes in 512 MB steps: at 114 rows a second,
# 6 to 16 days. Recovery does not wait on it — that is WAL replication's job
# (§3a) — so the delay is the published table's freshness, not durability.
DEFAULT_COMPACT_SIZE = 512 * 1024 * 1024

# How much of a compacted file is sorted and written at a time, in Arrow's
# in-memory bytes, when nothing says.
#
# Compaction streams: it reads its inputs in offset order, sorts this much at a
# time by `sort_by`, and writes it as one row group. So this, not the file
# size, is what bounds its memory — about 2.5x this at the peak, measured — and
# a file of any size is written in the same footprint.
#
# Sorting a row group rather than the whole file is a choice about reads. A
# file sorted end to end by a `sort_by` that is not arrival order spreads every
# offset range across one stretch per sort key: measured, a 100k-offset read
# went from 4 requests to 27 and 3x the time. Sorted a row group at a time, an
# offset or time range stays within one or two row groups, as it did when a
# whole compacted file was this size.
DEFAULT_ROW_GROUP_SIZE = 64 * 1024 * 1024

# How much of a log's history the staging table keeps on disk, when nothing
# says (`LogConfig.staging_max_bytes`): about seven finished files at the
# default `target_compact_size`, besides the in-progress one. Per log, so a
# machine running many multiplies it.
DEFAULT_STAGING_MAX_BYTES = 4 * 1024 * 1024 * 1024

# Also the default for a retired log's `litelink.truncate`, which has no
# config to read it from (#181).
DEFAULT_PUBLISHED_SNAPSHOT_RETENTION = timedelta(hours=1)


@dataclass(frozen=True, slots=True)
class LogConfig:
    """SPEC §12.

    The defaults are the spec's worked examples, not measured optima — §7's
    numbers come from a 2 vCPU box and every one of these wants re-measuring on
    target hardware.
    """

    # The ONLY seal trigger, and therefore the size of every file this library
    # writes. §7 makes it a READ-LATENCY knob before a file-size one: the
    # buffer is the entire variable cost of a hot read.
    #
    # There is deliberately no `max_age` beside it: a size is not a schedule.
    # Bounding what a crash can lose without WAL replication is the
    # maintainer's call, made explicitly — `seal(flush=True)` and
    # `publish(flush=True)` on its RPO interval — and the short files a flush
    # cuts are compaction's to fold into the in-progress file (§6, #162), so
    # the files a log ends with do not depend on how often it flushes.
    #
    # BYTES, not rows. §13.3 is the deciding argument: a row-count bound can
    # exceed a byte-based memory limit, so it loses the race to the OOM killer
    # in exactly the situation the bound exists to prevent.
    #
    # UNCOMPRESSED bytes, in memory — the Arrow table's `nbytes`, not the size
    # of the file that results.
    # Deliberate, and the one thing to understand before setting it. A file
    # holding 8 MiB of rows lands at 8 MiB on disk if they are incompressible
    # and under 1 MiB if they repeat, so on-disk size is an OUTPUT here, never
    # the target. Sizing by it instead would be sizing by the compression
    # ratio: rows per file would swing with the data, and what a reader pays to
    # hold a file — which is the uncompressed size, whatever the file cost to
    # store — would be unbounded. This way it is bounded by construction, and
    # bounded per file is what lets a scan bound its total: N files open at
    # once cost N times this, which is the number to divide a memory budget by
    # when choosing read parallelism.
    #
    # So expect sealed files smaller than this on disk. Compaction is where
    # files are sized on disk (`target_compact_size`), because it streams and
    # so has no reason to bound a file by memory.
    #
    # The 8 MiB default is §7's row guidance restated — its table puts a 20k-row
    # buffer at 8.0 MB at the 400-byte row it measured, and 20k rows is the
    # ceiling it recommends.
    #
    # That equivalence is what does NOT generalise. §7's buffer cost is per ROW
    # (SQLite is row-oriented: 1.0 us/row at 20k, 2.3 us/row at 180k), so a
    # stream of 40-byte rows reaches 8 MiB at 200k rows and a read-latency
    # ceiling meant to hold at 20k is breached tenfold. Bytes bound memory;
    # rows bound read latency; they are different failure modes. A narrow-row
    # stream may eventually need `min(target_seal_size, target_seal_rows)` —
    # deliberately not added now, on one knob until a real workload demands the
    # second.
    target_seal_size: int = 8 * 1024 * 1024
    # The second half of §7's argument, and the one `target_seal_size` cannot
    # make. Buffer cost is per ROW — SQLite is row-oriented, 1.0 us/row at 20k
    # and 2.3 us/row at 180k — so a stream of 40-byte rows reaches 8 MiB at
    # 200k rows and breaches a read-latency ceiling meant to hold at 20k,
    # tenfold, while every byte-based check reports the buffer is fine.
    #
    # Bytes bound memory; rows bound read latency. Both are CEILINGS on one
    # file, so the seal cuts at whichever is reached FIRST.
    #
    # None means no row limit, which is the right default: a narrow-row stream
    # is the case that needs this and a library cannot guess the row width.
    target_seal_rows: int | None = None

    # §6. How big a file should END UP, ON DISK, which is not the same
    # question as how much may sit in the buffer, and the two pull in opposite
    # directions.
    #
    # §7 wants the seal SMALL: the buffer is what a hot read scans, so its size
    # is read latency. The published table wants files LARGE: every file a
    # query cannot prune is opened, a footer read and on object storage a
    # request each, which a wide scan or a filter on a column outside `sort_by`
    # pays per file. Planning is not where file count costs — the offset
    # boundary is read from one manifest's entries, measured at 1.6 ms over one
    # file and 3.0 ms over 64, and cached per table version.
    #
    # Splitting them gives compaction a job: converting sealed chunks into
    # published-shaped ones. Eligibility follows for free — `publish` only takes
    # files compaction has finished with, so a freshly sealed file is a merge
    # candidate and is not published until it has been converted.
    #
    # The price is write amplification, and it is bounded rather than ongoing:
    # every row is written twice locally, once at seal and once at compaction,
    # and read once in between. Bounded because a converted file is already at
    # the target, so it is never a candidate again.
    #
    # On disk, because compaction streams: its memory is set by
    # `target_row_group_size`, so nothing ties the file to what a process can
    # hold. None means `DEFAULT_COMPACT_SIZE`.
    target_compact_size: int | None = None
    # §6. How much new sealed data, ON DISK, compaction waits for before it
    # rewrites the in-progress file to absorb it (#162). Every rewrite reads
    # and writes the whole in-progress file, so this bounds the rewrites per
    # finished file — target / step of them, about 4.5 local writes per row at
    # the default — however often `compact` runs. Smaller grows the file
    # sooner, leaving fewer seals local, for more local writes. None means an
    # eighth of `compact_size`.
    target_compact_step_size: int | None = None
    # Arrow bytes compaction sorts and writes as one row group: its memory
    # bound, and the unit an offset range is localised to inside a file. See
    # `DEFAULT_ROW_GROUP_SIZE`.
    target_row_group_size: int = DEFAULT_ROW_GROUP_SIZE
    # A row ceiling on the same row groups, for a log that wants one: a row
    # group closes at whichever it reaches first. None is none. Bytes stay the
    # primary limit, because a row count alone cannot bound rows of unbounded
    # width — a million 10 KB payloads is 10 GB. pyarrow's own writer caps at
    # 1,048,576 rows; Parquet's and Iceberg's guidance is in bytes.
    target_row_group_rows: int | None = None

    @property
    def compact_size(self) -> int:
        """The on-disk file size compaction aims for."""
        return self.target_compact_size or DEFAULT_COMPACT_SIZE

    @property
    def compact_step(self) -> int:
        """How much new sealed data, on disk, the in-progress file absorbs per
        rewrite: `target_compact_step_size`, or an eighth of the target."""
        if self.target_compact_step_size is not None:
            return self.target_compact_step_size

        return max(1, self.compact_size // 8)

    # §8. The staging table's two limits, both CEILINGS: a file is evicted
    # once either says so, so the tighter one wins. Every log has a published
    # table (#98), local by default, that already holds every row, so the
    # staging table is a cache for hot reads, and it must be bounded by at
    # least one of these (`validate`).
    #
    # Neither evicts a file publish has not taken (I4), nor one compaction is
    # still working on (#160, #162): those stay whatever the limits say, so
    # staging can sit above them by about one `target_compact_size`, or by a
    # publish backlog — which is a stalled publish, not a retention problem.
    #
    # By AGE: files written longer ago than this go. It should exceed the
    # longest hot-path lookback, with margin. None is no age limit. Zero means
    # "evict on publish" — pure archival capture, hot reads limited to the
    # buffer.
    staging_retention: timedelta | None = None
    # By SIZE ON DISK: the newest files that fit stay, older ones go — for a
    # stream fast enough that its window is more disk than the machine has, and
    # the bound every log gets by default (`DEFAULT_STAGING_MAX_BYTES`). With
    # `staging_retention` set too, the window is honoured up to this. None is
    # no size limit, which needs an age limit instead.
    staging_max_bytes: int | None = DEFAULT_STAGING_MAX_BYTES

    # §3a. Continuous WAL shipping, which is the ONLY thing bounding RPO now
    # that the seal has no timer: a stream that goes quiet holds its last
    # partial file's worth of rows indefinitely.
    #
    # A declaration rather than a supervisor. It is read — `evict("buffer")`
    # consults it on every eviction, and validation refuses it without a remote
    # published table to replicate to — but litelink never starts the sidecar.
    # That is a separate process reading the WAL, which is exactly why
    # replication does not put the network in the write path, and litestream is
    # explicit that two instances must never replicate one database. Supervising
    # it belongs in deployment code, where it is visible: see
    # `examples/adsb/maintainer.py`.
    wal_replication: bool = False

    # §3a. When a maintenance pass finds at least this share of `buffer.db` on
    # SQLite's free list, reclaim it with a VACUUM. None never reclaims, which
    # is the behaviour every version before this had.
    #
    # **Off by default, because the cost lands on the write path.** VACUUM takes
    # an exclusive lock and rebuilds the file, so it stalls appends for as long
    # as the LIVE data takes to copy — 0.3 s at 35 MB, measured. Only the
    # deployment knows whether its arrival rate can absorb that, so litelink
    # will not decide it: `reclaim("buffer")` is the manual door, and
    # this setting is for the deployments that would rather it happened on the
    # ordinary pass.
    #
    # **What it buys is paid by FAILOVER, not by this process.** SQLite never
    # returns freed pages to the OS, and litestream replicates the FILE — so a
    # log that seals and publishes for months makes every `restore` download and
    # apply its dead space. Measured on a 1-day-old capture: 457 MB holding
    # 20,658 live rows, 92% of its pages free, restoring in 12.5 s against 0.8 s
    # for the same content vacuumed. A log without `wal_replication` is never
    # restored from a replica, so it can leave this None for ever and lose
    # nothing but disk.
    #
    # 0.5 is the value to reach for. The win scales with what is RECLAIMED and
    # the cost with what is KEPT, so the trade only improves above it, and a
    # buffer hovering under the line ships at most 2x the bytes it needs —
    # bounded, unlike the growth it replaces.
    vacuum_free_ratio: float | None = None

    # §3a. How far BACK a restore can go, which is not how much is kept safe.
    #
    # The distinction is the whole of this setting. A restore always recovers
    # the LATEST replicated state; retention bounds point-in-time depth and
    # never endangers the current point. So the question it answers is "how old
    # a moment might I want to restore to", and the answer follows from the
    # published table: once publish has pushed a range, that range is
    # recoverable from object storage, and WAL history older than the
    # un-published window is covering something that is covered twice.
    #
    # **A duration, because litestream has nothing else.** The obvious spelling
    # is "retain WAL above the published offset" and it is not expressible:
    # v0.5.16's knobs are `snapshot.interval`, `snapshot.retention` and
    # `l0-retention`, all durations, and its CLI has no `snapshot` verb to
    # force one after a publish pass and make a duration behave like an offset.
    # So this is the un-published window stated as time.
    #
    # That window is append -> seal -> compact -> publish, which no library can
    # know in advance: it depends on the arrival rate and on how often a
    # maintainer runs. `examples/adsb/tail.py` reports the lag it actually is. Set
    # this from that, with margin.
    #
    # None leaves litestream on its own defaults (24h/24h as of v0.5.16).
    wal_retention: timedelta | None = None
    # §6/§8. How long a superseded snapshot, and the files only it referenced,
    # survive. One per table, because their readers differ (#113).
    #
    # Each must exceed the longest scan of its table: expiry deletes files an
    # open scan is still reading (I6). Nothing else rests on them. A log's
    # offsets are its point-in-time reads, so an Iceberg snapshot is never
    # kept for time travel, and both are bounded.
    #
    # The published table's is longer because its readers are other machines
    # and other engines, holding a metadata pointer this process cannot see.
    # The staging table's readers are this log's own scans.
    staging_snapshot_retention: timedelta = timedelta(minutes=15)
    published_snapshot_retention: timedelta = DEFAULT_PUBLISHED_SNAPSHOT_RETENTION

    # §6. What counts as "big enough to leave alone" is `settled_size` of the
    # target, not its own setting — see `_maintenance.settled_size`.
    compact_min_files: int = 4

    # The Parquet codec every data file is written with — a seal, a compaction,
    # a bulk ingest.
    #
    # **A setting rather than a constant, because the right answer is a
    # property of the payload.** §15.5 requires NONE for blob columns: sensor
    # payloads and media are already compressed, and a codec will spend CPU
    # proving it. A text or JSON payload is the opposite shape.
    #
    # zstd by default, and the default is what changed. Every write site used
    # to call `pq.write_table` with no codec at all, taking pyarrow's Snappy —
    # measured on a 200k-row JSON payload column, sorted as this library writes
    # it: Snappy 97 bytes/row at 2.07x, zstd 51 bytes/row at 3.93x. On a real
    # 177M-row published table that is 34.8 GB against roughly 15 GB.
    #
    # It is not a size-for-speed trade, which is why this is a default and not
    # advice. The same measurement put zstd's full-scan read at 0.65x Snappy's,
    # because there is less to read and decompressing it is cheap; the cost is
    # write CPU, 1.9x, against a write path that is fsync-bound and a published
    # table push that is network-bound.
    #
    # Changing it is safe at any time and rewrites nothing. Parquet records the
    # codec per column chunk, so a table holding both reads correctly —
    # verified across `scan` and `sql` — and existing files are never touched:
    # a new codec applies to what is written from then on.
    compression: str = "zstd"

    def to_json(self) -> str:
        """Serialised for the `meta` table, so `open` recovers the policy.

        Durations as seconds rather than any richer encoding: this is read by
        the next process to open the log, and a float is the one representation
        that cannot drift between library versions.
        """
        return json.dumps(
            {
                "target_seal_size": self.target_seal_size,
                "target_compact_size": self.target_compact_size,
                "target_compact_step_size": self.target_compact_step_size,
                "target_seal_rows": self.target_seal_rows,
                "target_row_group_size": self.target_row_group_size,
                "target_row_group_rows": self.target_row_group_rows,
                "staging_retention": (
                    None
                    if self.staging_retention is None
                    else self.staging_retention.total_seconds()
                ),
                "staging_max_bytes": self.staging_max_bytes,
                "wal_replication": self.wal_replication,
                "vacuum_free_ratio": self.vacuum_free_ratio,
                "wal_retention": (
                    None
                    if self.wal_retention is None
                    else self.wal_retention.total_seconds()
                ),
                "staging_snapshot_retention": (
                    self.staging_snapshot_retention.total_seconds()
                ),
                "published_snapshot_retention": (
                    self.published_snapshot_retention.total_seconds()
                ),
                "compact_min_files": self.compact_min_files,
                "compression": self.compression,
            }
        )

    @classmethod
    def from_json(cls, encoded: str) -> LogConfig:
        """Recover the policy, tolerating a record written by another version.

        Every field falls back to its default when absent, because that is what
        an older record MEANS: the log was written before the setting existed,
        so it was running the default. Reading them positionally instead made
        adding any setting break `open` on every existing log — a config that
        cannot be read is a log that cannot be opened, over a policy value that
        was never load-bearing.

        Unknown keys are ignored for the same reason from the other direction:
        a log touched by a newer version stays openable by an older one, minus
        the setting it does not have.
        """
        raw = json.loads(encoded)
        defaults = cls()
        # Written under the #98 names; a config written before them used
        # `local_retention`, read when the new key is absent.
        retention = raw.get(
            "staging_retention", raw.get("local_retention", defaults.staging_retention)
        )
        wal = raw.get("wal_retention", defaults.wal_retention)
        # A config written before #113 held one `snapshot_retention` for both
        # tables. It fills both, so a value raised for long scans keeps
        # protecting them on the published table as well.
        legacy = raw.get("snapshot_retention")
        staging_snapshots = raw.get("staging_snapshot_retention", legacy)
        published_snapshots = raw.get("published_snapshot_retention", legacy)

        return cls(
            target_seal_size=raw.get("target_seal_size", defaults.target_seal_size),
            target_compact_size=raw.get(
                "target_compact_size", defaults.target_compact_size
            ),
            target_compact_step_size=raw.get(
                "target_compact_step_size", defaults.target_compact_step_size
            ),
            target_seal_rows=raw.get("target_seal_rows", defaults.target_seal_rows),
            target_row_group_size=raw.get(
                "target_row_group_size", defaults.target_row_group_size
            ),
            target_row_group_rows=raw.get(
                "target_row_group_rows", defaults.target_row_group_rows
            ),
            staging_retention=(
                retention
                if isinstance(retention, timedelta) or retention is None
                else timedelta(seconds=retention)
            ),
            # Absent from a config written before the cap existed, which reads
            # as the default: a log that kept everything, the old default, is
            # bounded from its next pass. A stored `staging_rows`, retired with
            # it, is ignored like any key this version does not know.
            staging_max_bytes=raw.get("staging_max_bytes", defaults.staging_max_bytes),
            wal_replication=raw.get("wal_replication", defaults.wal_replication),
            vacuum_free_ratio=raw.get("vacuum_free_ratio", defaults.vacuum_free_ratio),
            wal_retention=(
                wal
                if isinstance(wal, timedelta) or wal is None
                else timedelta(seconds=wal)
            ),
            staging_snapshot_retention=(
                defaults.staging_snapshot_retention
                if staging_snapshots is None
                else timedelta(seconds=staging_snapshots)
            ),
            published_snapshot_retention=(
                defaults.published_snapshot_retention
                if published_snapshots is None
                else timedelta(seconds=published_snapshots)
            ),
            compact_min_files=raw.get("compact_min_files", defaults.compact_min_files),
            compression=raw.get("compression", defaults.compression),
        )
