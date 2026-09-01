"""The operational policy, and nothing else (SPEC §12).

Its own module because `Buffer` needs it and `log` imports `Buffer`. Kept in
`log`, that is a cycle — which the buffer worked around with a deferred import
inside the one method that reads it, twice. A deferred import is a cycle you
have decided to live with; this is the cycle not existing.

Nothing here imports from the package, which is what keeps it that way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta

# How much larger a compacted file is than a sealed one, when nothing says.
#
# The two are at odds by nature: §7 wants the seal small because the buffer is
# what a hot read scans, and both object storage and Parquet want files large.
# Eight is chosen to be comfortably past the point where per-file overhead
# dominates — measured at 44 ms to read the offset boundary over 64 files
# against 1.0 ms over one — while keeping compaction's peak memory, which is
# one run held as a single Arrow table, to something a maintainer process can
# hold. On the default 8 MiB seal that is 64 MiB.
COMPACT_MULTIPLE = 8


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
    # There is deliberately no `max_age` beside it. A timer sealing a quiet
    # stream emits a small file every interval for ever — the layout §6 exists
    # to repair — and it made the knob do double duty as an RPO policy, so
    # shrinking the window to lose less on a crash produced worse files. §3a
    # names that trade and WAL replication is what breaks it: freshness in the
    # cloud is replication's job, not the seal's. With the timer gone every cut
    # lands exactly here, so no undersized file is ever written.
    #
    # BYTES, not rows. §13.3 is the deciding argument: a row-count bound can
    # exceed a byte-based memory limit, so it loses the race to the OOM killer
    # in exactly the situation the bound exists to prevent.
    #
    # UNCOMPRESSED bytes, in memory — not the size of the file that results.
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
    # So expect files smaller than this on disk, and set it larger than an
    # on-disk file target would be. Everything downstream is stated in the same
    # currency — compaction sizes a merge by what its inputs HOLD, carried in
    # the `extent` row the seal measured it into, never by what they compressed
    # to (see `_maintenance.runs`).
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
    # file, so the seal cuts at whichever is reached FIRST — the mirror of
    # `local_retention` and `local_rows`, which are floors and take whichever
    # retains more.
    #
    # None means no row limit, which is the right default: a narrow-row stream
    # is the case that needs this and a library cannot guess the row width.
    target_seal_rows: int | None = None

    # §6. How big a file should END UP, which is not the same question as how
    # much may sit in the buffer, and the two pull in opposite directions.
    #
    # §7 wants the seal SMALL: the buffer is what a hot read scans, so its size
    # is read latency. The archive wants files LARGE: measured against S3, a
    # 9 kB file takes 648 ms to upload and almost all of that is the round
    # trip, so halving file size doubles the cost of archiving the same stream.
    # One knob cannot serve both — it did, and compaction could therefore never
    # produce a file bigger than a seal, which is why it was a no-op.
    #
    # Splitting them gives compaction a job: converting sealed chunks into
    # archive-shaped ones. Eligibility follows for free — `sync` only takes
    # files compaction has finished with, so raising this above the seal size
    # means a freshly sealed file is a merge candidate and is not archived
    # until it has been converted.
    #
    # The price is write amplification, and it is bounded rather than ongoing:
    # every row is written twice locally, once at seal and once at compaction,
    # and read once in between. Bounded because a converted file is already at
    # the target, so it is never a candidate again — eight seals become one
    # compacted file, once.
    #
    # None means `COMPACT_MULTIPLE` times the seal size, and the conversion is
    # therefore ON by default even with no archive. A local-only log gets the
    # same benefit at read time: file count is a measured cost here, not a
    # reputation — reading the offset boundary from manifest statistics
    # measured 1.0 ms over one file and 44 ms over 64.
    #
    # It is a MULTIPLE for a reason. Sealed files are uniform, so merging whole
    # files lands exactly on the target when it divides and short when it does
    # not: three 1 MiB files against a 4 MiB target give 3 MiB files for ever,
    # 25% under what was asked for, with nothing to indicate why.
    target_compact_size: int | None = None
    target_compact_rows: int | None = None

    @property
    def compact_size(self) -> int:
        """The file size compaction aims for.

        `COMPACT_MULTIPLE` times the seal size unless set. On the 8 MiB default
        seal that is 64 MiB of rows — under Parquet's usual advice once
        compression is applied, and far above the size at which per-file
        overhead dominates a scan.
        """
        return self.target_compact_size or self.target_seal_size * COMPACT_MULTIPLE

    @property
    def compact_rows(self) -> int | None:
        """The row ceiling compaction respects. Scaled like `compact_size`, so
        setting only the seal's row limit does not silently cap conversion at
        one seal's worth."""
        if self.target_compact_rows is not None:
            return self.target_compact_rows

        if self.target_seal_rows is None:
            return None

        return self.target_seal_rows * COMPACT_MULTIPLE

    # §8. Must exceed the longest hot-path lookback WITH margin.
    #
    # None keeps everything locally and grows without bound. Zero means "evict
    # on upload" — pure archival capture, hot reads limited to the buffer — and
    # presupposes an archive: with `archive=None` it would delete each file as
    # soon as it sealed, so the pair is rejected at construction rather than
    # honoured.
    local_retention: timedelta | None = None
    # §8, the other half of the same policy. A window in time and a count of
    # rows bound different things, and which one binds depends on a rate the
    # library cannot know: an hour of a quiet stream is a handful of rows, and
    # an hour of a busy one is more local disk than the machine has. Set both
    # and eviction keeps whichever retains MORE — they are floors on what must
    # stay readable without touching the network, so the binding one is the one
    # that keeps more.
    #
    # The mirror image of the seal's `min(target_seal_size, target_seal_rows)`
    # in §12, deliberately: there the two are ceilings and the tighter wins,
    # here they are floors and the looser does.
    #
    # Rows, not files, because it is a statement about the data — "the last
    # million entries stay local" survives a change to either size target, and
    # "the last ten files" does not.
    #
    # FOLLOW-UP: no third floor in BYTES, and deliberately. These two answer
    # "what can I query without touching the network", which is how queries are
    # written — the last hour, the last million rows. "The last 10 GB" is not a
    # statement any query makes, and rows already stand in for bytes since
    # bytes are roughly rows times width.
    #
    # What is genuinely missing is the opposite: nothing bounds local disk from
    # ABOVE, so with both of these unset it grows without limit. That wants a
    # CAP rather than a floor, and it composes the other way — `max` after the
    # `min` below, because a cap evicts MORE and can therefore violate both
    # floors. It would measure on-disk size (`DataFile.size`), one of the few
    # places where that is the right unit. And it could not be honoured at all
    # while sync is behind, since I4 forbids evicting what the archive lacks —
    # a log breaching its floors to stay under a cap is misconfigured and
    # should say so rather than quietly serving every read from object storage.
    local_rows: int | None = None

    # §3a. Continuous WAL shipping, which is the ONLY thing bounding RPO now
    # that the seal has no timer: a stream that goes quiet holds its last
    # partial file's worth of rows indefinitely.
    #
    # A declaration rather than a supervisor. It is read — `_discard_on_seal`
    # consults it on every seal, and validation refuses it without an archive
    # to replicate to — but litelink never starts the sidecar. That is a separate
    # process reading the WAL, which is exactly why replication does not put
    # the network in the write path, and litestream is explicit that two
    # instances must never replicate one database. Supervising it belongs in
    # deployment code, where it is visible: see `examples/adsb/maintainer.py`.
    wal_replication: bool = False
    # §3a. How far BACK a restore can go, which is not how much is kept safe.
    #
    # The distinction is the whole of this setting. A restore always recovers
    # the LATEST replicated state; retention bounds point-in-time depth and
    # never endangers the current point. So the question it answers is "how old
    # a moment might I want to restore to", and the answer follows from the
    # archive: once sync has pushed a range, that range is recoverable from
    # object storage, and WAL history older than the un-archived window is
    # covering something that is covered twice.
    #
    # **A duration, because litestream has nothing else.** The obvious spelling
    # is "retain WAL above the archived offset" and it is not expressible:
    # v0.5.16's knobs are `snapshot.interval`, `snapshot.retention` and
    # `l0-retention`, all durations, and its CLI has no `snapshot` verb to
    # force one after a sync and make a duration behave like an offset. So this
    # is the un-archived window stated as time.
    #
    # That window is append -> seal -> compact -> sync, which no library can
    # know in advance: it depends on the arrival rate and on how often a
    # maintainer runs. `examples/adsb/tail.py` reports the lag it actually is. Set
    # this from that, with margin.
    #
    # None leaves litestream on its own defaults (24h/24h as of v0.5.16).
    wal_retention: timedelta | None = None
    # §6/§8. Must exceed the longest scan: expiry deletes files an open scan is
    # still reading (I6).
    snapshot_retention: timedelta = timedelta(hours=1)

    # §6. What counts as "big enough to leave alone" is `settled_size` of the
    # target, not its own setting — see `_maintenance.settled_size`.
    compact_min_files: int = 4

    # The Parquet codec every data file is written with — a seal, a compaction,
    # an archive rewrite, a bulk ingest.
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
    # 177M-row archive that is 34.8 GB against roughly 15 GB.
    #
    # It is not a size-for-speed trade, which is why this is a default and not
    # advice. The same measurement put zstd's full-scan read at 0.65x Snappy's,
    # because there is less to read and decompressing it is cheap; the cost is
    # write CPU, 1.9x, against a write path that is fsync-bound and an archive
    # push that is network-bound.
    #
    # Changing it is safe at any time and rewrites nothing. Parquet records the
    # codec per column chunk, so a table holding both reads correctly —
    # verified across `scan` and `sql` — and existing files are never touched.
    # `rewrite_archive` is what re-cuts history into the new one, when the size
    # is worth the transfer.
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
                "target_seal_rows": self.target_seal_rows,
                "target_compact_rows": self.target_compact_rows,
                "local_retention": (
                    None
                    if self.local_retention is None
                    else self.local_retention.total_seconds()
                ),
                "local_rows": self.local_rows,
                "wal_replication": self.wal_replication,
                "wal_retention": (
                    None
                    if self.wal_retention is None
                    else self.wal_retention.total_seconds()
                ),
                "snapshot_retention": self.snapshot_retention.total_seconds(),
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
        retention = raw.get("local_retention", defaults.local_retention)
        wal = raw.get("wal_retention", defaults.wal_retention)
        snapshots = raw.get("snapshot_retention")

        return cls(
            target_seal_size=raw.get("target_seal_size", defaults.target_seal_size),
            target_compact_size=raw.get(
                "target_compact_size", defaults.target_compact_size
            ),
            target_seal_rows=raw.get("target_seal_rows", defaults.target_seal_rows),
            target_compact_rows=raw.get(
                "target_compact_rows", defaults.target_compact_rows
            ),
            local_retention=(
                retention
                if isinstance(retention, timedelta) or retention is None
                else timedelta(seconds=retention)
            ),
            local_rows=raw.get("local_rows", defaults.local_rows),
            wal_replication=raw.get("wal_replication", defaults.wal_replication),
            wal_retention=(
                wal
                if isinstance(wal, timedelta) or wal is None
                else timedelta(seconds=wal)
            ),
            snapshot_retention=(
                defaults.snapshot_retention
                if snapshots is None
                else timedelta(seconds=snapshots)
            ),
            compact_min_files=raw.get("compact_min_files", defaults.compact_min_files),
            compression=raw.get("compression", defaults.compression),
        )
