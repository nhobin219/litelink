"""The three-way read, built per query (SPEC §7).

DuckDB does the reading: pyiceberg resolves the pointer, DuckDB scans, and both
legs of the union run in one engine. `table.scan().to_arrow()` is not used —
its planning happens in Python and costs ~100 ms per scan, paid on every query.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from litelink._prune import terms
from litelink._published import Published
from litelink._s3 import S3Options
from litelink._tiers import (
    BUFFER,
    KEY,
    PUBLISHED,
    STAGING,
    TIERS,
    UNKNOWN,
    StoredTiers,
    entry,
)
from litelink._types import column_type
from litelink.manifest import Term, build, prune

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import PathLike

    from litelink._buffer import Buffer
    from litelink._layout import Layout
    from litelink._table import LogTable

VIEW = "log"

# The name the buffer's unsealed tail is registered under, re-registered per
# query. NOT an attached database: see `Buffer.rows_from` for why letting
# DuckDB open the SQLite file corrupts it.
BUFFER_REL = "buf_tail"

# The library-owned column (§2), which the boundary filters on.
OFFSET = "litelink_offset"


def secret_sql(options: S3Options) -> str:
    """DuckDB's spelling of the same credentials pyiceberg was given.

    Separate from the query path so it can be tested without object storage —
    the fault it exists to prevent only appeared against real AWS, where writes
    worked and every read of the published table came back 403.
    """
    parts = ["TYPE s3"]
    if options.access_key is not None and options.secret_key is not None:
        parts.append(f"KEY_ID '{options.access_key}'")
        parts.append(f"SECRET '{options.secret_key}'")
    else:
        # Nothing explicit means the credentials live where every AWS tool
        # looks for them: a profile, instance metadata, SSO. pyiceberg and s3fs
        # resolve those themselves, so WRITES worked while reads got a secret
        # with no keys — which DuckDB treats as anonymous, so a log on an
        # ordinary AWS host published happily and answered every published read
        # with 403. A local endpoint never showed it, because there the keys
        # are always passed explicitly.
        #
        # `credential_chain` is DuckDB's equivalent of that resolution. The
        # else-branch rather than the default, because an explicit key must
        # still win — see `S3Options.resolved`.
        #
        # `REFRESH auto`, because the chain resolves its credentials when the
        # secret is CREATED, and a long-lived connection would otherwise keep
        # presenting an STS token after it expires (#108).
        parts.append("PROVIDER credential_chain")
        parts.append("REFRESH auto")

    if options.region is not None:
        parts.append(f"REGION '{options.region}'")

    if options.endpoint is not None:
        # DuckDB wants host:port with the scheme carried by USE_SSL, unlike
        # pyiceberg which takes a URL. One S3Options, two spellings.
        without_scheme = options.endpoint.split("://", 1)[-1]
        parts.append(f"ENDPOINT '{without_scheme}'")
        parts.append(f"USE_SSL {str(options.endpoint.startswith('https')).lower()}")
        parts.append("URL_STYLE 'path'")

    return f"CREATE OR REPLACE SECRET litelink_s3 ({', '.join(parts)})"


class ExtensionMissing(RuntimeError):
    """A DuckDB extension the read path needs is not on this machine (§7)."""


def _duckdb_library_version() -> str:
    """The version DuckDB's extension repository is keyed on.

    `duckdb.__version__` is the DISTRIBUTION version — what pip resolved —
    while extensions are published under the LIBRARY version, which duckdb
    exposes separately. They agree for every release duckdb has published so
    far (0 post-releases in 129), but a `1.5.5.post1` would make them differ,
    and the dependency range admits one: `<1.5.6` is a bound on the
    distribution.

    A post-release would then miss the bundle for all three extensions, which
    degrades to a network fetch online and to an unreadable log offline. One
    `getattr` removes the whole question.
    """
    return str(getattr(duckdb, "__duckdb_version__", duckdb.__version__)).lstrip("v")


def _bundled_extension(connection: duckdb.DuckDBPyConnection, name: str) -> Path | None:
    """The copy shipped in the wheel, if there is one this DuckDB can load.

    **Checked against the RUNNING version and platform, never assumed.** DuckDB
    builds extensions per version and per platform — the download path is
    `/v1.5.5/linux_amd64/...` — so a bundle is only usable by the exact DuckDB
    it was fetched for. `duckdb>=1.5.5` has no ceiling, so a user can perfectly
    well resolve a newer one than the wheel was built against, and loading a
    mismatched extension is not something to find out at read time.

    Returning None on a miss is deliberate: the caller falls through to the
    ordinary `LOAD`, which finds a machine-provisioned copy if there is one and
    otherwise raises the message that says how to get one. The bundle is a fast
    path, not the mechanism.
    """
    root = Path(__file__).resolve().parent / ".bin"
    if not root.is_dir():
        return None

    platform = connection.execute("PRAGMA platform").fetchone()
    if platform is None:
        return None

    candidate = (
        root / _duckdb_library_version() / str(platform[0]) / f"{name}.duckdb_extension"
    )

    return candidate if candidate.is_file() else None


def load_extension(
    connection: duckdb.DuckDBPyConnection, name: str, *, remote: bool
) -> None:
    """`LOAD name`, with an error that says what to do about it.

    **LOAD-time autoinstall is per-extension, and not to be relied on either
    way.** Verified against duckdb 1.5.5, each in a fresh process with an empty
    `extension_directory` and `autoinstall_known_extensions` reporting True:
    `LOAD iceberg` silently downloads and succeeds, `LOAD httpfs` raises. So
    the reported failure is not a machine with autoinstall switched off — the
    two extensions simply behave differently, and nothing here should assume
    which.

    What the caller got instead was DuckDB's error: a filesystem path inside
    `~/.duckdb`, and advice to run `INSTALL httpfs` — a remedy §7 argues
    against, naming nothing this repo ships. `iceberg` reaches the same error
    the moment the machine is genuinely offline, which is the case §7 is about,
    so both go through here.

    **Installing here instead is the other way to make that traceback go away,
    and it is refused.** §7 makes provisioning an obligation discharged at
    build or deploy time, and `install_duckdb_extensions.py --check` exists to
    assert a machine has discharged it. A read path that quietly downloads is
    precisely what that check is written to detect, so adding one would leave
    the check passing on a machine it was meant to fail.

    `remote` says whether this extension is needed only for the published tier,
    which changes what the message should tell the reader: a log whose
    published table is a local directory never loads `httpfs`, so someone who
    hits it has either configured a remote published table or is reading one,
    and someone who has not can ignore it entirely. It also picks the flag for
    the contributor recipe.
    """
    bundled = _bundled_extension(connection, name)
    if bundled is not None:
        try:
            connection.execute(f"LOAD '{bundled}'")
        except duckdb.Error:
            # The bundle is a fast path, not a trapdoor. A copy that arrived
            # wrong — a mangling proxy, a partial install, a corrupt download —
            # would otherwise break a machine that has a perfectly good
            # extension in its own DuckDB home, and break it with a raw
            # `IOException` naming a path inside site-packages rather than the
            # message below that says how to fix it.
            #
            # DuckDB verifies the signature on LOAD, so a substituted extension
            # is refused rather than run; what falling through buys is that the
            # refusal is recoverable.
            pass
        else:
            return

    try:
        connection.execute(f"LOAD {name}")
    except duckdb.IOException as exc:
        flag = " --remote" if remote else ""
        source = " FROM community" if name in _COMMUNITY else ""
        tier = (
            "\n\nOnly an S3 published table needs this. A log publishing to a "
            "local directory never loads it, so if yours does you can ignore it."
            if remote
            else ""
        )
        msg = (
            f"the DuckDB `{name}` extension is not installed on this machine. "
            f"It is not compiled into the duckdb wheel, and an explicit LOAD "
            f"does not fetch it, so it has to be provisioned:\n"
            f"\n"
            f"litelink's platform wheels ship it, built for the DuckDB they "
            f"pin — so either this is a pure-Python wheel, or duckdb "
            f"{duckdb.__version__} is not the version those extensions were "
            f"built for.\n"
            f"\n"
            f"Fetch it into this machine's DuckDB home, which needs network:\n"
            f"\n"
            f'    python -c "import duckdb; '
            f"duckdb.connect().execute('INSTALL {name}{source}')\"\n"
            f"\n"
            f"Extensions are built per DuckDB version and platform, so one "
            f"fetched for a different duckdb will not load."
            f"\n\nFrom a checkout:    just duckdb-extensions{flag}"
            f"{tier}"
        )
        raise ExtensionMissing(msg) from exc


def uses_credential_chain(options: S3Options) -> bool:
    """Whether `secret_sql` builds a `credential_chain` secret for `options`.

    A chain secret needs DuckDB's `aws` extension, which resolves the chain;
    `httpfs` alone cannot. Explicit keys make a plain `config` secret and need
    only `httpfs`.
    """
    return options.access_key is None or options.secret_key is None


def load_s3_extensions(
    connection: duckdb.DuckDBPyConnection, options: S3Options
) -> None:
    """`httpfs`, and `aws` when the secret will use the credential chain.

    `aws` is loaded explicitly, from the bundle or this machine's DuckDB home,
    like every other extension here. Left to DuckDB, creating a chain secret
    autoinstalls it: a silent download online, and offline a failure that
    reads as missing credentials when the profile is right there.
    """
    load_extension(connection, "httpfs", remote=True)
    if uses_credential_chain(options):
        load_extension(connection, "aws", remote=True)


def create_secret(target: duckdb.DuckDBPyConnection, options: S3Options) -> None:
    """Create or replace the S3 secret on `target`, from resolved `options`.

    A credential chain that finds nothing is refused by DuckDB at creation, with
    `Secret Validation Failure: … Credential Chain: 'config'` — true, and no use
    to someone who does not know which chain or what it wanted. Raised instead
    as an error naming the three ways to supply credentials, DuckDB's kept as
    the cause.
    """
    try:
        target.execute(secret_sql(options))
    except duckdb.Error as exc:
        if options.access_key is not None and options.secret_key is not None:
            raise

        msg = (
            "no S3 credentials were found for reading a published table on S3. "
            "Pass them explicitly (S3Options(access_key=..., secret_key=...)), "
            "set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or configure an "
            f"AWS profile this machine can use. DuckDB said: {exc}"
        )
        raise RuntimeError(msg) from exc


def install_s3_secret(
    connection: duckdb.DuckDBPyConnection, s3_options: S3Options | None = None
) -> None:
    """Load `httpfs` (and `aws` for a credential chain) and create or replace
    the S3 secret on `connection`.

    What `duckdb_connection(s3_options=...)` does after loading the read path, for
    a connection litelink did not build — one shared database handing out
    cursors, say — and for credentials that have changed since. Credentials come
    from `s3_options`, or with none given from the environment and then the AWS
    credential chain. A chain secret refreshes itself (`REFRESH auto`), so an
    expiring STS token is not a reason to call this; rotated explicit keys are.

    Secrets belong to the database, not the cursor, so one call covers every
    cursor of `connection`. Raises `ExtensionMissing` if `httpfs` is not
    provisioned, and `RuntimeError`, naming the fix, if no credentials are
    found.
    """
    resolved = (s3_options or S3Options()).resolved()
    load_s3_extensions(connection, resolved)
    create_secret(connection, resolved)


# Extensions published from DuckDB's COMMUNITY repository rather than the core
# one, which changes the install line an error has to give.
_COMMUNITY = frozenset({"cache_httpfs"})


def cache_directory(cache_key: str | PathLike[str] | None = None) -> Path:
    """Where a connection's disk cache lives (#118).

    Under `$XDG_CACHE_HOME/litelink`, or `~/.cache/litelink`:

    - **a relative `cache_key`** names a directory there —
      `cache_key="stream-uuid"` is `~/.cache/litelink/stream-uuid`. The key is
      the caller's, because what deserves its own cache is something only the
      caller knows: a streamcast stream is a composition of logs, and is
      cached by its id.
    - **an absolute one** is used as given, for a cache on its own volume.
    - **None** is `~/.cache/litelink/default` — a SIBLING of the keyed
      directories, never their parent, so no cache's eviction can reach into
      another's.

    Sharing a directory between processes is safe: Iceberg never reuses a
    file name, so a cached block is never stale. Never `cache_httpfs`'s own
    default under `/tmp`, which many systems clear at boot.
    """
    root = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "litelink"
    if cache_key is None:
        return root / "default"

    key = Path(cache_key)
    if key.is_absolute():
        return key

    if ".." in key.parts:
        msg = f"cache_key {str(cache_key)!r} would reach outside {root}"
        raise ValueError(msg)

    return root / key


@dataclass(frozen=True, slots=True)
class ReadCache:
    """How a connection caches what it reads from S3 (#118). See
    `duckdb_connection`."""

    memory_cache: bool = True
    disk_cache: bool = False
    cache_key: str | PathLike[str] | None = None
    disk_cache_volume_limit: float = 0.8

    def __post_init__(self) -> None:
        if not 0 < self.disk_cache_volume_limit <= 1:
            msg = (
                "disk_cache_volume_limit is how full the cache's volume may get, "
                f"a share in (0, 1]; got {self.disk_cache_volume_limit}"
            )
            raise ValueError(msg)


# Every Iceberg table's version hint, whatever its location.
_HINT_EXCLUSION = r".*/metadata/version-hint\.text$"


def install_read_cache(connection: duckdb.DuckDBPyConnection, cache: ReadCache) -> None:
    """Configure `connection`'s read caches as `cache` says (#118).

    Two layers, because `cache_httpfs` runs ONE mode at a time:

    - **Disk** (`disk_cache`): the `cache_httpfs` community extension in
      on-disk mode, wrapping httpfs. Survives restarts — measured, a new
      process reusing the directory served every block from it and read 0
      bytes from S3.
    - **Memory** (`memory_cache`): DuckDB's own external file cache, in its
      buffer manager, for the connection's lifetime. Loading `cache_httpfs`
      switches it OFF (to avoid caching twice), so it is set back explicitly.
      With both on, hot blocks come from RAM and warm ones from disk.
      `memory_cache=False` also turns off `cache_httpfs`'s own read-through
      memory cache for its disk reader (~128 MB per process), so the flag
      means what it says.

    **The disk cache is bounded by its volume, not its size.** `cache_httpfs`
    has no byte cap; it evicts once free space on the cache's volume falls
    below a floor, which by default is 5% — a default-on cache would fill the
    disk the log may be writing to. `disk_cache_volume_limit` is how full that
    volume may get, converted here to the floor. It measures EVERYTHING on the
    volume: other data filling it past the limit shrinks the cache, possibly
    to nothing, which is the intent — the log's own writes come first.

    Eviction stays on the extension's default policy (by creation time): its
    LRU alternative is documented for a single process, and this cache is
    shared. Its block size is left alone, since changing it invalidates every
    cached file.

    Settings are GLOBAL so every cursor of `connection` gets them. The memory
    cache applies to every connection, local reads included; the disk cache
    only to a remote one, since only S3 reads go through httpfs.
    """
    if cache.disk_cache:
        load_extension(connection, "cache_httpfs", remote=True)
        directory = cache_directory(cache.cache_key)
        directory.mkdir(parents=True, exist_ok=True)
        floor = int(
            shutil.disk_usage(directory).total * (1 - cache.disk_cache_volume_limit)
        )
        connection.execute("SET GLOBAL cache_httpfs_type = 'on_disk'")
        connection.execute(
            "SET GLOBAL cache_httpfs_cache_directory = ?", [str(directory)]
        )
        connection.execute(
            "SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = ?", [floor]
        )
        connection.execute(
            "SET GLOBAL cache_httpfs_disk_cache_reader_enable_memory_cache = ?",
            [cache.memory_cache],
        )
        # The one object a reader touches that changes (#141). Excluded from
        # the disk cache, so a stale hint is never served from disk, in this
        # process or a later one. Not enough on its own: the file-handle cache
        # ignores exclusions and keeps the hint's handle for up to an hour, so
        # a later read of it in this process fails DuckDB's ETag check rather
        # than seeing the new one — measured; without the exclusion it silently
        # returned the old snapshot. Readers resolve the hint with
        # `current_metadata` instead. Exclusions belong to the database and
        # are not deduplicated, so added once.
        excluded = {
            row[0]
            for row in connection.execute(
                "SELECT * FROM cache_httpfs_list_exclusion_regex()"
            ).fetchall()
        }
        if _HINT_EXCLUSION not in excluded:
            connection.execute(
                "SELECT cache_httpfs_add_exclusion_regex(?)", [_HINT_EXCLUSION]
            ).fetchall()

    connection.execute(
        "SET GLOBAL enable_external_file_cache = ?", [cache.memory_cache]
    )


def current_metadata(location: str, *, s3_options: S3Options | None = None) -> str:
    """The current `metadata.json` of the published table at `location` (#141).

    Read from the table's `metadata/version-hint.text`, outside DuckDB, so that
    no DuckDB cache can serve an old one. Scan the path this returns:

        con = litelink.duckdb_connection(s3_options=options, disk_cache=True)
        metadata = litelink.current_metadata("s3://bucket/prefix/trades",
                                             s3_options=options)
        con.sql(f"SELECT count(*) FROM iceberg_scan('{metadata}')")

    **The hint is the one object a reader touches that changes.** Metadata,
    manifest and data files are written once under unique names, so every
    cache is correct for them. Resolved through a connection with
    `disk_cache=True`, the hint itself goes through `cache_httpfs`, whose
    file-handle cache keeps the first handle for up to an hour — so
    `iceberg_scan('s3://…/trades', version_name_format=…)` there fails an ETag
    check once the table has published again, rather than reading the new
    snapshot — and reading the hint itself through that connection,
    `read_text('…/metadata/version-hint.text')`, silently returns the old one.
    Never read the hint through a disk-cached connection: call this, per read
    that should see new publishes; it is one small GET.

    `location` is the published table's location, `s3://…` or `file:///…`.
    Raises `FileNotFoundError` when it has no hint — nothing has been
    published there, or it is not a table that writes one.
    """
    from litelink._table import VERSION_HINT, shared_file_io

    base = location.rstrip("/")
    properties = (s3_options or S3Options()).resolved().catalog_properties()
    hint = shared_file_io(properties, base).new_input(f"{base}/metadata/{VERSION_HINT}")
    if not hint.exists():
        msg = (
            f"no {VERSION_HINT} under {base}/metadata: nothing has been published there"
        )
        raise FileNotFoundError(msg)

    version = hint.open().read().decode().strip()
    if not version:
        msg = f"{base}/metadata/{VERSION_HINT} is empty"
        raise FileNotFoundError(msg)

    return f"{base}/metadata/{version}.metadata.json"


def duckdb_connection(
    *,
    s3_options: S3Options | None = None,
    memory_cache: bool = True,
    disk_cache: bool = False,
    cache_key: str | PathLike[str] | None = None,
    disk_cache_volume_limit: float = 0.8,
) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection provisioned to read a published table (#108).

    `avro` and `iceberg` are loaded from the extensions litelink's platform
    wheels bundle, or from this machine's DuckDB home — never fetched: §7 makes
    provisioning a build or deploy step, and the first read must not be a
    network read. A missing one raises `ExtensionMissing`, saying how to
    provision it.

    **`s3_options` is what makes a connection read S3.** Given, it also runs
    `install_s3_secret`: `httpfs`, and the S3 secret from those options, where
    an empty `S3Options()` takes everything from the environment and then the
    AWS credential chain. A machine with no credentials at all raises
    `RuntimeError` naming the fix, here rather than as a 403 at the first
    query. Without it nothing S3 is loaded, so a local reader pays nothing for
    it. Reading a published table on S3 from another machine, through the
    table's current metadata (`current_metadata`, which a `disk_cache`
    connection needs to see new publishes):

        options = litelink.S3Options()
        con = litelink.duckdb_connection(s3_options=options)
        metadata = litelink.current_metadata("s3://bucket/prefix/trades",
                                             s3_options=options)
        con.sql(f"SELECT count(*) FROM iceberg_scan('{metadata}')")

    **Reads are cached** (#118), in two layers:

    - `memory_cache` (on): DuckDB's external file cache, for this
      connection's lifetime. Every connection, local reads included.
    - `disk_cache` (OFF, and only with `s3_options`): the bundled
      `cache_httpfs` extension on disk, in `cache_directory(cache_key)`,
      surviving restarts and shared by every process using the same key. It
      evicts once its VOLUME is `disk_cache_volume_limit` full — counting
      everything on that volume, not just the cache.

    **For a reader on another machine**, which is what it is for: a log's own
    host reads its published table rarely, and a disk cache there would put
    back on local disk exactly what eviction removed. So litelink's own
    handles never cache to disk; a caller reading published tables from
    elsewhere — streamcast's `Stream.snapshot`, which spans several logs and
    caches by stream — asks for it here, with a key it chooses. The cache
    belongs to the DATABASE this builds, shared by all its cursors, so a
    caller pooling connections pools per key. See `install_read_cache`.

    **Never read a table's `version-hint.text` through a `disk_cache`
    connection** — neither by scanning the table's directory, which fails an
    ETag check after a new publish, nor with `read_text`, which silently
    returns the old hint. Resolve the table with `current_metadata` and scan
    the path it returns.

    A new connection per call, which the caller owns. Building one costs about
    a quarter of a second, nearly all of it `LOAD iceberg` (#102), so hold on
    to it. It is also the factory every log's reader is built with.
    """
    # Refused rather than ignored: the disk cache wraps httpfs, which only an
    # S3 connection loads, so a local one would ask for a cache and get none.
    if disk_cache and s3_options is None:
        msg = "disk_cache caches S3 reads; pass s3_options as well"
        raise ValueError(msg)

    cache = ReadCache(
        memory_cache=memory_cache,
        disk_cache=disk_cache,
        cache_key=cache_key,
        disk_cache_volume_limit=disk_cache_volume_limit,
    )

    connection = duckdb.connect()
    # `avro` BEFORE `iceberg`, and quietly: `iceberg`'s init auto-installs it
    # otherwise, which needs the network and defeats the point of bundling.
    # Missing is not fatal here — a machine-provisioned `iceberg` may resolve
    # it however it already does — so the failure to report is `iceberg`'s.
    with contextlib.suppress(ExtensionMissing, duckdb.Error):
        load_extension(connection, "avro", remote=False)

    load_extension(connection, "iceberg", remote=False)
    # No ATTACH of the buffer database. `Buffer.rows_from` records what that
    # cost: two SQLite libraries in one process is silent corruption, not a
    # slow path.
    if s3_options is not None:
        install_s3_secret(connection, s3_options)

    install_read_cache(connection, cache)

    return connection


class Reader:
    """A DuckDB connection with the buffer's tail registered on it, and the
    union it builds."""

    def __init__(
        self,
        layout: Layout,
        table: LogTable,
        buffer: Buffer,
        connect: Callable[[], duckdb.DuckDBPyConnection],
        published: Published,
    ) -> None:
        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._connect_to = connect
        # The shared published object, not a table handle: it opens the table
        # on first use, so a reader whose queries the published table cannot
        # answer never touches the network (I5).
        self._published = published
        self._remote_ready = False
        self._aws_ready = False
        # Which tiers a query needs, per tier (#90). Read from disk per query;
        # it re-decodes only when a write has bumped the tier rows' generation.
        self._stored = StoredTiers(buffer)
        self._connection: duckdb.DuckDBPyConnection | None = None
        # This reader's own, guarding the DuckDB connection and the view built
        # on it. Its own rather than the handle's, because a query must not wait
        # behind a maintenance pass — one waited 21.5 s behind a compaction
        # when the two shared a lock. Reads still serialise against each other:
        # `register` below is connection-global, so two concurrent scans on one
        # connection would swap each other's buffer leg.
        self._lock = threading.Lock()

    @property
    def _schema(self) -> pa.Schema:
        """Read, never remembered — a reader opened before a schema change
        must not serve the old columns for the rest of its life.

        `.table`, not `.schema`: this is the shape the DuckDB view is built
        in, and every projection here names `litelink_offset` first.
        """
        return self._buffer.shape().table

    def _prepare_remote(
        self, cursor: duckdb.DuckDBPyConnection
    ) -> tuple[str, tuple[int, int]] | None:
        """Load httpfs, install the credentials, return the published table's
        pointer and the offset range it covers.

        None when there is no published table or it holds nothing yet, which is
        the signal to build the union without a third leg rather than to fail.

        The range comes back with the pointer because the union needs it when
        the staging table holds nothing: with no staging extent there is no
        boundary between the published table and the buffer, and the registered
        tail can hold rows the published table has since taken ownership of.
        """
        published = self._published.table()
        if published is None:
            return None

        published.reload()
        # Paired, not two reads. `snapshot()` exists because a pointer and an
        # extent taken separately can come from different commits — and this
        # handle is shared, so a concurrent `publish` advances it between them.
        # Unpaired, the empty-extent branch below scans a NEWER published
        # snapshot while cutting the buffer at an OLDER extent, and every row
        # registered in between comes back from both legs.
        location, covered = published.snapshot()
        if covered is None:
            return None

        if not self._published.remote():
            # A local published table is read like the staging table: no
            # extension to load and no credentials to install.
            return location, covered

        options = self._published.s3.resolved()
        # Once per connection each. `httpfs` and `aws` are not in the local
        # read path, so a log whose queries never need the remote published
        # table never pays for them — §7's rule that a hot read is offline.
        # Credentials are resolved per query, so `aws` is loaded the first time
        # one resolves to the chain rather than only at the first.
        if not self._remote_ready:
            load_extension(self._connect(), "httpfs", remote=True)
            self._remote_ready = True

        if not self._aws_ready and uses_credential_chain(options):
            load_extension(self._connect(), "aws", remote=True)
            self._aws_ready = True

        create_secret(cursor, options)

        return location, covered

    def query(self, sql: str, *, published: bool = True) -> pa.RecordBatchReader:
        """Run `sql` against a freshly built `log` relation.

        **Which tiers it reads is decided here, per query.** The buffer unless
        its offsets rule it out; the staging table and the published table only
        when the log's tier manifest says they could hold a row `sql` matches —
        see `_tiers`. So a query bounded inside the staging window never
        touches the network, and one that asks for history reads it.

        The relation is rebuilt per call and cannot be held across calls.
        Resolving the table per query is §7's rule, not an optimisation: every
        commit writes a new metadata JSON, so a reader holding the snapshot it
        opened with reports an empty log after the writer's first seal.

        **Each query gets its own cursor**, and that is what makes the returned
        reader safe to hold. A reader is lazy — `_cast_to` keeps it streaming —
        so the caller drains it after this returns. On one shared connection,
        the next query's `register` and `CREATE OR REPLACE TEMP VIEW` land
        underneath a reader still streaming from those same names. Measured: a
        reader over 200 rows returned 0 after another query ran on the
        connection. Not perturbed — destroyed.

        A DuckDB cursor is an independent connection over the same database,
        with its own registrations and temp views, so one query cannot reach
        into another's. Verified directly rather than assumed.

        `published=False` drops the published leg whatever the terms say: the
        caller has asked for the staging table and the buffer only.
        """
        # Buffer first, table second, and the order is the correctness
        # argument. A seal commits its file and THEN deletes the rows it
        # covered, so between those two moments a row is in both tiers and
        # after them it is only in the table. Reading the buffer first means
        # anything a seal removes afterwards is already in the snapshot
        # resolved below; reading it second would let a seal land in between
        # and leave those rows in neither leg.
        #
        # The floor here only bounds how much is read — §7's point about a
        # deferred delete not inflating a query. It is not the boundary; that
        # is decided after, against a snapshot that cannot then move.
        with self._lock:
            # The lock covers building the cursor, not the query. Creating one
            # touches the shared connection; running on it does not. Built
            # first now, because the query's terms are parsed on it and they
            # decide whether the buffer is read at all.
            cursor = self._connect().cursor()

        found = terms(cursor, sql, self._schema)

        self._table.reload()
        floor = self._table.span()
        # The buffer is ruled out only by offset, from its lowest offset, and
        # only BEFORE its rows are read — skipping that read is the point.
        # Sound whenever it is taken: the lowest offset only rises, so a
        # query below it now matches nothing the buffer can later hold.
        boundary = None if floor is None else floor[1]
        tail = (
            self._buffer.rows_from(boundary)
            if self._buffer_could_match(found, boundary)
            else self._buffer.no_rows()
        )

        cursor.register(BUFFER_REL, tail)

        # Resolving per query is §7's rule. Both halves in one call, or a
        # commit between them pairs a new snapshot with an old boundary.
        self._table.reload()
        location, extent = self._table.snapshot()

        # The published table AFTER the staging snapshot, and that order is the
        # correctness argument. The published leg is bounded by `extent[0]` —
        # the oldest offset still in staging — so every offset below it must be
        # in the published snapshot this reads. Resolved first, it can be older
        # than the bound it is measured against: a publish pass registering
        # [100, 200) and an eviction dropping them both land in between, and the
        # reader pairs a published snapshot ending at 99 with a staging extent
        # starting at 200. Rows 100-199 are then in no leg at all.
        #
        # After, it cannot happen. I4 means nothing is evicted before it is
        # registered, so a published snapshot taken later than the staging one
        # holds everything the staging one has given up.
        local, needed = self._tiers(found, location, extent)
        remote = self._prepare_remote(cursor) if published and needed else None
        # Built every query now rather than cached against its own text. The
        # cache existed to skip reinstalling an identical view on a shared
        # connection; a fresh cursor has no view to reuse, and a CREATE VIEW
        # over an already-registered relation is cheap.
        cursor.execute(
            f"CREATE OR REPLACE TEMP VIEW {VIEW} AS "
            f"{self._union(location, extent, remote, local=local)}"
        )
        reader = cursor.execute(sql).to_arrow_reader()

        return _cast_to(reader, self._schema)

    def _buffer_could_match(
        self, found: tuple[Term, ...], boundary: int | None = None
    ) -> bool:
        """Whether the buffer's leg could hold a row matching `found`, by offset
        alone. Decides only WHETHER to read the buffer; where its leg is cut
        is `_union`'s, from exact resolved values.

        The buffer has no column statistics — computing them would cost the
        read this avoids — but `litelink_offset` is the log's sequence, so the
        leg's range is known: from a floor up, open-ended, since rows keep
        arriving. A query entirely below the floor cannot match it.

        **The floor is `max(lowest buffered offset, boundary)`**, where
        `boundary` is the staging table's end (None when staging is empty).
        Neither alone is always the tighter, which is why both are read:

        - **Sealed rows not yet evicted** — the ordinary state between passes,
          since a seal no longer deletes its rows (#122). The lowest offset is
          a sealed row BELOW the boundary that the leg will never return, so
          the boundary is the floor. Pruning on the lowest offset alone was a
          regression: the buffer stopped being ruled out for any scan below
          the tail.
        - **Just after `evict("buffer")`** — the lowest offset is the first
          unsealed row, normally EQUAL to the boundary. Either serves.
        - **A gap above staging** — offsets reserved and never filled, as a
          failed `ingest` leaves (accepted, see `ingest`). The first buffered
          row sits ABOVE the boundary, so the lowest offset is the floor, and
          a scan falling inside the gap skips the buffer. This is why the
          boundary alone is not enough.
        - **No staging table** — no boundary, so the floor is the lowest
          buffered offset. It may be a row the published table also holds
          (a crash between `publish` and `evict("buffer")`, or a restored
          log), which only costs a read: `_union` cuts the leg at the
          published table's exact span, so nothing is returned twice.

        **Not the tier manifest's `published` row**, though it looks like the
        cutoff wanted in that last case. That row may OVERSTATE what the
        published table holds — eviction widens it before its commit, and
        overstating is its safe direction for skipping the published table.
        For skipping the buffer it is the unsafe direction: a query under an
        overstated bound would skip buffer rows the published table lacks.
        """
        # Closed by `retire()`: an empty range at the log's end, which no
        # query needs, with offset terms or without.
        closed = self._stored.load().get(BUFFER)
        if closed is not None:
            unit = entry(BUFFER, closed.offsets, self._schema, closed.statistics)
            return bool(prune(build([unit], key=KEY), [BUFFER], found, key=KEY))

        if not any(column == OFFSET for column, _, _ in found):
            return True

        lowest = self._buffer.lowest_offset()
        if lowest is None:
            return True

        floor = lowest if boundary is None else max(lowest, boundary)

        unit = entry(BUFFER, (floor, None), self._schema, UNKNOWN)

        return bool(prune(build([unit], key=KEY), [BUFFER], found, key=KEY))

    def _tiers(
        self,
        found: tuple[Term, ...],
        location: str,
        extent: tuple[int, int] | None,
    ) -> tuple[bool, bool]:
        """`(staging, published)`: which of the two tiers the query needs (#90).

        The staging tier's row is the rollup of the snapshot this read resolved
        (`location`), so the decision and the data are one version. The
        published table's is the row in `buffer.db`, read AFTER that snapshot:
        eviction widens it before the commit that moves rows below the staging
        table, so a row read later covers everything the resolved snapshot has
        given up. Neither touches the network.

        With no terms, only a tier known to be empty is skipped, and a staging
        table with an extent is not empty — so the staging rollup, 2–3 ms after
        each commit, is not computed for a query it cannot narrow.
        """
        entries = []
        stored = self._stored.load()
        published = stored.get(PUBLISHED)
        if published is not None:
            entries.append(
                entry(PUBLISHED, published.offsets, self._schema, published.statistics)
            )

        if extent is not None and found:
            # The stored rollup when it is of exactly the version this read
            # resolved; else this process's own, computed once per version.
            cached = stored.get(STAGING)
            local = (
                cached.statistics
                if cached is not None and cached.version == location
                else self._table.statistics_at(location)
            )
            if local is not None:
                entries.append(entry(STAGING, extent, self._schema, local))

        table = build(entries, key=KEY) if entries else None
        kept = prune(table, TIERS, found, key=KEY)

        return STAGING in kept, PUBLISHED in kept

    def _union(
        self,
        location: str,
        extent: tuple[int, int] | None,
        remote: tuple[str, tuple[int, int]] | None = None,
        *,
        local: bool = True,
    ) -> str:
        """The hot read: the staging table, plus the buffer above its extent.

        `location` is passed rather than re-read, so the snapshot scanned and
        the boundary cutting the buffer are the same one.
        """
        # Bound once. `_schema` reads through the buffer now, so touching it
        # inside the comprehension below would be a keyed `meta` read per
        # COLUMN rather than per query.
        schema = self._schema
        columns = tuple(schema.names)
        # Cast the buffer side explicitly rather than letting UNION ALL
        # reconcile: SQLite's per-value typing comes through loosely, and a
        # column that holds integers in every row can still surprise the union
        # (§7). Aliased back to the bare name, or the buffer-only leg exposes
        # columns called `CAST(b."x" AS BIGINT)`.
        casts = ", ".join(
            f'b."{c}"::{column_type(schema.field(c).type).duckdb} AS "{c}"'
            for c in columns
        )

        projection = ", ".join(f'"{c}"' for c in columns)
        buffered = f"SELECT {casts} FROM {BUFFER_REL} b"
        if extent is None:
            # Nothing in the staging table. The published table can still hold
            # history — a log evicted down to nothing is §8's
            # `staging_retention=0` shape — and then everything else is in the
            # buffer.
            if remote is None:
                return buffered

            # The buffer still needs a floor, and with no staging extent the
            # published table supplies it. The tail was registered against an
            # earlier read, so it can hold rows that have since been sealed,
            # published and evicted — leaving the staging table empty again and
            # both legs claiming them. Bounding here is what the `extent[1]` cut does in
            # the branch below, from the only boundary available.
            location, covered = remote

            return (
                f"SELECT {projection} FROM iceberg_scan('{location}')"
                f' UNION ALL {buffered} WHERE b."{OFFSET}" >= {covered[1]}'
            )

        legs = []
        if remote is not None:
            # Strictly below what the staging table holds. `extent[0]` is the
            # oldest offset still in staging, so anything at or above it is
            # served from the staging table rather than the published one.
            legs.append(
                f"SELECT {projection} FROM iceberg_scan('{remote[0]}')"
                f' WHERE "{OFFSET}" < {extent[0]}'
            )

        if local:
            # Skipped when the tier manifest rules the staging table out. Its
            # extent still bounds the other two legs, so they cover exactly
            # what they would have beside it.
            legs.append(f"SELECT {projection} FROM iceberg_scan('{location}')")

        # The buffer bound is applied here as well as pushed into SQLite. The
        # registered tail was read against an earlier floor, so it can still
        # hold rows this snapshot has since taken ownership of; without this
        # they would appear in both legs.
        legs.append(f'{buffered} WHERE b."{OFFSET}" >= {extent[1]}')

        return " UNION ALL ".join(legs)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """The connection, built on first read and kept.

        Lazily, so a log that only ever appends never pays for one.
        """
        if self._connection is None:
            self._connection = self._connect_to()

        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


def _cast_to(reader: pa.RecordBatchReader, schema: pa.Schema) -> pa.RecordBatchReader:
    """Cast a reader's batches to the declared column types — the DuckDB edge.

    DuckDB has one string type and one blob type and returns the 32-bit-offset
    Arrow forms for both, so a column declared `large_binary` would otherwise
    come back as `binary`: a silent contradiction of what the caller asked for.

    Lazy, so a streaming read stays streaming. Nearly free — widening offsets
    shares the data buffer, measured at 0.03 ms for 50,000 rows over 21 MB —
    which is why this is done on every batch rather than only when it differs.

    A projection selects a subset of columns, so the target is narrowed to
    whatever the query actually returned.
    """
    target = pa.schema(
        [
            schema.field(name) if name in schema.names else reader.schema.field(name)
            for name in reader.schema.names
        ]
    )
    if target.equals(reader.schema):
        return reader

    return pa.RecordBatchReader.from_batches(
        target, (batch.cast(target) for batch in reader)
    )
