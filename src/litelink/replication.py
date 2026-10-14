"""The WAL replication primitives a log's replication is built from (§3a).

A log's databases are replicated by a litestream sidecar the deployment runs,
from a config litelink writes, and brought back by a one-shot restore. These
are the functions that do it, public: the config, the control socket, the
flush, the restore and the binary they run. Each takes a `Replicated` (a name,
a directory, the SQLite databases under it), which a log's own layout is, and
which any object with those three members is too.

    @dataclass(frozen=True)
    class Databases:
        name: str
        directory: Path
        databases: tuple[Path, ...]

    layout = Databases("app", root / "app", (root / "app" / "app.db",))
    config = layout.directory / "litestream.yml"
    config.write_text(litestream_config(layout, "s3://bucket/prefix", s3_options))
    # ... the deployment runs `litestream replicate -config <config>` ...
    flush(layout, layout.databases[0])          # in the replica before continuing

    # On a machine that has no database:
    restored = restore_database(config, layout.databases[0], s3_options=s3_options)

Nothing here starts the continuous sidecar. A library that supervised it
would own its lifecycle and could become a second replicator of one database,
which is the corruption litestream warns about. `flush` is a client of the
running sidecar and `restore_database` a one-shot command that exits.

**One directory is one sidecar.** The config names each database by its path
relative to `directory`, the replica goes to `<published>/<name>/_wal`, and the
control socket is derived from `directory`. Two layouts with one directory
would write one config over the other and share one socket, so the second
stops the first replicating with no error. A log's directory is the log's.
"""

from litelink._replication import (
    Replicated,
    control_socket,
    destination,
    flush,
    litestream_binary,
    litestream_config,
    restore_database,
)

__all__ = [
    "Replicated",
    "control_socket",
    "destination",
    "flush",
    "litestream_binary",
    "litestream_config",
    "restore_database",
]
