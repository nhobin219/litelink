"""An optional writer lease; scheduling and writer fencing belong to the caller."""

from __future__ import annotations

import json
import math
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from litelink._layout import LOCAL_SCHEME, Layout, validate_published
from litelink._lease_store import Conflict, FileStore, S3Store, Store
from litelink._s3 import S3Options


@dataclass(frozen=True, slots=True)
class Held:
    """The record observed or written, with a Unix wall-clock deadline."""

    owner: str
    token: str
    expires_at: float


class Leased(RuntimeError):
    """Acquisition found a live lease (including the configured skew margin)."""

    def __init__(self, holder: Held):
        self.holder = holder
        self.expires_at = holder.expires_at
        super().__init__(f"leased by {holder.owner!r} until {holder.expires_at}")


class LeaseLost(RuntimeError):
    """The handle no longer owns the last version it successfully wrote."""


def _decode(data: bytes) -> Held | None:
    value = json.loads(data)
    if (
        not isinstance(value, dict)
        or set(value) != {"owner", "token", "expires_at"}
        or not isinstance(value["owner"], str)
        or not isinstance(value["token"], str)
        or not value["token"]
        or type(value["expires_at"]) not in (int, float)
        or not math.isfinite(value["expires_at"])
        or (not value["owner"] and value["expires_at"] != 0)
    ):
        raise ValueError("invalid lease record")

    return Held(**value) if value["owner"] else None


class Lease:
    """One handle's authority; state() never adopts another handle's version."""

    def __init__(self, store: Store, owner: str, ttl_s: float, skew_s: float):
        self._store = store
        self._owner = owner
        self._ttl_s = ttl_s
        self._skew_s = skew_s
        self._held: Held | None = None
        self._version: str | None = None
        self._lock = threading.Lock()

    def state(self) -> Held | None:
        """Read current ownership, including expired records; no I/O is cached."""
        record = self._store.read()

        return _decode(record[0]) if record else None

    def acquire(self) -> Held:
        """Create or CAS an expired record; refuse a live owner's record."""
        with self._lock:
            if self._held is not None:
                raise RuntimeError("already acquired; renew or release this handle")

            for _ in range(4):
                record = self._store.read()
                held = _decode(record[0]) if record else None
                if held is not None and time.time() < held.expires_at + self._skew_s:
                    raise Leased(held)

                try:
                    return self._write(record[1] if record else None)
                except Conflict:
                    continue

            raise LeaseLost("lease changed repeatedly during acquisition; retry")

    def renew(self) -> Held:
        """Extend only the last successfully written, unexpired version."""
        with self._lock:
            self._require_held()
            try:
                return self._write(self._version)
            except Conflict as exc:
                self._held = None
                self._version = None
                raise LeaseLost("lease changed before renewal") from exc

    def release(self) -> None:
        """CAS a tombstone; a stale releaser cannot remove a new owner."""
        with self._lock:
            self._require_held()
            try:
                self._write(self._version, released=True)
            except Conflict as exc:
                raise LeaseLost("lease changed before release") from exc
            finally:
                self._held = None
                self._version = None

    def _require_held(self) -> None:
        if self._held is None or time.time() >= self._held.expires_at - self._skew_s:
            self._held = None
            self._version = None
            raise LeaseLost("lease not held or expired")

    def _write(self, expected: str | None, *, released: bool = False) -> Held:
        held = Held(
            "" if released else self._owner,
            uuid.uuid4().hex,
            0 if released else time.time() + self._ttl_s,
        )
        # A fresh token for EVERY mutation prevents equal-content ETags from
        # reviving stale authority when two operations share a clock tick.
        data = json.dumps(asdict(held), allow_nan=False).encode()
        self._version = self._store.write(data, expected)
        self._held = held

        return held


def lease(
    published: str,
    name: str,
    *,
    owner: str,
    ttl_s: float = 60,
    skew_s: float = 0,
    s3_options: S3Options | None = None,
) -> Lease:
    """Build a lease over the same published/name location as new/restore.

    S3 construction probes conditional-write support before touching the lease.
    Local construction does no I/O. Existing log operations do not enforce this
    optional lease; the caller must stop its writer before its deadline.
    """
    validate_published(published)
    layout = Layout(Path("."), name)
    if not owner or not isinstance(owner, str):
        raise ValueError("owner must be a nonempty string")

    if not math.isfinite(ttl_s) or not math.isfinite(skew_s) or not 0 <= skew_s < ttl_s:
        raise ValueError("ttl_s must be finite and positive; 0 <= skew_s < ttl_s")

    location = layout.published_table_location(published).rstrip("/") + "/_lease.json"
    store: Store
    if location.startswith(LOCAL_SCHEME):
        store = FileStore(Path(location.removeprefix(LOCAL_SCHEME)))
    else:
        import boto3
        from botocore.config import Config

        options = (s3_options or S3Options()).resolved()
        client = boto3.client(
            "s3",
            endpoint_url=options.endpoint,
            aws_access_key_id=options.access_key,
            aws_secret_access_key=options.secret_key,
            region_name=options.region,
            # A timeout may hide a committed PUT. Do not retry writes and then
            # mistake the precondition failure for proof that nothing committed.
            config=Config(
                retries={"total_max_attempts": 1},
                # The table I/O resolves its endpoint through S3Options.
                # SDK-only overrides must not send ownership to another store.
                ignore_configured_endpoint_urls=True,
            ),
        )
        bucket, _, key = location.removeprefix("s3://").partition("/")
        backend = S3Store(client, bucket, key)
        backend.verify()
        store = backend

    return Lease(store, owner, ttl_s, skew_s)
