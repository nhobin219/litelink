"""Conditional storage for the optional published-location lease (SPEC §5a)."""

from __future__ import annotations

import contextlib
import hashlib
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import ClientError

from litelink._fs import fsync


class ConditionalWritesUnsupported(RuntimeError):
    """The backend cannot enforce both creation and replacement conditions."""


class Conflict(Exception):
    """The record changed since it was read."""


class Store(Protocol):
    def read(self) -> tuple[bytes, str] | None: ...

    def write(self, data: bytes, expected: str | None) -> str: ...


class FileStore:
    def __init__(self, path: Path):
        self.path = path

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        # The lock inode must survive replacement of the record, including
        # release. Unlinking it lets two processes lock different inodes.
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        # A lease may precede the table itself. Persist every ancestor's
        # directory entry too: fsync of the record's directory alone cannot
        # preserve a newly created published prefix after a power loss.
        for directory in (self.path.parent, *self.path.parent.parents):
            fsync(directory)

        with self.path.with_suffix(".lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _read(self) -> tuple[bytes, str] | None:
        try:
            data = self.path.read_bytes()
        except FileNotFoundError:
            return None

        return data, hashlib.sha256(data).hexdigest()

    def read(self) -> tuple[bytes, str] | None:
        with self._locked():
            return self._read()

    def write(self, data: bytes, expected: str | None) -> str:
        with self._locked():
            current = self._read()
            if (current[1] if current else None) != expected:
                raise Conflict

            if expected is None:
                fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())

            else:
                temporary = self.path.with_name(f"{self.path.name}.{uuid.uuid4().hex}")
                try:
                    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(fd, "wb") as stream:
                        stream.write(data)
                        stream.flush()
                        os.fsync(stream.fileno())

                    os.replace(temporary, self.path)
                finally:
                    temporary.unlink(missing_ok=True)

            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

        return hashlib.sha256(data).hexdigest()


class S3Store:
    def __init__(self, client: Any, bucket: str, key: str):
        self.client = client
        self.bucket = bucket
        self.key = key

    def read(self) -> tuple[bytes, str] | None:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "NoSuchKey":
                return None

            raise

        with response["Body"] as body:
            return body.read(), response["ETag"]

    def write(self, data: bytes, expected: str | None) -> str:
        condition = {"IfNoneMatch": "*"} if expected is None else {"IfMatch": expected}
        try:
            response = self.client.put_object(
                Bucket=self.bucket, Key=self.key, Body=data, **condition
            )
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in {"PreconditionFailed", "ConditionalRequestConflict"}:
                raise Conflict from exc

            if code in {"NotImplemented", "InvalidRequest", "InvalidArgument"}:
                raise ConditionalWritesUnsupported(
                    "S3 backend must support PutObject If-None-Match and If-Match"
                ) from exc

            raise

        return response["ETag"]

    def verify(self) -> None:
        # Some S3-compatible stores silently ignore unknown headers. An API
        # success alone cannot establish that the backend arbitrates writers.
        probe = S3Store(
            self.client, self.bucket, f"{self.key}.probe-{uuid.uuid4().hex}"
        )
        created = False
        try:
            version = probe.write(b"initial", None)
            created = True
            for expected in (None, '"litelink-deliberately-wrong-etag"'):
                try:
                    probe.write(b"must-not-be-written", expected)
                except Conflict:
                    continue

                raise ConditionalWritesUnsupported(
                    "S3 backend ignored a conditional write; refusing lease"
                )

            probe.write(b"replacement", version)
        except Conflict as exc:
            raise ConditionalWritesUnsupported(
                "S3 backend rejected a valid conditional write; refusing lease"
            ) from exc
        finally:
            if created:
                # Only this unique probe is removed; lease records are never
                # deleted, because a stale release must not delete a successor.
                self.client.delete_object(Bucket=self.bucket, Key=probe.key)
