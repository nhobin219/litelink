"""Fixtures shared by the tests that need object storage.

Skipped unless an S3-compatible endpoint answers — `just rustfs` brings one up
locally, and the same tests run against AWS by pointing `AWS_ENDPOINT_URL`
elsewhere or unsetting it.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import pytest

from litelink._s3 import S3Options

if TYPE_CHECKING:
    from collections.abc import Iterator


def options() -> S3Options:
    """Explicit for rustfs, environment for anything else.

    `just rustfs` is the default because it needs no credentials to exist
    anywhere; exporting `AWS_ENDPOINT_URL` runs the same tests against another
    endpoint, and unsetting it runs them against AWS.
    """
    if os.environ.get("AWS_ENDPOINT_URL"):
        return S3Options().resolved()

    return S3Options(
        endpoint="http://127.0.0.1:9000",
        access_key="litelink",
        secret_key="litelink-secret",
        region="us-east-1",
    ).resolved()


def filesystem(s3: S3Options):  # noqa: ANN201  — s3fs is an optional import
    s3fs = pytest.importorskip("s3fs", reason="the `s3` extra is not installed")

    return s3fs.S3FileSystem(
        key=s3.access_key,
        secret=s3.secret_key,
        client_kwargs={"endpoint_url": s3.endpoint, "region_name": s3.region},
    )


@pytest.fixture(scope="session")
def s3() -> S3Options:
    """The endpoint, or a skip. Reachability is checked once, by listing.

    A connection error means no endpoint is running and the tier is untestable
    here; anything else is a real failure and must not be swallowed into a
    skip, or a broken archive would look like an absent one.
    """
    resolved = options()
    fs = filesystem(resolved)
    try:
        fs.ls("/")
    except OSError as exc:
        pytest.skip(f"no S3 endpoint at {resolved.endpoint}: {exc}")

    return resolved


@pytest.fixture
def bucket(s3: S3Options) -> Iterator[str]:
    """A fresh bucket per test, removed afterwards.

    Per test rather than shared: these assert on object counts and on what a
    catalog holds, and a bucket carrying another test's files makes both
    meaningless.
    """
    fs = filesystem(s3)
    name = f"litelink-test-{uuid.uuid4().hex[:12]}"
    fs.mkdir(name)
    try:
        yield name
    finally:
        fs.rm(name, recursive=True)
