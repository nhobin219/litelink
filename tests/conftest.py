"""Fixtures shared by the tests that need object storage.

Skipped unless an S3-compatible endpoint answers — `just rustfs` brings one up
locally, and the same tests run against AWS by pointing `AWS_ENDPOINT_URL`
elsewhere or unsetting it.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from typing import TYPE_CHECKING

import pytest

from litelink._s3 import S3Options

# Names a bucket to share across the run, instead of creating one per test.
_BUCKET = "LITELINK_TEST_BUCKET"

if TYPE_CHECKING:
    from collections.abc import Iterator


def options() -> S3Options:
    """Explicit for rustfs, environment for anything else.

    `just rustfs` is the default because it needs no credentials to exist
    anywhere. Naming a bucket through `LITELINK_TEST_BUCKET` — or pointing
    `AWS_ENDPOINT_URL` somewhere else — switches to whatever the environment
    resolves, which on AWS is the ordinary credential chain: profile, instance
    metadata, SSO. Nothing here restates a key that boto3 already knows.
    """
    if os.environ.get("AWS_ENDPOINT_URL") or os.environ.get(_BUCKET):
        return S3Options().resolved()

    return S3Options(
        endpoint="http://127.0.0.1:9000",
        access_key="litelink",
        secret_key="litelink-secret",
        region="us-east-1",
    ).resolved()


def filesystem(s3: S3Options):  # noqa: ANN201  — s3fs is an optional import
    s3fs = pytest.importorskip(
        "s3fs",
        reason=(
            "s3fs is missing — it is a dev dependency used by the test "
            "fixtures, not by litelink. Run `uv sync`."
        ),
    )

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
    except Exception as exc:  # noqa: BLE001
        # Broad on purpose. This used to catch `OSError`, which is what a
        # refused connection raises through pyarrow — but s3fs answers with
        # `botocore.exceptions.EndpointConnectionError`, which is not one. The
        # result was 91 ERRORS instead of 91 skips the moment s3fs was
        # installed without an endpoint to talk to.
        #
        # Anything at all here means the tier cannot be exercised, and the
        # honest response is to say so once rather than to fail every test in
        # it with the same connection error.
        pytest.skip(
            f"no S3 endpoint at {resolved.endpoint}: {type(exc).__name__}: {exc}"
        )

    return resolved


@pytest.fixture
def bucket(s3: S3Options) -> Iterator[str]:
    """Somewhere isolated to write, removed afterwards.

    A whole bucket per test against a local endpoint, where buckets are free
    and a leftover one is a container restart away from gone. Against a real
    account, `LITELINK_TEST_BUCKET` names one bucket and each test gets a
    prefix inside it — creating and destroying eleven buckets per run is slow,
    rate-limited, and leaves debris in someone's account if a run is
    interrupted.

    Either way the value is a location rather than a bucket name, so a caller
    writing `s3://{bucket}/prefix` gets an isolated one of its own.
    """
    fs = filesystem(s3)
    shared = os.environ.get(_BUCKET)
    if shared:
        location = f"{shared.strip('/')}/run-{uuid.uuid4().hex[:12]}"
    else:
        location = f"litelink-test-{uuid.uuid4().hex[:12]}"
        fs.mkdir(location)

    try:
        yield location
    finally:
        # Best effort. A failed cleanup must not turn a passing test red, and
        # against a shared bucket the prefix may legitimately not exist —
        # nothing wrote to it.
        with contextlib.suppress(Exception):
            fs.rm(location, recursive=True)
