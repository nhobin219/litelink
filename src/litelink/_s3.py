"""Credentials for the archive tier (SPEC §5).

Deliberately NOT part of `LogConfig`. Everything in that object is persisted to
the buffer's `meta` table so `open` can recover the log's policy, and secrets
are the one kind of setting that must not be written into the thing they
protect — a log directory is copied, backed up and attached from other
machines, and a key inside it travels with all of that.

So credentials are arguments to `new`/`open`: per-process, never stored. Passed
explicitly or read from the environment, with explicit winning — which is what
makes "test against a local endpoint, then against AWS" a change of environment
rather than a change of code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Any

# Read when an argument is not supplied. `AWS_ENDPOINT_URL` is the variable the
# AWS SDKs and CLI already honour for an S3-compatible endpoint, so pointing at
# rustfs or MinIO needs no litelink-specific name.
_ENV = {
    "endpoint": "AWS_ENDPOINT_URL",
    "access_key": "AWS_ACCESS_KEY_ID",
    "secret_key": "AWS_SECRET_ACCESS_KEY",
    "region": "AWS_REGION",
}


@dataclass(frozen=True, slots=True)
class S3Options:
    """How to reach the archive's object store.

    Every field optional, because AWS resolves all of them itself from instance
    metadata, a profile, or the environment. An endpoint is only needed to point
    somewhere that is not AWS.
    """

    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    region: str | None = None

    @classmethod
    def from_env(cls) -> S3Options:
        """Whatever the environment supplies, with anything unset left None."""
        return cls(**{field: os.environ.get(var) for field, var in _ENV.items()})

    def resolved(self) -> S3Options:
        """This, with unset fields filled from the environment.

        Explicit wins. A caller that passed a key meant that key, and silently
        preferring an ambient one would make a test that thought it was talking
        to a local endpoint talk to whatever the shell happened to hold.
        """
        env = self.from_env()

        return replace(
            self,
            **{
                field: getattr(env, field)
                for field in _ENV
                if getattr(self, field) is None
            },
        )

    def catalog_properties(self) -> dict[str, str]:
        """As pyiceberg names them, omitting anything still unset.

        Omitted rather than passed as None, so pyiceberg falls back to its own
        resolution — which on AWS is the whole credential chain.
        """
        named = {
            "s3.endpoint": self.endpoint,
            "s3.access-key-id": self.access_key,
            "s3.secret-access-key": self.secret_key,
            "s3.region": self.region,
        }

        return {key: value for key, value in named.items() if value is not None}


def filesystem(options: S3Options) -> Any:
    """A pyarrow filesystem for the archive, from the same options as the log.

    pyarrow rather than s3fs: `pyiceberg[pyarrow]` is already a runtime
    dependency and s3fs is not, so a caller that needed it would fail on
    exactly the installs that have an archive to reach.

    Here rather than beside its first caller, because it now has two — the
    migration tool and the snapshot path — and a second spelling of "build a
    filesystem from these options" is a second place for the endpoint override
    to be forgotten.
    """
    from pyarrow.fs import S3FileSystem

    resolved = options.resolved()
    named = {
        "access_key": resolved.access_key,
        "secret_key": resolved.secret_key,
        "region": resolved.region,
        "endpoint_override": resolved.endpoint,
    }

    return S3FileSystem(**{k: v for k, v in named.items() if v is not None})
