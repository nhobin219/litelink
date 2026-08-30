"""`preflight` — the check for provisioning that looks complete.

Every tier past local disk needs something the wheel cannot carry, and each
one goes missing quietly: litestream is not needed until a restore, `httpfs`
is not compiled into the duckdb wheel, and an archive can be configured
against credentials that do not work. None of the three is visible in code and
all three are checkable in about a second.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

import litelink
from litelink._preflight import Check, Report, preflight

if TYPE_CHECKING:
    from litelink._s3 import S3Options

pytestmark = pytest.mark.s3


def test_a_report_is_readable_and_actionable() -> None:
    """It has to survive being printed into a startup log and read by a human.

    A health endpoint walks `checks`; a probe prints the report; a service that
    must not come up half-provisioned calls `raise_if_not_ready`.
    """
    good = Report((Check("a", ok=True, detail="fine"),))
    bad = Report(
        (Check("a", ok=True, detail="fine"), Check("b", ok=False, detail="no"))
    )

    assert good.ok
    assert not bad.ok
    assert "PASS" in str(good)
    assert "READY" in str(good)
    assert "NOT READY" in str(bad)

    good.raise_if_not_ready()
    with pytest.raises(RuntimeError, match="not provisioned"):
        bad.raise_if_not_ready()

    # The failure names itself; the passing check is not repeated into it.
    with pytest.raises(RuntimeError, match="b: no") as caught:
        bad.raise_if_not_ready()

    assert "a: fine" not in str(caught.value)


def test_the_local_tier_needs_nothing_beyond_the_wheel() -> None:
    """The claim the README makes, asserted.

    `iceberg` is the only extension a local-first log loads, and it is the one
    DuckDB will autoinstall — so a machine with the wheel and nothing else
    should pass with replication switched off.
    """
    report = preflight(replication=False)

    assert report.ok, str(report)
    assert [check.name for check in report.checks] == ["duckdb read path"]


def test_it_finds_litestream_and_reports_the_version() -> None:
    """Presence is not enough: v0.5.0 changed the config format.

    `_replication` writes one shape, so an older binary passes every presence
    check and then fails at the restore — which is the moment this whole module
    exists to move earlier.
    """
    if shutil.which("litestream") is None and not os.access(".bin/litestream", os.X_OK):
        pytest.skip("litestream is not provisioned — run `just litestream`")

    report = preflight(replication=True)
    check = next(c for c in report.checks if c.name == "litestream")

    assert check.ok, check.detail
    assert "0.5" in check.detail, "the version has to be reported, not just presence"


def test_it_reports_an_archive_it_cannot_read(bucket: str, s3: S3Options) -> None:
    """The case `new` deliberately cannot refuse.

    Configuring an archive is a statement of intent — credentials commonly
    attach to a box after the log is configured — so `new` lets bad ones
    through and nothing on the write path finds out until the first `sync`.
    Here there is a human asking, so a refusal is reportable.
    """
    where = f"s3://{bucket}/prefix"
    wrong = replace(s3, access_key="wrong-key", secret_key="wrong-secret")

    report = preflight(archive=where, s3=wrong, replication=False)
    check = next(c for c in report.checks if c.name.startswith("archive"))

    assert not check.ok
    assert not report.ok
    assert "wrong" not in check.detail.lower() or "key" not in check.detail.lower(), (
        "the report goes into logs; it must not echo the secret it was given"
    )


def test_it_passes_an_archive_that_is_merely_empty(bucket: str, s3: S3Options) -> None:
    """Empty is not broken, and conflating them would make the check useless.

    An archive nothing has been pushed to yet is the ordinary state of a log
    on its first day. `archive_extent` answers None there and raises only when
    the bucket answers with a refusal, which is the distinction this reports.
    """
    report = preflight(archive=f"s3://{bucket}/never-written", s3=s3, replication=False)
    check = next(c for c in report.checks if c.name.startswith("archive"))

    assert check.ok, check.detail
    assert "nothing published" in check.detail


def test_the_module_entry_point_exits_nonzero_when_not_ready(
    bucket: str, s3: S3Options
) -> None:
    """`python -m litelink` is the point-and-shoot form, so its EXIT CODE is
    the contract — a provisioning step runs it and stops on failure."""
    resolved = s3.resolved()
    environment = dict(os.environ)
    environment.update(
        {
            "AWS_ENDPOINT_URL": resolved.endpoint or "",
            "AWS_ACCESS_KEY_ID": "wrong-key",
            "AWS_SECRET_ACCESS_KEY": "wrong-secret",
            "AWS_REGION": resolved.region or "us-east-1",
        }
    )
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "litelink", f"s3://{bucket}/prefix", "s"],
        capture_output=True,
        env=environment,
        timeout=120,
        check=False,
    )

    assert done.returncode == 1, done.stdout.decode()
    assert b"NOT READY" in done.stdout


def test_preflight_is_exported() -> None:
    """It is the first thing a new operator runs, so it is public."""
    assert "preflight" in litelink.__all__
    assert litelink.preflight is preflight


def test_a_warning_is_visible_without_failing_the_report() -> None:
    """A risk worth acting on is not a reason to refuse to provision.

    `Check.warning` is a third state on top of `ok`. The case it exists for is
    the host clock: a VM on the `tsc` clocksource CAN crash-loop the sidecar,
    but plenty of such guests never do, so failing would be a false alarm — and
    burying it in a PASS line would be missed, which is the whole problem with
    this failure. It logs successful syncs right up to each panic.

    Falsify by making `warning` fail the report, or by rendering it as PASS.
    """
    risky = Check("host clock", ok=True, detail="tsc on a kvm guest", warning=True)
    report = Report((Check("a", ok=True, detail="fine"), risky))

    assert report.ok, "a warning must not refuse to provision"
    assert "WARN" in str(risky)
    assert "PASS" not in str(risky)
    assert "READY, with warnings" in str(report)

    # And a failure still dominates a warning.
    broken = Report((risky, Check("b", ok=False, detail="no")))
    assert not broken.ok
    assert "NOT READY" in str(broken)


def test_the_clock_check_does_not_sample_and_does_not_fail() -> None:
    """It reports a COMBINATION, because sampling cannot prove absence.

    The obvious implementation — spin for a second counting backwards steps —
    was measured on a KVM guest running `tsc`, which is the affected
    configuration: 105 million samples over 20 seconds, zero regressions, while
    the upstream reporter saw 4 in 45 seconds on their host. So a sampling
    check prints PASS on hardware that can still crash-loop, and false
    confidence is worse than no check.

    It also must not fail: this is a risk, not a defect, and refusing to
    provision over it would be wrong on the many guests that are fine.
    """
    from litelink._preflight import _clocksource

    started = time.monotonic()
    check = _clocksource()
    elapsed = time.monotonic() - started

    assert elapsed < 1.0, (
        f"the clock check took {elapsed:.2f}s — it must not be sampling, "
        f"because sampling cannot prove the clock is well behaved"
    )
    assert check.ok, "a clock risk must never fail provisioning"

    # Whatever this host is, the detail has to say which clocksource it saw.
    assert "clocksource" in check.detail or "not reported" in check.detail

    if check.warning:
        # On an affected host it must say the two things that are easy to get
        # wrong: what to check, and that the fix needs to persist.
        assert "check restarts, not log lines" in check.detail
        assert "persistent" in check.detail
