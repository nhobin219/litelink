"""Offline checks for the matrix, schedule and statistical decision."""

from __future__ import annotations

import pytest
from append_cap import abba, bootstrap, cell_name, gate, matrix


def observations(rate_ratio: float, tail_ratio: float) -> list[dict]:
    rows = []
    for rep in range(15):
        # Shared drift differs by orders of magnitude: independent resampling
        # would lose pairing and turn these exact ratios into broad intervals.
        drift = 2.0**rep
        for reference in ("OFF", "BASE"):
            for variant in abba(reference, bool(rep % 2)):
                rows.append(
                    {
                        "rep": rep,
                        "reference": reference,
                        "variant": variant,
                        "rows_s": drift * (rate_ratio if variant == "ON" else 1),
                        "p99_us": drift * (tail_ratio if variant == "ON" else 1),
                    }
                )

    return rows


def test_matrix_and_balanced_schedule():
    cells = matrix([1, 16, 256, 4096])
    assert len(cells) == len({cell_name(c) for c in cells}) == 18
    assert sum(c["kind"] == "e2e" or c["batch"] >= 16 for c in cells) == 14
    assert {c["group"] for c in cells if c["kind"] == "e2e"} == {False, True}
    for reference in ("OFF", "BASE"):
        assert abba(reference, False) == (reference, "ON", "ON", reference)
        assert abba(reference, True) == ("ON", reference, reference, "ON")


def test_bootstrap_preserves_pairs_and_reports_absolute_intervals():
    report = bootstrap(observations(1, 1), 100, 447221)
    assert report["estimates"]["BASE"]["rows_s"]["median"] == 128
    assert report["estimates"]["BASE"]["rows_s"]["ci95"][0] < 128
    assert report["estimates"]["BASE"]["rows_s"]["ci95"][1] > 128
    for comparison in report["comparisons"].values():
        assert comparison["rows_s"] == {"ratio": 1, "ci95": [1, 1]}
        assert comparison["p99_us"] == {"ratio": 1, "ci95": [1, 1]}
        assert comparison["pass"]


@pytest.mark.parametrize("rate,tail", [(0.97, 1), (1, 1.06)])
def test_each_metric_can_fail_the_gate(rate, tail):
    report = bootstrap(observations(rate, tail), 100, 447221)
    assert all(not c["pass"] for c in report["comparisons"].values())


def test_both_ci_bounds_must_clear_thresholds():
    assert gate({"rows_s": {"ci95": [0.98, 1.1]}, "p99_us": {"ci95": [0.9, 1.05]}})
    assert not gate({"rows_s": {"ci95": [0.979, 1.1]}, "p99_us": {"ci95": [0.9, 1]}})
    assert not gate({"rows_s": {"ci95": [1, 1.1]}, "p99_us": {"ci95": [0.9, 1.051]}})
