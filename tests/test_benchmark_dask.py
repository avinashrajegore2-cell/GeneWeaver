import pytest

from benchmarks.benchmark_dask import summarize_trials


def test_summarize_trials_calculates_medians():
    summary = summarize_trials(
        times=[2.0, 1.0, 3.0],
        memories=[20.0, 10.0, 30.0],
    )

    assert summary["median_seconds"] == 2.0
    assert summary["min_seconds"] == 1.0
    assert summary["max_seconds"] == 3.0
    assert summary["median_client_traced_peak_mb"] == 20.0


def test_summarize_trials_rejects_empty_measurements():
    with pytest.raises(ValueError, match="at least one"):
        summarize_trials(times=[], memories=[])


def test_summarize_trials_rejects_mismatched_measurements():
    with pytest.raises(ValueError, match="same number"):
        summarize_trials(times=[1.0, 2.0], memories=[10.0])
