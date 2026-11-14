import pytest

from src.geneweaver.chunked_aligner import find_off_targets_chunked
from src.geneweaver.numpy_aligner import find_off_targets_numpy


def test_chunked_matches_numpy_results():
    reference = "ACGT" * 50
    guide = "ACGTACGT"

    expected = find_off_targets_numpy(reference, guide, 2)
    actual = find_off_targets_chunked(
        reference, guide, max_mismatches=2, chunk_size=20
    )

    assert actual == expected


def test_chunked_finds_boundary_match():
    reference = "T" * 10 + "ACGTACGT" + "T" * 10
    guide = "ACGTACGT"

    results = find_off_targets_chunked(
        reference, guide, max_mismatches=0, chunk_size=12
    )

    assert any(result["position"] == 10 for result in results)


def test_chunked_rejects_invalid_chunk_size():
    with pytest.raises(ValueError, match="chunk_size"):
        find_off_targets_chunked("ACGT", "AC", chunk_size=0)


def test_chunked_rejects_empty_guide():
    with pytest.raises(ValueError, match="Guide sequence"):
        find_off_targets_chunked("ACGT", "")


def test_chunked_rejects_negative_mismatches():
    with pytest.raises(ValueError, match="max_mismatches"):
        find_off_targets_chunked("ACGT", "AC", max_mismatches=-1)


def test_chunked_guide_longer_than_reference():
    results = find_off_targets_chunked("ACG", "ACGT", chunk_size=2)
    assert results == []
