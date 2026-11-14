import pytest

from src.geneweaver.cpu_aligner import (
    count_mismatches,
    find_off_targets,
)


def test_count_mismatches():
    assert count_mismatches("ATGC", "ATGA") == 1


def test_count_mismatches_case_insensitive():
    assert count_mismatches("atgc", "ATGC") == 0


def test_count_mismatches_different_lengths():
    with pytest.raises(ValueError):
        count_mismatches("ATGC", "ATG")


def test_find_exact_match():
    results = find_off_targets(
        reference="TTTATGCAAATTT",
        guide="ATGC",
        max_mismatches=0,
    )

    assert len(results) == 1
    assert results[0]["position"] == 3
    assert results[0]["mismatches"] == 0


def test_find_off_targets_with_mismatches():
    results = find_off_targets(
        reference="ATGCATGAATGT",
        guide="ATGC",
        max_mismatches=1,
    )

    assert len(results) == 3
    assert results[0]["mismatches"] == 0


def test_negative_mismatch_threshold():
    with pytest.raises(ValueError):
        find_off_targets("ATGC", "ATGC", -1)


def test_empty_guide():
    with pytest.raises(ValueError):
        find_off_targets("ATGC", "")


def test_invalid_dna_bases():
    with pytest.raises(ValueError):
        find_off_targets("ATGX", "ATGC")
