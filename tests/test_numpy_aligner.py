import pytest

from src.geneweaver.numpy_aligner import (
    encode_dna,
    find_off_targets_numpy,
)


def test_encode_dna():
    encoded = encode_dna("ACGTN")

    assert encoded.tolist() == [0, 1, 2, 3, 4]


def test_encode_dna_case_insensitive():
    assert encode_dna("acgt").tolist() == [0, 1, 2, 3]


def test_encode_dna_invalid_base():
    with pytest.raises(ValueError):
        encode_dna("ATGX")


def test_numpy_exact_match():
    results = find_off_targets_numpy(
        reference="TTTATGCAAATTT",
        guide="ATGC",
        max_mismatches=0,
    )

    assert len(results) == 1
    assert results[0]["position"] == 3
    assert results[0]["mismatches"] == 0


def test_numpy_mismatch_search():
    results = find_off_targets_numpy(
        reference="ATGCATGAATGT",
        guide="ATGC",
        max_mismatches=1,
    )

    assert len(results) == 3
    assert results[0]["mismatches"] == 0


def test_numpy_empty_guide():
    with pytest.raises(ValueError):
        find_off_targets_numpy("ATGC", "")


def test_numpy_negative_threshold():
    with pytest.raises(ValueError):
        find_off_targets_numpy("ATGC", "ATGC", -1)


def test_numpy_guide_longer_than_reference():
    results = find_off_targets_numpy(
        reference="ATGC",
        guide="ATGCAT",
    )

    assert results == []
