import pytest

from src.geneweaver.cuda_aligner import (
    encode_dna,
    find_off_targets_cuda,
)


def test_encode_dna():
    result = encode_dna("ACGTN")
    assert result.tolist() == [0, 1, 2, 3, 4]


def test_encode_dna_case_insensitive():
    result = encode_dna("acgtn")
    assert result.tolist() == [0, 1, 2, 3, 4]


def test_encode_dna_invalid_base():
    with pytest.raises(ValueError):
        encode_dna("ACGTX")


def test_cuda_empty_guide():
    with pytest.raises(ValueError, match="Guide sequence"):
        find_off_targets_cuda("ACGT", "", 3)


def test_cuda_negative_mismatches():
    with pytest.raises(ValueError, match="max_mismatches"):
        find_off_targets_cuda("ACGT", "ACGT", -1)


def test_cuda_requires_gpu():
    from numba import cuda

    if not cuda.is_available():
        with pytest.raises(RuntimeError, match="CUDA GPU is unavailable"):
            find_off_targets_cuda("ACGTACGT", "ACGT", 1)
