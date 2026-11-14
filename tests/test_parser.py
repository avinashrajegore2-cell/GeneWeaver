import pytest

from src.geneweaver.parser import read_fasta, chunk_sequence


def test_read_fasta():
    records = read_fasta("data/input/sample.fasta")

    assert len(records) == 2
    assert records[0]["id"] == "chr1_sample"
    assert records[0]["sequence"] == "ATGCGTACGTAGCTAGCTAGGCTAACGT"


def test_chunk_sequence():
    chunks = chunk_sequence("ATGCGTACGTAGCTAG", 5)

    assert chunks == ["ATGCG", "TACGT", "AGCTA", "G"]


def test_chunk_sequence_invalid_size():
    with pytest.raises(ValueError):
        chunk_sequence("ATGC", 0)


def test_chunk_sequence_invalid_bases():
    with pytest.raises(ValueError):
        chunk_sequence("ATGX", 2)


def test_read_fasta_missing_file():
    with pytest.raises(FileNotFoundError):
        read_fasta("data/input/missing.fasta")
