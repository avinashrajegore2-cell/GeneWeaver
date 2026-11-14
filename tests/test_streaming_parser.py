import pytest

from src.geneweaver.parser import iter_fasta_chunks


def test_streams_sequence_in_chunks(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">gene1\nACGTACGTAC\n", encoding="utf-8")

    chunks = list(iter_fasta_chunks(fasta, chunk_size=4))

    assert [chunk["sequence"] for chunk in chunks] == [
        "ACGT", "ACGT", "AC"
    ]
    assert [chunk["start"] for chunk in chunks] == [0, 4, 8]


def test_streams_multiple_records(tmp_path):
    fasta = tmp_path / "multiple.fasta"
    fasta.write_text(
        ">gene1 first record\nACGTAC\n"
        ">gene2 second record\nTTGGCC\n",
        encoding="utf-8",
    )

    chunks = list(iter_fasta_chunks(fasta, chunk_size=4))

    assert [chunk["record_id"] for chunk in chunks] == [
        "gene1", "gene1", "gene2", "gene2"
    ]
    assert [chunk["sequence"] for chunk in chunks] == [
        "ACGT", "AC", "TTGG", "CC"
    ]


def test_streams_lowercase_as_uppercase(tmp_path):
    fasta = tmp_path / "lowercase.fasta"
    fasta.write_text(">gene1\nacgtac\n", encoding="utf-8")

    chunks = list(iter_fasta_chunks(fasta, chunk_size=10))

    assert chunks[0]["sequence"] == "ACGTAC"


def test_rejects_invalid_bases(tmp_path):
    fasta = tmp_path / "invalid.fasta"
    fasta.write_text(">gene1\nACGTXYZ\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid DNA bases"):
        list(iter_fasta_chunks(fasta, chunk_size=4))


def test_rejects_empty_record(tmp_path):
    fasta = tmp_path / "empty.fasta"
    fasta.write_text(">gene1\n>gene2\nACGT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Empty sequence"):
        list(iter_fasta_chunks(fasta, chunk_size=4))


def test_rejects_invalid_chunk_size(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">gene1\nACGT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="chunk_size"):
        list(iter_fasta_chunks(fasta, chunk_size=0))


def test_rejects_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(iter_fasta_chunks(tmp_path / "missing.fasta"))
