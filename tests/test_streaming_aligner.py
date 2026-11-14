import pytest

from src.geneweaver.streaming_aligner import find_off_targets_streaming


def test_finds_match_across_chunk_boundary(tmp_path):
    fasta = tmp_path / "boundary.fasta"
    fasta.write_text(">gene1\nTTTTACGTACGTAAAA\n", encoding="utf-8")

    results = find_off_targets_streaming(
        fasta, "ACGTAC", max_mismatches=0, chunk_size=5
    )

    assert any(result["position"] == 4 for result in results)


def test_streaming_matches_cpu_results(tmp_path):
    from src.geneweaver.cpu_aligner import find_off_targets

    sequence = "ACGTACGTGGGGACGT"
    fasta = tmp_path / "compare.fasta"
    fasta.write_text(f">gene1\n{sequence}\n", encoding="utf-8")

    expected = find_off_targets(sequence, "ACGT", 1)
    actual = find_off_targets_streaming(
        fasta, "ACGT", max_mismatches=1, chunk_size=6
    )

    actual_positions = [
        {
            "position": result["position"],
            "sequence": result["sequence"],
            "mismatches": result["mismatches"],
        }
        for result in actual
    ]

    assert actual_positions == expected


def test_does_not_match_across_records_with_same_id(tmp_path):
    fasta = tmp_path / "same_id.fasta"
    fasta.write_text(
        ">gene1\nAAAA\n"
        ">gene1\nCCCC\n",
        encoding="utf-8",
    )

    results = find_off_targets_streaming(
        fasta, "AACC", max_mismatches=0, chunk_size=4
    )

    assert results == []


def test_reports_record_ids_and_positions(tmp_path):
    fasta = tmp_path / "records.fasta"
    fasta.write_text(
        ">gene1\nACGTAC\n"
        ">gene2\nTTACGT\n",
        encoding="utf-8",
    )

    results = find_off_targets_streaming(
        fasta, "ACGT", max_mismatches=0, chunk_size=3
    )

    assert any(
        result["record_id"] == "gene1" and result["position"] == 0
        for result in results
    )
    assert any(
        result["record_id"] == "gene2" and result["position"] == 2
        for result in results
    )


def test_rejects_empty_guide(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">gene1\nACGT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Guide sequence"):
        find_off_targets_streaming(fasta, "")


def test_rejects_negative_mismatch_threshold(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">gene1\nACGT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="max_mismatches"):
        find_off_targets_streaming(fasta, "AC", max_mismatches=-1)


def test_rejects_invalid_chunk_size(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">gene1\nACGT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="chunk_size"):
        find_off_targets_streaming(fasta, "AC", chunk_size=0)
