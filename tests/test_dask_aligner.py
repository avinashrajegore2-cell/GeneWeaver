from pathlib import Path

import pytest

from src.geneweaver.dask_aligner import find_off_targets_dask


def test_dask_matches_numpy_reference(tmp_path):
    from src.geneweaver.numpy_aligner import find_off_targets_numpy

    reference = "AACCGGTTAACCGGTT"
    guide = "AACCG"

    fasta = tmp_path / "sample.fasta"
    fasta.write_text(f">chr1\n{reference}\n", encoding="utf-8")

    expected = find_off_targets_numpy(reference, guide, 1)
    actual = find_off_targets_dask(
        fasta, guide, max_mismatches=1, chunk_size=6, n_workers=2
    )

    assert [
        (hit["position"], hit["sequence"], hit["mismatches"])
        for hit in actual
    ] == [
        (hit["position"], hit["sequence"], hit["mismatches"])
        for hit in expected
    ]


def test_dask_finds_chunk_boundary_match(tmp_path):
    fasta = tmp_path / "boundary.fasta"
    fasta.write_text(">chr1\nTTTAACCGGG\n", encoding="utf-8")

    results = find_off_targets_dask(
        fasta, "AACCG", max_mismatches=0, chunk_size=4, n_workers=2
    )

    assert any(hit["position"] == 3 for hit in results)


def test_dask_does_not_match_across_records(tmp_path):
    fasta = tmp_path / "records.fasta"
    fasta.write_text(">chr1\nTTTAA\n>chr2\nCCGGG\n", encoding="utf-8")

    results = find_off_targets_dask(
        fasta, "AACCG", max_mismatches=0, chunk_size=4, n_workers=2
    )

    assert results == []


def test_dask_rejects_empty_guide(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">chr1\nAACCGG\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Guide sequence"):
        find_off_targets_dask(fasta, "")


def test_dask_rejects_negative_mismatch_threshold(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">chr1\nAACCGG\n", encoding="utf-8")

    with pytest.raises(ValueError, match="max_mismatches"):
        find_off_targets_dask(fasta, "AACCG", max_mismatches=-1)


def test_dask_rejects_invalid_chunk_size(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">chr1\nAACCGG\n", encoding="utf-8")

    with pytest.raises(ValueError, match="chunk_size"):
        find_off_targets_dask(fasta, "AACCG", chunk_size=0)


def test_dask_rejects_invalid_worker_count(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">chr1\nAACCGG\n", encoding="utf-8")

    with pytest.raises(ValueError, match="n_workers"):
        find_off_targets_dask(fasta, "AACCG", n_workers=0)

def test_preserves_fasta_record_order_and_repeated_ids(tmp_path):
    from src.geneweaver.dask_aligner import find_off_targets_dask
    from src.geneweaver.streaming_aligner import find_off_targets_streaming

    guide = "ACGTAC"
    fasta = tmp_path / "ordering.fasta"
    fasta.write_text(
        ">chrZ\n" + "T" * 10 + guide + "T" * 44 + "\n"
        ">chrA\n" + "T" * 20 + guide + "T" * 34 + "\n"
        ">chrZ\n" + "T" * 5 + guide + "T" * 49 + "\n"
    )

    expected = find_off_targets_streaming(
        fasta, guide, max_mismatches=0, chunk_size=7
    )
    actual = find_off_targets_dask(
        fasta, guide, max_mismatches=0, chunk_size=7, n_workers=2
    )

    normalize = lambda hits: [
        (hit["record_id"], hit["position"]) for hit in hits
    ]

    assert normalize(actual) == normalize(expected)
    assert normalize(actual) == [
        ("chrZ", 10),
        ("chrA", 20),
        ("chrZ", 5),
    ]
