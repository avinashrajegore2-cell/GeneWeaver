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

def test_dask_batches_chunks_and_preserves_result_order(tmp_path):
    from src.geneweaver.streaming_aligner import find_off_targets_streaming

    guide = "ACGTAC"
    reference = (
        "T" * 6
        + guide
        + "T" * 18
        + guide
        + "T" * 18
        + guide
    )

    fasta = tmp_path / "batched.fasta"
    fasta.write_text(f">chr1\n{reference}\n", encoding="utf-8")

    expected = find_off_targets_streaming(
        fasta, guide, max_mismatches=0, chunk_size=6
    )
    actual = find_off_targets_dask(
        fasta,
        guide,
        max_mismatches=0,
        chunk_size=6,
        n_workers=2,
        task_batch_size=4,
    )

    normalize = lambda hits: [
        (hit["record_id"], hit["position"]) for hit in hits
    ]

    assert normalize(actual) == normalize(expected)
    assert normalize(actual) == [
        ("chr1", 6),
        ("chr1", 30),
        ("chr1", 54),
    ]


def test_dask_rejects_invalid_task_batch_size(tmp_path):
    fasta = tmp_path / "sample.fasta"
    fasta.write_text(">chr1\nAACCGG\n", encoding="utf-8")

    with pytest.raises(ValueError, match="task_batch_size"):
        find_off_targets_dask(
            fasta,
            "AACCG",
            task_batch_size=0,
        )


def test_dask_with_single_in_flight_batch_preserves_order(tmp_path):
    guide = "ACGTAC"
    reference = "TTTTTTACGTACGGGGGGACGTACCCCCCCACGTAC"
    fasta = tmp_path / "single_in_flight.fasta"
    fasta.write_text(f">chr1\n{reference}\n", encoding="utf-8")

    actual = find_off_targets_dask(
        fasta,
        guide,
        max_mismatches=0,
        chunk_size=6,
        n_workers=2,
        task_batch_size=1,
        max_in_flight=1,
    )

    assert [
        (hit["record_id"], hit["position"])
        for hit in actual
    ] == [
        ("chr1", 6),
        ("chr1", 18),
        ("chr1", 30),
    ]


@pytest.mark.parametrize("max_in_flight", [0, -1])
def test_dask_rejects_invalid_max_in_flight(tmp_path, max_in_flight):
    fasta = tmp_path / "invalid_in_flight.fasta"
    fasta.write_text(">chr1\nACGTACGT\n", encoding="utf-8")

    with pytest.raises(ValueError, match="max_in_flight"):
        find_off_targets_dask(
            fasta,
            "ACGT",
            max_in_flight=max_in_flight,
        )


from pathlib import Path

def test_dask_adaptive_mode_matches_numpy_reference(tmp_path):
    from src.geneweaver.parser import read_fasta
    from src.geneweaver.numpy_aligner import find_off_targets_numpy

    fasta_path = tmp_path / "adaptive.fasta"
    fasta_path.write_text(
        ">record1\n"
        "ACGTACGTACGTACGTACGT\n"
        ">record2\n"
        "TTTTACGTACGTACGTAAAA\n",
        encoding="utf-8",
    )

    expected = []
    for record in read_fasta(fasta_path):
        record_results = find_off_targets_numpy(
            record["sequence"],
            guide="ACGT",
            max_mismatches=1,
        )
        for result in record_results:
            expected.append({
                "record_id": record["id"],
                **result,
            })

    actual = find_off_targets_dask(
        fasta_path,
        guide="ACGT",
        max_mismatches=1,
        chunk_size=5,
        n_workers=2,
        task_batch_size=2,
        adaptive=True,
    )

    assert actual == expected





def test_adaptive_batch_size_increases_for_fast_tasks():
    from src.geneweaver.dask_aligner import _adjust_batch_size

    assert _adjust_batch_size(4, 0.10) == 5


def test_adaptive_batch_size_decreases_for_slow_tasks():
    from src.geneweaver.dask_aligner import _adjust_batch_size

    assert _adjust_batch_size(4, 1.00) == 3


def test_adaptive_batch_size_stays_within_minimum():
    from src.geneweaver.dask_aligner import _adjust_batch_size

    assert _adjust_batch_size(1, 1.00) == 1


def test_adaptive_batch_size_stays_within_maximum():
    from src.geneweaver.dask_aligner import _adjust_batch_size

    assert _adjust_batch_size(16, 0.10) == 16


def test_adaptive_batch_size_is_unchanged_for_moderate_tasks():
    from src.geneweaver.dask_aligner import _adjust_batch_size

    assert _adjust_batch_size(4, 0.50) == 4
