from pathlib import Path

import numpy as np
from dask.distributed import Client, LocalCluster

from src.geneweaver.parser import iter_fasta_chunks
from src.geneweaver.numpy_aligner import encode_dna


def _align_chunk(
    record_id,
    combined_start,
    sequence,
    new_chunk_start,
    guide,
    max_mismatches,
):
    """Align a chunk, including its previous-chunk overlap."""
    guide_array = encode_dna(guide)
    sequence_array = encode_dna(sequence)
    guide_length = len(guide_array)

    if len(sequence_array) < guide_length:
        return []

    windows = np.lib.stride_tricks.sliding_window_view(
        sequence_array, guide_length
    )
    mismatch_counts = np.count_nonzero(
        windows != guide_array, axis=1
    )
    positions = np.flatnonzero(
        mismatch_counts <= max_mismatches
    )

    results = []

    for position in positions:
        global_position = combined_start + int(position)

        # Only report windows that include new bases from this chunk.
        if global_position + guide_length <= new_chunk_start:
            continue

        results.append({
            "record_id": record_id,
            "position": global_position,
            "sequence": sequence[position:position + guide_length],
            "mismatches": int(mismatch_counts[position]),
        })

    return results


def _align_chunk_batch(chunk_tasks, guide, max_mismatches):
    """Align several independent chunks in one Dask task."""
    batch_results = []

    for chunk_task in chunk_tasks:
        batch_results.extend(
            _align_chunk(
                *chunk_task,
                guide,
                max_mismatches,
            )
        )

    return batch_results


def find_off_targets_dask(
    file_path: str | Path,
    guide: str,
    max_mismatches: int = 3,
    chunk_size: int = 100_000,
    n_workers: int = 2,
    task_batch_size: int = 8,
    processes: bool = False,
) -> list[dict]:
    """Find off-targets using Dask with configurable task batching."""

    if not guide:
        raise ValueError("Guide sequence cannot be empty.")
    if max_mismatches < 0:
        raise ValueError("max_mismatches cannot be negative.")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")
    if n_workers <= 0:
        raise ValueError("n_workers must be greater than zero.")
    if task_batch_size <= 0:
        raise ValueError("task_batch_size must be greater than zero.")

    guide = guide.upper()
    encode_dna(guide)
    guide_length = len(guide)

    results = []
    futures = []
    pending_batch = []
    current_record = None
    carryover = ""

    # Threads are a reliable first step for Windows-based testing.
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        processes=processes,
        dashboard_address=None,
    )

    try:
        with Client(cluster) as client:
            for item in iter_fasta_chunks(file_path, chunk_size):
                record_id = item["record_id"]
                chunk_start = item["start"]
                sequence = item["sequence"]

                if record_id != current_record or chunk_start == 0:
                    carryover = ""
                    current_record = record_id

                combined = carryover + sequence
                combined_start = chunk_start - len(carryover)

                pending_batch.append((
                    record_id,
                    combined_start,
                    combined,
                    chunk_start,
                ))

                if len(pending_batch) >= task_batch_size:
                    futures.append(
                        client.submit(
                            _align_chunk_batch,
                            pending_batch,
                            guide,
                            max_mismatches,
                        )
                    )
                    pending_batch = []

                keep = min(guide_length - 1, len(combined))
                carryover = combined[-keep:] if keep else ""

            if pending_batch:
                futures.append(
                    client.submit(
                        _align_chunk_batch,
                        pending_batch,
                        guide,
                        max_mismatches,
                    )
                )

            for batch_results in client.gather(futures):
                results.extend(batch_results)

    finally:
        cluster.close()

    return results
