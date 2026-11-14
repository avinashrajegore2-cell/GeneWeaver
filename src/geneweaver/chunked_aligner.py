import numpy as np

from src.geneweaver.numpy_aligner import encode_dna


def find_off_targets_chunked(
    reference: str,
    guide: str,
    max_mismatches: int = 3,
    chunk_size: int = 100_000,
) -> list[dict]:
    """Find off-targets using memory-efficient chunked NumPy alignment."""

    if not guide:
        raise ValueError("Guide sequence cannot be empty.")

    if max_mismatches < 0:
        raise ValueError("max_mismatches cannot be negative.")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")

    reference_array = encode_dna(reference)
    guide_array = encode_dna(guide)

    guide_length = len(guide_array)

    if guide_length > len(reference_array):
        return []

    results = []

    for chunk_start in range(0, len(reference_array), chunk_size):
        chunk_end = min(
            chunk_start + chunk_size + guide_length - 1,
            len(reference_array),
        )

        chunk = reference_array[chunk_start:chunk_end]

        if len(chunk) < guide_length:
            continue

        windows = np.lib.stride_tricks.sliding_window_view(
            chunk,
            guide_length,
        )

        mismatch_counts = np.count_nonzero(
            windows != guide_array,
            axis=1,
        )

        positions = np.flatnonzero(
            mismatch_counts <= max_mismatches
        )

        for local_position in positions:
            global_position = chunk_start + int(local_position)

            if global_position >= chunk_start + chunk_size:
                continue

            results.append(
                {
                    "position": global_position,
                    "sequence": reference[
                        global_position:global_position + guide_length
                    ].upper(),
                    "mismatches": int(mismatch_counts[local_position]),
                }
            )

    return results
