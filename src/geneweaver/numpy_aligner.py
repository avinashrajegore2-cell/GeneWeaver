import numpy as np

BASE_TO_INT = {
    "A": 0,
    "C": 1,
    "G": 2,
    "T": 3,
    "N": 4,
}


def encode_dna(sequence: str) -> np.ndarray:
    """Convert DNA bases into numeric values."""
    sequence = sequence.upper()

    try:
        return np.array(
            [BASE_TO_INT[base] for base in sequence],
            dtype=np.uint8,
        )
    except KeyError as error:
        raise ValueError(f"Invalid DNA base: {error.args[0]}") from error


def find_off_targets_numpy(
    reference: str,
    guide: str,
    max_mismatches: int = 3,
) -> list[dict]:
    """Find similar DNA regions using NumPy vectorization."""

    if not guide:
        raise ValueError("Guide sequence cannot be empty.")

    if max_mismatches < 0:
        raise ValueError("max_mismatches cannot be negative.")

    reference_array = encode_dna(reference)
    guide_array = encode_dna(guide)

    guide_length = len(guide_array)

    if guide_length > len(reference_array):
        return []

    # Create sliding windows over the reference.
    windows = np.lib.stride_tricks.sliding_window_view(
        reference_array,
        guide_length,
    )

    # Count mismatches in every window.
    mismatch_counts = np.count_nonzero(
        windows != guide_array,
        axis=1,
    )

    # Find windows within the mismatch threshold.
    positions = np.flatnonzero(mismatch_counts <= max_mismatches)

    results = []

    for position in positions:
        results.append(
            {
                "position": int(position),
                "sequence": reference[position : position + guide_length].upper(),
                "mismatches": int(mismatch_counts[position]),
            }
        )

    return results
