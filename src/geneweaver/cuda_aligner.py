import numpy as np
from numba import cuda

THREADS_PER_BLOCK = 256


@cuda.jit
def count_mismatches_kernel(reference, guide, mismatch_counts):
    """
    Count DNA mismatches for each sliding window.

    One CUDA thread processes one reference window.
    """
    window_index = cuda.grid(1)

    guide_length = guide.size

    if window_index < mismatch_counts.size:
        mismatches = 0

        for j in range(guide_length):
            if reference[window_index + j] != guide[j]:
                mismatches += 1

        mismatch_counts[window_index] = mismatches


def encode_dna(sequence: str) -> np.ndarray:
    """Encode DNA bases as uint8 values."""
    base_map = {
        "A": 0,
        "C": 1,
        "G": 2,
        "T": 3,
        "N": 4,
    }

    sequence = sequence.upper()

    try:
        return np.array(
            [base_map[base] for base in sequence],
            dtype=np.uint8,
        )
    except KeyError as error:
        raise ValueError(f"Invalid DNA base: {error.args[0]}") from error


def find_off_targets_cuda(
    reference: str,
    guide: str,
    max_mismatches: int = 3,
) -> list[dict]:
    """
    Find matching DNA windows using a CUDA GPU.

    Requires a compatible NVIDIA GPU.
    """
    if not guide:
        raise ValueError("Guide sequence cannot be empty.")

    if max_mismatches < 0:
        raise ValueError("max_mismatches cannot be negative.")

    if not cuda.is_available():
        raise RuntimeError(
            "CUDA GPU is unavailable. " "Use an NVIDIA CUDA-compatible GPU."
        )

    reference_array = encode_dna(reference)
    guide_array = encode_dna(guide)

    guide_length = len(guide_array)

    if guide_length > len(reference_array):
        return []

    window_count = len(reference_array) - guide_length + 1

    mismatch_counts = np.empty(
        window_count,
        dtype=np.int32,
    )

    device_reference = cuda.to_device(reference_array)
    device_guide = cuda.to_device(guide_array)
    device_counts = cuda.device_array(
        window_count,
        dtype=np.int32,
    )

    blocks = (window_count + THREADS_PER_BLOCK - 1) // THREADS_PER_BLOCK

    count_mismatches_kernel[blocks, THREADS_PER_BLOCK](
        device_reference,
        device_guide,
        device_counts,
    )

    device_counts.copy_to_host(mismatch_counts)

    positions = np.flatnonzero(mismatch_counts <= max_mismatches)

    return [
        {
            "position": int(position),
            "sequence": reference[position : position + guide_length].upper(),
            "mismatches": int(mismatch_counts[position]),
        }
        for position in positions
    ]
