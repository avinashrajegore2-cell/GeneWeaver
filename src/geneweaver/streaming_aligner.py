import numpy as np

from src.geneweaver.numpy_aligner import encode_dna
from src.geneweaver.parser import iter_fasta_chunks


def find_off_targets_streaming(
    file_path,
    guide: str,
    max_mismatches: int = 3,
    chunk_size: int = 100_000,
) -> list[dict]:
    """Find off-targets directly from a FASTA file using bounded chunks."""

    if not guide:
        raise ValueError("Guide sequence cannot be empty.")

    if max_mismatches < 0:
        raise ValueError("max_mismatches cannot be negative.")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")

    guide = guide.upper()
    guide_array = encode_dna(guide)
    guide_length = len(guide_array)

    results = []

    current_record = None
    carryover = ""
    carryover_start = 0

    for item in iter_fasta_chunks(file_path, chunk_size):
        record_id = item["record_id"]
        chunk_start = item["start"]
        sequence = item["sequence"]

        if chunk_start == 0:
            carryover = ""
            carryover_start = 0

        if record_id != current_record:
            current_record = record_id
            carryover = ""
            carryover_start = 0

        combined_sequence = carryover + sequence
        combined_start = carryover_start

        if len(combined_sequence) >= guide_length:
            encoded = encode_dna(combined_sequence)

            windows = np.lib.stride_tricks.sliding_window_view(
                encoded,
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
                global_position = combined_start + int(local_position)

                # Only report windows whose final base belongs to
                # the newly received chunk.
                if global_position + guide_length <= chunk_start:
                    continue

                results.append(
                    {
                        "record_id": record_id,
                        "position": global_position,
                        "sequence": combined_sequence[
                            local_position:local_position + guide_length
                        ],
                        "mismatches": int(mismatch_counts[local_position]),
                    }
                )

        keep = min(guide_length - 1, len(combined_sequence))
        carryover = combined_sequence[-keep:] if keep else ""
        carryover_start = (
            chunk_start + len(sequence) - len(carryover)
        )

    return results

