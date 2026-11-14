def count_mismatches(sequence_a: str, sequence_b: str) -> int:
    """Count differences between two DNA sequences of equal length."""
    if len(sequence_a) != len(sequence_b):
        raise ValueError("Sequences must have equal length.")

    return sum(a != b for a, b in zip(sequence_a.upper(), sequence_b.upper()))


def find_off_targets(
    reference: str,
    guide: str,
    max_mismatches: int = 3,
) -> list[dict]:
    """
    Find reference genome regions similar to a CRISPR guide sequence.

    This is a simple ungapped sliding-window baseline.
    """
    reference = reference.upper()
    guide = guide.upper()

    valid_bases = set("ACGTN")

    if not guide:
        raise ValueError("Guide sequence cannot be empty.")

    if set(reference) - valid_bases:
        raise ValueError("Reference contains invalid DNA bases.")

    if set(guide) - valid_bases:
        raise ValueError("Guide contains invalid DNA bases.")

    if max_mismatches < 0:
        raise ValueError("max_mismatches cannot be negative.")

    guide_length = len(guide)
    matches = []

    for start in range(len(reference) - guide_length + 1):
        window = reference[start : start + guide_length]
        mismatches = count_mismatches(guide, window)

        if mismatches <= max_mismatches:
            matches.append(
                {
                    "position": start,
                    "sequence": window,
                    "mismatches": mismatches,
                }
            )

    return matches
