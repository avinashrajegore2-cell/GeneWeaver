from pathlib import Path
from Bio import SeqIO

VALID_BASES = set("ACGTN")


def read_fasta(file_path: str | Path) -> list[dict]:
    """Read FASTA records and validate their DNA sequences."""
    file_path = Path(file_path)

    if not file_path.is_file():
        raise FileNotFoundError(f"FASTA file not found: {file_path}")

    records = []

    for record in SeqIO.parse(file_path, "fasta"):
        sequence = str(record.seq).upper()

        if not sequence:
            raise ValueError(f"Empty sequence: {record.id}")

        invalid_bases = set(sequence) - VALID_BASES
        if invalid_bases:
            raise ValueError(
                f"Invalid DNA bases in {record.id}: " f"{sorted(invalid_bases)}"
            )

        records.append(
            {
                "id": record.id,
                "description": record.description,
                "sequence": sequence,
            }
        )

    if not records:
        raise ValueError("No FASTA records found.")

    return records


def chunk_sequence(sequence: str, chunk_size: int = 100_000) -> list[str]:
    """Split a DNA sequence into non-overlapping chunks."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")

    sequence = sequence.upper()

    invalid_bases = set(sequence) - VALID_BASES
    if invalid_bases:
        raise ValueError(f"Invalid DNA bases: {sorted(invalid_bases)}")

    return [sequence[i : i + chunk_size] for i in range(0, len(sequence), chunk_size)]
