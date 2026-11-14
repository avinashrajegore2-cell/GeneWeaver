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


def iter_fasta_chunks(
    file_path: str | Path,
    chunk_size: int = 100_000,
):
    """Stream validated FASTA records as bounded-size DNA chunks.

    Each yielded dictionary contains:
    record_id, description, start, and sequence.
    Positions are zero-based within each FASTA record.
    """
    file_path = Path(file_path)

    if not file_path.is_file():
        raise FileNotFoundError(f"FASTA file not found: {file_path}")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")

    record_id = None
    description = None
    buffer = ""
    position = 0
    record_length = 0
    records_found = 0

    with file_path.open("r", encoding="utf-8") as fasta_file:
        for line_number, raw_line in enumerate(fasta_file, start=1):
            line = raw_line.strip()

            if not line:
                continue

            if line.startswith(">"):
                if record_id is not None:
                    if record_length == 0:
                        raise ValueError(f"Empty sequence: {record_id}")

                    if buffer:
                        yield {
                            "record_id": record_id,
                            "description": description,
                            "start": position,
                            "sequence": buffer,
                        }

                header = line[1:].strip()

                if not header:
                    raise ValueError(
                        f"Empty FASTA header at line {line_number}."
                    )

                record_id = header.split()[0]
                description = header
                buffer = ""
                position = 0
                record_length = 0
                records_found += 1
                continue

            if record_id is None:
                raise ValueError(
                    f"Sequence data before FASTA header at line {line_number}."
                )

            sequence = "".join(line.split()).upper()
            invalid_bases = set(sequence) - VALID_BASES

            if invalid_bases:
                raise ValueError(
                    f"Invalid DNA bases in {record_id}: "
                    f"{sorted(invalid_bases)}"
                )

            record_length += len(sequence)
            buffer += sequence

            while len(buffer) >= chunk_size:
                chunk = buffer[:chunk_size]
                buffer = buffer[chunk_size:]

                yield {
                    "record_id": record_id,
                    "description": description,
                    "start": position,
                    "sequence": chunk,
                }

                position += len(chunk)

    if record_id is not None:
        if record_length == 0:
            raise ValueError(f"Empty sequence: {record_id}")

        if buffer:
            yield {
                "record_id": record_id,
                "description": description,
                "start": position,
                "sequence": buffer,
            }

    if records_found == 0:
        raise ValueError("No FASTA records found.")
