import sys
import time
import random
from pathlib import Path

# Add the GeneWeaver project root to Python's import path.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.geneweaver.cpu_aligner import find_off_targets


def generate_dna(length: int, seed: int = 42) -> str:
    """Generate reproducible random DNA."""
    rng = random.Random(seed)
    return "".join(rng.choices("ACGT", k=length))


def main():
    genome_size = 1_000_000
    guide_length = 20

    print("GeneWeaver CPU Alignment Benchmark")
    print("-" * 40)

    reference = generate_dna(genome_size)
    guide = reference[500_000 : 500_000 + guide_length]

    print(f"Genome size: {genome_size:,} base pairs")
    print(f"Guide length: {guide_length} base pairs")
    print(f"Guide sequence: {guide}")

    start_time = time.perf_counter()

    results = find_off_targets(
        reference=reference,
        guide=guide,
        max_mismatches=3,
    )

    elapsed = time.perf_counter() - start_time

    print(f"Matches found: {len(results)}")
    print(f"Execution time: {elapsed:.4f} seconds")
    print(f"Throughput: {genome_size / elapsed:,.0f} base pairs/second")


if __name__ == "__main__":
    main()
