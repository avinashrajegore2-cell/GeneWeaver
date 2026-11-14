import random
import time
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.geneweaver.numpy_aligner import find_off_targets_numpy
from src.geneweaver.chunked_aligner import find_off_targets_chunked


def generate_dna(length, seed=42):
    rng = random.Random(seed)
    return "".join(rng.choices("ACGT", k=length))


def benchmark(function, reference, guide, **kwargs):
    function(reference, guide, **kwargs)

    start = time.perf_counter()
    results = function(reference, guide, **kwargs)
    elapsed = time.perf_counter() - start

    return elapsed, results


def main():
    genome_size = 1_000_000
    guide_length = 20

    reference = generate_dna(genome_size)
    guide = reference[500_000:500_000 + guide_length]

    print("GeneWeaver: NumPy vs Chunked NumPy Benchmark")
    print("=" * 50)
    print(f"Reference size: {genome_size:,} base pairs")
    print(f"Guide length: {guide_length}")
    print("Mismatch threshold: 3")
    print()

    numpy_time, numpy_results = benchmark(
        find_off_targets_numpy,
        reference,
        guide,
        max_mismatches=3,
    )

    chunked_time, chunked_results = benchmark(
        find_off_targets_chunked,
        reference,
        guide,
        max_mismatches=3,
        chunk_size=100_000,
    )

    print(f"Standard NumPy: {numpy_time:.4f} seconds")
    print(f"Chunked NumPy:  {chunked_time:.4f} seconds")
    print(f"Standard matches: {len(numpy_results)}")
    print(f"Chunked matches:  {len(chunked_results)}")

    if numpy_results != chunked_results:
        raise RuntimeError("Alignment results do not match!")

    print()
    print(f"Chunked/NumPy time ratio: {chunked_time / numpy_time:.2f}x")
    print("Result verification: PASSED")


if __name__ == "__main__":
    main()
