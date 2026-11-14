import sys
import time
import random
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.geneweaver.cpu_aligner import find_off_targets
from src.geneweaver.numpy_aligner import find_off_targets_numpy


def generate_dna(length: int, seed: int = 42) -> str:
    rng = random.Random(seed)
    return "".join(rng.choices("ACGT", k=length))


def benchmark(function, reference, guide, repeats=3):
    """Warm up and measure average execution time."""
    function(reference, guide, max_mismatches=3)

    times = []

    for _ in range(repeats):
        start = time.perf_counter()
        results = function(
            reference,
            guide,
            max_mismatches=3,
        )
        times.append(time.perf_counter() - start)

    return sum(times) / len(times), results


def main():
    genome_size = 1_000_000
    guide_length = 20

    reference = generate_dna(genome_size)
    guide = reference[500_000 : 500_000 + guide_length]

    print("GeneWeaver: CPU vs NumPy Benchmark")
    print("=" * 45)
    print(f"Genome size: {genome_size:,} base pairs")
    print(f"Guide length: {guide_length}")
    print(f"Mismatch threshold: 3")
    print()

    cpu_time, cpu_results = benchmark(find_off_targets, reference, guide)

    numpy_time, numpy_results = benchmark(find_off_targets_numpy, reference, guide)

    print(f"Python CPU time: {cpu_time:.4f} seconds")
    print(f"NumPy CPU time:  {numpy_time:.4f} seconds")

    print()
    print(f"Python matches: {len(cpu_results)}")
    print(f"NumPy matches:  {len(numpy_results)}")

    if cpu_results != numpy_results:
        raise RuntimeError("Results differ between implementations!")

    speedup = cpu_time / numpy_time

    print()
    print(f"Speedup: {speedup:.2f}x")
    print("Result verification: PASSED")


if __name__ == "__main__":
    main()
