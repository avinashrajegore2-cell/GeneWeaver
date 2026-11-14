import random
import sys
import time
import tracemalloc
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.geneweaver.numpy_aligner import find_off_targets_numpy
from src.geneweaver.streaming_aligner import find_off_targets_streaming


def generate_fasta(path, length, seed=42):
    """Generate a reproducible FASTA file incrementally."""
    rng = random.Random(seed)
    guide = "ACGTACGTACGTACGTACGT"
    target_position = length // 2
    block_size = 10_000

    with path.open("w", encoding="utf-8") as fasta:
        fasta.write(">synthetic_genome\n")

        for start in range(0, length, block_size):
            size = min(block_size, length - start)
            block = "".join(rng.choices("ACGT", k=size))

            if start <= target_position < start + size:
                offset = target_position - start
                block = (
                    block[:offset]
                    + guide
                    + block[offset + len(guide):]
                )

            fasta.write(block + "\n")

    return guide


def measure_streaming(path, guide):
    tracemalloc.start()
    start = time.perf_counter()

    results = find_off_targets_streaming(
        path,
        guide,
        max_mismatches=3,
        chunk_size=100_000,
    )

    elapsed = time.perf_counter() - start
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return elapsed, peak_memory / (1024 * 1024), results


def measure_numpy(path, guide):
    tracemalloc.start()
    start = time.perf_counter()

    reference = "".join(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.startswith(">")
    )

    results = find_off_targets_numpy(
        reference,
        guide,
        max_mismatches=3,
    )

    elapsed = time.perf_counter() - start
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return elapsed, peak_memory / (1024 * 1024), results


def main():
    genome_size = 1_000_000

    print("GeneWeaver: NumPy vs Streaming Benchmark")
    print("=" * 50)
    print(f"Reference size: {genome_size:,} base pairs")
    print("Guide length: 20")
    print("Mismatch threshold: 3")
    print()

    with tempfile.TemporaryDirectory() as temp_dir:
        fasta_path = Path(temp_dir) / "benchmark.fasta"
        guide = generate_fasta(fasta_path, genome_size)

        streaming_time, streaming_memory, streaming_results = (
            measure_streaming(fasta_path, guide)
        )

        numpy_time, numpy_memory, numpy_results = measure_numpy(
            fasta_path, guide
        )

        streaming_comparable = [
            {
                "position": result["position"],
                "sequence": result["sequence"],
                "mismatches": result["mismatches"],
            }
            for result in streaming_results
        ]

        if streaming_comparable != numpy_results:
            raise RuntimeError("Streaming and NumPy results differ!")

        print(f"NumPy time:             {numpy_time:.4f} seconds")
        print(f"Streaming time:         {streaming_time:.4f} seconds")
        print()
        print(f"NumPy traced peak:      {numpy_memory:.2f} MB")
        print(f"Streaming traced peak:  {streaming_memory:.2f} MB")
        print()
        print(f"NumPy matches:          {len(numpy_results)}")
        print(f"Streaming matches:      {len(streaming_results)}")
        print("Result verification:   PASSED")


if __name__ == "__main__":
    main()
