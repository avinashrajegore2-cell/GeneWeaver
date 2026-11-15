import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import random
import tempfile
import time
import tracemalloc

from src.geneweaver.streaming_aligner import find_off_targets_streaming
from src.geneweaver.dask_aligner import find_off_targets_dask


def generate_fasta(path, length=1_000_000, line_size=50_000):
    random.seed(42)
    guide = "ACGTACGTACGTACGTACGT"

    sequence = "".join(random.choices("ACGT", k=length))
    sequence = sequence[:500_000] + guide + sequence[500_000 + len(guide):]

    with path.open("w", encoding="utf-8") as fasta:
        fasta.write(">synthetic_reference\n")
        for start in range(0, len(sequence), line_size):
            fasta.write(sequence[start:start + line_size] + "\n")

    return guide


def measure(function):
    tracemalloc.start()
    start = time.perf_counter()
    result = function()
    elapsed = time.perf_counter() - start
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return result, elapsed, peak_memory / (1024 * 1024)


def normalize(results):
    return sorted(
        (
            item["record_id"],
            item["position"],
            item["sequence"],
            item["mismatches"],
        )
        for item in results
    )


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        fasta_path = Path(temp_dir) / "benchmark.fasta"
        guide = generate_fasta(fasta_path)

        print("GeneWeaver: Streaming vs Dask Benchmark")
        print("=" * 48)
        print("Reference size: 1,000,000 base pairs")
        print("Guide length:", len(guide))
        print("Inserted exact match at position: 500000")
        print("Chunk size: 100,000")
        print("Dask workers: 2")
        print()

        streaming_results, streaming_time, streaming_memory = measure(
            lambda: find_off_targets_streaming(
                fasta_path, guide, max_mismatches=3,
                chunk_size=100_000,
            )
        )

        dask_results, dask_time, dask_memory = measure(
            lambda: find_off_targets_dask(
                fasta_path, guide, max_mismatches=3,
                chunk_size=100_000, n_workers=2,
            )
        )

        print(f"Streaming time:          {streaming_time:.4f} seconds")
        print(f"Dask time:               {dask_time:.4f} seconds")
        print(f"Streaming traced peak:   {streaming_memory:.2f} MB")
        print(f"Dask traced peak:        {dask_memory:.2f} MB")
        print()
        print("Streaming matches:", len(streaming_results))
        print("Dask matches:     ", len(dask_results))

        if normalize(streaming_results) != normalize(dask_results):
            raise RuntimeError("Benchmark result verification FAILED")

        if not any(
            hit["position"] == 500_000 and hit["mismatches"] == 0
            for hit in dask_results
        ):
            raise RuntimeError("Inserted exact match was not detected")

        print("Result verification: PASSED")
        print("Known match verification: PASSED")
        print(f"Streaming/Dask time ratio: {streaming_time / dask_time:.2f}x")


if __name__ == "__main__":
    main()
