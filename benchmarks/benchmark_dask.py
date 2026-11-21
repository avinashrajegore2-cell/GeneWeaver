import argparse
import csv
import os
import random
import statistics
import sys
import tempfile
import time
import tracemalloc
import threading
from pathlib import Path

import psutil

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.geneweaver.streaming_aligner import find_off_targets_streaming
from src.geneweaver.dask_aligner import find_off_targets_dask


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark GeneWeaver Dask scalability."
    )
    parser.add_argument(
        "--reference-size",
        type=int,
        default=5_000_000,
        help="Reference length in bases (default: 5000000).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Chunk size in bases (default: 100000).",
    )
    parser.add_argument(
        "--workers",
        nargs="+",
        type=int,
        default=[1, 2, 4, 8],
        help="Dask worker counts (default: 1 2 4 8).",
    )
    parser.add_argument(
        "--batch-sizes",
        nargs="+",
        type=int,
        default=[1, 2, 4, 8, 16],
        help="Chunks per Dask task (default: 1 2 4 8 16).",
    )
    parser.add_argument(
        "--scheduling-modes",
        nargs="+",
        choices=("fixed", "adaptive"),
        default=["fixed", "adaptive"],
        help="Scheduling modes to compare (default: fixed adaptive).",
    )
    parser.add_argument(
        "--processes",
        action="store_true",
        help="Use separate Dask worker processes instead of threads.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Trials per configuration (default: 3).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmarks/dask_scalability_results.csv"),
        help="CSV output path.",
    )
    args = parser.parse_args()

    if args.reference_size < 500_020:
        parser.error("--reference-size must be at least 500020 bases.")
    if args.chunk_size <= 0:
        parser.error("--chunk-size must be greater than zero.")
    if any(worker <= 0 for worker in args.workers):
        parser.error("All worker counts must be greater than zero.")
    if any(size <= 0 for size in args.batch_sizes):
        parser.error("All batch sizes must be greater than zero.")
    if args.repeats <= 0:
        parser.error("--repeats must be greater than zero.")

    return args


def generate_fasta(path, length, line_size=50_000):
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
    try:
        start = time.perf_counter()
        result = function()
        elapsed = time.perf_counter() - start
        _, peak_memory = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    return result, elapsed, peak_memory / (1024 * 1024)



class ProcessTreeMemorySampler:
    """Sample RSS for the benchmark process and its descendants."""

    def __init__(self, interval=0.05):
        self.interval = interval
        self.process = psutil.Process(os.getpid())
        self.peak_bytes = 0
        self.stop_event = threading.Event()
        self.thread = None

    def _sample_once(self):
        try:
            total = self.process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return

        try:
            processes = self.process.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            processes = []

        for process in processes:
            try:
                total += process.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        self.peak_bytes = max(self.peak_bytes, total)

    def _sample(self):
        while not self.stop_event.is_set():
            self._sample_once()
            self.stop_event.wait(self.interval)

    def __enter__(self):
        self._sample_once()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop_event.set()
        self.thread.join()
        self._sample_once()
        return False

    @property
    def peak_mb(self):
        return self.peak_bytes / (1024 * 1024)


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


def validate_args(args):
    if args.reference_size < 500_020:
        raise ValueError(
            "Reference size must be at least 500020 bases "
            "to contain the inserted match."
        )
    if args.chunk_size <= 0:
        raise ValueError("Chunk size must be greater than zero.")
    if args.repeats <= 0:
        raise ValueError("Repeats must be greater than zero.")
    if any(worker <= 0 for worker in args.workers):
        raise ValueError("Worker counts must be greater than zero.")
    if 1 not in args.workers:
        raise ValueError("Include worker count 1 for speedup comparisons.")
    if any(batch <= 0 for batch in args.batch_sizes):
        raise ValueError("Batch sizes must be greater than zero.")


def main():
    args = parse_args()
    validate_args(args)

    workers_list = list(dict.fromkeys(args.workers))
    batch_sizes = list(dict.fromkeys(args.batch_sizes))
    scheduling_modes = list(dict.fromkeys(args.scheduling_modes))

    print("GeneWeaver: Dask Scalability Benchmark")
    print("=" * 52)
    print(f"Reference size: {args.reference_size:,} bases")
    print(f"Chunk size: {args.chunk_size:,}")
    print(f"Worker counts: {workers_list}")
    print(f"Initial task batch sizes: {batch_sizes}")
    print(f"Scheduling modes: {scheduling_modes}")
    print(f"Worker process mode: {args.processes}")
    print(f"Trials per configuration: {args.repeats}")
    print(f"Available logical CPUs: {os.cpu_count()}")
    print()

    with tempfile.TemporaryDirectory() as temp_dir:
        fasta_path = Path(temp_dir) / "benchmark.fasta"
        guide = generate_fasta(fasta_path, args.reference_size)

        print("Running streaming reference benchmark...")

        streaming_results, streaming_time, streaming_memory = measure(
            lambda: find_off_targets_streaming(
                fasta_path,
                guide,
                max_mismatches=3,
                chunk_size=args.chunk_size,
            )
        )

        expected = normalize(streaming_results)

        print(f"Streaming time: {streaming_time:.4f} seconds")
        print(f"Streaming matches: {len(streaming_results)}")
        print(f"Streaming client traced peak: {streaming_memory:.2f} MB")
        print()

        if not any(
            hit["position"] == 500_000 and hit["mismatches"] == 0
            for hit in streaming_results
        ):
            raise RuntimeError("Streaming reference missed the known exact match.")

        summary_rows = []

        for scheduling_mode in scheduling_modes:
            adaptive = scheduling_mode == "adaptive"

            for batch_size in batch_sizes:
                for worker_count in workers_list:
                    trial_times = []
                    trial_memories = []
                    trial_process_tree_memories = []
                    match_count = None

                    print(
                        f"Testing mode={scheduling_mode}, "
                        f"workers={worker_count}, "
                        f"initial_batch_size={batch_size}"
                    )

                    for trial in range(1, args.repeats + 1):
                        with ProcessTreeMemorySampler() as memory_sampler:
                            dask_results, elapsed, peak_memory = measure(
                                lambda: find_off_targets_dask(
                                    fasta_path,
                                    guide,
                                    max_mismatches=3,
                                    chunk_size=args.chunk_size,
                                    n_workers=worker_count,
                                    task_batch_size=batch_size,
                                    processes=args.processes,
                                    adaptive=adaptive,
                                )
                            )

                        if normalize(dask_results) != expected:
                            raise RuntimeError(
                                "Result verification failed for "
                                f"mode={scheduling_mode}, "
                                f"workers={worker_count}, "
                                f"batch_size={batch_size}, trial={trial}."
                            )

                        if not any(
                            hit["position"] == 500_000
                            and hit["mismatches"] == 0
                            for hit in dask_results
                        ):
                            raise RuntimeError(
                                "Dask missed the known exact match."
                            )

                        trial_times.append(elapsed)
                        trial_memories.append(peak_memory)
                        trial_process_tree_memories.append(
                            memory_sampler.peak_mb
                        )
                        match_count = len(dask_results)

                        print(
                            f"  Trial {trial}/{args.repeats}: "
                            f"{elapsed:.4f}s, "
                            f"{peak_memory:.2f} MB traced, "
                            f"{memory_sampler.peak_mb:.2f} MB process-tree RSS"
                        )

                    summary_rows.append({
                        "scheduling_mode": scheduling_mode,
                        "reference_size": args.reference_size,
                        "chunk_size": args.chunk_size,
                        "workers": worker_count,
                        "task_batch_size": batch_size,
                        "repeats": args.repeats,
                        "median_seconds": statistics.median(trial_times),
                        "min_seconds": min(trial_times),
                        "max_seconds": max(trial_times),
                        "median_client_traced_peak_mb": statistics.median(
                            trial_memories
                        ),
                        "median_process_tree_peak_rss_mb": statistics.median(
                            trial_process_tree_memories
                        ),
                        "process_workers": args.processes,
                        "matches": match_count,
                    })

                    print()

    baseline_times = {
        (row["scheduling_mode"], row["task_batch_size"]): row["median_seconds"]
        for row in summary_rows
        if row["workers"] == 1
    }

    for row in summary_rows:
        baseline = baseline_times[
            (row["scheduling_mode"], row["task_batch_size"])
        ]
        row["speedup_vs_1_worker"] = (
            baseline / row["median_seconds"]
            if row["median_seconds"] > 0
            else 0
        )

    output_path = args.output
    if not output_path.is_absolute():
        output_path = PROJECT_ROOT / output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(summary_rows[0].keys())

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)

    print("=" * 88)
    print("SCHEDULING MODE COMPARISON")
    print("=" * 88)
    print(
        f"{'Mode':>10} {'Workers':>8} {'Batch':>8} {'Median (s)':>12} "
        f"{'Traced MB':>12} {'Process RSS MB':>16} {'Speedup':>10}"
    )

    for row in sorted(
        summary_rows,
        key=lambda item: (
            item["scheduling_mode"],
            item["workers"],
            item["task_batch_size"],
        ),
    ):
        print(
            f"{row['scheduling_mode']:>10} "
            f"{row['workers']:>8} "
            f"{row['task_batch_size']:>8} "
            f"{row['median_seconds']:>12.4f} "
            f"{row['median_client_traced_peak_mb']:>12.2f} "
            f"{row['median_process_tree_peak_rss_mb']:>16.2f} "
            f"{row['speedup_vs_1_worker']:>9.2f}x"
        )

    best = min(summary_rows, key=lambda row: row["median_seconds"])

    print()
    print("Result verification: PASSED for every configuration and trial.")
    print(
        "Fastest configuration: "
        f"{best['scheduling_mode']} mode, "
        f"{best['workers']} workers, "
        f"initial batch size {best['task_batch_size']}, "
        f"{best['median_seconds']:.4f} seconds median."
    )
    print(f"CSV results saved to: {output_path}")
    print()
    print(
        "Memory note: traced MB is client-process Python allocation from "
        "tracemalloc. Process RSS MB is sampled RSS summed across the benchmark "
        "process and its descendants, including Dask processes where present. "
        "Sampling can miss brief peaks, and summed RSS can count shared pages "
        "more than once."
    )

if __name__ == "__main__":
    main()
