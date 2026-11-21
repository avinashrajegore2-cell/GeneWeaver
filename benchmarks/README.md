# GeneWeaver Benchmark Report

## CPU vs NumPy Alignment

### Test configuration
- Reference genome: 1,000,000 base pairs
- Guide length: 20 base pairs
- Maximum mismatches: 3
- Repetitions: 3 after warm-up
- Input: Reproducible synthetic DNA

### Results

| Metric | Python CPU | NumPy |
|---|---:|---:|
| Execution time | 2.9942 seconds | 0.1847 seconds |
| Matches found | 1 | 1 |

### Performance
- Measured speedup: 16.21x
- Result verification: Passed

### Notes
This is an initial synthetic benchmark.
It does not represent full human-genome performance.
GPU acceleration and large-genome memory optimization remain future work.
## Chunked NumPy Alignment

### Test configuration
- Reference genome: 1,000,000 base pairs
- Guide length: 20 base pairs
- Maximum mismatches: 3
- Chunk size: 100,000 base pairs
- Input: Reproducible synthetic DNA

### Results

| Metric | Standard NumPy | Chunked NumPy |
|---|---:|---:|
| Execution time | 0.1620 seconds | 0.1575 seconds |
| Matches found | 1 | 1 |

### Performance
- Chunked/standard execution time ratio: 0.97x
- Result verification: Passed
- Approximate measured time reduction: 2.8%

### Notes
This is a single-run synthetic benchmark, not a statistically
conclusive performance comparison.
Chunking reduces the size of intermediate comparison arrays.
The reference sequence is still encoded in memory in full.
Further memory optimization is needed for genome-scale FASTA processing.

## Streaming FASTA Alignment

### Test configuration
- Reference genome: 1,000,000 base pairs
- Guide length: 20 base pairs
- Maximum mismatches: 3
- Streaming chunk size: 100,000 base pairs
- Input: Reproducible synthetic FASTA

### Results

| Metric | Standard NumPy | Streaming |
|---|---:|---:|
| Execution time | 0.1666 seconds | 0.2252 seconds |
| Peak traced memory | 28.67 MB | 3.83 MB |
| Matches found | 2 | 2 |

### Performance
- Streaming traced-memory reduction: approximately 86.6%
- Standard/streaming traced-memory ratio: approximately 7.48x
- Streaming execution was approximately 35% slower in this run.
- Result verification: Passed

### Notes
This is a single-run synthetic benchmark.
Memory was measured using Python tracemalloc and does not
represent total process RAM or all native NumPy allocations.
The results demonstrate lower traced Python memory usage,
not a statistically conclusive performance improvement.

## Dask Distributed Alignment Benchmark

### Configuration
- Reference: 1,000,000 synthetic DNA base pairs
- Guide length: 20 base pairs
- Mismatch threshold: 3
- Chunk size: 100,000 base pairs
- Dask workers: 2 local workers, one thread each
- Scheduler: Local Dask cluster (`processes=False`)
- An exact guide match was inserted at position 500,000.

### Results

| Metric | Streaming | Dask |
|---|---:|---:|
| Runtime | 0.1928 s | 0.7434 s |
| Traced peak memory | 3.95 MB | 9.97 MB |
| Matches found | 2 | 2 |

- Result verification: PASSED
- Known exact-match verification: PASSED
- Dask runtime was approximately 3.86x the streaming runtime.
- Dask traced peak memory was approximately 2.52x the streaming result.

### Analysis and limitations

For this 1-million-base single-machine workload, Dask was slower than the streaming implementation. Cluster initialization, task scheduling, and result collection introduced overhead that outweighed the benefit of distributing this relatively small workload.

This benchmark demonstrates local Dask task scheduling, not multi-machine or multi-GPU execution. The workers used local threads (`processes=False`). Traced memory comes from Python `tracemalloc` and does not represent total process or system RAM.

The benchmark uses synthetic DNA and is not a human-genome performance measurement. Larger workloads and a multi-machine environment are needed to evaluate Dask's scalability.

## Dask Scalability and Process Memory Benchmark

### Requirements
- Run using the GeneWeaver Python environment.
- `dask` and `distributed` are required for Dask execution.
- `psutil` is required for process-tree RSS measurement.

### Usage

Run the default thread-based benchmark:

    python .\benchmarks\benchmark_dask.py

Run with separate worker processes:

    python .\benchmarks\benchmark_dask.py --processes

Example smaller test:

    python .\benchmarks\benchmark_dask.py --reference-size 600000 --chunk-size 100000 --workers 1 2 --batch-sizes 4 8 --repeats 2

### Memory metrics

- Traced MB measures client-process Python allocations using `tracemalloc`.
- Process RSS MB is the sampled RSS summed across the benchmark process and its descendants.
- In thread mode, worker memory is part of the benchmark process.
- In process mode, the total can include Dask worker and scheduler processes.
- Sampling can miss brief peaks, and summed RSS can double-count shared memory pages.

These measurements are local process-memory observations, not total system memory or worker-only memory.

## Adaptive Dask Scheduling Experiment

### Configuration

- Synthetic DNA reference: 5 million and 20 million base pairs.
- Chunk size: 100,000 base pairs.
- Guide length: 20 base pairs.
- Maximum mismatches: 3.
- Scheduling modes: fixed and adaptive.
- Initial task batch sizes: 1, 2, 4, 8, and 16 (5M); 1, 4, 8, and 16 (20M).
- Worker counts: 1, 2, 4, and 8 (5M); 1 and 2 (20M).
- Repetitions: 3 per configuration.
- Worker execution: local threads (`processes=False`).
- Every Dask result was verified against the streaming reference.

### Selected results

The following are median runtimes from the three repetitions.

| Reference size | Mode | Workers | Initial batch | Median runtime |
|---|---|---:|---:|---:|
| 5M | Fixed | 1 | 16 | 1.3830 s |
| 5M | Adaptive | 1 | 16 | 1.6528 s |
| 20M | Fixed | 2 | 8 | 5.1980 s |
| 20M | Adaptive | 2 | 8 | 4.7918 s |

At 5M bases, the fastest fixed configuration was approximately 16.3% faster than the fastest adaptive configuration. At 20M bases, adaptive scheduling was approximately 7.8% faster than fixed scheduling at the same two-worker, batch-size-8 configuration.

### Memory observations

Process-tree RSS was higher for adaptive scheduling in the measured configurations. For example:

- At 5M bases, one worker and initial batch size 16: fixed 154.92 MB; adaptive 281.33 MB.
- At 20M bases, two workers and initial batch size 8: fixed 262.44 MB; adaptive 291.28 MB.

These are sampled process-tree RSS measurements, not worker-only memory measurements. RSS can include shared pages and is sensitive to the local runtime environment.

### Interpretation and limitations

Adaptive scheduling adjusts the task batch size using observed end-to-end task completion latency. This latency includes queueing and scheduling time, not only worker execution time. The initial batch size is capped at 16 in adaptive mode, and the in-flight submission limit is bounded using the active worker count.

The results are workload-dependent: adaptive scheduling did not improve the 5M-base benchmark, but improved the 20M-base result at the tested two-worker, batch-size-8 configuration. It also showed higher process-tree RSS in the measured comparisons.

Adaptive scheduling therefore remains an opt-in experimental feature. Fixed scheduling remains the default. These synthetic, single-machine experiments do not establish performance for human-genome datasets or multi-machine clusters.

### Reproducing the experiments

5-million-base comparison:

    .\.venv-cuda\Scripts\python.exe .\benchmarks\benchmark_dask.py --reference-size 5000000 --chunk-size 100000 --workers 1 2 4 8 --batch-sizes 1 2 4 8 16 --scheduling-modes fixed adaptive --repeats 3 --output .\benchmarks\dask_adaptive_5m.csv

20-million-base comparison:

    .\.venv-cuda\Scripts\python.exe .\benchmarks\benchmark_dask.py --reference-size 20000000 --chunk-size 100000 --workers 1 2 --batch-sizes 1 4 8 16 --scheduling-modes fixed adaptive --repeats 3 --output .\benchmarks\dask_adaptive_20m.csv
