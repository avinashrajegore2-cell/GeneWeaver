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

## Process-Based Dask Scheduling Experiment

### Configuration

- Synthetic DNA reference sizes: 5 million and 20 million base pairs.
- Chunk size: 100,000 base pairs.
- Guide length: 20 base pairs.
- Maximum mismatches: 3.
- Scheduling modes: fixed and adaptive.
- Worker execution: separate Dask worker processes (`processes=True`).
- Repetitions: 3 per configuration.
- Every Dask result was verified against the streaming reference.

The 5M experiment tested 1, 2, 4, and 8 workers with batch sizes 1, 2, 4, 8, and 16. The 20M experiment tested 1 and 2 workers with batch sizes 1, 4, 8, and 16.

### Selected results

| Reference size | Mode | Workers | Initial batch | Median runtime |
|---|---|---:|---:|---:|
| 5M | Fixed | 1 | 16 | 3.3125 s |
| 5M | Adaptive | 1 | 2 | 3.1757 s |
| 20M | Fixed | 2 | 16 | 4.8080 s |
| 20M | Adaptive | 2 | 16 | 5.2322 s |

At 5M bases, adaptive was approximately 4.1% faster than fixed at the best observed configurations. At 20M bases, fixed was approximately 8.1% faster than adaptive.

### Memory and scaling observations

Process-tree RSS increased with worker count. In the 5M experiment, eight workers used approximately 680-735 MB of process-tree RSS and were substantially slower than one worker.

In the 20M experiment, two workers produced the fastest configurations among the tested worker counts.

Process-tree RSS is sampled across the benchmark process and its descendants. It can count shared pages more than once, and sampling may miss brief peaks.

### Interpretation and limitations

Separate worker processes introduce memory and startup overhead. They did not improve the best runtime over threads in the 5M experiment. At 20M, the best thread and process configurations were nearly tied.

Adaptive scheduling had a small advantage at 5M, while fixed scheduling was faster at 20M in the latest process run. These results are specific to the tested synthetic workloads and machine. They do not establish a universally optimal worker count or scheduling mode.

Adaptive scheduling remains opt-in, and fixed scheduling remains the default.

### Reproducing the experiments

5-million-base process-mode comparison:

    .\.venv-cuda\Scripts\python.exe .\benchmarks\benchmark_dask.py --reference-size 5000000 --chunk-size 100000 --workers 1 2 4 8 --batch-sizes 1 2 4 8 16 --scheduling-modes fixed adaptive --repeats 3 --processes --output .\benchmarks\dask_process_5m.csv

20-million-base process-mode comparison:

    .\.venv-cuda\Scripts\python.exe .\benchmarks\benchmark_dask.py --reference-size 20000000 --chunk-size 100000 --workers 1 2 --batch-sizes 1 4 8 16 --scheduling-modes fixed adaptive --repeats 3 --processes --output .\benchmarks\dask_process_20m.csv

## Serial Streaming vs Dask Performance Comparison

### Purpose and configuration

This experiment compares the streaming serial implementation against local thread-based Dask scheduling. It uses the same synthetic reference, guide, mismatch threshold, and chunk size for both implementations.

- Synthetic DNA reference sizes: 5 million and 20 million base pairs.
- Chunk size: 100,000 base pairs.
- Guide length: 20 base pairs.
- Maximum mismatches: 3.
- Dask scheduling modes: fixed and adaptive.
- Dask worker counts: 1, 2, 4, and 8.
- Initial task batch sizes: 1, 2, 4, 8, and 16.
- Repetitions: 3 per configuration, including the streaming baseline.
- Worker execution: local threads (`processes=False`).
- Every Dask result was verified against the streaming reference.

The 20M experiment used 1 and 2 workers, with batch sizes 1, 4, 8, and 16.

### Selected results

| Reference size | Execution mode | Configuration | Median runtime | Relative speed |
|---|---|---|---:|---:|
| 5M | Streaming | Serial | 0.8569 s | 1.00x |
| 5M | Dask fixed | 1 worker, batch 8 | 1.3992 s | 0.61x |
| 5M | Dask adaptive | 1 worker, batch 4 | 1.4510 s | 0.59x |
| 20M | Streaming | Serial | 3.3922 s | 1.00x |
| 20M | Dask fixed | 2 workers, batch 8 | 5.0109 s | 0.68x |
| 20M | Dask adaptive | 2 workers, batch 16 | 4.8122 s | 0.70x |

Relative speed is calculated as the streaming median runtime divided by the Dask median runtime. Values below 1.0x mean Dask was slower than serial streaming.

At 5M bases, the fastest Dask configuration was approximately 63% slower than streaming. At 20M bases, the fastest Dask configuration was approximately 42% slower.

### Memory observations

The streaming baseline's median client-process Python traced peak was approximately 3.95 MB at both reference sizes.

The fastest Dask configurations had median client-process traced peaks of approximately 13.86 MB at 5M and 45.33 MB at 20M.

These traced allocations are not directly equivalent to total process memory. Process-tree RSS is separately sampled across the benchmark process and its descendants. RSS may include shared pages and sampling can miss brief peaks.

### Interpretation and limitations

For both tested reference sizes, serial streaming outperformed every tested Dask configuration. Dask scheduling, task submission, and coordination overhead outweighed the benefits of parallel execution on this single-machine workload.

Adaptive scheduling improved the best Dask runtime over the best fixed configuration at 20M, but neither mode surpassed serial streaming. At 5M, fixed scheduling produced the fastest Dask result.

These results do not mean Dask cannot help with larger workloads, distributed execution, or different hardware. They show that Dask did not provide a single-machine runtime improvement for these particular synthetic workloads and configurations.

Streaming should remain the performance baseline for these tests. Dask remains an optional distributed-scheduling capability rather than a demonstrated speed optimization for these cases.

### Reproducing the experiments

5-million-base serial-versus-Dask comparison:

    .\.venv-cuda\Scripts\python.exe .\benchmarks\benchmark_dask.py --reference-size 5000000 --chunk-size 100000 --workers 1 2 4 8 --batch-sizes 1 2 4 8 16 --scheduling-modes fixed adaptive --repeats 3 --output "$env:TEMP\geneweaver_serial_dask_5m_threads.csv"

20-million-base serial-versus-Dask comparison:

    .\.venv-cuda\Scripts\python.exe .\benchmarks\benchmark_dask.py --reference-size 20000000 --chunk-size 100000 --workers 1 2 --batch-sizes 1 2 4 8 16 --scheduling-modes fixed adaptive --repeats 3 --output "$env:TEMP\geneweaver_serial_dask_20m_threads.csv"
