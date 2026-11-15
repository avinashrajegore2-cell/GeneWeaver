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
