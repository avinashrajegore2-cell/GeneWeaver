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