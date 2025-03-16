# Telemy Database Performance Tests

This directory contains benchmark tests for comparing different time-series database implementations for use in the Telemy application.

## Available Benchmark Tests

- **tsdb_benchmark**: Standalone benchmark for Prometheus TSDB performance
- **frostdb_benchmark**: Standalone benchmark for FrostDB performance

## Results

The comparison results between the two database implementations are available in:
- `db_comparison_results.md`: Detailed performance metrics and analysis

## Running the Benchmarks

### TSDB Benchmark
```powershell
cd src/tests/tsdb_benchmark
go run main.go
```

For the full benchmark with 1M records:
```powershell
cd src/tests/tsdb_benchmark
go run main.go full
```

### FrostDB Benchmark
```powershell
cd src/tests/frostdb_benchmark
go run main.go
```

For the full benchmark with 1M records:
```powershell
cd src/tests/frostdb_benchmark
go run main.go full
```

## Key Findings

- **FrostDB**: Excels at high-throughput data insertion but uses more storage
- **TSDB**: More storage-efficient but significantly slower for insertions

Refer to `db_comparison_results.md` for the complete performance analysis and detailed metrics. 