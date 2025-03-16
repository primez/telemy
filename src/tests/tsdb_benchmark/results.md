Starting TSDB Performance Benchmark
==================================
Running full benchmark including 1M records test

Running benchmarks for 10000 records...
Benchmarking insert operation with 10000 records...
Insert completed in 54.742701ms - 182672.75 records/sec
Storage size: 1.30 MB (136.00 bytes/record)
Benchmarking query operation with 100 queries...
Query completed in 30.454193ms - 3283.62 queries/sec

Running benchmarks for 100000 records...
Benchmarking insert operation with 100000 records...
  Progress: 10% complete (10000/100000 records)
  Progress: 20% complete (20000/100000 records)
  Progress: 30% complete (30000/100000 records)
  Progress: 40% complete (40000/100000 records)
  Progress: 50% complete (50000/100000 records)
  Progress: 60% complete (60000/100000 records)
  Progress: 70% complete (70000/100000 records)
  Progress: 80% complete (80000/100000 records)
  Progress: 90% complete (90000/100000 records)
Insert completed in 547.738592ms - 182568.84 records/sec
Storage size: 13.06 MB (136.99 bytes/record)
Benchmarking query operation with 100 queries...
Query completed in 7.546802ms - 13250.65 queries/sec

Running benchmarks for 1000000 records...
Benchmarking insert operation with 1000000 records...
  Progress: 10% complete (100000/1000000 records)
  Progress: 20% complete (200000/1000000 records)
  Progress: 30% complete (300000/1000000 records)
  Progress: 40% complete (400000/1000000 records)
  Progress: 50% complete (500000/1000000 records)
  Progress: 60% complete (600000/1000000 records)
  Progress: 70% complete (700000/1000000 records)
  Progress: 80% complete (800000/1000000 records)
  Progress: 90% complete (900000/1000000 records)
Insert completed in 7.301345858s - 136961.05 records/sec
Storage size: 131.63 MB (138.02 bytes/record)
Benchmarking query operation with 100 queries...
Query completed in 13.647616ms - 7327.29 queries/sec

Benchmark Results Summary:
=========================
   Operation|   Data Size|       Duration|   Storage (MB)|Records/sec
   ---------|   ---------|       --------|   ------------|-----------
      Insert|       10000|    54.742701ms|           1.30|182672.75
       Query|       10000|    30.454193ms|           1.30|3283.62
      Insert|      100000|   547.738592ms|          13.06|182568.84
       Query|      100000|     7.546802ms|          13.06|13250.65
      Insert|     1000000|   7.301345858s|         131.63|136961.05
       Query|     1000000|    13.647616ms|         131.63|7327.29