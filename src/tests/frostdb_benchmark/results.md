Starting FrostDB Performance Benchmark
=====================================
Running full benchmark including 1M records test

Running benchmarks for 10000 records...
Benchmarking insert operation with 10000 records...
level=info db=benchmark_db msg="recovering db" name=benchmark_db
level=info db=benchmark_db msg="failed to load latest snapshot" db=benchmark_db err="no valid snapshots found"
level=info db=benchmark_db msg="db recovered" wal_replay_duration=3.684µs watermark=0
level=info db=benchmark_db msg="snapshot on close completed" duration=14.804254ms
level=info db=benchmark_db msg="closing DB"
level=info db=benchmark_db msg="closed all tables"
Insert completed in 28.748143ms - 347848.55 records/sec
Storage size: 64.50 MB (6763.49 bytes/record)
Waiting for database to be properly closed...
Benchmarking query operation with 100 queries...
level=info db=benchmark_db msg="recovering db" name=benchmark_db
level=info db=benchmark_db msg="db recovered" wal_replay_duration=6.64µs watermark=12 snapshot_tx=12 snapshot_load_duration=29.503249ms
level=info db=benchmark_db msg="snapshot on close completed" duration=16.756723ms
level=info db=benchmark_db msg="closing DB"
level=info db=benchmark_db msg="closed all tables"
Query completed in 477.598509ms - 209.38 queries/sec

Running benchmarks for 100000 records...
Benchmarking insert operation with 100000 records...
level=info db=benchmark_db msg="recovering db" name=benchmark_db
level=info db=benchmark_db msg="failed to load latest snapshot" db=benchmark_db err="no valid snapshots found"
level=info db=benchmark_db msg="db recovered" wal_replay_duration=4.332µs watermark=0
  Progress: 10% complete (10000/100000 records)
  Progress: 20% complete (20000/100000 records)
  Progress: 30% complete (30000/100000 records)
  Progress: 40% complete (40000/100000 records)
  Progress: 50% complete (50000/100000 records)
  Progress: 60% complete (60000/100000 records)
  Progress: 70% complete (70000/100000 records)
  Progress: 80% complete (80000/100000 records)
  Progress: 90% complete (90000/100000 records)
level=info db=benchmark_db msg="snapshot on close completed" duration=14.56137ms
level=info db=benchmark_db msg="closing DB"
level=info db=benchmark_db msg="closed all tables"
Insert completed in 263.380675ms - 379678.58 records/sec
Storage size: 68.79 MB (721.29 bytes/record)
Waiting for database to be properly closed...
Benchmarking query operation with 100 queries...
level=info db=benchmark_db msg="recovering db" name=benchmark_db
level=info db=benchmark_db msg="db recovered" wal_replay_duration=5.927µs watermark=102 snapshot_tx=102 snapshot_load_duration=29.182009ms
level=info db=benchmark_db msg="snapshot on close completed" duration=18.826512ms
level=info db=benchmark_db msg="closing DB"
level=info db=benchmark_db msg="closed all tables"
Query completed in 3.94473014s - 25.35 queries/sec

Running benchmarks for 1000000 records...
Benchmarking insert operation with 1000000 records...
level=info db=benchmark_db msg="recovering db" name=benchmark_db
level=info db=benchmark_db msg="failed to load latest snapshot" db=benchmark_db err="no valid snapshots found"
level=info db=benchmark_db msg="db recovered" wal_replay_duration=7.958µs watermark=0
  Progress: 10% complete (100000/1000000 records)
  Progress: 20% complete (200000/1000000 records)
  Progress: 30% complete (300000/1000000 records)
  Progress: 40% complete (400000/1000000 records)
  Progress: 50% complete (500000/1000000 records)
  Progress: 60% complete (600000/1000000 records)
  Progress: 70% complete (700000/1000000 records)
  Progress: 80% complete (800000/1000000 records)
  Progress: 90% complete (900000/1000000 records)
level=info db=benchmark_db msg="snapshot on close completed" duration=15.972474ms
level=info db=benchmark_db msg="closing DB"
level=info db=benchmark_db msg="closed all tables"
Insert completed in 5.573098259s - 179433.41 records/sec
Storage size: 177.51 MB (186.13 bytes/record)
Waiting for database to be properly closed...
Benchmarking query operation with 100 queries...
level=info db=benchmark_db msg="recovering db" name=benchmark_db
level=info db=benchmark_db msg="db recovered" wal_replay_duration=7.82µs watermark=1002 snapshot_tx=1002 snapshot_load_duration=43.087461ms
level=info db=benchmark_db msg="snapshot on close completed" duration=15.931677ms
level=info db=benchmark_db msg="closing DB"
level=info db=benchmark_db msg="closed all tables"
Query completed in 14.211978289s - 7.04 queries/sec

Benchmark Results Summary:
=========================
   Operation|   Data Size|        Duration|   Storage (MB)|Records/sec
   ---------|   ---------|        --------|   ------------|-----------
      Insert|       10000|     28.748143ms|          64.50|347848.55
       Query|       10000|    477.598509ms|          64.50|209.38
      Insert|      100000|    263.380675ms|          68.79|379678.58
       Query|      100000|     3.94473014s|          68.79|25.35
      Insert|     1000000|    5.573098259s|         177.51|179433.41
       Query|     1000000|   14.211978289s|         177.51|7.04