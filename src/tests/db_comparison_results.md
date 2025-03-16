# Database Performance Comparison: FrostDB vs TSDB

This document compares the performance of FrostDB and Prometheus TSDB for time-series data storage and retrieval operations.

## Test Environment
- Windows 10
- Test data: Logs with identical schema
- Batch size: 1,000 records
- Query test: 100 queries with component and level filtering

## Performance Comparison

| Database | Operation | Data Size | Duration | Storage (MB) | Operations/sec | Storage Efficiency |
|----------|-----------|-----------|----------|--------------|----------------|-------------------|
| FrostDB  | Insert    | 10,000    | 26.06ms  | 64.50        | 383,710.01     | 6,763.49 bytes/record |
| TSDB     | Insert    | 10,000    | 2.30s    | 1.57         | 4,346.86       | 164.92 bytes/record |
| FrostDB  | Query     | 10,000    | 506.99ms | 64.50        | 197.24         | - |
| TSDB     | Query     | 10,000    | 688.24ms | 1.57         | 145.30         | - |
| FrostDB  | Insert    | 100,000   | 246.77ms | 68.79        | 405,236.54     | 721.29 bytes/record |
| TSDB     | Insert    | 100,000   | 20.43s   | 15.82        | 4,893.65       | 165.92 bytes/record |
| FrostDB  | Query     | 100,000   | 3.78s    | 68.79        | 26.43          | - |
| TSDB     | Query     | 100,000   | 7.86s    | 15.82        | 12.73          | - |

## Performance Ratios (FrostDB:TSDB)

| Data Size | Insert Speed | Query Speed | Storage Size | Storage Efficiency |
|-----------|--------------|-------------|--------------|-------------------|
| 10,000    | 88.3x faster | 1.4x faster | 41.1x larger | 41.0x less efficient |
| 100,000   | 82.8x faster | 2.1x faster | 4.3x larger  | 4.3x less efficient |

## Analysis

1. **Insert Performance**:
   - FrostDB is dramatically faster at insertions (80-90x) compared to TSDB
   - This makes FrostDB better suited for high-throughput log ingestion scenarios

2. **Query Performance**:
   - FrostDB shows moderate query performance advantages (1.4-2.1x faster)
   - The query advantage increases with larger datasets

3. **Storage Efficiency**:
   - TSDB is significantly more storage-efficient
   - TSDB uses only ~165 bytes per record regardless of dataset size
   - FrostDB's storage efficiency improves with scale (6,763 → 721 bytes/record)

4. **Scalability**:
   - FrostDB showed better scalability as dataset size increased
   - Storage efficiency improved dramatically with larger datasets

## Conclusion

FrostDB excels at high-throughput ingestion scenarios, making it well-suited for applications requiring rapid data insertion, while TSDB provides significantly better storage efficiency. The choice between them would depend on whether insert performance or storage efficiency is the primary concern.

For applications where storage space is limited but extremely high insert rates aren't critical, TSDB would be preferable. For applications requiring maximum insert throughput and where storage is less constrained, FrostDB would be the better choice. 