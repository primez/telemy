prime@5CG11270DM:/mnt/d/Hobbies/telemy/app/src/tools/frostdb$ go run main.go 
level=info db=logs_db msg="recovering db" name=logs_db
level=info db=logs_db msg="failed to load latest snapshot" db=logs_db err="no valid snapshots found"
level=info db=logs_db msg="db recovered" wal_replay_duration=4.532µs watermark=0
Generating and inserting log samples...
Write completed. Inserted 1000000 record batches in 13.079897243s

-------------------------------------------------------------
QUERY 1: 100 queries with different level and component values
-------------------------------------------------------------
Running 100 queries with different combinations...
Query 1 (level=level_0, component=component_0) Result batch 1:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 828 more rows

Query 1 (level=level_0, component=component_0) Result batch 2:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 829 more rows

Query 1 (level=level_0, component=component_0) Result batch 3:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 829 more rows

Query 1 (level=level_0, component=component_0) Result batch 4:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 828 more rows

Query 1 (level=level_0, component=component_0) Result batch 5:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 828 more rows

Query 1 (level=level_0, component=component_0) Result batch 6:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 829 more rows

Query 1 (level=level_0, component=component_0) Result batch 7:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 828 more rows

Query 1 (level=level_0, component=component_0) Result batch 8:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 828 more rows

Query 1 (level=level_0, component=component_0) Result batch 9:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 828 more rows

Query 1 (level=level_0, component=component_0) Result batch 10:
Schema: labels.component (dictionary<values=binary, indices=uint32, ordered=false>), labels.level (dictionary<values=binary, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_0", labels.level="level_0"
Row 1: labels.component="component_0", labels.level="level_0"
Row 2: labels.component="component_0", labels.level="level_0"
Row 3: labels.component="component_0", labels.level="level_0"
Row 4: labels.component="component_0", labels.level="level_0"
... and 829 more rows

Query 2 (level=level_1, component=component_1) Result batch 1:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 828 more rows

Query 2 (level=level_1, component=component_1) Result batch 2:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 829 more rows

Query 2 (level=level_1, component=component_1) Result batch 3:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 828 more rows

Query 2 (level=level_1, component=component_1) Result batch 4:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 828 more rows

Query 2 (level=level_1, component=component_1) Result batch 5:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 828 more rows

Query 2 (level=level_1, component=component_1) Result batch 6:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 829 more rows

Query 2 (level=level_1, component=component_1) Result batch 7:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 829 more rows

Query 2 (level=level_1, component=component_1) Result batch 8:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 828 more rows

Query 2 (level=level_1, component=component_1) Result batch 9:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 828 more rows

Query 2 (level=level_1, component=component_1) Result batch 10:
Schema: labels.component (dictionary<values=binary, indices=uint32, ordered=false>), labels.level (dictionary<values=binary, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_1", labels.level="level_1"
Row 1: labels.component="component_1", labels.level="level_1"
Row 2: labels.component="component_1", labels.level="level_1"
Row 3: labels.component="component_1", labels.level="level_1"
Row 4: labels.component="component_1", labels.level="level_1"
... and 829 more rows

Query 3 (level=level_2, component=component_2) Result batch 1:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 829 more rows

Query 3 (level=level_2, component=component_2) Result batch 2:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 829 more rows

Query 3 (level=level_2, component=component_2) Result batch 3:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 828 more rows

Query 3 (level=level_2, component=component_2) Result batch 4:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 828 more rows

Query 3 (level=level_2, component=component_2) Result batch 5:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 828 more rows

Query 3 (level=level_2, component=component_2) Result batch 6:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 828 more rows

Query 3 (level=level_2, component=component_2) Result batch 7:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 828 more rows

Query 3 (level=level_2, component=component_2) Result batch 8:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 833 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 828 more rows

Query 3 (level=level_2, component=component_2) Result batch 9:
Schema: labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.level (dictionary<values=utf8, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 829 more rows

Query 3 (level=level_2, component=component_2) Result batch 10:
Schema: labels.component (dictionary<values=binary, indices=uint32, ordered=false>), labels.level (dictionary<values=binary, indices=uint32, ordered=false>)
Record contains 834 rows. Showing first 5 rows:
Row 0: labels.component="component_2", labels.level="level_2"
Row 1: labels.component="component_2", labels.level="level_2"
Row 2: labels.component="component_2", labels.level="level_2"
Row 3: labels.component="component_2", labels.level="level_2"
Row 4: labels.component="component_2", labels.level="level_2"
... and 829 more rows

Completed 10/100 queries...
Completed 20/100 queries...
Completed 30/100 queries...
Completed 40/100 queries...
Completed 50/100 queries...
Completed 60/100 queries...
Completed 70/100 queries...
Completed 80/100 queries...
Completed 90/100 queries...
Completed 100/100 queries...

Query Summary:
Total queries: 100
Total records: 1000
Total duration: 788.60191ms
Average duration: 7.886019ms
Min duration: 6.50289ms
Max duration: 12.620678ms

Query Time Distribution:
6.50289ms to 7.726447ms: 63 queries
7.726447ms to 8.950004ms: 23 queries
8.950004ms to 10.173561ms: 7 queries
10.173561ms to 11.397118ms: 3 queries
11.397118ms to 12.620675ms: 4 queries

-------------------------------------------------------------
QUERY 2: Logs within a time range
-------------------------------------------------------------
Time query completed. Found 0 record batches in 402.044µs

-------------------------------------------------------------
QUERY 3: Aggregation by log level
-------------------------------------------------------------
Calculating count, sum, min and max grouped by log level:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), count(value) (int64), sum(value) (int64), min(timestamp) (int64), max(timestamp) (int64)
Record contains 4 rows:
Row 0: labels.level="level_0", count(value)=25000, sum(value)=23749950000, min(timestamp)=1742146433808557494, max(timestamp)=1742146533277561525
Row 1: labels.level="level_1", count(value)=25000, sum(value)=23749975000, min(timestamp)=1742146433807558376, max(timestamp)=1742146533276564795
Row 2: labels.level="level_2", count(value)=25000, sum(value)=23750000000, min(timestamp)=1742146433806564117, max(timestamp)=1742146533275565817
Row 3: labels.level="level_3", count(value)=25000, sum(value)=23750025000, min(timestamp)=1742146433805565078, max(timestamp)=1742146533274566868

Aggregation query completed in 4.60876ms

-------------------------------------------------------------
QUERY 4: Full-text search in stacktrace
-------------------------------------------------------------
Searching for 'TimeoutException' in stacktrace:
Full-text search result batch 1:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_0", labels.stacktrace="981000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146452756651759

Full-text search result batch 2:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_2", labels.stacktrace="941000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146492614008355

Full-text search result batch 3:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_0", labels.stacktrace="921000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146512537052115

Full-text search result batch 4:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_1", labels.stacktrace="991000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146442785954533

Full-text search result batch 5:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_2", labels.stacktrace="971000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146462718916118

Full-text search result batch 6:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_1", labels.stacktrace="961000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146472682756037

Full-text search result batch 7:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_2", labels.stacktrace="911000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146522505594970

Full-text search result batch 8:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_1", labels.stacktrace="931000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146502573762429

Full-text search result batch 9:
Schema: labels.level (dictionary<values=utf8, indices=uint32, ordered=false>), labels.component (dictionary<values=utf8, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=utf8, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_0", labels.stacktrace="951000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146482646640743

Full-text search result batch 10:
Schema: labels.level (dictionary<values=binary, indices=uint32, ordered=false>), labels.component (dictionary<values=binary, indices=uint32, ordered=false>), labels.stacktrace (dictionary<values=binary, indices=uint32, ordered=false>), timestamp (int64)
Record contains 1 rows:
Row 0: labels.level="level_0", labels.component="component_1", labels.stacktrace="901000: System.Threading.Tasks.TaskCanceledException: The operation was canceled.
 ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.
   --- End of inner exception stack trace ---
   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)
   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)
   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)
   at Sys", timestamp=1742146532278402269

Full-text search query completed. Found 10 record batches in 37.336925ms