# StreamDB

High-performance sharded time-series storage with sparse indexing and adaptive batching.

## Overview

StreamDB is a specialized storage engine designed for high-throughput stream ingestion with efficient time-range queries. It combines FasterLog's append-only log with SQLite sparse indexing to achieve:

- **Non-blocking writes**: <100μs latency, never waits for index updates
- **Efficient range queries**: O(log N) index lookup + bounded scan
- **Automatic retention**: Configurable data lifecycle management
- **Adaptive indexing**: Self-tunes based on write pressure

## Architecture

### Components

```
┌─────────────┐
│   Clients   │  (ASP.NET request threads)
└──────┬──────┘
       │ Append(secondaryIndex, payload, primaryIndex)
       ▼
┌─────────────────────────────────────────────────────────┐
│                      StreamDB                           │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐          │
│  │  Shard 0  │  │  Shard 1  │  │  Shard 2  │  ...     │
│  │ FasterLog │  │ FasterLog │  │ FasterLog │          │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘          │
│        │              │              │                  │
│        └──────────────┴──────────────┘                  │
│                       │                                 │
│              ┌────────▼────────┐                        │
│              │  Pending Queue  │  (bounded, lock-free) │
│              └────────┬────────┘                        │
│                       │                                 │
│              ┌────────▼────────┐                        │
│              │  FlushWorker    │  (dedicated thread)   │
│              │  - Batch writes │                        │
│              │  - Durability   │                        │
│              └────────┬────────┘                        │
│                       ▼                                 │
│              ┌─────────────────┐                        │
│              │ SQLite Index DB │                        │
│              │ (secondary_index, │                        │
│              │  ts) → log_addr   │                        │
│              └─────────────────┘                        │
└─────────────────────────────────────────────────────────┘
```

### Sharding

Records are distributed across 4 FasterLog shards using: `secondaryIndex & 0x3`

This reduces file handles and enables parallel I/O while maintaining per-secondary-index write ordering.

## Record Format

Each record stored in FasterLog has a 16-byte header followed by a variable-length payload:

```
┌──────────────┬────────────────┬──────────────┬─────────────────┬──────────────────┐
│ 8B: long     │ 4B: int        │ 2B: ushort   │ 2B: ushort      │ N bytes: payload │
│ primary_idx  │ secondary_idx  │ version      │ payload length  │ (opaque bytes)   │
│ PRIMARY IDX  │ SECONDARY IDX  │              │                 │                  │
└──────────────┴────────────────┴──────────────┴─────────────────┴──────────────────┘
```

- **Primary Index**: Used for range queries and ordering (e.g. timestamp, sequence number)
- **Secondary Index**: Used for sharding and filtering (e.g., device ID, sensor ID, user ID)
- **Version**: Schema version for payload format evolution
- **Payload**: Opaque bytes — StreamDB never interprets these; **max 65,535 bytes (≈64 KB)** since the length is stored as a `ushort`

## Usage

### Initialization

```csharp
var streamDb = new StreamDB(
    baseDir: "streams",
    retentionPeriod: TimeSpan.FromDays(60),
    checkpointInterval: TimeSpan.FromHours(1),
    logger: logger
);
```

### Writing Data

```csharp
// Serialize payload (exclude fields stored in header)
var payload = new GpsPayload { Lat = 37.77, Long = -122.42, Accuracy = 5.0f, Speed = 10.0f, Heading = 90.0f };

// Append with header fields + raw payload bytes
streamDb.Append(
    primaryIndex: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
    secondaryIndex: sensorId,
    version: StreamVersions.GpsV1,
    payload: MemoryMarshal.AsBytes(new ReadOnlySpan<GpsPayload>(in payload))
);
```

**Constraints:**
- Writes are serialized per secondary index (upstream responsibility)
- Primary index should ideally be monotonically increasing per secondary index for optimal performance
- Out-of-order (non-monotonic) primary index values are handled transparently via the late arrivals side store
- The late arrivals path incurs a synchronous SQLite write, so it's designed for occasional out-of-order data, not as the primary write path

### Reading Data

**Single secondary index:**
```csharp
List<StreamEntry> data = streamDb.ReadRange(
    secondaryIndex: sensorId,
    startPrimaryIndex: startTime,
    endPrimaryIndex: endTime,
    limit: 1000  // 0 = unlimited
);

// Deserialize payload on read:
foreach (var entry in data)
{
    var gps = MemoryMarshal.Read<GpsPayload>(entry.Payload);
    Console.WriteLine($"SecondaryIndex {entry.SecondaryIndex} at {entry.PrimaryIndex}: ({gps.Lat}, {gps.Long})");
}
```

**Multiple secondary indexes (optimized):**
```csharp
Dictionary<int, List<StreamEntry>> data = streamDb.ReadRange(
    secondaryIndexes: new[] { 1234, 5678, 9012 },
    startPrimaryIndex: startTime,
    endPrimaryIndex: endTime,
    limit: 1000
);
```

**All secondary indexes:**
```csharp
Dictionary<int, List<StreamEntry>> data = streamDb.ReadRange(
    startPrimaryIndex: startTime,
    endPrimaryIndex: endTime,
    limit: 1000
);
```

### Monitoring

```csharp
var stats = streamDb.GetStats();
Console.WriteLine($"Scale Up: {stats.ScaleUp}");
Console.WriteLine($"Scale Down: {stats.ScaleDown}");
Console.WriteLine($"Dropped Entries: {stats.Dropped}");
Console.WriteLine($"Adaptive Index: {stats.AdaptiveIdx}");
Console.WriteLine($"Queue Depth: {stats.PendingIdxQueueLen}");
```

## Write Path Details

### 1. Append to FasterLog
- **In-memory operation**: ~50-100μs
- **Lock-free**: Multiple threads can append concurrently
- **Per-shard**: Reduces contention

### 2. Sparse Indexing (Adaptive)
- **Every Nth write** gets indexed (N = 16 to 4096)
- **Enqueued asynchronously**: Never blocks the caller
- **Bounded queue**: Drops entries when full (graceful degradation)

### 3. Background Batching (FlushWorker)
```
1. Collect batch (adaptive size: 8-2048 entries)
2. Ensure FasterLog durability (CommitAndWait)
3. Write batch to SQLite in single transaction
4. Adjust parameters based on queue depth
```

### Adaptive Algorithm

| Queue Depth | Action | Index Frequency | Batch Size |
|-------------|--------|-----------------|------------|
| > 50% (1024) | Scale up | Decrease (↑N) | Increase |
| < 25% (512) | Scale down | Increase (↓N) | Decrease |
| 25-50% | Stable | Maintain | Maintain |

**Hysteresis:** Requires 5 batches between adjustments to prevent oscillation.

### Late Arrivals (Out-of-Order Writes)

StreamDB is optimized for monotonically increasing primary indexes, but handles occasional
out-of-order writes transparently via a **three-way routing** mechanism:

```
Normal write (pi ≥ maxPi):                      → FasterLog (lock-free, fast)
Jitter write (pi ≥ maxPi - jitterWindow):        → FasterLog (fast path, no SQLite)
Late arrival (pi < maxPi - jitterWindow):         → SQLite late_arrivals table (synchronous, correct)
Late arrival below retention floor:               → DROPPED (data already purged)
```

The **jitter window** allows writes that are slightly out of order (within a configurable
tolerance) to go directly to FasterLog instead of the SQLite side store. This is useful for
workloads where data sources deliver records with small timing variations — for example,
distributed sensors whose clocks are slightly skewed.

Configure the jitter window via the `jitterWindow` constructor parameter (default: `0`, meaning
no jitter tolerance — all out-of-order writes go to the side store):

```csharp
var db = new StreamDB(jitterWindow: 120); // tolerate 120 units of out-of-order
```

- On `Append`: tracks `maxPrimaryIndex` per secondary index. If the incoming primary index is
  lower than the max seen but within the jitter window, the entry is written to FasterLog.
  If it is below `maxPi - jitterWindow`, it is stored in the `late_arrivals` SQLite table.
- On `ReadRange`: scans extend past `endPrimaryIndex` by `jitterWindow` to ensure jitter
  entries (which may appear in the log after higher-primary-index entries) are not missed by
  early termination. Results are then filtered to the requested `[startPrimaryIndex, endPrimaryIndex]` range.
  Late arrivals from SQLite are merged as before.
- On retention: `late_arrivals` entries older than the retention cutoff are purged alongside
  the sparse index.

**Retention floor:** After each retention run, a floor is set at the retention cutoff.
Any late arrival whose primary index falls below this floor is **silently dropped** —
the FasterLog data and sparse index for that range have already been truncated,
so accepting the write would create an orphaned side-store entry with no corresponding
log data. This effectively means: **you cannot insert data older than the retention period.**
The `DroppedLateArrivals` stat tracks these drops.

**Performance impact:** Zero for the monotonic (normal) path. Jitter-absorbed writes are as
fast as normal writes (no SQLite). Late arrivals beyond the jitter window incur a synchronous
SQLite write, which is acceptable for infrequent out-of-order data. Reads with a non-zero
jitter window scan slightly further but skip non-matching entries efficiently.

## Read Path Details

### 1. SQLite Index Lookup
```sql
SELECT log_address 
FROM stream_index 
WHERE secondary_index = ? AND primary_index <= ?
ORDER BY primary_index DESC 
LIMIT 1
```
Returns the nearest indexed address ≤ startPrimaryIndex (or 0 if none exists).

### 2. FasterLog Scan
- **Start from indexed address**: Bounded scan distance
- **Filter by secondary index**: Fast path without full deserialization
- **Filter by primary index**: [startPrimaryIndex, endPrimaryIndex]
- **Stop early**: When primary index > endPrimaryIndex (safe for monotonic data)

### 3. Late Arrivals Merge
- Query the `late_arrivals` SQLite table for the same secondary index(es) and time range
- Merge results with the FasterLog scan results, sorted by primary index
- Transparent to the caller — `ReadRange` always returns the complete, correctly-ordered result

### Multi-Index Optimization
- Groups secondary indexes by shard
- Finds MIN(address) per shard across all indexes
- Single scan per shard (instead of N scans)

## Background Maintenance

### Checkpointing (Hourly)

**Purpose:** Control SQLite WAL growth

```
1. Acquire write lock (blocks index writes)
2. PRAGMA wal_checkpoint(FULL)
   - Syncs WAL to main database file
   - Blocks concurrent SQLite writers
   - Allows SQLite readers to proceed
3. Release write lock
4. Signal FlushWorker to process accumulated entries
```

**Impact:**
- Index writes: Blocked ~100-500ms
- FasterLog writes: **Unaffected**, continue normally

### Retention (Daily)

**Purpose:** Remove old data based on retention policy

```
1. DELETE FROM stream_index WHERE primary_index < cutoffPi
2. For each shard:
   - Find MIN(log_address) still referenced
   - TruncateUntil(minAddr) on FasterLog
3. Reclaim disk space
```

**Concurrency:** Does not block index writes (WAL mode allows concurrent INSERT during DELETE).

## Concurrency Model

### Locks

| Operation | Lock Type | Concurrency |
|-----------|-----------|-------------|
| Append (FasterLog) | Lock-free | Fully concurrent |
| Append (late arrival) | Read lock | Blocked during checkpoint |
| Enqueue index | Lock-free (ConcurrentQueue) | Fully concurrent |
| Index write (FlushWorker) | Read lock | Blocked during checkpoint |
| Checkpoint | Write lock | Blocks all SQLite writers |
| Retention | Maintenance lock | Exclusive with checkpoint |

### Thread Model

- **Client threads**: Any number of ASP.NET request threads
- **FlushWorker**: 1 dedicated long-running thread
- **Timers**: Threadpool threads (checkpoint, retention)

### Guarantees

✅ **Per-index monotonicity**: Primary indexes strictly increase per secondary index in FasterLog; out-of-order writes go to side store  
✅ **Non-blocking writes**: FasterLog append never waits; late arrival writes may briefly block during checkpoint (~100-500ms, hourly)  
✅ **Durability before indexing**: Log committed before SQLite insert  
✅ **No deadlocks**: Lock hierarchy enforced  
✅ **Graceful degradation**: Drops index entries when overwhelmed  

## Recovery & Consistency

### Startup Recovery

```csharp
RecoverIndex()
```

For each shard:
1. Call `FasterLog.TryRecoverLatest()` → get valid address range
2. Delete SQLite entries pointing outside [BeginAddress, TailAddress)
3. Result: Index only references durable log data

### Crash Scenarios

| Scenario | Impact | Recovery |
|----------|--------|----------|
| Crash with uncommitted FasterLog | Data lost | TryRecoverLatest excludes uncommitted |
| Crash with uncommitted SQLite | Index entries lost | Acceptable (sparse) |
| Index ahead of log | Invalid pointers | RecoverIndex removes them |
| Pending queue entries | Lost on restart | Acceptable (sparse index) |

## Performance Characteristics

### Write Performance
- **Latency**: ~50-100μs per append (in-memory)
- **Throughput**: Limited by FasterLog (hundreds of thousands/sec per shard)
- **Batching**: Amortizes SQLite overhead across entries

### Read Performance
- **Index lookup**: O(log N) where N = indexed entries per secondary index
- **Scan distance**: O(M) where M = entries between index points
- **Typical M**: 16-4096 entries (adaptive)
- **Multi-index**: Single scan per shard (efficient)

### Storage
- **FasterLog**: Append-only, 4KB page size
- **SQLite**: WITHOUT ROWID, primary key (secondary_index, primary_index); plus `late_arrivals` table for out-of-order entries
- **Index density**: 1 entry per 16-4096 writes (adaptive)

### Memory
- **FasterLog buffer**: ~16MB per shard (configurable)
- **Pending queue**: ~2048 entries × entry size
- **SQLite connections**: Small pool, reused

## Configuration

### Constructor Parameters

```csharp
public StreamDB(
    string? baseDir = null,              // Default: "streams"
    TimeSpan? retentionPeriod = null,    // Default: 60 days
    TimeSpan? checkpointInterval = null, // Default: 1 hour
    long jitterWindow = 0,              // Default: 0 (no jitter tolerance)
    ILogger<StreamDB>? logger = null
)
```

### Adaptive Tuning Parameters

Configured in code (edit `AdaptiveTuning` array):
```csharp
(int indexSpacing, int batchSize, int indexMask)[]
```

Current range: 16x to 4096x index spacing, 8 to 2048 batch size.

### Queue Parameters

```csharp
QueueCapacity = 2048;       // Maximum queue size
QueueHighWaterMark = 1024;  // 50% - trigger scale up
QueueLowWaterMark = 512;    // 25% - trigger scale down
```

## Best Practices

### ✅ Do
- Maintain per-secondary-index monotonic primary indexes when possible (optimal performance)
- Serialize writes per secondary index (upstream)
- Monitor `GetStats()` for health
- Use multi-index overload for batch queries
- Set appropriate retention periods

### ❌ Don't
- Rely on frequent out-of-order writes (side store is for occasional late arrivals, not a primary write path)
- Call `WaitForPendingWrites()` in production (testing only)
- Set very short checkpoint intervals (<10 minutes)
- Use small shard counts on high write volumes
- Expect every write to be indexed (it's sparse)

## Troubleshooting

### High Dropped Index Entries

**Symptom:** `stats.Dropped` increasing rapidly

**Causes:**
- SQLite writes too slow
- Insufficient batching
- I/O bottleneck

**Solutions:**
1. Check disk I/O performance
2. Increase checkpoint interval (reduce WAL overhead)
3. Reduce retention scan frequency
4. Consider faster storage (NVMe)

### Slow Read Queries

**Symptom:** ReadRange takes multiple seconds

**Causes:**
- Sparse index forcing long scans
- Index entries dropped during high load
- Reading very old data (near truncation point)

**Solutions:**
1. Monitor adaptive index level (`stats.AdaptiveIdx`)
2. Increase queue capacity to reduce drops
3. Optimize SQLite (VACUUM, ANALYZE)
4. Consider shorter retention periods

### High Memory Usage

**Symptom:** StreamDB using excessive memory

**Causes:**
- Large pending queue
- FasterLog page buffers
- Too many concurrent reads

**Solutions:**
1. Check `stats.PendingIdxQueueLen`
2. Reduce FasterLog `MemorySizeBits` (edit code)
3. Implement read throttling upstream

## Type Requirements

### Secondary Index
- Type: `int`
- Represents any logical grouping key (device ID, sensor ID, user ID, region code, etc.)
- Used for shard assignment via `secondaryIndex & ShardMask`

### T (Payload)
- Must be `unmanaged` (value type, no references)
- Fixed size
- StreamDB stores it as opaque bytes — you define and deserialize it

Example:
```csharp
[StructLayout(LayoutKind.Sequential)]
public struct SensorReading
{
    public float Temperature;
    public float Humidity;
    public float Latitude;
    public float Longitude;
}
```

## Testing

### Wait for Pending Writes
```csharp
streamDb.WaitForPendingWrites();  // Testing only!
```

Blocks until all pending index entries are written to SQLite. **Do not use in production.**

### Trigger Maintenance
```csharp
streamDb.RunCheckpoint();  // Force checkpoint
streamDb.RunRetention();   // Force retention cleanup
```

Internal methods exposed for testing. Normally run on timers.

## Disposal

```csharp
streamDb.Dispose();
```

Cleanup sequence:
1. Stop timers
2. Signal FlushWorker to exit
3. Wait up to 2 seconds for graceful shutdown
4. Dispose shards (commits FasterLog)
5. Dispose SQLite connection pool

**Note:** Pending index entries are **not** flushed on dispose (acceptable for sparse index).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.
