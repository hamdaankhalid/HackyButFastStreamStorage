# StreamDB

High-performance sharded time-series storage for device telemetry with sparse indexing and adaptive batching.

## Overview

StreamDB is a specialized storage engine designed for high-throughput device telemetry ingestion with efficient time-range queries. It combines FasterLog's append-only log with SQLite sparse indexing to achieve:

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
       │ Append(deviceId, item, timestamp)
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
│              │ (device, ts) →  │                        │
│              │   log_address   │                        │
│              └─────────────────┘                        │
└─────────────────────────────────────────────────────────┘
```

### Sharding

Records are distributed across 4 FasterLog shards using: `secondaryIndex & 0x3`

This reduces file handles and enables parallel I/O while maintaining per-device write ordering.

## Record Format

Each record stored in FasterLog has a 16-byte header followed by a variable-length payload:

```
┌──────────────┬────────────────┬──────────────┬─────────────────┬──────────────────┐
│ 8B: long     │ 4B: int        │ 2B: ushort   │ 2B: ushort      │ N bytes: payload │
│ timestamp    │ secondary_idx  │ version      │ payload length  │ (opaque bytes)   │
│ PRIMARY IDX  │ SECONDARY IDX  │              │                 │                  │
└──────────────┴────────────────┴──────────────┴─────────────────┴──────────────────┘
```

- **Timestamp** (primary index): Used for range queries and ordering
- **Secondary Index**: Used for sharding and filtering (e.g., device ID)
- **Version**: Schema version for payload format evolution
- **Payload**: Opaque bytes — StreamDB never interprets these

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
    secondaryIndex: deviceId,
    payload: MemoryMarshal.AsBytes(new ReadOnlySpan<GpsPayload>(in payload)),
    timestamp: DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
    version: StreamVersions.GpsV1
);
```

**Constraints:**
- Timestamps must be monotonically increasing per secondary index
- Writes are serialized per secondary index (upstream responsibility)

### Reading Data

**Single device:**
```csharp
List<StreamEntry> data = streamDb.ReadRange(
    secondaryIndex: deviceId,
    startTs: startTime,
    endTs: endTime,
    limit: 1000  // 0 = unlimited
);

// Deserialize payload on read:
foreach (var entry in data)
{
    var gps = MemoryMarshal.Read<GpsPayload>(entry.Payload);
    Console.WriteLine($"Device {entry.SecondaryIndex} at {entry.Timestamp}: ({gps.Lat}, {gps.Long})");
}
```

**Multiple devices (optimized):**
```csharp
Dictionary<int, List<StreamEntry>> data = streamDb.ReadRange(
    secondaryIndexes: new[] { 1234, 5678, 9012 },
    startTs: startTime,
    endTs: endTime,
    limit: 1000
);
```

**All devices:**
```csharp
Dictionary<int, List<StreamEntry>> data = streamDb.ReadRange(
    startTs: startTime,
    endTs: endTime,
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

## Read Path Details

### 1. SQLite Index Lookup
```sql
SELECT log_address 
FROM stream_index 
WHERE device_id = ? AND timestamp <= ?
ORDER BY timestamp DESC 
LIMIT 1
```
Returns the nearest indexed address ≤ startTs (or 0 if none exists).

### 2. FasterLog Scan
- **Start from indexed address**: Bounded scan distance
- **Filter by deviceId**: Fast path without full deserialization
- **Filter by timestamp**: [startTs, endTs]
- **Stop early**: When timestamp > endTs (safe due to monotonicity)

### Multi-Device Optimization
- Groups devices by shard
- Finds MIN(address) per shard across all devices
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
1. DELETE FROM stream_index WHERE timestamp < cutoffTs
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
| Enqueue index | Lock-free (ConcurrentQueue) | Fully concurrent |
| Index write (FlushWorker) | Read lock | Allows concurrent writes |
| Checkpoint | Write lock | Blocks index writes only |
| Retention | Maintenance lock | Exclusive with checkpoint |

### Thread Model

- **Client threads**: Any number of ASP.NET request threads
- **FlushWorker**: 1 dedicated long-running thread
- **Timers**: Threadpool threads (checkpoint, retention)

### Guarantees

✅ **Per-device monotonicity**: Timestamps strictly increase  
✅ **Non-blocking writes**: FasterLog append never waits  
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
- **Index lookup**: O(log N) where N = indexed entries per device
- **Scan distance**: O(M) where M = entries between index points
- **Typical M**: 16-4096 entries (adaptive)
- **Multi-device**: Single scan per shard (efficient)

### Storage
- **FasterLog**: Append-only, 4KB page size
- **SQLite**: WITHOUT ROWID, primary key (device_id, timestamp)
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
- Maintain per-device monotonic timestamps
- Serialize writes per device (upstream)
- Monitor `GetStats()` for health
- Use multi-device overload for batch queries
- Set appropriate retention periods

### ❌ Don't
- Write out-of-order timestamps for same device
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

### TDeviceId
- Must be `unmanaged`
- Supported: `ushort`, `uint`
- Located at offset 0 in telemetry struct

### T (Telemetry)
- Must be `unmanaged` (value type, no references)
- Fixed size
- Contains device ID at offset 0

Example:
```csharp
[StructLayout(LayoutKind.Sequential)]
public struct TelemetryData
{
    public ushort DeviceId;  // Must be first field
    public long Timestamp;
    public float Latitude;
    public float Longitude;
    // ... other fields
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

Part of the HopShip project.
