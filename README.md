# StreamDB

A high-throughput, schema-on-read stream storage engine for time-series data. Built on [Microsoft FASTER](https://github.com/microsoft/FASTER) and SQLite.

## What is StreamDB?

StreamDB is purpose-built for workloads where data arrives fast and needs to be queryable by time range — think IoT telemetry, sensor data, event logs, or any append-heavy stream. It pairs FASTER's append-only log for raw throughput with a sparse SQLite index for efficient lookups, giving you the best of both worlds: lock-free writes and bounded-scan reads.

### Key Properties

- **Non-blocking writes** — appends go to an in-memory FasterLog buffer; no fsync in the hot path
- **Adaptive sparse indexing** — automatically tunes index density based on write pressure (16x–4096x)
- **Schema-on-read** — records carry a version tag; callers deserialize at read time
- **Sharded storage** — distributes data across multiple FasterLog instances to reduce contention
- **Automatic retention** — configurable data lifecycle with background cleanup
- **Crash recovery** — reconciles the sparse index against durable log state on startup

## Quick Start

```csharp
// Create a StreamDB instance
var db = new StreamDB(
    baseDir: "my-streams",
    retentionPeriod: TimeSpan.FromDays(30),
    logger: logger
);

// Write (non-blocking, lock-free)
db.Append(secondaryIndex: deviceId, payload: bytes, timestamp: ts, version: 1);

// Read a time range for one device
List<StreamEntry> entries = db.ReadRange(deviceId, startTs, endTs);

// Read across multiple devices (single scan per shard)
Dictionary<int, List<StreamEntry>> multi = db.ReadRange(
    new[] { device1, device2, device3 }, startTs, endTs
);

// Monitor health
StreamDbStats stats = db.GetStats();
```

## Project Structure

| File | Description |
|------|-------------|
| `StreamDB.cs` | Core storage engine — sharding, write path, read path, maintenance |
| `StreamEntry.cs` | Record header layout and entry struct |
| `StreamVersionRegistry.cs` | Caller-side payload versioning and deserialization |
| `PooledConnection.cs` | Lightweight SQLite connection pool |

## Documentation

See the [`docs/`](docs/) directory for detailed technical documentation:

- [**Architecture & Internals**](docs/architecture.md) — write/read paths, adaptive algorithm, concurrency model, recovery, performance characteristics, configuration, and troubleshooting

## Dependencies

- [Microsoft.FASTER.Core](https://www.nuget.org/packages/Microsoft.FASTER.Core) — append-only log engine
- [Microsoft.Data.Sqlite](https://www.nuget.org/packages/Microsoft.Data.Sqlite) — sparse index storage

## License

[MIT](LICENSE)

