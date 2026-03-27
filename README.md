# StreamDB

A schema-on-read embedded stream storage engine for time-series data. Built on [Microsoft FASTER](https://github.com/microsoft/FASTER) and SQLite.

## What is StreamDB?

StreamDB is purpose-built for workloads where data arrives fast and needs to be queryable by time range — think IoT telemetry, sensor data, event logs, or any append-heavy stream. It pairs FASTER's append-only log for raw throughput with a sparse SQLite index for efficient lookups, giving you the best of both worlds: lock-free writes and bounded-scan reads.

### Data Model: Primary & Secondary Indexes

Every record in StreamDB is organized around two index concepts:

| Concept | Type | Purpose |
|---------|------|---------|
| **Primary Index** | `long` (monotonic key) | Range queries and ordering. Every record carries a primary index — commonly a Unix timestamp, but can be any monotonically increasing value (sequence number, score, etc.). |
| **Secondary Index** | `int` (grouping key) | Sharding and filtering. Represents any logical grouping — device ID, sensor ID, user ID, region, etc. |

The primary index determines **when** (or **where in sequence**) something happened. The secondary index determines **who/what** produced it. Together they enable efficient queries like "give me all readings from sensor 42 between primary index 1000 and 2000."

### Key Properties

- **Non-blocking writes** — appends go to an in-memory FasterLog buffer; no fsync in the hot path
- **Max payload size: 65,535 bytes (≈64 KB)** — payload length is stored as a `ushort` in the record header
- **Adaptive sparse indexing** — automatically tunes index density based on write pressure (16x–4096x)
- **Schema-on-read** — records carry a version tag; callers deserialize at read time
- **Sharded storage** — distributes data across multiple FasterLog instances to reduce contention
- **Automatic retention** — configurable data lifecycle with background cleanup
- **Crash recovery** — reconciles the sparse index against durable log state on startup

> **Note:** For best performance the primary index should be monotonically increasing per secondary index.
> Out-of-order writes are handled transparently but incur a small performance penalty (synchronous SQLite write).

## Quick Start

```csharp
using StreamDB;

// Create a StreamDB instance
var db = new StreamDB.StreamDB(
    baseDir: "my-streams",
    retentionPeriod: TimeSpan.FromDays(30),
    jitterWindow: 120, // tolerate 120 units of out-of-order primary index values
    logger: logger
);

// Write — primaryIndex is the primary index, secondaryIndex is the grouping key
db.Append(primaryIndex: pi, secondaryIndex: sensorId, version: 1, payload: bytes);

// Read a range for one secondary index
List<StreamEntry> entries = db.ReadRange(secondaryIndex: sensorId, startPrimaryIndex: from, endPrimaryIndex: to);

// Read across multiple secondary indexes (single scan per shard)
Dictionary<int, List<StreamEntry>> multi = db.ReadRange(
    secondaryIndexes: new[] { sensor1, sensor2, sensor3 }, startPrimaryIndex: from, endPrimaryIndex: to
);

// Monitor health
StreamDbStats stats = db.GetStats();
```

## Project Structure

```
├── src/StreamDB/              # Core library
├── samples/StreamDB.Sample/   # Console demo
├── tests/StreamDB.Tests/      # NUnit test suite
├── benchmarks/StreamDB.Benchmarks/ # BenchmarkDotNet vs SQLite & RocksDB
├── docs/architecture.md       # Technical documentation
└── LICENSE                    # Apache 2.0
```

### Running the Sample

```bash
dotnet run --project samples/StreamDB.Sample
```

## Documentation

See the [`docs/`](docs/) directory for detailed technical documentation:

- [**Architecture & Internals**](docs/architecture.md) — write/read paths, adaptive algorithm, concurrency model, recovery, performance characteristics, configuration, and troubleshooting

## Dependencies

- [Microsoft.FASTER.Core](https://www.nuget.org/packages/Microsoft.FASTER.Core) — append-only log engine
- [Microsoft.Data.Sqlite](https://www.nuget.org/packages/Microsoft.Data.Sqlite) — sparse index storage

## License

[Apache 2.0](LICENSE)

