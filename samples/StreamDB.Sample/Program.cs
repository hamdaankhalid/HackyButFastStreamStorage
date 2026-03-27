// Copyright 2025 Hamdaan Khalid
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using StreamDB;

// ── Setup ──────────────────────────────────────────────────────────────────────

using ILoggerFactory factory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Information));
ILogger<StreamDB.StreamDB> logger = factory.CreateLogger<StreamDB.StreamDB>();

string dataDir = Path.Combine(Path.GetTempPath(), "streamdb-sample");
if (Directory.Exists(dataDir)) Directory.Delete(dataDir, recursive: true);

using var db = new StreamDB.StreamDB(
    baseDir: dataDir,
    retentionPeriod: TimeSpan.FromDays(7),
    jitterWindow: 120, // tolerate 120 units of out-of-order primary index values
    logger: logger
);

Console.WriteLine("StreamDB sample");
Console.WriteLine($"Data directory: {dataDir}");
Console.WriteLine();

// ── Core Concepts ──────────────────────────────────────────────────────────────
//
// StreamDB uses two index concepts:
//
//   PRIMARY INDEX   = A monotonic long value used for range queries and ordering.
//                     Commonly a Unix timestamp, but can be any monotonically
//                     increasing value (sequence number, score, etc.).
//
//   SECONDARY INDEX = An integer grouping key — used for sharding and filtering.
//                     This can represent anything: a device ID, sensor ID, user ID,
//                     region code, etc. You choose what the secondary index means
//                     for your domain.
//
// In this sample, we use sensor IDs (1–4) as the secondary index and Unix
// timestamps as the primary index.

// ── Define a payload struct ────────────────────────────────────────────────────

// StreamDB stores opaque byte payloads with a version tag.
// You define the struct; StreamVersionRegistry handles deserialization.
var registry = new StreamVersionRegistry();
registry.Register<SensorReading>(version: 1);

// ── Write some data ────────────────────────────────────────────────────────────
// Each Append call takes:
//   secondaryIndex — the grouping key (here: sensor ID)
//   payload        — raw bytes of your struct
//   primaryIndex   — the primary index (here: Unix seconds)
//   version        — schema version for payload evolution

const int sensorCount = 4;
const int pointsPerSensor = 50;
long basePi = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 3600; // 1 hour ago

Console.WriteLine($"Writing {sensorCount * pointsPerSensor} records across {sensorCount} sensors...");
Console.WriteLine($"  Primary index:   Unix seconds (monotonic)");
Console.WriteLine($"  Secondary index: sensor ID (1–{sensorCount})");
Console.WriteLine();

for (int sensorId = 1; sensorId <= sensorCount; sensorId++)
{
    for (int i = 0; i < pointsPerSensor; i++)
    {
        var reading = new SensorReading
        {
            Temperature = 20.0f + sensorId + (i * 0.1f),
            Humidity = 45.0f + (i * 0.5f),
        };

        long pi = basePi + (i * 60); // one reading per minute
        ReadOnlySpan<byte> payload = MemoryMarshal.AsBytes(new ReadOnlySpan<SensorReading>(in reading));
        db.Append(primaryIndex: pi, secondaryIndex: sensorId, version: 1, payload: payload);
    }
}

// Ensure all index entries are flushed for deterministic reads in this demo
db.WaitForPendingWrites();
Console.WriteLine("Done writing.\n");

// ── Late arrivals (out-of-order writes) ────────────────────────────────────────
// StreamDB handles occasional out-of-order primary index values transparently.
// Late arrivals go to a SQLite side store and are merged into reads automatically.

Console.WriteLine("── Late arrivals demo ──");
Console.WriteLine("Writing 3 out-of-order entries for sensor 1:");
Console.WriteLine("  - 2 within jitter window (absorbed by FasterLog)");
Console.WriteLine("  - 1 beyond jitter window (routed to SQLite side store)");

// Within jitter window (basePi + 5*60 and basePi + 6*60 are within 120 of the max seen)
for (int i = 0; i < 2; i++)
{
    var lateReading = new SensorReading
    {
        Temperature = 99.0f + i,
        Humidity = 99.0f,
    };
    // These are slightly behind the max — within the jitter window of 120
    long latePi = basePi + (pointsPerSensor - 1) * 60 - 60 - (i * 30);
    ReadOnlySpan<byte> latePayload = MemoryMarshal.AsBytes(new ReadOnlySpan<SensorReading>(in lateReading));
    db.Append(primaryIndex: latePi, secondaryIndex: 1, version: 1, payload: latePayload);
}

// Beyond jitter window (far in the past)
{
    var lateReading = new SensorReading
    {
        Temperature = 99.9f,
        Humidity = 99.0f,
    };
    long latePi = basePi + (5 * 60); // far behind the max — beyond jitter window
    ReadOnlySpan<byte> latePayload = MemoryMarshal.AsBytes(new ReadOnlySpan<SensorReading>(in lateReading));
    db.Append(primaryIndex: latePi, secondaryIndex: 1, version: 1, payload: latePayload);
}
Console.WriteLine("Late arrivals written.\n");

// ── Read a time range for one secondary index ──────────────────────────────────
// Query by primary index range filtered to a single secondary index.
// This range includes both normal entries AND the late arrivals we just wrote.

long queryStartPi = basePi + (5 * 60); // start from minute 5 (where late arrivals begin)
long queryEndPi = basePi + (20 * 60);   // through minute 20

Console.WriteLine($"── Single-index read (sensor 1, minutes 5–20) ──");
Console.WriteLine($"(includes 3 late arrivals merged in primary index order)");
List<StreamEntry> entries = db.ReadRange(secondaryIndex: 1, startPrimaryIndex: queryStartPi, endPrimaryIndex: queryEndPi);
Console.WriteLine($"Got {entries.Count} entries:");
foreach (StreamEntry e in entries)
{
    SensorReading r = registry.Deserialize<SensorReading>(e);
    Console.WriteLine($"  pi={e.PrimaryIndex} (primary)  sensor={e.SecondaryIndex} (secondary)  temp={r.Temperature:F1}°C  humidity={r.Humidity:F1}%");
}
Console.WriteLine();

// ── Read across multiple secondary indexes ─────────────────────────────────────
// Query the same time range across several secondary indexes in one optimized scan.

Console.WriteLine($"── Multi-index read (sensors 1-3, same window) ──");
Dictionary<int, List<StreamEntry>> multi = db.ReadRange(
    secondaryIndexes: new[] { 1, 2, 3 },
    startPrimaryIndex: queryStartPi,
    endPrimaryIndex: queryEndPi
);
foreach ((int secondaryIndex, List<StreamEntry> sensorEntries) in multi)
{
    Console.WriteLine($"  Secondary index {secondaryIndex}: {sensorEntries.Count} entries");
}
Console.WriteLine();

// ── Stats ──────────────────────────────────────────────────────────────────────

StreamDbStats stats = db.GetStats();
Console.WriteLine($"── Stats ──");
Console.WriteLine($"  Scale up:         {stats.ScaleUp}");
Console.WriteLine($"  Scale down:       {stats.ScaleDown}");
Console.WriteLine($"  Dropped:          {stats.Dropped}");
Console.WriteLine($"  Adaptive:         {stats.AdaptiveIdx}");
Console.WriteLine($"  Pending:          {stats.PendingIdxQueueLen}");
Console.WriteLine($"  Late arrivals:    {stats.LateArrivals}");
Console.WriteLine($"  Jitter absorbed:  {stats.JitterAbsorbed}");
Console.WriteLine($"  Dropped (stale):  {stats.DroppedLateArrivals}");
Console.WriteLine();

// ── Cleanup ────────────────────────────────────────────────────────────────────
Console.WriteLine("Disposing...");
db.Dispose();
Directory.Delete(dataDir, recursive: true);
Console.WriteLine("Done.");

// ── Payload type ───────────────────────────────────────────────────────────────

[StructLayout(LayoutKind.Sequential)]
struct SensorReading
{
    public float Temperature;
    public float Humidity;
}
