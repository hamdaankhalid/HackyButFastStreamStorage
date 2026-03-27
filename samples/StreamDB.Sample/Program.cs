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
    logger: logger
);

Console.WriteLine("StreamDB sample");
Console.WriteLine($"Data directory: {dataDir}");
Console.WriteLine();

// ── Define a payload struct ────────────────────────────────────────────────────

// StreamDB stores opaque byte payloads with a version tag.
// You define the struct; StreamVersionRegistry handles deserialization.
var registry = new StreamVersionRegistry();
registry.Register<SensorReading>(version: 1);

// ── Write some data ────────────────────────────────────────────────────────────

const int sensorCount = 4;
const int pointsPerSensor = 50;
long baseTs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 3600; // 1 hour ago

Console.WriteLine($"Writing {sensorCount * pointsPerSensor} records across {sensorCount} sensors...");

for (int sensor = 1; sensor <= sensorCount; sensor++)
{
    for (int i = 0; i < pointsPerSensor; i++)
    {
        var reading = new SensorReading
        {
            Temperature = 20.0f + sensor + (i * 0.1f),
            Humidity = 45.0f + (i * 0.5f),
        };

        long ts = baseTs + (i * 60); // one reading per minute
        ReadOnlySpan<byte> payload = MemoryMarshal.AsBytes(new ReadOnlySpan<SensorReading>(in reading));
        db.Append(secondaryIndex: sensor, payload: payload, timestamp: ts, version: 1);
    }
}

// Ensure all index entries are flushed for deterministic reads in this demo
db.WaitForPendingWrites();
Console.WriteLine("Done writing.\n");

// ── Read a range for one sensor ────────────────────────────────────────────────

long queryStart = baseTs + (10 * 60); // skip first 10 minutes
long queryEnd = baseTs + (20 * 60);   // 10-minute window

Console.WriteLine($"── Single-sensor read (sensor 1, 10 min window) ──");
List<StreamEntry> entries = db.ReadRange(secondaryIndex: 1, startTs: queryStart, endTs: queryEnd);
Console.WriteLine($"Got {entries.Count} entries:");
foreach (StreamEntry e in entries)
{
    SensorReading r = registry.Deserialize<SensorReading>(e);
    Console.WriteLine($"  ts={e.Timestamp}  temp={r.Temperature:F1}°C  humidity={r.Humidity:F1}%");
}
Console.WriteLine();

// ── Multi-sensor read ──────────────────────────────────────────────────────────

Console.WriteLine($"── Multi-sensor read (sensors 1-3, same window) ──");
Dictionary<int, List<StreamEntry>> multi = db.ReadRange(
    secondaryIndexes: new[] { 1, 2, 3 },
    startTs: queryStart,
    endTs: queryEnd
);
foreach ((int sensor, List<StreamEntry> sensorEntries) in multi)
{
    Console.WriteLine($"  Sensor {sensor}: {sensorEntries.Count} entries");
}
Console.WriteLine();

// ── Stats ──────────────────────────────────────────────────────────────────────

StreamDbStats stats = db.GetStats();
Console.WriteLine($"── Stats ──");
Console.WriteLine($"  Scale up:   {stats.ScaleUp}");
Console.WriteLine($"  Scale down: {stats.ScaleDown}");
Console.WriteLine($"  Dropped:    {stats.Dropped}");
Console.WriteLine($"  Adaptive:   {stats.AdaptiveIdx}");
Console.WriteLine($"  Pending:    {stats.PendingIdxQueueLen}");
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
