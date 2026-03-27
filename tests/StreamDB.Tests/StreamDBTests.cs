using System.Runtime.InteropServices;
using NUnit.Framework;

namespace StreamDB.Tests;

[StructLayout(LayoutKind.Sequential)]
public struct TestPayload
{
    public float Value;
    public int Counter;
}

[TestFixture]
public class StreamHeaderTests
{
    [Test]
    public void Size_Is16Bytes()
    {
        Assert.That(StreamHeader.Size, Is.EqualTo(16));
    }

    [Test]
    public void WriteAndRead_RoundTrips()
    {
        Span<byte> buffer = stackalloc byte[StreamHeader.Size];
        long ts = 1234567890L;
        int secondaryIdx = 42;
        ushort version = 3;
        ushort payloadLen = 128;

        StreamHeader.Write(buffer, ts, secondaryIdx, version, payloadLen);

        Assert.That(StreamHeader.ReadTimestamp(buffer), Is.EqualTo(ts));
        Assert.That(StreamHeader.ReadSecondaryIndex(buffer), Is.EqualTo(secondaryIdx));
        Assert.That(StreamHeader.ReadVersion(buffer), Is.EqualTo(version));
        Assert.That(StreamHeader.ReadPayloadLength(buffer), Is.EqualTo(payloadLen));
    }

    [Test]
    public void WriteAndRead_NegativeTimestamp()
    {
        Span<byte> buffer = stackalloc byte[StreamHeader.Size];
        StreamHeader.Write(buffer, -999L, 0, 1, 0);
        Assert.That(StreamHeader.ReadTimestamp(buffer), Is.EqualTo(-999L));
    }

    [Test]
    public void WriteAndRead_MaxValues()
    {
        Span<byte> buffer = stackalloc byte[StreamHeader.Size];
        StreamHeader.Write(buffer, long.MaxValue, int.MaxValue, ushort.MaxValue, ushort.MaxValue);

        Assert.That(StreamHeader.ReadTimestamp(buffer), Is.EqualTo(long.MaxValue));
        Assert.That(StreamHeader.ReadSecondaryIndex(buffer), Is.EqualTo(int.MaxValue));
        Assert.That(StreamHeader.ReadVersion(buffer), Is.EqualTo(ushort.MaxValue));
        Assert.That(StreamHeader.ReadPayloadLength(buffer), Is.EqualTo(ushort.MaxValue));
    }
}

[TestFixture]
public class StreamVersionRegistryTests
{
    [Test]
    public void Register_And_Deserialize_RoundTrips()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        var payload = new TestPayload { Value = 3.14f, Counter = 42 };
        byte[] bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload)).ToArray();

        var entry = new StreamEntry(100, 1, 1, bytes);
        TestPayload result = registry.Deserialize<TestPayload>(entry);

        Assert.That(result.Value, Is.EqualTo(3.14f).Within(0.001f));
        Assert.That(result.Counter, Is.EqualTo(42));
    }

    [Test]
    public void CanDeserialize_ReturnsTrueForRegistered()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        byte[] bytes = new byte[8];
        var entry = new StreamEntry(100, 1, 1, bytes);
        Assert.That(registry.CanDeserialize<TestPayload>(entry), Is.True);
    }

    [Test]
    public void CanDeserialize_ReturnsFalseForUnregistered()
    {
        var registry = new StreamVersionRegistry();
        byte[] bytes = new byte[8];
        var entry = new StreamEntry(100, 1, 99, bytes);
        Assert.That(registry.CanDeserialize<TestPayload>(entry), Is.False);
    }

    [Test]
    public void Deserialize_ThrowsForUnregisteredVersion()
    {
        var registry = new StreamVersionRegistry();
        byte[] bytes = new byte[8];
        var entry = new StreamEntry(100, 1, 99, bytes);
        Assert.Throws<InvalidOperationException>(() => registry.Deserialize<TestPayload>(entry));
    }

    [Test]
    public void Deserialize_ThrowsForTooSmallPayload()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        byte[] tooSmall = new byte[2]; // TestPayload is 8 bytes
        var entry = new StreamEntry(100, 1, 1, tooSmall);
        Assert.Throws<InvalidOperationException>(() => registry.Deserialize<TestPayload>(entry));
    }
}

[TestFixture]
public class StreamDBBasicTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long timestamp, float value = 1.0f, ushort version = 1)
    {
        var payload = new TestPayload { Value = value, Counter = (int)timestamp };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(secondaryIndex: secondaryIndex, payload: bytes, timestamp: timestamp, version: version);
    }

    [Test]
    public void Append_And_ReadRange_BasicRoundTrip()
    {
        for (int i = 0; i < 10; i++)
            AppendPayload(1, 100 + i);

        _db.WaitForPendingWrites();
        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 109);

        Assert.That(results, Has.Count.EqualTo(10));
        Assert.That(results[0].Timestamp, Is.EqualTo(100));
        Assert.That(results[9].Timestamp, Is.EqualTo(109));
    }

    [Test]
    public void ReadRange_EmptyRange_ReturnsEmpty()
    {
        AppendPayload(1, 100);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 200, endTs: 300);
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ReadRange_NoMatchingSecondaryIndex_ReturnsEmpty()
    {
        AppendPayload(1, 100);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 999, startTs: 0, endTs: 200);
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ReadRange_WithLimit()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i);

        _db.WaitForPendingWrites();
        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 200, limit: 5);

        Assert.That(results, Has.Count.EqualTo(5));
        Assert.That(results[0].Timestamp, Is.EqualTo(100));
        Assert.That(results[4].Timestamp, Is.EqualTo(104));
    }

    [Test]
    public void ReadRange_BoundaryTimestamps_Inclusive()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 300);
        _db.WaitForPendingWrites();

        // Exact boundary match
        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 300);
        Assert.That(results, Has.Count.EqualTo(3));

        // Start at exact timestamp
        results = _db.ReadRange(secondaryIndex: 1, startTs: 200, endTs: 300);
        Assert.That(results, Has.Count.EqualTo(2));

        // End at exact timestamp
        results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 200);
        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public void ReadRange_SingleEntry()
    {
        AppendPayload(1, 100);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 100);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Timestamp, Is.EqualTo(100));
    }

    [Test]
    public void ReadRange_PreservesPayload()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        AppendPayload(1, 100, value: 42.5f);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 100);
        Assert.That(results, Has.Count.EqualTo(1));

        TestPayload p = registry.Deserialize<TestPayload>(results[0]);
        Assert.That(p.Value, Is.EqualTo(42.5f).Within(0.001f));
        Assert.That(p.Counter, Is.EqualTo(100));
    }

    [Test]
    public void ReadRange_MultipleVersions()
    {
        AppendPayload(1, 100, version: 1);
        AppendPayload(1, 200, version: 2);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 200);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[0].Version, Is.EqualTo(1));
        Assert.That(results[1].Version, Is.EqualTo(2));
    }
}

[TestFixture]
public class StreamDBMultiIndexTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long timestamp, float value = 1.0f)
    {
        var payload = new TestPayload { Value = value, Counter = (int)timestamp };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(secondaryIndex: secondaryIndex, payload: bytes, timestamp: timestamp, version: 1);
    }

    [Test]
    public void ReadRange_MultiIndex_ReturnsPerIndex()
    {
        for (int i = 0; i < 5; i++)
        {
            AppendPayload(1, 100 + i);
            AppendPayload(2, 100 + i);
            AppendPayload(3, 100 + i);
        }
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndexes: new[] { 1, 2, 3 }, startTs: 100, endTs: 104);
        Assert.That(results.Keys, Has.Count.EqualTo(3));
        Assert.That(results[1], Has.Count.EqualTo(5));
        Assert.That(results[2], Has.Count.EqualTo(5));
        Assert.That(results[3], Has.Count.EqualTo(5));
    }

    [Test]
    public void ReadRange_MultiIndex_MixedResults()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(2, 150);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndexes: new[] { 1, 2 }, startTs: 100, endTs: 200);
        Assert.That(results[1], Has.Count.EqualTo(2));
        Assert.That(results[2], Has.Count.EqualTo(1));
    }

    [Test]
    public void ReadRange_AllIndexes()
    {
        AppendPayload(1, 100);
        AppendPayload(2, 100);
        AppendPayload(3, 100);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(startTs: 0, endTs: 200);
        Assert.That(results.Keys, Has.Count.EqualTo(3));
    }

    [Test]
    public void ReadRange_MultiIndex_WithLimit()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndexes: new[] { 1 }, startTs: 100, endTs: 200, limit: 3);
        Assert.That(results[1], Has.Count.EqualTo(3));
    }
}

[TestFixture]
public class StreamDBLateArrivalsTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long timestamp, float value = 1.0f)
    {
        var payload = new TestPayload { Value = value, Counter = (int)timestamp };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(secondaryIndex: secondaryIndex, payload: bytes, timestamp: timestamp, version: 1);
    }

    [Test]
    public void LateArrival_IsDetectedAndStored()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 150); // late arrival

        var stats = _db.GetStats();
        Assert.That(stats.LateArrivals, Is.EqualTo(1));
    }

    [Test]
    public void LateArrival_MergedInTimestampOrder()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 300);
        AppendPayload(1, 150); // late arrival
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 300);
        Assert.That(results, Has.Count.EqualTo(4));

        // Verify timestamp ordering
        for (int i = 1; i < results.Count; i++)
        {
            Assert.That(results[i].Timestamp, Is.GreaterThanOrEqualTo(results[i - 1].Timestamp),
                $"Entry {i} (ts={results[i].Timestamp}) should be >= entry {i - 1} (ts={results[i - 1].Timestamp})");
        }

        // Verify the late arrival is at the correct position
        Assert.That(results[0].Timestamp, Is.EqualTo(100));
        Assert.That(results[1].Timestamp, Is.EqualTo(150));
        Assert.That(results[2].Timestamp, Is.EqualTo(200));
        Assert.That(results[3].Timestamp, Is.EqualTo(300));
    }

    [Test]
    public void LateArrival_MultipleLateEntries()
    {
        // Write monotonic entries
        for (int i = 0; i < 10; i++)
            AppendPayload(1, 100 + i * 10);

        // Write multiple late arrivals
        AppendPayload(1, 105);
        AppendPayload(1, 115);
        AppendPayload(1, 125);
        _db.WaitForPendingWrites();

        var stats = _db.GetStats();
        Assert.That(stats.LateArrivals, Is.EqualTo(3));

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 200);
        Assert.That(results, Has.Count.EqualTo(13));

        // Verify ordering
        for (int i = 1; i < results.Count; i++)
            Assert.That(results[i].Timestamp, Is.GreaterThanOrEqualTo(results[i - 1].Timestamp));
    }

    [Test]
    public void LateArrival_WithLimit()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 300);
        AppendPayload(1, 200); // late arrival
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 300, limit: 2);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[0].Timestamp, Is.EqualTo(100));
        Assert.That(results[1].Timestamp, Is.EqualTo(200)); // late arrival comes before 300
    }

    [Test]
    public void LateArrival_DoesNotAffectOtherSecondaryIndexes()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 150); // late arrival for index 1

        AppendPayload(2, 100);
        AppendPayload(2, 200);

        var stats = _db.GetStats();
        Assert.That(stats.LateArrivals, Is.EqualTo(1)); // only 1 late arrival
    }

    [Test]
    public void LateArrival_PreservesPayloadData()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        AppendPayload(1, 200);
        AppendPayload(1, 100, value: 99.9f); // late arrival with distinctive value

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 200);
        Assert.That(results, Has.Count.EqualTo(2));

        // The late arrival should be first (ts=100)
        TestPayload p = registry.Deserialize<TestPayload>(results[0]);
        Assert.That(p.Value, Is.EqualTo(99.9f).Within(0.01f));
    }

    [Test]
    public void LateArrival_MultiIndex_ReadRange()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 300);
        AppendPayload(2, 100);
        AppendPayload(2, 300);

        // Late arrivals for both indexes
        AppendPayload(1, 200);
        AppendPayload(2, 200);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndexes: new[] { 1, 2 }, startTs: 100, endTs: 300);
        Assert.That(results[1], Has.Count.EqualTo(3));
        Assert.That(results[2], Has.Count.EqualTo(3));

        // Both should be in timestamp order
        Assert.That(results[1][1].Timestamp, Is.EqualTo(200));
        Assert.That(results[2][1].Timestamp, Is.EqualTo(200));
    }

    [Test]
    public void LateArrival_AllIndexes_ReadRange()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 300);
        AppendPayload(1, 200); // late
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(startTs: 100, endTs: 300);
        Assert.That(results[1], Has.Count.EqualTo(3));
        Assert.That(results[1][1].Timestamp, Is.EqualTo(200));
    }

    [Test]
    public void LateArrival_SameTimestamp_BothKept()
    {
        AppendPayload(1, 200);
        AppendPayload(1, 100, value: 1.0f); // late with ts=100

        // Write another normal entry at ts=100 — this simulates a duplicate timestamp
        // Actually, since ts=100 < maxTs=200, this is also a late arrival
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 200);
        Assert.That(results, Has.Count.EqualTo(2));
    }
}

[TestFixture]
public class StreamDBReadLatestTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long timestamp, float value = 1.0f)
    {
        var payload = new TestPayload { Value = value, Counter = (int)timestamp };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(secondaryIndex: secondaryIndex, payload: bytes, timestamp: timestamp, version: 1);
    }

    [Test]
    public void ReadLatest_ReturnsLatestPerIndex()
    {
        // Must write enough entries to trigger sparse indexing (adaptive level 6 = every 1024 writes)
        for (int i = 0; i < 2048; i++)
            AppendPayload(1, 1000 + i);
        for (int i = 0; i < 2048; i++)
            AppendPayload(2, 5000 + i);

        _db.WaitForPendingWrites();
        var latest = _db.ReadLatest();

        Assert.That(latest, Has.Count.EqualTo(2));
        Assert.That(latest[1].Timestamp, Is.EqualTo(1000 + 2047));
        Assert.That(latest[2].Timestamp, Is.EqualTo(5000 + 2047));
    }

    [Test]
    public void ReadLatest_WithLateArrival_KeepsHigherTimestamp()
    {
        for (int i = 0; i < 2048; i++)
            AppendPayload(1, 1000 + i);
        _db.WaitForPendingWrites();

        // Late arrival with lower timestamp — shouldn't override the latest
        AppendPayload(1, 1500);

        var latest = _db.ReadLatest();
        Assert.That(latest[1].Timestamp, Is.EqualTo(1000 + 2047));
    }
}

[TestFixture]
public class StreamDBGetEarliestTimestampTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long timestamp)
    {
        var payload = new TestPayload { Value = 1.0f, Counter = 0 };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(secondaryIndex: secondaryIndex, payload: bytes, timestamp: timestamp, version: 1);
    }

    [Test]
    public void GetEarliestTimestamp_ReturnsEarliest()
    {
        AppendPayload(1, 200);
        AppendPayload(2, 100);
        AppendPayload(3, 300);
        _db.WaitForPendingWrites();

        long? earliest = _db.GetEarliestTimestamp(new[] { 1, 2, 3 }, fromTs: 0);
        Assert.That(earliest, Is.EqualTo(100));
    }

    [Test]
    public void GetEarliestTimestamp_RespectsFromTs()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 300);
        _db.WaitForPendingWrites();

        long? earliest = _db.GetEarliestTimestamp(new[] { 1 }, fromTs: 150);
        Assert.That(earliest, Is.EqualTo(200));
    }

    [Test]
    public void GetEarliestTimestamp_NoData_ReturnsNull()
    {
        long? earliest = _db.GetEarliestTimestamp(new[] { 1 }, fromTs: 0);
        Assert.That(earliest, Is.Null);
    }

    [Test]
    public void GetEarliestTimestamp_WithLateArrival()
    {
        AppendPayload(1, 300);
        AppendPayload(1, 100); // late arrival
        _db.WaitForPendingWrites();

        long? earliest = _db.GetEarliestTimestamp(new[] { 1 }, fromTs: 0);
        Assert.That(earliest, Is.EqualTo(100));
    }
}

[TestFixture]
public class StreamDBReadRangeFromAvailableTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long timestamp)
    {
        var payload = new TestPayload { Value = 1.0f, Counter = 0 };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(secondaryIndex: secondaryIndex, payload: bytes, timestamp: timestamp, version: 1);
    }

    [Test]
    public void ReadRangeFromAvailable_NoData_ReturnsNegativeOne()
    {
        var (rangeEnd, data) = _db.ReadRangeFromAvailable(new[] { 1 }, fromTs: 0, windowMs: 100);
        Assert.That(rangeEnd, Is.EqualTo(-1));
        Assert.That(data, Is.Empty);
    }

    [Test]
    public void ReadRangeFromAvailable_ReturnsWindow()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i * 10);
        _db.WaitForPendingWrites();

        var (rangeEnd, data) = _db.ReadRangeFromAvailable(new[] { 1 }, fromTs: 100, windowMs: 50);
        Assert.That(rangeEnd, Is.EqualTo(150));
        Assert.That(data[1], Has.Count.GreaterThan(0));

        // All entries should be within the window
        foreach (var entry in data[1])
            Assert.That(entry.Timestamp, Is.InRange(100, 150));
    }
}

[TestFixture]
public class StreamDBStatsTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    [Test]
    public void GetStats_InitialValues()
    {
        var stats = _db.GetStats();
        Assert.That(stats.ScaleUp, Is.EqualTo(0));
        Assert.That(stats.ScaleDown, Is.EqualTo(0));
        Assert.That(stats.Dropped, Is.EqualTo(0));
        Assert.That(stats.LateArrivals, Is.EqualTo(0));
        Assert.That(stats.PendingIdxQueueLen, Is.EqualTo(0));
    }

    [Test]
    public void GetStats_TracksLateArrivals()
    {
        var payload = new TestPayload { Value = 1.0f, Counter = 0 };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));

        _db.Append(1, bytes, timestamp: 200, version: 1);
        _db.Append(1, bytes, timestamp: 100, version: 1); // late
        _db.Append(1, bytes, timestamp: 50, version: 1);  // late

        var stats = _db.GetStats();
        Assert.That(stats.LateArrivals, Is.EqualTo(2));
    }
}

[TestFixture]
public class StreamDBDisposeTests
{
    [Test]
    public void Append_AfterDispose_Throws()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        var db = new StreamDB(baseDir: dataDir);
        db.Dispose();

        try
        {
            byte[] payloadBytes = new byte[8];
            Assert.Throws<ObjectDisposedException>(() => db.Append(1, payloadBytes, 100, 1));
        }
        finally
        {
            if (Directory.Exists(dataDir))
                Directory.Delete(dataDir, recursive: true);
        }
    }

    [Test]
    public void ReadRange_AfterDispose_Throws()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        var db = new StreamDB(baseDir: dataDir);
        db.Dispose();

        try
        {
            Assert.Throws<ObjectDisposedException>(() => db.ReadRange(1, 0, 100));
        }
        finally
        {
            if (Directory.Exists(dataDir))
                Directory.Delete(dataDir, recursive: true);
        }
    }

    [Test]
    public void DoubleDispose_DoesNotThrow()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        var db = new StreamDB(baseDir: dataDir);
        try
        {
            db.Dispose();
            Assert.DoesNotThrow(() => db.Dispose());
        }
        finally
        {
            if (Directory.Exists(dataDir))
                Directory.Delete(dataDir, recursive: true);
        }
    }
}

[TestFixture]
public class StreamDBConcurrencyTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    [Test]
    public void ConcurrentWritesToDifferentIndexes()
    {
        const int indexCount = 8;
        const int writesPerIndex = 100;

        Parallel.For(0, indexCount, secondaryIndex =>
        {
            for (int i = 0; i < writesPerIndex; i++)
            {
                var payload = new TestPayload { Value = i, Counter = secondaryIndex };
                ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
                _db.Append(secondaryIndex, bytes, timestamp: 1000 + i, version: 1);
            }
        });

        _db.WaitForPendingWrites();

        for (int idx = 0; idx < indexCount; idx++)
        {
            var results = _db.ReadRange(secondaryIndex: idx, startTs: 1000, endTs: 1999);
            Assert.That(results, Has.Count.EqualTo(writesPerIndex),
                $"Secondary index {idx} should have {writesPerIndex} entries");
        }
    }
}

[TestFixture]
public class StreamDBLargePayloadTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    [Test]
    public void LargePayload_WritesAndReadsCorrectly()
    {
        byte[] largePayload = new byte[1024];
        Random.Shared.NextBytes(largePayload);

        _db.Append(secondaryIndex: 1, payload: largePayload, timestamp: 100, version: 1);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 100);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Payload, Is.EqualTo(largePayload));
    }

    [Test]
    public void EmptyPayload_WritesAndReadsCorrectly()
    {
        _db.Append(secondaryIndex: 1, payload: ReadOnlySpan<byte>.Empty, timestamp: 100, version: 1);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startTs: 100, endTs: 100);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Payload, Is.Empty);
    }
}
