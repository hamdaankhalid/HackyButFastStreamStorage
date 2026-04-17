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
using NUnit.Framework;

namespace StreamDB.Tests;

[TestFixture]
public class StreamDBReadRangeFromAvailableTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir, initialAdaptiveIdx: 0);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long primaryIndex)
    {
        var payload = new TestPayload { Value = 1.0f, Counter = 0 };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: 1, payload: bytes);
    }

    [Test]
    public void ReadRangeFromAvailable_NoData_ReturnsNegativeOne()
    {
        (long rangeEnd, Dictionary<int, List<StreamEntry>>? data) = _db.ReadRangeFromAvailable(new[] { 1 }, fromPrimaryIndex: 0, window: 100);
        Assert.That(rangeEnd, Is.EqualTo(-1));
        Assert.That(data, Is.Empty);
    }

    [Test]
    public void ReadRangeFromAvailable_ReturnsWindow()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i * 10);
        _db.WaitForPendingWrites();

        // With initialAdaptiveIdx=0 (spacing=16), first index at count=16 → pi=250.
        // Query from 0: forward lookup finds pi=250, scan from 0 finds pi=100 as earliest.
        (long rangeEnd, Dictionary<int, List<StreamEntry>>? data) = _db.ReadRangeFromAvailable(new[] { 1 }, fromPrimaryIndex: 100, window: 50);
        Assert.That(rangeEnd, Is.EqualTo(150));
        Assert.That(data[1], Has.Count.GreaterThan(0));

        foreach (StreamEntry entry in data[1])
            Assert.That(entry.PrimaryIndex, Is.InRange(100, 150));
    }

    [Test]
    public void ReadRangeFromAvailable_SkewedDevices_ReturnsOldDeviceData()
    {
        // Need enough writes to produce sparse index entries (default spacing ~1024)
        const int entriesPerDevice = 2000;

        // Device 1 has old data at low primary indexes
        for (int i = 0; i < entriesPerDevice; i++)
            AppendPayload(1, 100 + i);

        // Devices 5, 9, 13 have data only at very high primary indexes (different shards)
        for (int i = 0; i < entriesPerDevice; i++)
        {
            AppendPayload(5, 1_000_000 + i);
            AppendPayload(9, 1_000_000 + i);
            AppendPayload(13, 1_000_000 + i);
        }
        _db.WaitForPendingWrites();

        // Query from primary index 0 with a small window — should find device 1's old data
        (long rangeEnd, Dictionary<int, List<StreamEntry>>? data) = _db.ReadRangeFromAvailable(
            new[] { 1, 5, 9, 13 }, fromPrimaryIndex: 0, window: 3000);

        Assert.That(rangeEnd, Is.GreaterThan(-1), "Should find data");
        Assert.That(rangeEnd, Is.LessThanOrEqualTo(3200), "Window should anchor on device 1's old data");

        // Device 1 should have entries in range
        Assert.That(data.ContainsKey(1), Is.True, "Device 1 should have data in range");
        Assert.That(data[1], Has.Count.GreaterThan(0));
        foreach (StreamEntry entry in data[1])
            Assert.That(entry.PrimaryIndex, Is.InRange(0, rangeEnd));

        // Devices 5, 9, 13 should NOT have entries (their data is at 1M+, far beyond the window)
        if (data.ContainsKey(5))
            Assert.That(data[5], Has.Count.EqualTo(0));
        if (data.ContainsKey(9))
            Assert.That(data[9], Has.Count.EqualTo(0));
        if (data.ContainsKey(13))
            Assert.That(data[13], Has.Count.EqualTo(0));
    }

    [Test]
    public void ReadRangeFromAvailable_SkewedDevices_SameShardMix()
    {
        const int entriesPerDevice = 2000;

        // Devices 0 and 4 map to the same shard (index & 3 == 0)
        // Device 0: old data
        for (int i = 0; i < entriesPerDevice; i++)
            AppendPayload(0, 100 + i);

        // Device 4: only recent data
        for (int i = 0; i < entriesPerDevice; i++)
            AppendPayload(4, 1_000_000 + i);

        _db.WaitForPendingWrites();

        (long rangeEnd, Dictionary<int, List<StreamEntry>>? data) = _db.ReadRangeFromAvailable(
            new[] { 0, 4 }, fromPrimaryIndex: 0, window: 3000);

        Assert.That(rangeEnd, Is.GreaterThan(-1));
        Assert.That(data.ContainsKey(0), Is.True);
        Assert.That(data[0], Has.Count.GreaterThan(0));

        // Device 4 should have no data in the old range
        if (data.ContainsKey(4))
            Assert.That(data[4], Has.Count.EqualTo(0));
    }
}
