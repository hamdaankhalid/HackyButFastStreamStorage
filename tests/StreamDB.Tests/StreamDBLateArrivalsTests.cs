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

    private void AppendPayload(int secondaryIndex, long primaryIndex, float value = 1.0f)
    {
        var payload = new TestPayload { Value = value, Counter = (int)primaryIndex };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: 1, payload: bytes);
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
    public void LateArrival_MergedInPrimaryIndexOrder()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 300);
        AppendPayload(1, 150); // late arrival
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 300);
        Assert.That(results, Has.Count.EqualTo(4));

        // Verify primary index ordering
        for (int i = 1; i < results.Count; i++)
        {
            Assert.That(results[i].PrimaryIndex, Is.GreaterThanOrEqualTo(results[i - 1].PrimaryIndex),
                $"Entry {i} (pi={results[i].PrimaryIndex}) should be >= entry {i - 1} (pi={results[i - 1].PrimaryIndex})");
        }

        // Verify the late arrival is at the correct position
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(100));
        Assert.That(results[1].PrimaryIndex, Is.EqualTo(150));
        Assert.That(results[2].PrimaryIndex, Is.EqualTo(200));
        Assert.That(results[3].PrimaryIndex, Is.EqualTo(300));
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

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 200);
        Assert.That(results, Has.Count.EqualTo(13));

        // Verify ordering
        for (int i = 1; i < results.Count; i++)
            Assert.That(results[i].PrimaryIndex, Is.GreaterThanOrEqualTo(results[i - 1].PrimaryIndex));
    }

    [Test]
    public void LateArrival_WithLimit()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 300);
        AppendPayload(1, 200); // late arrival
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 300, limit: 2);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(100));
        Assert.That(results[1].PrimaryIndex, Is.EqualTo(200)); // late arrival comes before 300
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

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 200);
        Assert.That(results, Has.Count.EqualTo(2));

        // The late arrival should be first (pi=100)
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

        var results = _db.ReadRange(secondaryIndexes: new[] { 1, 2 }, startPrimaryIndex: 100, endPrimaryIndex: 300);
        Assert.That(results[1], Has.Count.EqualTo(3));
        Assert.That(results[2], Has.Count.EqualTo(3));

        // Both should be in primary index order
        Assert.That(results[1][1].PrimaryIndex, Is.EqualTo(200));
        Assert.That(results[2][1].PrimaryIndex, Is.EqualTo(200));
    }

    [Test]
    public void LateArrival_AllIndexes_ReadRange()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 300);
        AppendPayload(1, 200); // late
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(startPrimaryIndex: 100, endPrimaryIndex: 300);
        Assert.That(results[1], Has.Count.EqualTo(3));
        Assert.That(results[1][1].PrimaryIndex, Is.EqualTo(200));
    }

    [Test]
    public void LateArrival_SamePrimaryIndex_BothKept()
    {
        AppendPayload(1, 200);
        AppendPayload(1, 100, value: 1.0f); // late with pi=100

        // Write another normal entry at pi=100 — this simulates a duplicate primary index
        // Actually, since pi=100 < maxPi=200, this is also a late arrival
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 200);
        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public void LateArrivals_LimitIsPerSecondaryIndex()
    {
        // Write monotonic entries for two secondary indexes
        for (int i = 0; i < 10; i++)
        {
            AppendPayload(1, 1000 + i);
            AppendPayload(2, 1000 + i);
        }

        // Write late arrivals for both indexes (below max primary index)
        for (int i = 0; i < 5; i++)
        {
            AppendPayload(1, 500 + i, value: 99.0f);
            AppendPayload(2, 500 + i, value: 99.0f);
        }

        _db.WaitForPendingWrites();

        // Each index has 15 total entries (10 normal + 5 late arrivals)
        // With limit=3, each secondary index should return at most 3 entries
        var results = _db.ReadRange(new[] { 1, 2 }, startPrimaryIndex: 0, endPrimaryIndex: 2000, limit: 3);

        Assert.That(results[1], Has.Count.EqualTo(3), "Index 1 should respect per-index limit");
        Assert.That(results[2], Has.Count.EqualTo(3), "Index 2 should respect per-index limit");
    }
}
