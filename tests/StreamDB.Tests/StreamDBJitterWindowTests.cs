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
public class StreamDBJitterWindowTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir, jitterWindow: 50);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long primaryIndex, float value = 1.0f, ushort version = 1)
    {
        var payload = new TestPayload { Value = value, Counter = (int)primaryIndex };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: version, payload: bytes);
    }

    [Test]
    public void JitterWindow_WithinWindow_WritesToFasterLog()
    {
        // Write a high primary index, then a slightly lower one within jitter window
        AppendPayload(1, 1000);
        AppendPayload(1, 970); // within jitter window of 50 (1000 - 970 = 30 < 50)

        _db.WaitForPendingWrites();

        var stats = _db.GetStats();
        Assert.That(stats.LateArrivals, Is.EqualTo(0), "Should NOT be a late arrival");
        Assert.That(stats.JitterAbsorbed, Is.EqualTo(1), "Should be absorbed by jitter window");

        // Verify it appears in ReadRange
        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 960, endPrimaryIndex: 1010);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.Any(e => e.PrimaryIndex == 970), Is.True, "Jitter entry should be readable");
        Assert.That(results.Any(e => e.PrimaryIndex == 1000), Is.True, "Normal entry should be readable");
    }

    [Test]
    public void JitterWindow_BeyondWindow_WritesToSideStore()
    {
        // Write a high primary index, then one far in the past beyond jitter window
        AppendPayload(1, 1000);
        AppendPayload(1, 900); // beyond jitter window of 50 (1000 - 900 = 100 > 50)

        _db.WaitForPendingWrites();

        var stats = _db.GetStats();
        Assert.That(stats.LateArrivals, Is.EqualTo(1), "Should be a late arrival");
        Assert.That(stats.JitterAbsorbed, Is.EqualTo(0), "Should NOT be absorbed by jitter window");

        // Verify it still appears in ReadRange (via late arrivals merge)
        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 890, endPrimaryIndex: 1010);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.Any(e => e.PrimaryIndex == 900), Is.True, "Late arrival should be readable");
    }

    [Test]
    public void JitterWindow_ReadRange_FindsJitterEntries()
    {
        // Write entries out of order within jitter window
        AppendPayload(1, 1000, value: 10.0f);
        AppendPayload(1, 980, value: 9.8f);  // within jitter window
        AppendPayload(1, 990, value: 9.9f);  // within jitter window
        AppendPayload(1, 1010, value: 10.1f);

        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 975, endPrimaryIndex: 1005);
        Assert.That(results, Has.Count.EqualTo(3)); // 980, 990, 1000

        // Verify entries are present (they may not be in perfect order since FasterLog is append-order)
        var piValues = results.Select(e => e.PrimaryIndex).ToList();
        Assert.That(piValues, Does.Contain(980L));
        Assert.That(piValues, Does.Contain(990L));
        Assert.That(piValues, Does.Contain(1000L));
    }

    [Test]
    public void JitterWindow_Stats_TracksJitterAbsorbed()
    {
        AppendPayload(1, 1000);
        AppendPayload(1, 980); // jitter absorbed (20 < 50)
        AppendPayload(1, 960); // jitter absorbed (1000 - 960 = 40 < 50)
        AppendPayload(1, 940); // beyond jitter (1000 - 940 = 60 > 50), late arrival
        AppendPayload(1, 500); // beyond jitter (1000 - 500 = 500 > 50), late arrival

        var stats = _db.GetStats();
        Assert.That(stats.JitterAbsorbed, Is.EqualTo(2), "Two writes should be absorbed by jitter window");
        Assert.That(stats.LateArrivals, Is.EqualTo(2), "Two writes should be late arrivals");
    }

    [Test]
    public void JitterWindow_ExactBoundary_WritesToFasterLog()
    {
        // Write exactly at the jitter boundary (maxPi - jitterWindow)
        AppendPayload(1, 1000);
        AppendPayload(1, 950); // exactly at boundary: 1000 - 50 = 950

        _db.WaitForPendingWrites();

        var stats = _db.GetStats();
        Assert.That(stats.JitterAbsorbed, Is.EqualTo(1), "Exact boundary should be absorbed");
        Assert.That(stats.LateArrivals, Is.EqualTo(0), "Should NOT be a late arrival");
    }

    [Test]
    public void JitterWindow_JustBeyondBoundary_WritesToSideStore()
    {
        // Write just past the jitter boundary
        AppendPayload(1, 1000);
        AppendPayload(1, 949); // just beyond: 1000 - 50 = 950 > 949

        _db.WaitForPendingWrites();

        var stats = _db.GetStats();
        Assert.That(stats.JitterAbsorbed, Is.EqualTo(0), "Should NOT be absorbed");
        Assert.That(stats.LateArrivals, Is.EqualTo(1), "Should be a late arrival");
    }
}
