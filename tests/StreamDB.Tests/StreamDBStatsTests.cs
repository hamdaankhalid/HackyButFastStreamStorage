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
public class StreamDBStatsTests
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

    [Test]
    public void GetStats_InitialValues()
    {
        var stats = _db.GetStats();
        Assert.That(stats.ScaleUp, Is.EqualTo(0));
        Assert.That(stats.ScaleDown, Is.EqualTo(0));
        Assert.That(stats.Dropped, Is.EqualTo(0));
        Assert.That(stats.LateArrivals, Is.EqualTo(0));
        Assert.That(stats.JitterAbsorbed, Is.EqualTo(0));
        Assert.That(stats.PendingIdxQueueLen, Is.EqualTo(0));
    }

    [Test]
    public void GetStats_TracksLateArrivals()
    {
        var payload = new TestPayload { Value = 1.0f, Counter = 0 };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));

        _db.Append(primaryIndex: 200, secondaryIndex: 1, version: 1, payload: bytes);
        _db.Append(primaryIndex: 100, secondaryIndex: 1, version: 1, payload: bytes); // late
        _db.Append(primaryIndex: 50, secondaryIndex: 1, version: 1, payload: bytes);  // late

        var stats = _db.GetStats();
        Assert.That(stats.LateArrivals, Is.EqualTo(2));
    }
}
