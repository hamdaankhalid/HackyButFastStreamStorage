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

    private void AppendPayload(int secondaryIndex, long primaryIndex, float value = 1.0f)
    {
        var payload = new TestPayload { Value = value, Counter = (int)primaryIndex };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: 1, payload: bytes);
    }

    [Test]
    public void ReadLatest_ReturnsLatestPerIndex()
    {
        // Interleave writes across both secondary indexes so both accumulate
        // write counts and trigger sparse index entries before backpressure
        // can escalate the adaptive tuning level.
        for (int i = 0; i < 2048; i++)
        {
            AppendPayload(1, 1000 + i);
            AppendPayload(2, 5000 + i);
        }

        _db.WaitForPendingWrites();
        var latest = _db.ReadLatest();

        Assert.That(latest, Has.Count.EqualTo(2));
        Assert.That(latest[1].PrimaryIndex, Is.EqualTo(1000 + 2047));
        Assert.That(latest[2].PrimaryIndex, Is.EqualTo(5000 + 2047));
    }

    [Test]
    public void ReadLatest_WithLateArrival_KeepsHigherPrimaryIndex()
    {
        for (int i = 0; i < 2048; i++)
            AppendPayload(1, 1000 + i);
        _db.WaitForPendingWrites();

        // Late arrival with lower primary index — shouldn't override the latest
        AppendPayload(1, 1500);

        var latest = _db.ReadLatest();
        Assert.That(latest[1].PrimaryIndex, Is.EqualTo(1000 + 2047));
    }
}
