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
public class StreamDBMultiIndexTests
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

    private void AppendPayload(int secondaryIndex, long primaryIndex, float value = 1.0f)
    {
        var payload = new TestPayload { Value = value, Counter = (int)primaryIndex };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: 1, payload: bytes);
    }

    [Test]
    public void ReadRange_MultiIndex_ReturnsPerIndex()
    {
        for (int i = 0; i < 20; i++)
        {
            AppendPayload(1, 100 + i);
            AppendPayload(2, 100 + i);
            AppendPayload(3, 100 + i);
        }
        _db.WaitForPendingWrites();

    Dictionary<int, List<StreamEntry>> results = _db.ReadRange(secondaryIndexes: new[] { 1, 2, 3 }, startPrimaryIndex: 100, endPrimaryIndex: 119);
        Assert.That(results.Keys, Has.Count.EqualTo(3));
        Assert.That(results[1], Has.Count.EqualTo(20));
        Assert.That(results[2], Has.Count.EqualTo(20));
        Assert.That(results[3], Has.Count.EqualTo(20));
    }

    [Test]
    public void ReadRange_MultiIndex_MixedResults()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i * 10);
        for (int i = 0; i < 20; i++)
            AppendPayload(2, 150 + i * 10);
        _db.WaitForPendingWrites();

    Dictionary<int, List<StreamEntry>> results = _db.ReadRange(secondaryIndexes: new[] { 1, 2 }, startPrimaryIndex: 100, endPrimaryIndex: 300);
        Assert.That(results[1], Has.Count.EqualTo(20));
        Assert.That(results[2], Has.Count.EqualTo(16)); // pi 150..300
    }

    [Test]
    public void ReadRange_AllIndexes()
    {
        for (int i = 0; i < 20; i++)
        {
            AppendPayload(1, 100 + i);
            AppendPayload(2, 100 + i);
            AppendPayload(3, 100 + i);
        }
        _db.WaitForPendingWrites();

    Dictionary<int, List<StreamEntry>> results = _db.ReadRange(startPrimaryIndex: 0, endPrimaryIndex: 200);
        Assert.That(results.Keys, Has.Count.EqualTo(3));
    }

    [Test]
    public void ReadRange_MultiIndex_WithLimit()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i);
        _db.WaitForPendingWrites();

    Dictionary<int, List<StreamEntry>> results = _db.ReadRange(secondaryIndexes: new[] { 1 }, startPrimaryIndex: 100, endPrimaryIndex: 200, limit: 3);
        Assert.That(results[1], Has.Count.EqualTo(3));
    }
}
