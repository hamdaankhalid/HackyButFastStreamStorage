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
public class StreamDBGetEarliestPrimaryIndexTests
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

    private void AppendPayload(int secondaryIndex, long primaryIndex)
    {
        var payload = new TestPayload { Value = 1.0f, Counter = 0 };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: 1, payload: bytes);
    }

    [Test]
    public void GetEarliestPrimaryIndex_ReturnsEarliest()
    {
        AppendPayload(1, 200);
        AppendPayload(2, 100);
        AppendPayload(3, 300);
        _db.WaitForPendingWrites();

        long? earliest = _db.GetEarliestPrimaryIndex(new[] { 1, 2, 3 }, fromPrimaryIndex: 0);
        Assert.That(earliest, Is.EqualTo(100));
    }

    [Test]
    public void GetEarliestPrimaryIndex_RespectsFromPrimaryIndex()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 300);
        _db.WaitForPendingWrites();

        long? earliest = _db.GetEarliestPrimaryIndex(new[] { 1 }, fromPrimaryIndex: 150);
        Assert.That(earliest, Is.EqualTo(200));
    }

    [Test]
    public void GetEarliestPrimaryIndex_NoData_ReturnsNull()
    {
        long? earliest = _db.GetEarliestPrimaryIndex(new[] { 1 }, fromPrimaryIndex: 0);
        Assert.That(earliest, Is.Null);
    }

    [Test]
    public void GetEarliestPrimaryIndex_WithLateArrival()
    {
        AppendPayload(1, 300);
        AppendPayload(1, 100); // late arrival
        _db.WaitForPendingWrites();

        long? earliest = _db.GetEarliestPrimaryIndex(new[] { 1 }, fromPrimaryIndex: 0);
        Assert.That(earliest, Is.EqualTo(100));
    }
}
