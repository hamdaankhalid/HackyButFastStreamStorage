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
    public void ReadRangeFromAvailable_NoData_ReturnsNegativeOne()
    {
        var (rangeEnd, data) = _db.ReadRangeFromAvailable(new[] { 1 }, fromPrimaryIndex: 0, window: 100);
        Assert.That(rangeEnd, Is.EqualTo(-1));
        Assert.That(data, Is.Empty);
    }

    [Test]
    public void ReadRangeFromAvailable_ReturnsWindow()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i * 10);
        _db.WaitForPendingWrites();

        var (rangeEnd, data) = _db.ReadRangeFromAvailable(new[] { 1 }, fromPrimaryIndex: 100, window: 50);
        Assert.That(rangeEnd, Is.EqualTo(150));
        Assert.That(data[1], Has.Count.GreaterThan(0));

        // All entries should be within the window
        foreach (var entry in data[1])
            Assert.That(entry.PrimaryIndex, Is.InRange(100, 150));
    }
}
