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

using NUnit.Framework;

namespace StreamDB.Tests;

[TestFixture]
public class StreamDBLargePayloadTests
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
    public void LargePayload_WritesAndReadsCorrectly()
    {
        byte[] largePayload = new byte[1024];
        Random.Shared.NextBytes(largePayload);

        _db.Append(primaryIndex: 100, secondaryIndex: 1, version: 1, payload: largePayload);
        _db.WaitForPendingWrites();

    List<StreamEntry> results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 100);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Payload, Is.EqualTo(largePayload));
    }

    [Test]
    public void EmptyPayload_WritesAndReadsCorrectly()
    {
        _db.Append(primaryIndex: 100, secondaryIndex: 1, version: 1, payload: ReadOnlySpan<byte>.Empty);
        _db.WaitForPendingWrites();

    List<StreamEntry> results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 100);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Payload, Is.Empty);
    }
}
