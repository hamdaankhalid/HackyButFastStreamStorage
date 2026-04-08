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
public class StreamDBConcurrencyTests
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
    public void ConcurrentWritesToDifferentIndexes()
    {
        const int indexCount = 8;
        const int writesPerIndex = 100;

        Parallel.For(0, indexCount, secondaryIndex =>
        {
            for (int i = 0; i < writesPerIndex; i++)
            {
                var payload = new TestPayload{ Value = i, Counter = secondaryIndex };
                ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
                _db.Append(primaryIndex: 1000 + i, secondaryIndex: secondaryIndex, version: 1, payload: bytes);
            }
        });

        _db.WaitForPendingWrites();

        for (int idx = 0; idx < indexCount; idx++)
        {
            var results = _db.ReadRange(secondaryIndex: idx, startPrimaryIndex: 1000, endPrimaryIndex: 1999);
            Assert.That(results, Has.Count.EqualTo(writesPerIndex),
                $"Secondary index {idx} should have {writesPerIndex} entries");
        }
    }
}
