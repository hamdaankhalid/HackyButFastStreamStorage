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
using StreamDB.LiteLsm;

namespace StreamDB.Tests;

[TestFixture]
public class LiteLsmTests
{
    private string _dataDir = null!;

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"litelsl-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dataDir);
        
        // Change to test directory so SegmentManager creates files there
        Environment.CurrentDirectory = _dataDir;
    }

    [TearDown]
    public void TearDown()
    {
        // Restore working directory before deleting temp dir to avoid
        // NUnit crash when CWD no longer exists.
        Environment.CurrentDirectory = Path.GetTempPath();
        
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private LiteLsm<TKey, TValue> CreateLsm<TKey, TValue>(SkipListType type = SkipListType.ClassBased)
        where TKey : unmanaged, IComparable<TKey>
        where TValue : unmanaged
    {
        return new LiteLsm<TKey, TValue>(() => type switch
        {
            SkipListType.ClassBased => new SkipList<TKey, TValue>(SegmentManager.SegmentSize),
            SkipListType.StructBased => new StructSkipList<TKey, TValue>(SegmentManager.SegmentSize),
            _ => throw new ArgumentException($"Unknown skip list type: {type}")
        });
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void Put_And_TryGet_SingleValue_ReturnsCorrectValue(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);

        // Act
        lsm.Put(100, 200);
        bool found = lsm.TryGet(100, out long value);

        // Assert
        Assert.That(found, Is.True);
        Assert.That(value, Is.EqualTo(200));
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void TryGet_NonExistentKey_ReturnsFalse(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        lsm.Put(100, 200);

        // Act
        bool found = lsm.TryGet(999, out long value);

        // Assert
        Assert.That(found, Is.False);
        Assert.That(value, Is.EqualTo(0));
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void Put_MultipleValues_AllRetrievable(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);

        // Act - insert 100 values
        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i * 10);
        }

        // Assert - verify all values
        for (long i = 1; i <= 100; i++)
        {
            bool found = lsm.TryGet(i, out long value);
            Assert.That(found, Is.True, $"Key {i} should be found");
            Assert.That(value, Is.EqualTo(i * 10), $"Key {i} should have value {i * 10}");
        }
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void QueryRange_ReturnsValuesInRange(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        for (long i = 1; i <= 20; i++)
        {
            lsm.Put(i, i * 100);
        }

        // Act
        var results = lsm.QueryRange(5, 10).ToList();

        // Assert
        Assert.That(results.Count, Is.EqualTo(6)); // 5, 6, 7, 8, 9, 10
        Assert.That(results[0].Key, Is.EqualTo(5));
        Assert.That(results[0].Value, Is.EqualTo(500));
        Assert.That(results[5].Key, Is.EqualTo(10));
        Assert.That(results[5].Value, Is.EqualTo(1000));
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void QueryRange_EmptyRange_ReturnsEmpty(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        lsm.Put(1, 10);
        lsm.Put(10, 100);

        // Act
        var results = lsm.QueryRange(2, 9).ToList();

        // Assert
        Assert.That(results, Is.Empty);
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void Put_MoreThanSegmentSize_AutoFlushesToDisk(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        int segmentSize = SegmentManager.SegmentSize;

        // Act - insert more than one segment worth of data
        for (long i = 1; i <= segmentSize + 100; i++)
        {
            lsm.Put(i, i * 2);
        }

        // Assert - all values should still be retrievable (some from disk)
        for (long i = 1; i <= segmentSize + 100; i++)
        {
            bool found = lsm.TryGet(i, out long value);
            Assert.That(found, Is.True, $"Key {i} should be found after flush");
            Assert.That(value, Is.EqualTo(i * 2));
        }
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void QueryRange_AfterFlush_ReturnsAllValues(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        int segmentSize = SegmentManager.SegmentSize;

        // Insert enough data to trigger a flush
        int totalInserts = segmentSize + 100;
        for (long i = 1; i <= totalInserts; i++)
        {
            lsm.Put(i, i * 3);
        }

        // Act - query range spanning both on-disk and in-memory data
        var results = lsm.QueryRange(500, totalInserts).ToList();

        // Assert - should get all values in range
        int expectedCount = totalInserts - 500 + 1; // 500 to totalInserts inclusive
        Assert.That(results.Count, Is.EqualTo(expectedCount));
        Assert.That(results, Is.Ordered.By("Key"));
        
        // Verify first and last
        Assert.That(results[0].Key, Is.EqualTo(500));
        Assert.That(results[0].Value, Is.EqualTo(1500));
        Assert.That(results[^1].Key, Is.EqualTo(totalInserts));
        Assert.That(results[^1].Value, Is.EqualTo(totalInserts * 3));
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void QueryRange_EntireDataset_ReturnsSortedResults(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i * 7);
        }

        // Act
        var results = lsm.QueryRange(1, 100).ToList();

        // Assert
        Assert.That(results.Count, Is.EqualTo(100));
        Assert.That(results, Is.Ordered.By("Key"));
        
        for (int i = 0; i < 100; i++)
        {
            Assert.That(results[i].Key, Is.EqualTo(i + 1));
            Assert.That(results[i].Value, Is.EqualTo((i + 1) * 7));
        }
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void TryGet_DifferentKeyValueTypes_WorksCorrectly(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<int, float>(skipListType);

        // Act
        lsm.Put(1, 1.5f);
        lsm.Put(2, 2.5f);
        lsm.Put(3, 3.5f);

        // Assert
        Assert.That(lsm.TryGet(1, out float v1), Is.True);
        Assert.That(v1, Is.EqualTo(1.5f));
        
        Assert.That(lsm.TryGet(2, out float v2), Is.True);
        Assert.That(v2, Is.EqualTo(2.5f));
        
        Assert.That(lsm.TryGet(3, out float v3), Is.True);
        Assert.That(v3, Is.EqualTo(3.5f));
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void GetStats_ReturnsCorrectCounts(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        
        // Act - insert some data but not enough to flush
        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i);
        }
        
        var stats = lsm.GetStats();

        // Assert
        Assert.That(stats.ActiveMemTableCount, Is.EqualTo(100));
        Assert.That(stats.NumSegments, Is.EqualTo(0)); // No flush yet
    }

    [TestCase(SkipListType.ClassBased)]
    [TestCase(SkipListType.StructBased)]
    public void GetStats_AfterFlush_ShowsSegments(SkipListType skipListType)
    {
        // Arrange
        var lsm = CreateLsm<long, long>(skipListType);
        int segmentSize = SegmentManager.SegmentSize;
        
        // Act - trigger a flush
        for (long i = 1; i <= segmentSize + 1; i++)
        {
            lsm.Put(i, i);
        }
        
        lsm.WaitForPendingFlush();
        var stats = lsm.GetStats();

        // Assert
        Assert.That(stats.NumSegments, Is.GreaterThan(0));
    }
}
