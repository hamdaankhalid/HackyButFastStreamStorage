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
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_dataDir))
        {
            try { Directory.Delete(_dataDir, recursive: true); } catch { /* best effort — mmap files may still be held */ }
        }
    }

    private LiteLsm<TKey, TValue> CreateLsm<TKey, TValue>()
        where TKey : unmanaged, IComparable<TKey>
        where TValue : unmanaged
    {
        return new LiteLsm<TKey, TValue>(
            () => new StructSkipList<TKey, TValue>(SegmentManager.SegmentSize),
            Path.Combine(_dataDir, "segments"));
    }

    [Test]
    public void Put_And_TryGet_SingleValue_ReturnsCorrectValue()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();

        // Act
        lsm.Put(100, 200);
        bool found = lsm.TryGet(100, out long value);

        // Assert
        Assert.That(found, Is.True);
        Assert.That(value, Is.EqualTo(200));
    }

    [Test]
    public void TryGet_NonExistentKey_ReturnsFalse()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
        lsm.Put(100, 200);

        // Act
        bool found = lsm.TryGet(999, out long value);

        // Assert
        Assert.That(found, Is.False);
        Assert.That(value, Is.EqualTo(0));
    }

    [Test]
    public void Put_MultipleValues_AllRetrievable()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();

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

    [Test]
    public void Iterator_ReturnsValuesInRange()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
        for (long i = 1; i <= 20; i++)
        {
            lsm.Put(i, i * 100);
        }

        // Act
        using var iter = lsm.GetIterator(5, 10);
        var results = iter.ReadAll().ToList();

        // Assert
        Assert.That(results.Count, Is.EqualTo(6)); // 5, 6, 7, 8, 9, 10
        Assert.That(results[0].Key, Is.EqualTo(5));
        Assert.That(results[0].Value, Is.EqualTo(500));
        Assert.That(results[5].Key, Is.EqualTo(10));
        Assert.That(results[5].Value, Is.EqualTo(1000));
    }

    [Test]
    public void Iterator_EmptyRange_ReturnsEmpty()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
        lsm.Put(1, 10);
        lsm.Put(10, 100);

        // Act
        using var iter = lsm.GetIterator(2, 9);
        var results = iter.ReadAll().ToList();

        // Assert
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void Put_MoreThanSegmentSize_AutoFlushesToDisk()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
        int segmentSize = SegmentManager.SegmentSize;

        // Act - insert more than one segment worth of data
        for (long i = 1; i <= segmentSize + 100; i++)
        {
            lsm.Put(i, i * 2);
        }
        lsm.WaitForPendingFlush();

        // Assert - all values should still be retrievable (some from disk)
        for (long i = 1; i <= segmentSize + 100; i++)
        {
            bool found = lsm.TryGet(i, out long value);
            Assert.That(found, Is.True, $"Key {i} should be found after flush");
            Assert.That(value, Is.EqualTo(i * 2));
        }
    }

    [Test]
    public void Iterator_AfterFlush_ReturnsAllValues()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
        int segmentSize = SegmentManager.SegmentSize;

        // Insert enough data to trigger a flush
        int totalInserts = segmentSize + 100;
        for (long i = 1; i <= totalInserts; i++)
        {
            lsm.Put(i, i * 3);
        }
        lsm.WaitForPendingFlush();

        // Act - iterate range spanning both on-disk and in-memory data
        using var iter = lsm.GetIterator(500, totalInserts);
        var results = iter.ReadAll().ToList();

        // Assert - should get all values in range
        int expectedCount = totalInserts - 500 + 1; // 500 to totalInserts inclusive
        Assert.That(results.Count, Is.EqualTo(expectedCount));
        Assert.That(results, Is.Ordered.By("Item1"));
        Assert.That(results[0].Key, Is.EqualTo(500));
        Assert.That(results[0].Value, Is.EqualTo(1500));
        Assert.That(results[^1].Key, Is.EqualTo(totalInserts));
        Assert.That(results[^1].Value, Is.EqualTo(totalInserts * 3));
    }

    [Test]
    public void Iterator_EntireDataset_ReturnsSortedResults()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i * 7);
        }

        // Act
        using var iter = lsm.GetIterator(1, 100);
        var results = iter.ReadAll().ToList();

        // Assert
        Assert.That(results.Count, Is.EqualTo(100));
        Assert.That(results, Is.Ordered.By("Item1"));
        
        for (int i = 0; i < 100; i++)
        {
            Assert.That(results[i].Key, Is.EqualTo(i + 1));
            Assert.That(results[i].Value, Is.EqualTo((i + 1) * 7));
        }
    }

    [Test]
    public void TryGet_DifferentKeyValueTypes_WorksCorrectly()
    {
        // Arrange
        var lsm = CreateLsm<int, float>();

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

    [Test]
    public void GetStats_ReturnsCorrectCounts()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
        
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

    [Test]
    public void GetStats_AfterFlush_ShowsSegments()
    {
        // Arrange
        var lsm = CreateLsm<long, long>();
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

    [Test]
    public void ConcurrentReadsAndWrites_DataAlwaysConsistent()
    {
        // Long-running test: writer inserts monotonic keys while background readers
        // continuously perform point queries and range queries. Verifies that
        // all reads return correct data even during flushes.
        var lsm = CreateLsm<long, long>();
        const int totalInserts = 50_000; // Triggers many flushes (SegmentSize=1024)
        var errors = new System.Collections.Concurrent.ConcurrentBag<string>();
        var cts = new CancellationTokenSource();
        long highWaterMark = 0; // Highest key fully committed by writer

        // Writer: inserts keys 1..totalInserts monotonically
        var writerTask = Task.Run(() =>
        {
            for (long i = 1; i <= totalInserts; i++)
            {
                lsm.Put(i, i * 10);
                Volatile.Write(ref highWaterMark, i);
            }
            lsm.WaitForPendingFlush();
            cts.Cancel();
        });

        // Reader 1: point queries on keys known to be written
        var pointReader = Task.Run(() =>
        {
            var rng = new Random(42);
            while (!cts.Token.IsCancellationRequested)
            {
                long hwm = Volatile.Read(ref highWaterMark);
                if (hwm < 10) { Thread.Yield(); continue; }

                long key = rng.NextInt64(1, hwm + 1);
                if (lsm.TryGet(key, out long value))
                {
                    if (value != key * 10)
                    {
                        errors.Add($"TryGet({key}) returned {value}, expected {key * 10}");
                    }
                }
                // Not finding the key is acceptable — it may be mid-flush
            }
        });

        // Reader 2: range queries via iterator on committed data
        var rangeReader = Task.Run(() =>
        {
            var rng = new Random(99);
            while (!cts.Token.IsCancellationRequested)
            {
                long hwm = Volatile.Read(ref highWaterMark);
                if (hwm < 100) { Thread.Yield(); continue; }

                long from = rng.NextInt64(1, hwm - 10);
                long to = Math.Min(from + rng.NextInt64(10, 500), hwm);

                using var iter = lsm.GetIterator(from, to);
                long? prevKey = null;
                foreach (var (key, value) in iter.ReadAll())
                {
                    // Keys must be in ascending order
                    if (prevKey.HasValue && key <= prevKey.Value)
                    {
                        errors.Add($"Range [{from},{to}]: key {key} not ascending after {prevKey.Value}");
                        break;
                    }

                    // Keys must be within the requested range
                    if (key < from || key > to)
                    {
                        errors.Add($"Range [{from},{to}]: key {key} out of bounds");
                        break;
                    }

                    // Values must match the formula
                    if (value != key * 10)
                    {
                        errors.Add($"Range [{from},{to}]: key {key} has value {value}, expected {key * 10}");
                        break;
                    }

                    prevKey = key;
                }
            }
        });

        // Wait for all tasks
        Task.WaitAll(writerTask, pointReader, rangeReader);

        // Final verification: all keys must now be retrievable
        for (long i = 1; i <= totalInserts; i++)
        {
            bool found = lsm.TryGet(i, out long val);
            if (!found)
                errors.Add($"Final check: key {i} not found");
            else if (val != i * 10)
                errors.Add($"Final check: key {i} has value {val}, expected {i * 10}");
        }

        // Final range query over entire dataset
        using var fullIter = lsm.GetIterator(1, totalInserts);
        var allResults = fullIter.ReadAll().ToList();
        Assert.That(allResults.Count, Is.EqualTo(totalInserts),
            $"Full range query returned {allResults.Count} entries, expected {totalInserts}");

        for (int i = 0; i < allResults.Count; i++)
        {
            if (allResults[i].Key != i + 1)
                errors.Add($"Full range: index {i} has key {allResults[i].Key}, expected {i + 1}");
            if (allResults[i].Value != (i + 1) * 10)
                errors.Add($"Full range: key {allResults[i].Key} has value {allResults[i].Value}, expected {(i + 1) * 10}");
        }

        Assert.That(errors, Is.Empty,
            $"Found {errors.Count} error(s):\n{string.Join("\n", errors.Take(20))}");
    }
}
