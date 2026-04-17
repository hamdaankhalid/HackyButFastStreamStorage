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
            Path.Combine(_dataDir, "segments"));
    }

    [Test]
    public void Put_And_TryGet_SingleValue_ReturnsCorrectValue()
    {
        // Arrange
        LiteLsm<long, long> lsm = CreateLsm<long, long>();

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
        LiteLsm<long, long> lsm = CreateLsm<long, long>();
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
        LiteLsm<long, long> lsm = CreateLsm<long, long>();

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
        LiteLsm<long, long> lsm = CreateLsm<long, long>();
        for (long i = 1; i <= 20; i++)
        {
            lsm.Put(i, i * 100);
        }

        // Act
        using LiteLsmIterator<long, long> iter = lsm.GetIterator(5, 10);
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
        LiteLsm<long, long> lsm = CreateLsm<long, long>();
        lsm.Put(1, 10);
        lsm.Put(10, 100);

        // Act
        using LiteLsmIterator<long, long> iter = lsm.GetIterator(2, 9);
        var results = iter.ReadAll().ToList();

        // Assert
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void Put_MoreThanSegmentSize_AutoFlushesToDisk()
    {
        // Arrange
        LiteLsm<long, long> lsm = CreateLsm<long, long>();
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

    [Test]
    public void Iterator_AfterFlush_ReturnsAllValues()
    {
        // Arrange
        LiteLsm<long, long> lsm = CreateLsm<long, long>();
        int segmentSize = SegmentManager.SegmentSize;

        // Insert enough data to trigger a flush
        int totalInserts = segmentSize + 100;
        for (long i = 1; i <= totalInserts; i++)
        {
            lsm.Put(i, i * 3);
        }

        // Act - iterate range spanning both on-disk and in-memory data
        using LiteLsmIterator<long, long> iter = lsm.GetIterator(500, totalInserts);
        var results = iter.ReadAll().ToList();

        // Assert - should get all values in range
        int expectedCount = totalInserts - 500 + 1; // 500 to totalInserts inclusive
        Assert.That(results.Count, Is.EqualTo(expectedCount));
        Assert.That(results, Is.Ordered.By("Key"));
        Assert.That(results[0].Key, Is.EqualTo(500));
        Assert.That(results[0].Value, Is.EqualTo(1500));
        Assert.That(results[^1].Key, Is.EqualTo(totalInserts));
        Assert.That(results[^1].Value, Is.EqualTo(totalInserts * 3));
    }

    [Test]
    public void Iterator_EntireDataset_ReturnsSortedResults()
    {
        // Arrange
        LiteLsm<long, long> lsm = CreateLsm<long, long>();
        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i * 7);
        }

        // Act
        using LiteLsmIterator<long, long> iter = lsm.GetIterator(1, 100);
        var results = iter.ReadAll().ToList();

        // Assert
        Assert.That(results.Count, Is.EqualTo(100));
        Assert.That(results, Is.Ordered.By("Key"));

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
        LiteLsm<int, float> lsm = CreateLsm<int, float>();

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
        LiteLsm<long, long> lsm = CreateLsm<long, long>();

        // Act - insert some data but not enough to flush
        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i);
        }

        LsmStats stats = lsm.GetStats();

        // Assert
        Assert.That(stats.ActiveMemTableCount, Is.EqualTo(100));
        Assert.That(stats.NumSegments, Is.EqualTo(0)); // No flush yet
    }

    [Test]
    public void GetStats_AfterFlush_ShowsSegments()
    {
        // Arrange — use a large memTableCapacity so only eviction creates segments
        var lsm = new LiteLsm<long, long>(
            Path.Combine(_dataDir, "segments"),
            memTableCapacity: 20_000);

        // Act - write enough to trigger eviction (node pool is ~10K)
        for (long i = 1; i <= 40_000; i++)
        {
            lsm.Put(i, i);
        }

        LsmStats stats = lsm.GetStats();

        // Assert
        Assert.That(stats.NumSegments, Is.GreaterThan(0));
    }

    // [Test]
    // public void ConcurrentReadsAndWrites_DataAlwaysConsistent()
    // {
    // // Long-running test: writer inserts monotonic keys while background readers
    // // continuously perform point queries and range queries. Verifies that
    // // all reads return correct data even during flushes.
    // LiteLsm<long, long> lsm = CreateLsm<long, long>();
    //     const int totalInserts = 50_000; // Triggers many flushes (SegmentSize=1024)
    //     var errors = new System.Collections.Concurrent.ConcurrentBag<string>();
    //     var cts = new CancellationTokenSource();
    //     long highWaterMark = 0; // Highest key fully committed by writer

    //     // Writer: inserts keys 1..totalInserts monotonically
    //     var writerTask = Task.Run(() =>
    //     {
    //         for (long i = 1; i <= totalInserts; i++)
    //         {
    //             lsm.Put(i, i * 10);
    //             Volatile.Write(ref highWaterMark, i);
    //         }
    //         cts.Cancel();
    //     });

    //     // Reader 1: point queries on keys known to be written
    //     var pointReader = Task.Run(() =>
    //     {
    //         var rng = new Random(42);
    //         while (!cts.Token.IsCancellationRequested)
    //         {
    //             long hwm = Volatile.Read(ref highWaterMark);
    //             if (hwm < 10) { Thread.Yield(); continue; }

    //             long key = rng.NextInt64(1, hwm + 1);
    //             if (lsm.TryGet(key, out long value))
    //             {
    //                 if (value != key * 10)
    //                 {
    //                     errors.Add($"TryGet({key}) returned {value}, expected {key * 10}");
    //                 }
    //             }
    //             // Not finding the key is acceptable — it may be mid-flush
    //         }
    //     });

    //     // Reader 2: range queries via iterator on committed data
    //     var rangeReader = Task.Run(() =>
    //     {
    //         var rng = new Random(99);
    //         while (!cts.Token.IsCancellationRequested)
    //         {
    //             long hwm = Volatile.Read(ref highWaterMark);
    //             if (hwm < 100) { Thread.Yield(); continue; }

    //             long from = rng.NextInt64(1, hwm - 10);
    //             long to = Math.Min(from + rng.NextInt64(10, 500), hwm);

    //             using LiteLsmIterator<long, long> iter = lsm.GetIterator(from, to);
    //             long? prevKey = null;
    //             foreach (KeyValuePair<long, long> kvp in iter.ReadAll())
    //             {
    //                 var key = kvp.Key;
    //                 // Keys must be in ascending order
    //                 if (prevKey.HasValue && key <= prevKey.Value)
    //                 {
    //                     errors.Add($"Range [{from},{to}]: key {key} not ascending after {prevKey.Value}");
    //                     break;
    //                 }

    //                 // Keys must be within the requested range
    //                 if (key < from || key > to)
    //                 {
    //                     errors.Add($"Range [{from},{to}]: key {key} out of bounds");
    //                     break;
    //                 }

    //                 // Values must match the formula
    //                 if (kvp.Value != key * 10)
    //                 {
    //                     errors.Add($"Range [{from},{to}]: key {key} has value {kvp.Value}, expected {key * 10}");
    //                     break;
    //                 }

    //                 prevKey = key;
    //             }
    //         }
    //     });

    //     // Wait for all tasks
    //     Task.WaitAll(writerTask, pointReader, rangeReader);

    //     // Final verification: all keys must now be retrievable
    //     for (long i = 1; i <= totalInserts; i++)
    //     {
    //         bool found = lsm.TryGet(i, out long val);
    //         if (!found)
    //             errors.Add($"Final check: key {i} not found");
    //         else if (val != i * 10)
    //             errors.Add($"Final check: key {i} has value {val}, expected {i * 10}");
    //     }

    //     // Final range query over entire dataset
    //     using LiteLsmIterator<long, long> fullIter = lsm.GetIterator(1, totalInserts);
    //     var allResults = fullIter.ReadAll().ToList();
    //     Assert.That(allResults.Count, Is.EqualTo(totalInserts),
    //         $"Full range query returned {allResults.Count} entries, expected {totalInserts}");

    //     for (int i = 0; i < allResults.Count; i++)
    //     {
    //         if (allResults[i].Key != i + 1)
    //             errors.Add($"Full range: index {i} has key {allResults[i].Key}, expected {i + 1}");
    //         if (allResults[i].Value != (i + 1) * 10)
    //             errors.Add($"Full range: key {allResults[i].Key} has value {allResults[i].Value}, expected {(i + 1) * 10}");
    //     }

    //     Assert.That(errors, Is.Empty,
    //         $"Found {errors.Count} error(s):\n{string.Join("\n", errors.Take(20))}");
    // }

    #region Eviction Tests

    [Test]
    public void Eviction_WritesExceedingCapacity_ContinueTransparently()
    {
        // Use a small memTableCapacity so the skiplist fills up and triggers eviction.
        // The default node pool is 10,000 (minus 1 for head = 9,999 usable).
        // With memTableCapacity=1024, normal flush triggers at 1024.
        // But if we write more than the node pool without flushing, eviction must kick in.
        // To test eviction specifically, we use a capacity larger than the node pool.
        var lsm = new LiteLsm<long, long>(
            Path.Combine(_dataDir, "segments"),
            memTableCapacity: 20_000); // Larger than node pool (10K), forces eviction

        // Write 15,000 entries — exceeds the 9,999 usable node slots
        for (long i = 1; i <= 15_000; i++)
        {
            lsm.Put(i, i * 100);
        }

        // Verify all entries are readable (some from evicted segments, some from memory)
        for (long i = 1; i <= 15_000; i++)
        {
            bool found = lsm.TryGet(i, out long value);
            Assert.That(found, Is.True, $"Key {i} should be found");
            Assert.That(value, Is.EqualTo(i * 100), $"Key {i} should have value {i * 100}");
        }
    }

    [Test]
    public void Eviction_EvictedDataReadableViaIterator()
    {
        var lsm = new LiteLsm<long, long>(
            Path.Combine(_dataDir, "segments"),
            memTableCapacity: 20_000);

        int totalEntries = 12_000; // Exceeds node pool, triggers eviction
        for (long i = 1; i <= totalEntries; i++)
        {
            lsm.Put(i, i * 7);
        }

        // Read entire range via iterator
        using LiteLsmIterator<long, long> iter = lsm.GetIterator(1, totalEntries);
        var results = iter.ReadAll().ToList();

        Assert.That(results.Count, Is.EqualTo(totalEntries));
        Assert.That(results, Is.Ordered.By("Key"));

        // Verify values
        for (int i = 0; i < totalEntries; i++)
        {
            Assert.That(results[i].Key, Is.EqualTo(i + 1));
            Assert.That(results[i].Value, Is.EqualTo((i + 1) * 7));
        }
    }

    [Test]
    public void Eviction_RetainedSkipListCorrectAfterEviction()
    {
        var lsm = new LiteLsm<long, long>(
            Path.Combine(_dataDir, "segments"),
            memTableCapacity: 20_000);

        // Write enough to trigger eviction, then check stats
        for (long i = 1; i <= 40_000; i++)
        {
            lsm.Put(i, i);
        }

        LsmStats stats = lsm.GetStats();
        // Active memtable should have fewer entries than total (some were evicted to segments)
        Assert.That(stats.ActiveMemTableCount, Is.LessThan(40_000),
            "Some entries should have been evicted from memory");
        Assert.That(stats.NumSegments, Is.GreaterThan(0),
            "Eviction should have created at least one segment");

        // Verify all entries are still readable
        for (long i = 1; i <= 40_000; i++)
        {
            bool found = lsm.TryGet(i, out long value);
            Assert.That(found, Is.True, $"Key {i} should be found after eviction");
            Assert.That(value, Is.EqualTo(i));
        }
    }

    // [Test]
    // public void Eviction_MultipleEvictionCycles()
    // {
    //     var lsm = new LiteLsm<long, long>(
    //         Path.Combine(_dataDir, "segments"),
    //         memTableCapacity: 20_000);

    //     // Write 25,000 entries — should trigger multiple eviction cycles
    //     // (node pool is ~10K, so at least 2-3 evictions needed)
    //     int total = 25_000;
    //     for (long i = 1; i <= total; i++)
    //     {
    //         lsm.Put(i, i * 3);
    //     }

    //     // Spot-check values across the entire range
    //     for (long i = 1; i <= total; i += 1000)
    //     {
    //         bool found = lsm.TryGet(i, out long value);
    //         Assert.That(found, Is.True, $"Key {i} should be found");
    //         Assert.That(value, Is.EqualTo(i * 3));
    //     }

    //     // Check first and last
    //     Assert.That(lsm.TryGet(1, out long first), Is.True);
    //     Assert.That(first, Is.EqualTo(3));
    //     Assert.That(lsm.TryGet(total, out long last), Is.True);
    //     Assert.That(last, Is.EqualTo(total * 3));
    // }

    #endregion

    #region Truncation Tests

    [Test]
    public void Truncate_WithinSkipListOnly_RemovesPrefix()
    {
        var lsm = CreateLsm<long, long>();

        // Insert entries that stay in memory (below eviction threshold)
        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i * 10);
        }

        // Truncate first 50 entries
        int truncated = lsm.Truncate(51);
        Assert.That(truncated, Is.EqualTo(50));

        // Keys 1-50 should be gone
        for (long i = 1; i <= 50; i++)
        {
            Assert.That(lsm.TryGet(i, out _), Is.False, $"Key {i} should be truncated");
        }

        // Keys 51-100 should still be present
        for (long i = 51; i <= 100; i++)
        {
            bool found = lsm.TryGet(i, out long value);
            Assert.That(found, Is.True, $"Key {i} should still exist");
            Assert.That(value, Is.EqualTo(i * 10));
        }
    }

    [Test]
    public void Truncate_SpanningDiskAndSkipList()
    {
        // Use default capacity — eviction happens at ~10K node pool limit
        var lsm = CreateLsm<long, long>();

        // Write enough to trigger at least one eviction (>9999 entries)
        for (long i = 1; i <= 11_000; i++)
        {
            lsm.Put(i, i * 5);
        }

        var stats = lsm.GetStats();
        Assert.That(stats.NumSegments, Is.GreaterThan(0), "Should have disk segments from eviction");
        Assert.That(stats.ActiveMemTableCount, Is.GreaterThan(0), "Should have in-memory entries");

        // Truncate past all disk segments into the skiplist
        long truncateKey = 10_000;
        int truncated = lsm.Truncate(truncateKey);
        Assert.That(truncated, Is.GreaterThan(0));

        // Keys below truncateKey should be gone
        Assert.That(lsm.TryGet(1, out _), Is.False, "Key 1 should be truncated");

        // Keys at and above truncateKey should remain
        for (long i = truncateKey; i <= 11_000; i += 200)
        {
            bool found = lsm.TryGet(i, out long value);
            Assert.That(found, Is.True, $"Key {i} should still exist");
            Assert.That(value, Is.EqualTo(i * 5));
        }
    }

    [Test]
    public void Truncate_PastAllData_RemovesEverything()
    {
        var lsm = CreateLsm<long, long>();

        for (long i = 1; i <= 100; i++)
        {
            lsm.Put(i, i);
        }

        int truncated = lsm.Truncate(200);
        Assert.That(truncated, Is.EqualTo(100));

        // Everything should be gone
        for (long i = 1; i <= 100; i++)
        {
            Assert.That(lsm.TryGet(i, out _), Is.False);
        }
    }

    [Test]
    public void Truncate_NoMatchingKeys_ReturnsZero()
    {
        var lsm = CreateLsm<long, long>();

        for (long i = 100; i <= 200; i++)
        {
            lsm.Put(i, i);
        }

        // Truncate below all keys
        int truncated = lsm.Truncate(50);
        Assert.That(truncated, Is.EqualTo(0));

        // All keys still present
        for (long i = 100; i <= 200; i++)
        {
            Assert.That(lsm.TryGet(i, out _), Is.True, $"Key {i} should exist");
        }
    }

    [Test]
    public void Truncate_WritesAfterTruncation_StillWork()
    {
        var lsm = CreateLsm<long, long>();

        for (long i = 1; i <= 50; i++)
        {
            lsm.Put(i, i);
        }

        lsm.Truncate(26); // Remove keys 1-25

        // Continue writing
        for (long i = 51; i <= 100; i++)
        {
            lsm.Put(i, i * 2);
        }

        // Old retained keys
        for (long i = 26; i <= 50; i++)
        {
            Assert.That(lsm.TryGet(i, out long v), Is.True);
            Assert.That(v, Is.EqualTo(i));
        }

        // New keys
        for (long i = 51; i <= 100; i++)
        {
            Assert.That(lsm.TryGet(i, out long v), Is.True);
            Assert.That(v, Is.EqualTo(i * 2));
        }
    }

    #endregion

    #region Deletion Tests

    [Test]
    public void Delete_InMemory_KeyNoLongerFound()
    {
        var lsm = CreateLsm<long, long>();

        for (long i = 1; i <= 10; i++)
            lsm.Put(i, i * 100);

        bool deleted = lsm.Delete(5);
        Assert.That(deleted, Is.True);

        Assert.That(lsm.TryGet(5, out _), Is.False, "Deleted key should not be found");
        Assert.That(lsm.TryGet(4, out long v4), Is.True);
        Assert.That(v4, Is.EqualTo(400));
        Assert.That(lsm.TryGet(6, out long v6), Is.True);
        Assert.That(v6, Is.EqualTo(600));
    }

    [Test]
    public void Delete_OnDisk_KeyNoLongerFound()
    {
        var lsm = CreateLsm<long, long>();

        // Write enough to trigger eviction
        for (long i = 1; i <= 11_000; i++)
            lsm.Put(i, i * 10);

        // Key 100 should be on disk (evicted)
        Assert.That(lsm.TryGet(100, out long before), Is.True);
        Assert.That(before, Is.EqualTo(1000));

        bool deleted = lsm.Delete(100);
        Assert.That(deleted, Is.True);

        Assert.That(lsm.TryGet(100, out _), Is.False, "Deleted on-disk key should not be found");

        // Adjacent keys still present
        Assert.That(lsm.TryGet(99, out _), Is.True);
        Assert.That(lsm.TryGet(101, out _), Is.True);
    }

    [Test]
    public void Delete_NonExistentKey_ReturnsFalse()
    {
        var lsm = CreateLsm<long, long>();
        lsm.Put(1, 10);

        bool deleted = lsm.Delete(999);
        Assert.That(deleted, Is.False);
    }

    [Test]
    public void Delete_DoubleDelete_SecondReturnsFalse()
    {
        var lsm = CreateLsm<long, long>();
        lsm.Put(1, 10);

        Assert.That(lsm.Delete(1), Is.True);
        Assert.That(lsm.Delete(1), Is.False);
    }

    [Test]
    public void Delete_IteratorSkipsDeletedEntries()
    {
        var lsm = CreateLsm<long, long>();

        for (long i = 1; i <= 20; i++)
            lsm.Put(i, i);

        // Delete some entries
        lsm.Delete(5);
        lsm.Delete(10);
        lsm.Delete(15);

        using var iter = lsm.GetIterator(1, 20);
        var results = iter.ReadAll().ToList();

        Assert.That(results.Count, Is.EqualTo(17), "Should skip 3 deleted entries");
        Assert.That(results.Select(r => r.Key), Does.Not.Contain(5L));
        Assert.That(results.Select(r => r.Key), Does.Not.Contain(10L));
        Assert.That(results.Select(r => r.Key), Does.Not.Contain(15L));
    }

    [Test]
    public void Delete_FirstAndLastEntryInSegment()
    {
        var lsm = CreateLsm<long, long>();

        for (long i = 1; i <= 11_000; i++)
            lsm.Put(i, i);

        // Delete first and last key in the evicted segment (keys 1 and 2499)
        lsm.Delete(1);
        lsm.Delete(2499);

        Assert.That(lsm.TryGet(1, out _), Is.False);
        Assert.That(lsm.TryGet(2499, out _), Is.False);
        Assert.That(lsm.TryGet(2, out _), Is.True);
        Assert.That(lsm.TryGet(2498, out _), Is.True);
    }

    #endregion
}
