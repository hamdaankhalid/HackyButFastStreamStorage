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
public class StreamDBFloorIndexTests
{
    private string _dataDir = null!;

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private static void AppendPayload(StreamDB db, int secondaryIndex, long primaryIndex, float value = 1.0f, ushort version = 1)
    {
        var payload = new TestPayload { Value = value, Counter = (int)primaryIndex };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: version, payload: bytes);
    }

    [Test]
    public void FirstWrite_IsIndexed_AndReadableWithoutFullScan()
    {
        // With initialAdaptiveIdx: 0 the spacing is 16 — only every 16th write gets indexed.
        // The first write should be force-indexed regardless.
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        using var db = new StreamDB(baseDir: _dataDir, initialAdaptiveIdx: 0);

        AppendPayload(db, secondaryIndex: 42, primaryIndex: 1000);
        db.WaitForPendingWrites();

    List<StreamEntry> results = db.ReadRange(secondaryIndex: 42, startPrimaryIndex: 1000, endPrimaryIndex: 1000);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(1000));
    }

    [Test]
    public void FirstWrite_MultipleSecondaryIndexes_AllIndexed()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        using var db = new StreamDB(baseDir: _dataDir, initialAdaptiveIdx: 0);

        // Write a single entry to each of several secondary indexes
        for (int idx = 0; idx < 10; idx++)
            AppendPayload(db, secondaryIndex: idx, primaryIndex: 500 + idx);

        db.WaitForPendingWrites();

    // All should be readable via their respective backward index hits
    Dictionary<int, List<StreamEntry>> results = db.ReadRange(
            Enumerable.Range(0, 10),
            startPrimaryIndex: 500,
            endPrimaryIndex: 509);

        Assert.That(results.Keys, Has.Count.EqualTo(10));
        for (int idx = 0; idx < 10; idx++)
        {
            Assert.That(results[idx], Has.Count.EqualTo(1));
            Assert.That(results[idx][0].PrimaryIndex, Is.EqualTo(500 + idx));
        }
    }

    [Test]
    public void AfterRetention_FloorEntries_PreventFullScan()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");

        // Use a very short retention so we can trigger it manually.
        // Write data with primary indexes as unix timestamps.
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long oldPi = now - 120; // 2 minutes ago
        long recentPi = now - 10; // 10 seconds ago

        using var db = new StreamDB(
            baseDir: _dataDir,
            retentionPeriod: TimeSpan.FromSeconds(60),
            initialAdaptiveIdx: 0);

        // Write exactly 15 old entries so that with spacing=16, count=16
        // lands on the first recent entry — guaranteeing it gets indexed
        // and survives truncation as the floor entry.
        for (int i = 0; i < 15; i++)
            AppendPayload(db, secondaryIndex: 1, primaryIndex: oldPi + i);

        for (int i = 0; i < 20; i++)
            AppendPayload(db, secondaryIndex: 1, primaryIndex: recentPi + i);

        db.WaitForPendingWrites();

    // Verify all recent data is readable before retention
    List<StreamEntry> beforeRetention = db.ReadRange(secondaryIndex: 1, startPrimaryIndex: recentPi, endPrimaryIndex: recentPi + 19);
        Assert.That(beforeRetention, Has.Count.EqualTo(20));

        // Run retention — should purge old data and re-insert floor entries
        db.RunRetention();

    // Recent data should still be fully readable after retention
    List<StreamEntry> afterRetention = db.ReadRange(secondaryIndex: 1, startPrimaryIndex: recentPi, endPrimaryIndex: recentPi + 19);
        Assert.That(afterRetention, Has.Count.EqualTo(20));
        Assert.That(afterRetention[0].PrimaryIndex, Is.EqualTo(recentPi));
    }

    [Test]
    public void AfterRetention_MultiIndex_FloorEntries_AllReadable()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");

        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long oldPi = now - 120;
        long recentPi = now - 10;

        using var db = new StreamDB(
            baseDir: _dataDir,
            retentionPeriod: TimeSpan.FromSeconds(60),
            initialAdaptiveIdx: 0);

        // Write 15 old + 20 recent entries per secondary index so count=16 lands on
        // the first recent entry, ensuring it gets indexed and survives truncation.
        for (int idx = 0; idx < 5; idx++)
        {
            for (int i = 0; i < 15; i++)
                AppendPayload(db, secondaryIndex: idx, primaryIndex: oldPi + i);
            for (int i = 0; i < 20; i++)
                AppendPayload(db, secondaryIndex: idx, primaryIndex: recentPi + i);
        }

        db.WaitForPendingWrites();

        db.RunRetention();

    // All secondary indexes should still return recent data after retention
    Dictionary<int, List<StreamEntry>> results = db.ReadRange(
            Enumerable.Range(0, 5),
            startPrimaryIndex: recentPi,
            endPrimaryIndex: recentPi + 19);

        Assert.That(results.Keys, Has.Count.EqualTo(5));
        foreach ((int idx, List<StreamEntry>? entries) in results)
        {
            Assert.That(entries, Has.Count.EqualTo(20), $"Secondary index {idx} should have 20 entries after retention");
        }
    }

    [Test]
    public void GetEarliestPrimaryIndex_AfterRetention_FindsData()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");

        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long oldPi = now - 120;
        long recentPi = now - 10;

        using var db = new StreamDB(
            baseDir: _dataDir,
            retentionPeriod: TimeSpan.FromSeconds(60),
            initialAdaptiveIdx: 0);

        for (int i = 0; i < 15; i++)
            AppendPayload(db, secondaryIndex: 7, primaryIndex: oldPi + i);
        for (int i = 0; i < 20; i++)
            AppendPayload(db, secondaryIndex: 7, primaryIndex: recentPi + i);

        db.WaitForPendingWrites();
        db.RunRetention();

        // Should find the earliest surviving entry (at or near the retention cutoff)
        long? earliest = db.GetEarliestPrimaryIndex(new[] { 7 }, recentPi);
        Assert.That(earliest, Is.Not.Null);
        Assert.That(earliest!.Value, Is.GreaterThanOrEqualTo(recentPi));
        Assert.That(earliest!.Value, Is.LessThanOrEqualTo(recentPi + 19));
    }
}
