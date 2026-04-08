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
public class StreamDBCheckpointTests
{
    private string _dataDir = null!;
    private StreamDB _db = null!;

    // Short interval so the checkpoint timer fires during the test
    private static readonly TimeSpan CheckpointInterval = TimeSpan.FromMilliseconds(500);

    [SetUp]
    public void SetUp()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        _db = new StreamDB(baseDir: _dataDir, checkpointInterval: CheckpointInterval, initialAdaptiveIdx: 0);
    }

    [TearDown]
    public void TearDown()
    {
        _db.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    private void AppendPayload(int secondaryIndex, long primaryIndex, float value = 1.0f, ushort version = 1)
    {
        var payload = new TestPayload { Value = value, Counter = (int)primaryIndex };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: version, payload: bytes);
    }

    private void WaitForCheckpoint() => Thread.Sleep(CheckpointInterval + CheckpointInterval);

    [Test]
    public void Checkpoint_DataReadableAfterCheckpoint()
    {
        for (int i = 0; i < 100; i++)
            AppendPayload(1, 1000 + i);

        _db.WaitForPendingWrites();
        WaitForCheckpoint();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 1000, endPrimaryIndex: 1099);
        Assert.That(results, Has.Count.EqualTo(100));
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(1000));
        Assert.That(results[99].PrimaryIndex, Is.EqualTo(1099));
    }

    [Test]
    public void Checkpoint_WritesWorkAfterCheckpoint()
    {
        for (int i = 0; i < 50; i++)
            AppendPayload(1, 1000 + i);

        _db.WaitForPendingWrites();
        WaitForCheckpoint();

        // Write more data after checkpoint
        for (int i = 0; i < 50; i++)
            AppendPayload(1, 2000 + i);

        _db.WaitForPendingWrites();

        var before = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 1000, endPrimaryIndex: 1049);
        var after = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 2000, endPrimaryIndex: 2049);

        Assert.That(before, Has.Count.EqualTo(50));
        Assert.That(after, Has.Count.EqualTo(50));
    }

    [Test]
    public void Checkpoint_MultipleCheckpointsPreserveData()
    {
        for (int i = 0; i < 50; i++)
            AppendPayload(1, 1000 + i);

        _db.WaitForPendingWrites();
        WaitForCheckpoint();

        for (int i = 0; i < 50; i++)
            AppendPayload(2, 2000 + i);

        _db.WaitForPendingWrites();
        WaitForCheckpoint();

        for (int i = 0; i < 50; i++)
            AppendPayload(3, 3000 + i);

        _db.WaitForPendingWrites();
        WaitForCheckpoint();

        var r1 = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 1000, endPrimaryIndex: 1049);
        var r2 = _db.ReadRange(secondaryIndex: 2, startPrimaryIndex: 2000, endPrimaryIndex: 2049);
        var r3 = _db.ReadRange(secondaryIndex: 3, startPrimaryIndex: 3000, endPrimaryIndex: 3049);

        Assert.That(r1, Has.Count.EqualTo(50));
        Assert.That(r2, Has.Count.EqualTo(50));
        Assert.That(r3, Has.Count.EqualTo(50));
    }

    [Test]
    public void Checkpoint_ConcurrentWritesDuringCheckpoint()
    {
        // Pre-populate some data
        for (int i = 0; i < 100; i++)
            AppendPayload(1, 1000 + i);
        _db.WaitForPendingWrites();

        // Write concurrently while checkpoint timer fires
        var writeTask = Task.Run(() =>
        {
            for (int i = 0; i < 200; i++)
                AppendPayload(2, 5000 + i);
        });

        WaitForCheckpoint();
        writeTask.Wait();
        _db.WaitForPendingWrites();

        var pre = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 1000, endPrimaryIndex: 1099);
        var concurrent = _db.ReadRange(secondaryIndex: 2, startPrimaryIndex: 5000, endPrimaryIndex: 5199);

        Assert.That(pre, Has.Count.EqualTo(100));
        Assert.That(concurrent, Has.Count.EqualTo(200));
    }

    [Test]
    public void Checkpoint_LateArrivalsPreservedAcrossCheckpoint()
    {
        // Write in order first
        for (int i = 0; i < 10; i++)
            AppendPayload(1, 1000 + i);

        _db.WaitForPendingWrites();

        // Write a late arrival (primary index below max seen)
        AppendPayload(1, 500, value: 99.0f);

        WaitForCheckpoint();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 500, endPrimaryIndex: 1009);
        Assert.That(results, Has.Count.EqualTo(11));
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(500), "Late arrival should appear first");
    }

    [Test]
    public void Checkpoint_PayloadIntegrityAfterCheckpoint()
    {
        // Write records with distinct values
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 1000 + i, value: i * 10.0f);

        _db.WaitForPendingWrites();
        WaitForCheckpoint();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 1000, endPrimaryIndex: 1019);
        Assert.That(results, Has.Count.EqualTo(20));

        for (int i = 0; i < 20; i++)
        {
            var entry = results[i];
            var payload = MemoryMarshal.Read<TestPayload>(entry.Payload);
            Assert.That(payload.Value, Is.EqualTo(i * 10.0f), $"Payload value mismatch at index {i}");
            Assert.That(payload.Counter, Is.EqualTo(1000 + i), $"Payload counter mismatch at index {i}");
        }
    }
}
