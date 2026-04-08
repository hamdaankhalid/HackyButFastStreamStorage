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
public class StreamDBBasicTests
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

    private void AppendPayload(int secondaryIndex, long primaryIndex, float value = 1.0f, ushort version = 1)
    {
        var payload = new TestPayload { Value = value, Counter = (int)primaryIndex };
        ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload));
        _db.Append(primaryIndex: primaryIndex, secondaryIndex: secondaryIndex, version: version, payload: bytes);
    }

    [Test]
    public void Append_And_ReadRange_BasicRoundTrip()
    {
        for (int i = 0; i < 10; i++)
            AppendPayload(1, 100 + i);

        _db.WaitForPendingWrites();
        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 109);

        Assert.That(results, Has.Count.EqualTo(10));
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(100));
        Assert.That(results[9].PrimaryIndex, Is.EqualTo(109));
    }

    [Test]
    public void ReadRange_EmptyRange_ReturnsEmpty()
    {
        AppendPayload(1, 100);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 200, endPrimaryIndex: 300);
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ReadRange_NoMatchingSecondaryIndex_ReturnsEmpty()
    {
        AppendPayload(1, 100);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 999, startPrimaryIndex: 0, endPrimaryIndex: 200);
        Assert.That(results, Is.Empty);
    }

    [Test]
    public void ReadRange_WithLimit()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i);

        _db.WaitForPendingWrites();
        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 200, limit: 5);

        Assert.That(results, Has.Count.EqualTo(5));
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(100));
        Assert.That(results[4].PrimaryIndex, Is.EqualTo(104));
    }

    [Test]
    public void ReadRange_BoundaryPrimaryIndexes_Inclusive()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 300);
        _db.WaitForPendingWrites();

        // Exact boundary match
        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 300);
        Assert.That(results, Has.Count.EqualTo(3));

        // Start at exact primary index
        results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 200, endPrimaryIndex: 300);
        Assert.That(results, Has.Count.EqualTo(2));

        // End at exact primary index
        results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 200);
        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public void ReadRange_SingleEntry()
    {
        AppendPayload(1, 100);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 100);
        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].PrimaryIndex, Is.EqualTo(100));
    }

    [Test]
    public void ReadRange_PreservesPayload()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        AppendPayload(1, 100, value: 42.5f);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 100);
        Assert.That(results, Has.Count.EqualTo(1));

        TestPayload p = registry.Deserialize<TestPayload>(results[0]);
        Assert.That(p.Value, Is.EqualTo(42.5f).Within(0.001f));
        Assert.That(p.Counter, Is.EqualTo(100));
    }

    [Test]
    public void ReadRange_MultipleVersions()
    {
        AppendPayload(1, 100, version: 1);
        AppendPayload(1, 200, version: 2);
        _db.WaitForPendingWrites();

        var results = _db.ReadRange(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 200);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[0].Version, Is.EqualTo(1));
        Assert.That(results[1].Version, Is.EqualTo(2));
    }

    [Test]
    public void ReadRangePooled_InvokesHandlerForEachEntry()
    {
        for (int i = 0; i < 10; i++)
            AppendPayload(1, 100 + i);

        _db.WaitForPendingWrites();

        var collected = new List<(long Pi, int Idx, ushort Ver)>();
        _db.ReadRangePooled(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 109,
            (in StreamEntryView entry) =>
            {
                collected.Add((entry.PrimaryIndex, entry.SecondaryIndex, entry.Version));
                return true;
            });

        Assert.That(collected, Has.Count.EqualTo(10));
        Assert.That(collected[0].Pi, Is.EqualTo(100));
        Assert.That(collected[9].Pi, Is.EqualTo(109));
    }

    [Test]
    public void ReadRangePooled_HandlerCanStopEarly()
    {
        for (int i = 0; i < 20; i++)
            AppendPayload(1, 100 + i);

        _db.WaitForPendingWrites();

        int count = 0;
        _db.ReadRangePooled(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 200,
            (in StreamEntryView entry) =>
            {
                count++;
                return count < 5; // stop after 5
            });

        Assert.That(count, Is.EqualTo(5));
    }

    [Test]
    public void ReadRangePooled_PayloadIsReadable()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        AppendPayload(1, 100, value: 77.7f);
        _db.WaitForPendingWrites();

        float capturedValue = 0;
        _db.ReadRangePooled(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 100,
            (in StreamEntryView entry) =>
            {
                var p = MemoryMarshal.Read<TestPayload>(entry.Payload);
                capturedValue = p.Value;
                return true;
            });

        Assert.That(capturedValue, Is.EqualTo(77.7f).Within(0.01f));
    }

    [Test]
    public void ReadRangePooled_IncludesLateArrivals()
    {
        AppendPayload(1, 100);
        AppendPayload(1, 200);
        AppendPayload(1, 300);
        AppendPayload(1, 150); // late arrival
        _db.WaitForPendingWrites();

        var primaryIndexes = new List<long>();
        _db.ReadRangePooled(secondaryIndex: 1, startPrimaryIndex: 100, endPrimaryIndex: 300,
            (in StreamEntryView entry) =>
            {
                primaryIndexes.Add(entry.PrimaryIndex);
                return true;
            });

        Assert.That(primaryIndexes, Has.Count.EqualTo(4));
        // Should be in primary index order with late arrival merged in
        Assert.That(primaryIndexes, Is.EqualTo(new long[] { 100, 150, 200, 300 }));
    }
}
