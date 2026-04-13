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
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using RocksDbSharp;
using StreamDB;
using StreamDB.LiteLsm;

[MemoryDiagnoser]
[SimpleJob(iterationCount: 5, warmupCount: 2)]
public class ReadBenchmarks
{
    private string _streamDbDir = null!;
    private string _sqliteDir = null!;
    private string _rocksDir = null!;
    private string _liteLsmDir = null!;

    private StreamDB.StreamDB _streamDb = null!;
    private SqliteConnection _sqliteConn = null!;
    private RocksDb _rocksDb = null!;
    private LiteLsm<long, BenchPayload> _liteLsm = null!;
    private LiteLsm<long, BenchPayload> _liteLsmInMemory = null!;

    private const int TotalRecords = 500_000;
    private const int SecondaryIndexCount = 4;
    private const long BasePi = 1_000_000;

    [GlobalSetup]
    public void Setup()
    {
        string baseTmp = Path.Combine(Path.GetTempPath(), "streamdb-bench-read");
        if (Directory.Exists(baseTmp)) Directory.Delete(baseTmp, true);

        byte[] payloadBytes = new byte[Marshal.SizeOf<BenchPayload>()];
        var payload = new BenchPayload { Latitude = 37.77, Longitude = -122.42, Speed = 60.0f, Heading = 90.0f };
        MemoryMarshal.Write(payloadBytes, in payload);

        // StreamDB setup
        _streamDbDir = Path.Combine(baseTmp, "streamdb");
        _streamDb = new StreamDB.StreamDB(baseDir: _streamDbDir);
        for (int i = 0; i < TotalRecords; i++)
            _streamDb.Append(BasePi + i, i % SecondaryIndexCount, 1, payloadBytes);
        _streamDb.WaitForPendingWrites();

        // SQLite setup
        _sqliteDir = Path.Combine(baseTmp, "sqlite");
        Directory.CreateDirectory(_sqliteDir);
        _sqliteConn = new SqliteConnection($"Data Source={Path.Combine(_sqliteDir, "bench.db")}");
        _sqliteConn.Open();
        using (var pragma = _sqliteConn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;";
            pragma.ExecuteNonQuery();
        }
        using (var create = _sqliteConn.CreateCommand())
        {
            create.CommandText = """
                CREATE TABLE IF NOT EXISTS data (
                    secondary_index INTEGER NOT NULL,
                    primary_index INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (secondary_index, primary_index)
                ) WITHOUT ROWID;
                """;
            create.ExecuteNonQuery();
        }
        using (var tx = _sqliteConn.BeginTransaction())
        {
            using var cmd = _sqliteConn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO data (secondary_index, primary_index, version, payload) VALUES ($sidx, $pi, $ver, $payload)";
            var pSidx = cmd.Parameters.Add("$sidx", SqliteType.Integer);
            var pPi = cmd.Parameters.Add("$pi", SqliteType.Integer);
            cmd.Parameters.AddWithValue("$ver", 1);
            cmd.Parameters.AddWithValue("$payload", payloadBytes);
            cmd.Prepare();
            for (int i = 0; i < TotalRecords; i++)
            {
                pSidx.Value = i % SecondaryIndexCount;
                pPi.Value = BasePi + i;
                cmd.ExecuteNonQuery();
            }
            tx.Commit();
        }

        // RocksDB setup
        _rocksDir = Path.Combine(baseTmp, "rocks");
        var options = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(options, _rocksDir);
        Span<byte> keyBuffer = stackalloc byte[12];
        using (var batch = new WriteBatch())
        {
            for (int i = 0; i < TotalRecords; i++)
            {
                int sidx = i % SecondaryIndexCount;
                long pi = BasePi + i;
                MemoryMarshal.Write(keyBuffer, in sidx);
                MemoryMarshal.Write(keyBuffer[4..], in pi);
                batch.Put(keyBuffer.ToArray(), payloadBytes);
            }
            _rocksDb.Write(batch);
        }

        // LiteLsm setup
        _liteLsmDir = Path.Combine(baseTmp, "litelms");
        Directory.CreateDirectory(_liteLsmDir);
        const int lsmCapacity = 32_768;
        _liteLsm = new LiteLsm<long, BenchPayload>(
            () => new StructSkipList<long, BenchPayload>(lsmCapacity),
            Path.Combine(_liteLsmDir, "segments"),
            memTableCapacity: lsmCapacity);
        var lsmPayload = new BenchPayload { Latitude = 37.77, Longitude = -122.42, Speed = 60.0f, Heading = 90.0f };
        for (int i = 0; i < TotalRecords; i++)
            _liteLsm.Put(BasePi + i, lsmPayload);

        // LiteLsm in-memory only: capacity larger than TotalRecords so nothing flushes to disk
        var liteLsmInMemDir = Path.Combine(baseTmp, "litelms-inmem");
        Directory.CreateDirectory(liteLsmInMemDir);
        const int inMemCapacity = TotalRecords + 1;
        _liteLsmInMemory = new LiteLsm<long, BenchPayload>(
            () => new StructSkipList<long, BenchPayload>(inMemCapacity),
            Path.Combine(liteLsmInMemDir, "segments"),
            memTableCapacity: inMemCapacity);
        for (int i = 0; i < TotalRecords; i++)
            _liteLsmInMemory.Put(BasePi + i, lsmPayload);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _streamDb?.Dispose();
        _sqliteConn?.Close();
        _sqliteConn?.Dispose();
        _rocksDb?.Dispose();

        string baseTmp = Path.Combine(Path.GetTempPath(), "streamdb-bench-read");
        if (Directory.Exists(baseTmp))
        {
            try { Directory.Delete(baseTmp, true); } catch { /* best effort */ }
        }
    }

    [Benchmark(Baseline = true)]
    public int StreamDB_RangeRead()
    {
        // Read 1000 entries for secondary index 0
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000; // ~1000 entries at stride 4
        var results = _streamDb.ReadRange(secondaryIndex: 0, startPrimaryIndex: startPi, endPrimaryIndex: endPi);
        return results.Count;
    }

    [Benchmark]
    public int StreamDB_RangeReadPooled()
    {
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;
        int count = 0;
        _streamDb.ReadRangePooled(secondaryIndex: 0, startPrimaryIndex: startPi, endPrimaryIndex: endPi,
            (in StreamDB.StreamEntryView entry) =>
            {
                count++;
                return true; // continue scanning
            });
        return count;
    }

    [Benchmark]
    public int SQLite_RangeRead()
    {
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;
        using var cmd = _sqliteConn.CreateCommand();
        cmd.CommandText = "SELECT primary_index, secondary_index, version, payload FROM data WHERE secondary_index = 0 AND primary_index >= $start AND primary_index <= $end ORDER BY primary_index";
        cmd.Parameters.AddWithValue("$start", startPi);
        cmd.Parameters.AddWithValue("$end", endPi);

        int count = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int RocksDB_RangeRead()
    {
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;

        byte[] startKey = new byte[12];
        byte[] endKey = new byte[12];
        int sidx = 0;
        MemoryMarshal.Write(startKey.AsSpan(), in sidx);
        MemoryMarshal.Write(startKey.AsSpan(4), in startPi);
        MemoryMarshal.Write(endKey.AsSpan(), in sidx);
        MemoryMarshal.Write(endKey.AsSpan(4), in endPi);

        int count = 0;
        using var iter = _rocksDb.NewIterator();
        iter.Seek(startKey);
        while (iter.Valid())
        {
      byte[] key = iter.Key();
            if (key.Length < 12) break;
            int keySidx = MemoryMarshal.Read<int>(key);
            long keyPi = MemoryMarshal.Read<long>(key.AsSpan(4));
            if (keySidx != 0 || keyPi > endPi) break;
            count++;
            iter.Next();
        }
        return count;
    }

    [Benchmark]
    public int StreamDB_MultiIndexRead()
    {
        // Read across all 4 secondary indexes in one call — StreamDB groups by shard
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;
        var results = _streamDb.ReadRange(
            secondaryIndexes: new[] { 0, 1, 2, 3 },
            startPrimaryIndex: startPi,
            endPrimaryIndex: endPi);
        return results.Values.Sum(list => list.Count);
    }

    [Benchmark]
    public int SQLite_MultiIndexRead()
    {
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;
        using var cmd = _sqliteConn.CreateCommand();
        cmd.CommandText = "SELECT primary_index, secondary_index, version, payload FROM data WHERE secondary_index IN (0,1,2,3) AND primary_index >= $start AND primary_index <= $end ORDER BY secondary_index, primary_index";
        cmd.Parameters.AddWithValue("$start", startPi);
        cmd.Parameters.AddWithValue("$end", endPi);

        int count = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int RocksDB_MultiIndexRead()
    {
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;
        int count = 0;

        // RocksDB must do a separate seek + scan per secondary index
        for (int sidx = 0; sidx < SecondaryIndexCount; sidx++)
        {
            byte[] startKey = new byte[12];
            MemoryMarshal.Write(startKey.AsSpan(), in sidx);
            MemoryMarshal.Write(startKey.AsSpan(4), in startPi);

            using var iter = _rocksDb.NewIterator();
            iter.Seek(startKey);
            while (iter.Valid())
            {
        byte[] key = iter.Key();
                if (key.Length < 12) break;
                int keySidx = MemoryMarshal.Read<int>(key);
                long keyPi = MemoryMarshal.Read<long>(key.AsSpan(4));
                if (keySidx != sidx || keyPi > endPi) break;
                count++;
                iter.Next();
            }
        }
        return count;
    }

    [Benchmark]
    public int LiteLsm_RangeRead()
    {
        // LiteLsm has no secondary indexes — query ~1000 entries to match other single-index reads
        long startPi = BasePi + 10_000;
        long endPi = startPi + 1000;
        using var iter = _liteLsm.GetIterator(startPi, endPi);
        int count = 0;
        foreach (var _ in iter.ReadAll()) count++;
        return count;
    }

    [Benchmark]
    public int LiteLsm_MultiKeyRead()
    {
        // Comparable to multi-index reads: query ~4000 entries
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;
        using var iter = _liteLsm.GetIterator(startPi, endPi);
        int count = 0;
        foreach (var _ in iter.ReadAll()) count++;
        return count;
    }

    [Benchmark]
    public int LiteLsm_InMemory_RangeRead()
    {
        // All data in memtable — no disk I/O
        long startPi = BasePi + 10_000;
        long endPi = startPi + 1000;
        using var iter = _liteLsmInMemory.GetIterator(startPi, endPi);
        int count = 0;
        foreach (var _ in iter.ReadAll()) count++;
        return count;
    }

    [Benchmark]
    public int LiteLsm_InMemory_MultiKeyRead()
    {
        long startPi = BasePi + 10_000;
        long endPi = startPi + 4000;
        using var iter = _liteLsmInMemory.GetIterator(startPi, endPi);
        int count = 0;
        foreach (var _ in iter.ReadAll()) count++;
        return count;
    }
}
