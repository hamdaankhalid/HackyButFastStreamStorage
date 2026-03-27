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
using BenchmarkDotNet.Running;
using Microsoft.Data.Sqlite;
using RocksDbSharp;

BenchmarkSwitcher.FromAssembly(typeof(WriteBenchmarks).Assembly).Run(args);

[StructLayout(LayoutKind.Sequential)]
public struct BenchPayload
{
    public double Latitude;
    public double Longitude;
    public float Speed;
    public float Heading;
}

[MemoryDiagnoser]
[SimpleJob(iterationCount: 3, warmupCount: 1)]
public class WriteBenchmarks
{
    private string _streamDbDir = null!;
    private string _sqliteDir = null!;
    private string _rocksDir = null!;

    private StreamDB.StreamDB _streamDb = null!;
    private SqliteConnection _sqliteConn = null!;
    private RocksDb _rocksDb = null!;
    private WriteOptions _syncWriteOptions = null!;

    private byte[] _payloadBytes = null!;

    [Params(10_000, 100_000)]
    public int RecordCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _payloadBytes = new byte[Marshal.SizeOf<BenchPayload>()];
        var payload = new BenchPayload { Latitude = 37.77, Longitude = -122.42, Speed = 60.0f, Heading = 90.0f };
        MemoryMarshal.Write(_payloadBytes, in payload);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        string baseTmp = Path.Combine(Path.GetTempPath(), "streamdb-bench");

        // StreamDB
        _streamDbDir = Path.Combine(baseTmp, $"streamdb-{Guid.NewGuid():N}");
        _streamDb = new StreamDB.StreamDB(baseDir: _streamDbDir);

        // SQLite
        _sqliteDir = Path.Combine(baseTmp, $"sqlite-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_sqliteDir);
        string sqlitePath = Path.Combine(_sqliteDir, "bench.db");
        _sqliteConn = new SqliteConnection($"Data Source={sqlitePath}");
        _sqliteConn.Open();
        using var pragma = _sqliteConn.CreateCommand();
        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;";
        pragma.ExecuteNonQuery();
        using var create = _sqliteConn.CreateCommand();
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

        // RocksDB
        _rocksDir = Path.Combine(baseTmp, $"rocks-{Guid.NewGuid():N}");
        var options = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(options, _rocksDir);
        _syncWriteOptions = new WriteOptions().SetSync(true);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _streamDb?.Dispose();
        _sqliteConn?.Close();
        _sqliteConn?.Dispose();
        _rocksDb?.Dispose();

        TryDelete(_streamDbDir);
        TryDelete(_sqliteDir);
        TryDelete(_rocksDir);
    }

    [Benchmark(Baseline = true)]
    public void StreamDB_SequentialWrites()
    {
        for (int i = 0; i < RecordCount; i++)
        {
            _streamDb.Append(
                primaryIndex: 1_000_000 + i,
                secondaryIndex: i % 4,
                version: 1,
                payload: _payloadBytes);
        }
        _streamDb.WaitForPendingWrites();
    }

    [Benchmark]
    public void StreamDB_AppendOnly()
    {
        for (int i = 0; i < RecordCount; i++)
        {
            _streamDb.Append(
                primaryIndex: 1_000_000 + i,
                secondaryIndex: i % 4,
                version: 1,
                payload: _payloadBytes);
        }
        // No WaitForPendingWrites — measures actual caller-perceived latency
    }

    [Benchmark]
    public void SQLite_SequentialWrites()
    {
        using var tx = _sqliteConn.BeginTransaction();
        using var cmd = _sqliteConn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO data (secondary_index, primary_index, version, payload) VALUES ($sidx, $pi, $ver, $payload)";
        var pSidx = cmd.Parameters.Add("$sidx", SqliteType.Integer);
        var pPi = cmd.Parameters.Add("$pi", SqliteType.Integer);
        var pVer = cmd.Parameters.Add("$ver", SqliteType.Integer);
        var pPayload = cmd.Parameters.Add("$payload", SqliteType.Blob);
        cmd.Prepare();

        pVer.Value = 1;
        pPayload.Value = _payloadBytes;

        for (int i = 0; i < RecordCount; i++)
        {
            pSidx.Value = i % 4;
            pPi.Value = 1_000_000 + i;
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    [Benchmark]
    public void RocksDB_SequentialWrites()
    {
        Span<byte> keyBuffer = stackalloc byte[12]; // 4B secondary_index + 8B primary_index
        using var batch = new WriteBatch();
        for (int i = 0; i < RecordCount; i++)
        {
            int secondaryIndex = i % 4;
            long primaryIndex = 1_000_000 + i;
            MemoryMarshal.Write(keyBuffer, in secondaryIndex);
            MemoryMarshal.Write(keyBuffer[4..], in primaryIndex);
            batch.Put(keyBuffer.ToArray(), _payloadBytes);
        }
        _rocksDb.Write(batch, _syncWriteOptions);
    }

    private static void TryDelete(string? path)
    {
        if (path != null && Directory.Exists(path))
        {
            try { Directory.Delete(path, true); } catch { /* best effort */ }
        }
    }
}

[MemoryDiagnoser]
[SimpleJob(iterationCount: 3, warmupCount: 1)]
public class ConcurrentWriteBenchmarks
{
    private string _streamDbDir = null!;
    private string _sqliteDir = null!;
    private string _rocksDir = null!;

    private StreamDB.StreamDB _streamDb = null!;
    private SqliteConnection _sqliteConn = null!;
    private readonly Lock _sqliteLock = new();
    private RocksDb _rocksDb = null!;
    private WriteOptions _syncWriteOptions = null!;

    private byte[] _payloadBytes = null!;

    [Params(8, 32)]
    public int DeviceCount { get; set; }

    private const int WritesPerDevice = 5_000;

    [GlobalSetup]
    public void Setup()
    {
        _payloadBytes = new byte[Marshal.SizeOf<BenchPayload>()];
        var payload = new BenchPayload { Latitude = 37.77, Longitude = -122.42, Speed = 60.0f, Heading = 90.0f };
        MemoryMarshal.Write(_payloadBytes, in payload);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        string baseTmp = Path.Combine(Path.GetTempPath(), "streamdb-bench-concurrent");

        _streamDbDir = Path.Combine(baseTmp, $"streamdb-{Guid.NewGuid():N}");
        _streamDb = new StreamDB.StreamDB(baseDir: _streamDbDir);

        _sqliteDir = Path.Combine(baseTmp, $"sqlite-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_sqliteDir);
        _sqliteConn = new SqliteConnection($"Data Source={Path.Combine(_sqliteDir, "bench.db")}");
        _sqliteConn.Open();
        using var pragma = _sqliteConn.CreateCommand();
        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;";
        pragma.ExecuteNonQuery();
        using var create = _sqliteConn.CreateCommand();
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

        _rocksDir = Path.Combine(baseTmp, $"rocks-{Guid.NewGuid():N}");
        var options = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(options, _rocksDir);
        _syncWriteOptions = new WriteOptions().SetSync(true);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _streamDb?.Dispose();
        _sqliteConn?.Close();
        _sqliteConn?.Dispose();
        _rocksDb?.Dispose();

        TryDelete(_streamDbDir);
        TryDelete(_sqliteDir);
        TryDelete(_rocksDir);
    }

    [Benchmark(Baseline = true)]
    public void StreamDB_ConcurrentDeviceWrites()
    {
        // Each device writes on its own thread — StreamDB shards lock-free across FasterLog instances
        Parallel.For(0, DeviceCount, deviceId =>
        {
            byte[] payload = _payloadBytes;
            for (int i = 0; i < WritesPerDevice; i++)
            {
                _streamDb.Append(
                    primaryIndex: 1_000_000 + i,
                    secondaryIndex: deviceId,
                    version: 1,
                    payload: payload);
            }
        });
        _streamDb.WaitForPendingWrites();
    }

    [Benchmark]
    public void SQLite_ConcurrentDeviceWrites()
    {
        // SQLite only supports one writer — threads batch into per-device transactions serialized by lock
        Parallel.For(0, DeviceCount, deviceId =>
        {
            lock (_sqliteLock)
            {
                using var tx = _sqliteConn.BeginTransaction();
                using var cmd = _sqliteConn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT INTO data (secondary_index, primary_index, version, payload) VALUES ($sidx, $pi, $ver, $payload)";
                var pSidx = cmd.Parameters.Add("$sidx", SqliteType.Integer);
                var pPi = cmd.Parameters.Add("$pi", SqliteType.Integer);
                cmd.Parameters.AddWithValue("$ver", 1);
                cmd.Parameters.AddWithValue("$payload", _payloadBytes);
                cmd.Prepare();

                pSidx.Value = deviceId;
                for (int i = 0; i < WritesPerDevice; i++)
                {
                    pPi.Value = 1_000_000 + i;
                    cmd.ExecuteNonQuery();
                }
                tx.Commit();
            }
        });
    }

    [Benchmark]
    public void RocksDB_ConcurrentDeviceWrites()
    {
        // Each device batches writes then syncs once — fair comparison to StreamDB's sharded approach
        Parallel.For(0, DeviceCount, deviceId =>
        {
            byte[] keyBuffer = new byte[12];
            using var batch = new WriteBatch();
            for (int i = 0; i < WritesPerDevice; i++)
            {
                int sidx = deviceId;
                long pi = 1_000_000 + i;
                MemoryMarshal.Write(keyBuffer.AsSpan(), in sidx);
                MemoryMarshal.Write(keyBuffer.AsSpan(4), in pi);
                batch.Put(keyBuffer.ToArray(), _payloadBytes);
            }
            _rocksDb.Write(batch, _syncWriteOptions);
        });
    }

    private static void TryDelete(string? path)
    {
        if (path != null && Directory.Exists(path))
        {
            try { Directory.Delete(path, true); } catch { /* best effort */ }
        }
    }
}

[MemoryDiagnoser]
[SimpleJob(iterationCount: 3, warmupCount: 1)]
public class ReadBenchmarks
{
    private string _streamDbDir = null!;
    private string _sqliteDir = null!;
    private string _rocksDir = null!;

    private StreamDB.StreamDB _streamDb = null!;
    private SqliteConnection _sqliteConn = null!;
    private RocksDb _rocksDb = null!;

    private const int TotalRecords = 100_000;
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
            var key = iter.Key();
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
                var key = iter.Key();
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
}
