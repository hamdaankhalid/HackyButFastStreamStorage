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
        // SQLite only supports one writer — batch 512 per transaction, serialized by lock
        const int batchSize = 512;
        Parallel.For(0, DeviceCount, deviceId =>
        {
            for (int start = 0; start < WritesPerDevice; start += batchSize)
            {
                int end = Math.Min(start + batchSize, WritesPerDevice);
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
                    for (int i = start; i < end; i++)
                    {
                        pPi.Value = 1_000_000 + i;
                        cmd.ExecuteNonQuery();
                    }
                    tx.Commit();
                }
            }
        });
    }

    [Benchmark]
    public void RocksDB_ConcurrentDeviceWrites()
    {
        // Each device batches 512 writes then syncs — matches StreamDB's FlushWorker batch pattern
        const int batchSize = 512;
        Parallel.For(0, DeviceCount, deviceId =>
        {
            byte[] keyBuffer = new byte[12];
            for (int start = 0; start < WritesPerDevice; start += batchSize)
            {
                int end = Math.Min(start + batchSize, WritesPerDevice);
                using var batch = new WriteBatch();
                for (int i = start; i < end; i++)
                {
                    int sidx = deviceId;
                    long pi = 1_000_000 + i;
                    MemoryMarshal.Write(keyBuffer.AsSpan(), in sidx);
                    MemoryMarshal.Write(keyBuffer.AsSpan(4), in pi);
                    batch.Put(keyBuffer.ToArray(), _payloadBytes);
                }
                _rocksDb.Write(batch, _syncWriteOptions);
            }
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
