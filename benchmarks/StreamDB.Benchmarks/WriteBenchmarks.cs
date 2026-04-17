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
public class WriteBenchmarks
{
    private string _streamDbDir = null!;
    private string _sqliteDir = null!;
    private string _rocksDir = null!;
    private string _liteLsmDir = null!;

    private StreamDB.StreamDB _streamDb = null!;
    private SqliteConnection _sqliteConn = null!;
    private RocksDb _rocksDb = null!;
    private LiteLsm<long, BenchPayload> _liteLsm = null!;
    private WriteOptions _syncWriteOptions = null!;

    private byte[] _payloadBytes = null!;

    [Params(200_000, 1_000_000)]
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
        using SqliteCommand pragma = _sqliteConn.CreateCommand();
        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;";
        pragma.ExecuteNonQuery();
        using SqliteCommand create = _sqliteConn.CreateCommand();
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
    DbOptions options = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(options, _rocksDir);
        _syncWriteOptions = new WriteOptions().SetSync(true);

        // LiteLsm
        _liteLsmDir = Path.Combine(baseTmp, $"litelms-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_liteLsmDir);
        const int lsmCapacity = 32_768;
        _liteLsm = new LiteLsm<long, BenchPayload>(
            Path.Combine(_liteLsmDir, "segments"),
            memTableCapacity: lsmCapacity);
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
        TryDelete(_liteLsmDir);
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
    public void SQLite_SequentialWrites()
    {
        // Batch in chunks of 512 with a commit (fsync) per batch — matches StreamDB's FlushWorker pattern
        const int batchSize = 512;
        using SqliteCommand cmd = _sqliteConn.CreateCommand();
        cmd.CommandText = "INSERT INTO data (secondary_index, primary_index, version, payload) VALUES ($sidx, $pi, $ver, $payload)";
    SqliteParameter pSidx = cmd.Parameters.Add("$sidx", SqliteType.Integer);
    SqliteParameter pPi = cmd.Parameters.Add("$pi", SqliteType.Integer);
    SqliteParameter pVer = cmd.Parameters.Add("$ver", SqliteType.Integer);
    SqliteParameter pPayload = cmd.Parameters.Add("$payload", SqliteType.Blob);
        cmd.Prepare();

        pVer.Value = 1;
        pPayload.Value = _payloadBytes;

        for (int start = 0; start < RecordCount; start += batchSize)
        {
            int end = Math.Min(start + batchSize, RecordCount);
            using SqliteTransaction tx = _sqliteConn.BeginTransaction();
            cmd.Transaction = tx;
            for (int i = start; i < end; i++)
            {
                pSidx.Value = i % 4;
                pPi.Value = 1_000_000 + i;
                cmd.ExecuteNonQuery();
            }
            tx.Commit(); // fsync per batch
        }
    }

    [Benchmark]
    public void RocksDB_SequentialWrites()
    {
        // Batch in chunks of 512 with a sync write per batch — matches StreamDB's FlushWorker pattern
        const int batchSize = 512;
        byte[] keyBuffer = new byte[12];

        for (int start = 0; start < RecordCount; start += batchSize)
        {
            int end = Math.Min(start + batchSize, RecordCount);
            using var batch = new WriteBatch();
            for (int i = start; i < end; i++)
            {
                int secondaryIndex = i % 4;
                long primaryIndex = 1_000_000 + i;
                MemoryMarshal.Write(keyBuffer.AsSpan(), in secondaryIndex);
                MemoryMarshal.Write(keyBuffer.AsSpan(4), in primaryIndex);
                batch.Put(keyBuffer.ToArray(), _payloadBytes);
            }
            _rocksDb.Write(batch, _syncWriteOptions); // fsync per batch
        }
    }

    [Benchmark]
    public void LiteLsm_SequentialWrites()
    {
        var payload = new BenchPayload { Latitude = 37.77, Longitude = -122.42, Speed = 60.0f, Heading = 90.0f };
        for (int i = 0; i < RecordCount; i++)
        {
            _liteLsm.Put(1_000_000 + i, payload);
        }
    }

    private static void TryDelete(string? path)
    {
        if (path != null && Directory.Exists(path))
        {
            try { Directory.Delete(path, true); } catch { /* best effort */ }
        }
    }
}
