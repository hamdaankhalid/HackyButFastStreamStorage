using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
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
[SimpleJob(RuntimeMoniker.Net90, iterationCount: 3, warmupCount: 1)]
public class WriteBenchmarks
{
    private string _streamDbDir = null!;
    private string _sqliteDir = null!;
    private string _rocksDir = null!;

    private StreamDB.StreamDB _streamDb = null!;
    private SqliteConnection _sqliteConn = null!;
    private RocksDb _rocksDb = null!;

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
        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
        pragma.ExecuteNonQuery();
        using var create = _sqliteConn.CreateCommand();
        create.CommandText = """
            CREATE TABLE IF NOT EXISTS data (
                secondary_index INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                version INTEGER NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY (secondary_index, timestamp)
            ) WITHOUT ROWID;
            """;
        create.ExecuteNonQuery();

        // RocksDB
        _rocksDir = Path.Combine(baseTmp, $"rocks-{Guid.NewGuid():N}");
        var options = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(options, _rocksDir);
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
                secondaryIndex: i % 4,
                payload: _payloadBytes,
                timestamp: 1_000_000 + i,
                version: 1);
        }
        _streamDb.WaitForPendingWrites();
    }

    [Benchmark]
    public void SQLite_SequentialWrites()
    {
        using var tx = _sqliteConn.BeginTransaction();
        using var cmd = _sqliteConn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO data (secondary_index, timestamp, version, payload) VALUES ($sidx, $ts, $ver, $payload)";
        var pSidx = cmd.Parameters.Add("$sidx", SqliteType.Integer);
        var pTs = cmd.Parameters.Add("$ts", SqliteType.Integer);
        var pVer = cmd.Parameters.Add("$ver", SqliteType.Integer);
        var pPayload = cmd.Parameters.Add("$payload", SqliteType.Blob);
        cmd.Prepare();

        pVer.Value = 1;
        pPayload.Value = _payloadBytes;

        for (int i = 0; i < RecordCount; i++)
        {
            pSidx.Value = i % 4;
            pTs.Value = 1_000_000 + i;
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    [Benchmark]
    public void RocksDB_SequentialWrites()
    {
        Span<byte> keyBuffer = stackalloc byte[12]; // 4B secondary_index + 8B timestamp
        using var batch = new WriteBatch();
        for (int i = 0; i < RecordCount; i++)
        {
            int secondaryIndex = i % 4;
            long timestamp = 1_000_000 + i;
            MemoryMarshal.Write(keyBuffer, in secondaryIndex);
            MemoryMarshal.Write(keyBuffer[4..], in timestamp);
            batch.Put(keyBuffer.ToArray(), _payloadBytes);
        }
        _rocksDb.Write(batch);
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
[SimpleJob(RuntimeMoniker.Net90, iterationCount: 3, warmupCount: 1)]
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
    private const long BaseTs = 1_000_000;

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
            _streamDb.Append(i % SecondaryIndexCount, payloadBytes, BaseTs + i, 1);
        _streamDb.WaitForPendingWrites();

        // SQLite setup
        _sqliteDir = Path.Combine(baseTmp, "sqlite");
        Directory.CreateDirectory(_sqliteDir);
        _sqliteConn = new SqliteConnection($"Data Source={Path.Combine(_sqliteDir, "bench.db")}");
        _sqliteConn.Open();
        using (var pragma = _sqliteConn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
            pragma.ExecuteNonQuery();
        }
        using (var create = _sqliteConn.CreateCommand())
        {
            create.CommandText = """
                CREATE TABLE IF NOT EXISTS data (
                    secondary_index INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (secondary_index, timestamp)
                ) WITHOUT ROWID;
                """;
            create.ExecuteNonQuery();
        }
        using (var tx = _sqliteConn.BeginTransaction())
        {
            using var cmd = _sqliteConn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO data (secondary_index, timestamp, version, payload) VALUES ($sidx, $ts, $ver, $payload)";
            var pSidx = cmd.Parameters.Add("$sidx", SqliteType.Integer);
            var pTs = cmd.Parameters.Add("$ts", SqliteType.Integer);
            cmd.Parameters.AddWithValue("$ver", 1);
            cmd.Parameters.AddWithValue("$payload", payloadBytes);
            cmd.Prepare();
            for (int i = 0; i < TotalRecords; i++)
            {
                pSidx.Value = i % SecondaryIndexCount;
                pTs.Value = BaseTs + i;
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
                long ts = BaseTs + i;
                MemoryMarshal.Write(keyBuffer, in sidx);
                MemoryMarshal.Write(keyBuffer[4..], in ts);
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
        long startTs = BaseTs + 10_000;
        long endTs = startTs + 4000; // ~1000 entries at stride 4
        var results = _streamDb.ReadRange(secondaryIndex: 0, startTs: startTs, endTs: endTs);
        return results.Count;
    }

    [Benchmark]
    public int SQLite_RangeRead()
    {
        long startTs = BaseTs + 10_000;
        long endTs = startTs + 4000;
        using var cmd = _sqliteConn.CreateCommand();
        cmd.CommandText = "SELECT timestamp, secondary_index, version, payload FROM data WHERE secondary_index = 0 AND timestamp >= $start AND timestamp <= $end ORDER BY timestamp";
        cmd.Parameters.AddWithValue("$start", startTs);
        cmd.Parameters.AddWithValue("$end", endTs);

        int count = 0;
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) count++;
        return count;
    }

    [Benchmark]
    public int RocksDB_RangeRead()
    {
        long startTs = BaseTs + 10_000;
        long endTs = startTs + 4000;

        byte[] startKey = new byte[12];
        byte[] endKey = new byte[12];
        int sidx = 0;
        MemoryMarshal.Write(startKey.AsSpan(), in sidx);
        MemoryMarshal.Write(startKey.AsSpan(4), in startTs);
        MemoryMarshal.Write(endKey.AsSpan(), in sidx);
        MemoryMarshal.Write(endKey.AsSpan(4), in endTs);

        int count = 0;
        using var iter = _rocksDb.NewIterator();
        iter.Seek(startKey);
        while (iter.Valid())
        {
            var key = iter.Key();
            if (key.Length < 12) break;
            int keySidx = MemoryMarshal.Read<int>(key);
            long keyTs = MemoryMarshal.Read<long>(key.AsSpan(4));
            if (keySidx != 0 || keyTs > endTs) break;
            count++;
            iter.Next();
        }
        return count;
    }
}
