using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;
using Microsoft.Data.Sqlite;

namespace WebServer.Services
{
    /// <summary>
    /// StreamDBManager shards device writes across a fixed pool of FasterLog instances
    /// and uses a shared SQLite database for sparse timestamp→address index lookups.
    ///
    /// Each device is deterministically assigned to a shard via <c>deviceId &amp; ShardMask</c>.
    /// Multiple devices share the same log, reducing file handle and resource overhead.
    ///
    /// Write path: Append struct bytes to the device's shard, every Nth write per device insert (timestamp, address) into SQLite.
    /// Read path:  Query SQLite for the closest address ≤ startTs, scan the shard filtering by deviceId.
    ///
    /// Retention: A background timer periodically removes index entries older than the retention
    /// period and truncates each FasterLog shard up to the minimum surviving address.
    /// </summary>
    public sealed class StreamDBManager : IDisposable
    {
        // How many writes between sparse index entries. This keeps the index small while bounding scan distance.
        private const int IndexEveryN = 1 << 7;
        private const int IndexEveryNMask = IndexEveryN - 1;

        // Number of FasterLog shards – must be a power of 2.
        private const int ShardCount = 1 << 2;
        // Bit-mask for fast shard selection: deviceId & ShardMask == deviceId % ShardCount.
        private const int ShardMask = ShardCount - 1;

        private readonly string _baseDir;
        private readonly LogShard[] _shards;
        private readonly ConcurrentDictionary<ushort, int> _writeCounts = new();
        private readonly string _connectionString;
        private readonly ConcurrentQueue<SqliteConnection> _connPool = new();
        private readonly ConcurrentQueue<(ushort DeviceId, long Timestamp, long Address)> _pendingIndexInserts = new();
        private readonly Lock _flushLock = new();
        private const int FlushBatchSize = 32;
        private readonly TimeSpan _retentionPeriod;
        private readonly Timer? _retentionTimer;
        private bool _disposed;

        public StreamDBManager(string? baseDir = null, TimeSpan? retentionPeriod = null)
        {
            _baseDir = baseDir ?? "streams";
            Directory.CreateDirectory(_baseDir);

            _retentionPeriod = retentionPeriod ?? TimeSpan.FromDays(60);

            string dbPath = Path.Combine(_baseDir, Constants.StreamIndexDbName);
            _connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = dbPath,
                Pooling = false // We manage our own pool
            }.ToString();

            using var init = GetConnection();
            using var cmd = init.Connection.CreateCommand();
            cmd.CommandText =
                """
                CREATE TABLE IF NOT EXISTS stream_index (
                    device_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    log_address INTEGER NOT NULL,
                    PRIMARY KEY (device_id, timestamp)
                ) WITHOUT ROWID;
                """;
            cmd.ExecuteNonQuery();

            _shards = new LogShard[ShardCount];
            for (int i = 0; i < ShardCount; i++)
            {
                _shards[i] = new LogShard(i, _baseDir);
            }

            // After recovery, validate that the sparse index does not reference addresses
            // beyond what FasterLog actually committed. A crash can leave SQLite rows that
            // point to data that was never fsync'd in the log.
            RecoverIndex();

            // Timer dueTime/period must fit in uint.MaxValue-1 ms (~49.7 days).
            // Cap the interval to 24 hours; the retention cutoff still uses the full _retentionPeriod.
            TimeSpan timerInterval = _retentionPeriod <= TimeSpan.FromDays(49) ? _retentionPeriod : TimeSpan.FromDays(1);
            _retentionTimer = new Timer(
                _ => RunRetention(),
                null,
                timerInterval,
                timerInterval);
        }

        private PooledConnection GetConnection()
        {
            if (_connPool.TryDequeue(out var conn))
                return new PooledConnection(conn, _connPool);

            conn = new SqliteConnection(_connectionString);
            conn.Open();

            using var pragma = conn.CreateCommand();
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;";
            pragma.ExecuteNonQuery();

            return new PooledConnection(conn, _connPool);
        }

        private readonly struct PooledConnection : IDisposable
        {
            private readonly SqliteConnection connection;
            private readonly ConcurrentQueue<SqliteConnection> pool;

            public PooledConnection(SqliteConnection connection, ConcurrentQueue<SqliteConnection> pool)
            {
                this.connection = connection;
                this.pool = pool;
            }

            public SqliteConnection Connection => connection;
            public void Dispose() => pool.Enqueue(connection);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private LogShard GetShard(ushort deviceId) => _shards[deviceId & ShardMask];

        public void Append<T>(ushort deviceId, T item, long timestamp) where T : unmanaged
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            LogShard shard = GetShard(deviceId);
            long address = shard.Enqueue(item);
            int count = _writeCounts.AddOrUpdate(deviceId, 1, static (_, prev) => prev + 1);

            // since IndexEveryN is a power of 2, this is a cheap way to check if we've hit the Nth write
            if ((count & IndexEveryNMask) == 0)
            {
                _pendingIndexInserts.Enqueue((deviceId, timestamp, address));
                FlushPendingIndexInserts();
            }
        }

        public List<T> ReadRange<T>(ushort deviceId, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0) where T : unmanaged
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            long scanFrom = LookupNearestAddress(deviceId, startTs);

            LogShard shard = GetShard(deviceId);
            return shard.ScanRange(deviceId, scanFrom, startTs, endTs, getTimestamp, limit);
        }

        public Dictionary<ushort, List<T>> ReadRange<T>(IEnumerable<ushort> deviceIds, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0) where T : unmanaged
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // Group devices by shard so we can scan each shard at most once.
            var shardGroups = new Dictionary<int, (HashSet<ushort> Devices, long MinAddress)>();

            foreach (ushort deviceId in deviceIds)
            {
                int shardIndex = deviceId & ShardMask;
                long addr = LookupNearestAddress(deviceId, startTs);

                if (shardGroups.TryGetValue(shardIndex, out var group))
                {
                    group.Devices.Add(deviceId);
                    if (addr < group.MinAddress)
                        shardGroups[shardIndex] = (group.Devices, addr);
                }
                else
                {
                    shardGroups[shardIndex] = (new HashSet<ushort> { deviceId }, addr);
                }
            }

            var result = new Dictionary<ushort, List<T>>();

            foreach (var (shardIndex, (devices, minAddress)) in shardGroups)
            {
                var shardResults = _shards[shardIndex].ScanRange(devices, minAddress, startTs, endTs, getTimestamp, limit);

                foreach (var (deviceId, items) in shardResults)
                {
                    result[deviceId] = items;
                }
            }

            return result;
        }

        private void FlushPendingIndexInserts()
        {
            if (!_flushLock.TryEnter())
                return; // Another thread is already flushing

            try
            {
                if (_pendingIndexInserts.IsEmpty)
                    return;

                using var pooled = GetConnection();
                using var tx = pooled.Connection.BeginTransaction();
                using var cmd = pooled.Connection.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT OR REPLACE INTO stream_index (device_id, timestamp, log_address) VALUES ($did, $ts, $addr)";
                var pDid = cmd.Parameters.Add("$did", SqliteType.Integer);
                var pTs = cmd.Parameters.Add("$ts", SqliteType.Integer);
                var pAddr = cmd.Parameters.Add("$addr", SqliteType.Integer);
                cmd.Prepare();

                int flushed = 0;
                while (flushed < FlushBatchSize && _pendingIndexInserts.TryDequeue(out var entry))
                {
                    pDid.Value = (int)entry.DeviceId;
                    pTs.Value = entry.Timestamp;
                    pAddr.Value = entry.Address;
                    cmd.ExecuteNonQuery();
                    flushed++;
                }

                tx.Commit();
            }
            finally
            {
                _flushLock.Exit();
            }
        }

        /// <summary>
        /// Find the largest indexed timestamp ≤ startTs for this device.
        /// Returns the corresponding FasterLog address, or 0 if no index entry exists (scan from beginning).
        /// </summary>
        private long LookupNearestAddress(ushort deviceId, long startTs)
        {
            using var pooled = GetConnection();
            using var cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "SELECT log_address FROM stream_index WHERE device_id = $did AND timestamp <= $ts ORDER BY timestamp DESC LIMIT 1";
            cmd.Parameters.AddWithValue("$did", (int)deviceId);
            cmd.Parameters.AddWithValue("$ts", startTs);
            object? result = cmd.ExecuteScalar();
            return result is long addr ? addr : 0L;
        }

        /// <summary>
        /// Validates the sparse index against the actual FasterLog address ranges after recovery.
        /// For each shard, removes any index entries whose log_address falls outside the valid
        /// range [BeginAddress, TailAddress). TailAddress is used rather than SafeTailAddress
        /// because after TryRecoverLatest the tail includes all entries recovered from disk,
        /// even those not yet committed. Only addresses beyond TailAddress are truly absent.
        /// </summary>
        private void RecoverIndex()
        {
            using var pooled = GetConnection();
            using var tx = pooled.Connection.BeginTransaction();
            using var cmd = pooled.Connection.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText =
                """
                DELETE FROM stream_index
                WHERE (device_id & $mask) = $shard
                  AND (log_address < $begin OR log_address >= $tail)
                """;
            var pMask = cmd.Parameters.Add("$mask", SqliteType.Integer);
            var pShard = cmd.Parameters.Add("$shard", SqliteType.Integer);
            var pBegin = cmd.Parameters.Add("$begin", SqliteType.Integer);
            var pTail = cmd.Parameters.Add("$tail", SqliteType.Integer);
            cmd.Prepare();

            pMask.Value = ShardMask;
            for (int i = 0; i < ShardCount; i++)
            {
                pShard.Value = i;
                pBegin.Value = _shards[i].BeginAddress;
                pTail.Value = _shards[i].TailAddress;
                cmd.ExecuteNonQuery();
            }

            tx.Commit();
        }

        /// <summary>
        /// Runs the retention policy: deletes index entries older than the retention cutoff
        /// and truncates each FasterLog shard up to the minimum surviving address.
        /// </summary>
        internal void RunRetention()
        {
            long cutoffTs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - (long)_retentionPeriod.TotalSeconds;
            PurgeIndexBefore(cutoffTs);
            TruncateShards(cutoffTs);
        }

        /// <summary>
        /// Delete all sparse-index rows whose timestamp is strictly before <paramref name="cutoffTs"/>.
        /// </summary>
        private void PurgeIndexBefore(long cutoffTs)
        {
            using var pooled = GetConnection();
            using var cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM stream_index WHERE timestamp < $cutoff";
            cmd.Parameters.AddWithValue("$cutoff", cutoffTs);
            cmd.ExecuteNonQuery();

            // Checkpoint the WAL to reclaim disk space. Because pooled connections are never
            // truly closed during normal operation, SQLite's automatic end-of-connection
            // checkpoint never fires. TRUNCATE mode writes the WAL back into the main
            // database file and resets the WAL to zero length.
            using var wal = pooled.Connection.CreateCommand();
            wal.CommandText = "PRAGMA wal_checkpoint(TRUNCATE)";
            wal.ExecuteNonQuery();
        }

        /// <summary>
        /// For each shard, find the minimum surviving log_address across all devices that map
        /// to that shard, then truncate the FasterLog up to that address.
        /// </summary>
        private void TruncateShards(long cutoffTs)
        {
            // For each shard, determine the safe truncation address.
            // Query the minimum log_address still alive in the index per shard.
            using var pooled = GetConnection();

            for (int shardIndex = 0; shardIndex < ShardCount; shardIndex++)
            {
                // Find the minimum log_address among index entries that survived the purge
                // and belong to devices assigned to this shard.
                // Since we can't easily filter by shard in SQL (no shard column),
                // we use the global minimum address for this shard from all devices.
                using var cmd = pooled.Connection.CreateCommand();
                cmd.CommandText =
                    """
                    SELECT MIN(log_address) FROM stream_index
                    WHERE (device_id & $mask) = $shard
                    """;
                cmd.Parameters.AddWithValue("$mask", ShardMask);
                cmd.Parameters.AddWithValue("$shard", shardIndex);
                object? result = cmd.ExecuteScalar();

                if (result is long minAddr && minAddr > 0)
                {
                    _shards[shardIndex].TruncateUntil(minAddr);
                }
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _retentionTimer?.Dispose();

            // Flush any remaining pending index inserts
            lock (_flushLock)
            {
                if (!_pendingIndexInserts.IsEmpty)
                {
                    using var pooled = GetConnection();
                    using var tx = pooled.Connection.BeginTransaction();
                    using var cmd = pooled.Connection.CreateCommand();
                    cmd.Transaction = tx;
                    cmd.CommandText = "INSERT OR REPLACE INTO stream_index (device_id, timestamp, log_address) VALUES ($did, $ts, $addr)";
                    var pDid = cmd.Parameters.Add("$did", SqliteType.Integer);
                    var pTs = cmd.Parameters.Add("$ts", SqliteType.Integer);
                    var pAddr = cmd.Parameters.Add("$addr", SqliteType.Integer);
                    cmd.Prepare();

                    while (_pendingIndexInserts.TryDequeue(out var entry))
                    {
                        pDid.Value = (int)entry.DeviceId;
                        pTs.Value = entry.Timestamp;
                        pAddr.Value = entry.Address;
                        cmd.ExecuteNonQuery();
                    }

                    tx.Commit();
                }
            }

            foreach (var shard in _shards)
            {
                shard.Dispose();
            }

            while (_connPool.TryDequeue(out var conn))
            {
                conn.Dispose();
            }
        }

        /// <summary>
        /// A single FasterLog shard shared by multiple devices.
        /// Devices are assigned to shards via <c>deviceId &amp; ShardMask</c>.
        /// </summary>
        private sealed class LogShard : IDisposable
        {
            private readonly FasterLog _log;

            public LogShard(int shardIndex, string baseDir)
            {
                string logPath = Path.Combine(baseDir, string.Format(Constants.ShardLogFmt, shardIndex));
                var settings = new FasterLogSettings
                {
                    LogDevice = Devices.CreateLogDevice(logPath, deleteOnClose: false),
                    PageSizeBits = 22,   // 4 MB pages
                    MemorySizeBits = 24, // 16 MB in-memory
                    AutoCommit = true,
                    AutoRefreshSafeTailAddress = true,
                    TryRecoverLatest = true
                };
                _log = new FasterLog(settings);
            }

            public long Enqueue<T>(T item) where T : unmanaged
            {
                ReadOnlySpan<byte> payload = MemoryMarshal.AsBytes(new ReadOnlySpan<T>(in item));
                return _log.Enqueue(payload);
            }

            /// <summary>
            /// Scan the shard from <paramref name="fromAddress"/> and collect items belonging to
            /// <paramref name="deviceId"/> whose timestamp falls within [startTs, endTs].
            /// Delegates to the multi-device overload with a single-element set.
            /// </summary>
            public List<T> ScanRange<T>(ushort deviceId, long fromAddress, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0) where T : unmanaged
            {
                var results = ScanRange(new HashSet<ushort>(1) { deviceId }, fromAddress, startTs, endTs, getTimestamp, limit);
                return results[deviceId];
            }

            /// <summary>
            /// Scan the shard once from <paramref name="fromAddress"/> and collect items for all
            /// devices in <paramref name="deviceIds"/> whose timestamps fall within [startTs, endTs].
            /// Returns a dictionary keyed by deviceId.
            ///
            /// Because multiple devices share this shard, entries are interleaved.
            /// Per-device timestamps are monotonic, so once a device passes <paramref name="endTs"/>
            /// it is marked finished; the scan ends when every device is finished.
            /// </summary>
            public Dictionary<ushort, List<T>> ScanRange<T>(HashSet<ushort> deviceIds, long fromAddress, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0) where T : unmanaged
            {
                var results = new Dictionary<ushort, List<T>>(deviceIds.Count);
                foreach (ushort id in deviceIds)
                    results[id] = new List<T>();

                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return results;

                // Track which devices have passed endTs or hit the limit so we can stop early when all are done.
                var finished = new HashSet<ushort>();
                bool hasLimit = limit > 0;

                int itemSize = Unsafe.SizeOf<T>();
                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < itemSize)
                        continue;

                    // All telemetry structs have DeviceId (ushort) at offset 0.
                    // Fast-path filter: skip entries from other devices without deserializing the full struct.
                    ushort entryDeviceId = MemoryMarshal.Read<ushort>(entry.AsSpan(0, sizeof(ushort)));
                    if (!deviceIds.Contains(entryDeviceId) || finished.Contains(entryDeviceId))
                        continue;

                    T item = MemoryMarshal.Read<T>(entry.AsSpan(0, entryLength));
                    long ts = getTimestamp(item);

                    if (ts > endTs)
                    {
                        finished.Add(entryDeviceId);
                        if (finished.Count == deviceIds.Count)
                            break; // All devices past endTs
                        continue;
                    }

                    if (ts >= startTs)
                    {
                        results[entryDeviceId].Add(item);

                        if (hasLimit && results[entryDeviceId].Count >= limit)
                        {
                            finished.Add(entryDeviceId);
                            if (finished.Count == deviceIds.Count)
                                break;
                        }
                    }
                }

                return results;
            }

            public long BeginAddress => _log.BeginAddress;
            public long TailAddress => _log.TailAddress;

            /// <summary>
            /// Truncate the log up to the given address, freeing disk space for old entries.
            /// </summary>
            public void TruncateUntil(long untilAddress) => _log.TruncateUntil(untilAddress);

            public void Dispose()
            {
                _log.Commit(true);
                _log.Dispose();
            }
        }
    }
}
