using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;
using Microsoft.Data.Sqlite;

namespace WebServer.Storage
{
    /// <summary>
    /// Schema-on-read stream storage engine. Shards writes across a fixed pool of FasterLog
    /// instances and uses a shared SQLite database for sparse timestamp→address index lookups.
    ///
    /// Records are stored with a 16-byte header:
    /// [8B timestamp (primary index)] [4B secondary index] [2B version] [2B payload length] [payload bytes]
    ///
    /// The secondary index is used for sharding (<c>secondaryIndex &amp; ShardMask</c>) and scan filtering.
    /// For telemetry data the secondary index stores the device ID.
    ///
    /// Write path: Append header + payload bytes to the shard, every Nth write insert (timestamp, address) into SQLite.
    /// Read path:  Query SQLite for the closest address ≤ startTs, scan the shard filtering by secondary index.
    ///
    /// Retention: A background timer periodically removes index entries older than the retention
    /// period and truncates each FasterLog shard up to the minimum surviving address.
    /// </summary>
    public sealed class StreamDB : IDisposable
    {
        #region member variables and Consts

        private const string StreamIndexDbName = "stream_index.db";
        private const string ShardLogFmt = "shard_{0}.log";

        // Adaptive indexing parameters - dynamically adjusted based on queue backpressure

        /*
        Adaptive Algorithm Understanding:
        Depth of the queue tells us whether the SQLite writer is keeping with rate of incoming writes.
        If the queue is growing, it means the writer is falling behind. To help the writer we can reduce the frequency of index inserts.
        We also increase the size of transaction batching to amortize the cost of each commit over more entries. 
        This allows the writer to catch up during high load.
        */
        private readonly (int indexSpacing, int batchSize, int indexMask)[] AdaptiveTuning =
        {
            (1 << 4, 1 << 3, (1 << 4) - 1),    // idx 0: high frequency, small batches
            (1 << 5, 1 << 4, (1 << 5) - 1),   // idx 1
            (1 << 6, 1 << 5, (1 << 6) - 1),   // idx 2
            (1 << 7, 1 << 6, (1 << 7) - 1),   // idx 3
            (1 << 8, 1 << 7, (1 << 8) - 1),  // idx 4
            (1 << 9, 1 << 8, (1 << 9) - 1),  // idx 5
            (1 << 10, 1 << 9, (1 << 10) - 1), // idx 6: low frequency, big batches
            (1 << 11, 1 << 10, (1 << 11) - 1), // idx 7: very low frequency, very big batches
            (1 << 12, 1 << 11, (1 << 12) - 1) // idx 8: ultra low frequency, ultra big batches
        };

        // Now I only need to move this slider which points to readonly data.
        // Since int is atomic I can do this without even interlocked.
        // This can be changed without any synchronization.
        private int _adaptiveIdx = 6;  // Start more conservative for heavy concurrent loads

        // Bounded queue parameters
        private const int QueueCapacity = 2048;       // Maximum queue size
        private const int QueueHighWaterMark = 1024;  // 50% - start reducing index frequency
        private const int QueueLowWaterMark = 512;    // 25% - start increasing index frequency (raised to prevent oscillation)

        // Number of FasterLog shards – must be a power of 2.
        private const int ShardCount = 1 << 2;
        // Bit-mask for fast shard selection: deviceId & ShardMask == deviceId % ShardCount.
        private const int ShardMask = ShardCount - 1;

        private readonly string _baseDir;
        private readonly LogShard[] _shards;
        private readonly ConcurrentDictionary<int, int> _writeCounts = new();
        private readonly string _connectionString;
        private readonly ConcurrentQueue<SqliteConnection> _connPool = new();
        private readonly ConcurrentQueue<(int DeviceId, long Timestamp, long Address)> _pendingIndexInserts = new();
        private readonly TimeSpan _retentionPeriod;
        private readonly Timer _retentionTimer;
        private readonly Timer _checkpointTimer;
        private readonly TimeSpan _checkpointInterval;
        private readonly ILogger<StreamDB>? _logger;
        private readonly Task _bgFlushRunner;
        private readonly ManualResetEventSlim _flushSignal = new(false);
        private readonly Lock _maintenanceLock = new(); // Ensures only one maintenance operation at a time
        private readonly ReaderWriterLockSlim _indexWriteLock = new(LockRecursionPolicy.NoRecursion);

        // Interlocked counter for pending index inserts. This is cheaper than calling _pendingIndexInserts.Count which is O(n) on ConcurrentQueue.
        private int _pendingIndexCount = 0;

        private bool _disposed;

        // Statistics tracking
        private long _scaleUpCount = 0;    // Number of times we scaled up (reduced indexing frequency)
        private long _scaleDownCount = 0;  // Number of times we scaled down (increased indexing frequency)
        private long _droppedIndexEntries = 0; // Number of index entries dropped due to queue full

        // Hysteresis: prevent rapid oscillation by requiring multiple batches before adjusting
        private int _batchesSinceLastAdjustment = 0;
        private const int MinBatchesBetweenAdjustments = 5;

        #endregion

        public StreamDB(string? baseDir = null, TimeSpan? retentionPeriod = null, TimeSpan? checkpointInterval = null, ILogger<StreamDB>? logger = null)
        {
            _logger = logger;
            _baseDir = baseDir ?? "streams";
            Directory.CreateDirectory(_baseDir);

            _retentionPeriod = retentionPeriod ?? TimeSpan.FromDays(60);
            _checkpointInterval = checkpointInterval ?? TimeSpan.FromHours(1);

            string dbPath = Path.Combine(_baseDir, StreamIndexDbName);
            _connectionString = new SqliteConnectionStringBuilder
            {
                DataSource = dbPath,
                Pooling = false // We manage our own pool
            }.ToString();

            using PooledConnection init = GetConnection();
            using SqliteCommand cmd = init.Connection.CreateCommand();
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

            // Maybe we can compact and do other maintenance ops on sqlite at this point?

            // Timer dueTime/period must fit in uint.MaxValue-1 ms (~49.7 days).
            TimeSpan retentionInterval = TimeSpan.FromDays(1);

            _retentionTimer = new Timer(
                _ => RunRetention(),
                null,
                retentionInterval,
                retentionInterval);

            // Periodic checkpoint timer to control write barriers
            _checkpointTimer = new Timer(
                _ => RunCheckpoint(),
                null,
                _checkpointInterval,
                _checkpointInterval);

            _bgFlushRunner = Task.Factory.StartNew(FlushWorker, TaskCreationOptions.LongRunning);
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
            using PooledConnection pooled = GetConnection();
            using SqliteTransaction tx = pooled.Connection.BeginTransaction();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText =
                """
                DELETE FROM stream_index
                WHERE (device_id & $mask) = $shard
                  AND (log_address < $begin OR log_address >= $tail)
                """;
            SqliteParameter pMask = cmd.Parameters.Add("$mask", SqliteType.Integer);
            SqliteParameter pShard = cmd.Parameters.Add("$shard", SqliteType.Integer);
            SqliteParameter pBegin = cmd.Parameters.Add("$begin", SqliteType.Integer);
            SqliteParameter pTail = cmd.Parameters.Add("$tail", SqliteType.Integer);
            cmd.Prepare();

            pMask.Value = ShardMask;
            for (int i = 0; i < ShardCount; i++)
            {
                pShard.Value = i;
                pBegin.Value = _shards[i].BeginAddress;
                pTail.Value = _shards[i].TailAddress;
                cmd.ExecuteNonQuery();

                _logger?.LogInformation("RecoverIndex: shard {ShardIndex} -> valid address range [{Begin}, {Tail})",
                    i, pBegin.Value, pTail.Value);
            }

            tx.Commit();
        }

        #region WriteMethods

        /// <summary>
        /// Background worker that processes batched index inserts.
        /// Wakes up when signaled and drains the pending queue in batches,
        /// ensuring log writes are never blocked by SQLite operations.
        /// Adaptively adjusts indexing frequency and batch size based on queue backpressure.
        /// </summary>
        private void FlushWorker()
        {
            // long lived allocations in the worker to avoid repeated allocations in the hot path
            List<(int DeviceId, long Timestamp, long Address)> batch = new List<(int DeviceId, long Timestamp, long Address)>(capacity: AdaptiveTuning[^1].batchSize);
            HashSet<int> referencedShards = new HashSet<int>(capacity: ShardCount);
            var maxAddrPerShard = new long[ShardCount];

            while (!_disposed)
            {
                // Wait for signal or timeout (5 seconds) to check for pending work
                _flushSignal.Wait(TimeSpan.FromSeconds(5));
                _flushSignal.Reset();

                if (_disposed)
                    break;

                // Process all pending batches until queue is empty
                while (!_pendingIndexInserts.IsEmpty && !_disposed)
                {
                    // Check tuning on every batch for responsiveness (before acquiring lock)
                    AdaptivelyTuneParameters();

                    // Acquire read lock - allows concurrent flushes but blocks during checkpoint (write lock)
                    // Use blocking lock to ensure queue continues draining even during checkpoints
                    _indexWriteLock.EnterReadLock();
                    try
                    {
                        // Collect a batch of entries and ensure FasterLog durability before SQLite indexing
                        batch.Clear();
                        referencedShards.Clear();

                        try
                        {
                            // Collect a batch of entries to process
                            int adaptiveIdx = Volatile.Read(ref _adaptiveIdx);
                            (int _, int batchSize, int _) = AdaptiveTuning[adaptiveIdx];

                            int collected = 0;
                            while (collected < batchSize && _pendingIndexInserts.TryDequeue(out (int DeviceId, long Timestamp, long Address) entry))
                            {
                                // Successfully dequeued - decrement count immediately
                                Interlocked.Decrement(ref _pendingIndexCount);

                                batch.Add(entry);
                                var shardIndex = entry.DeviceId & ShardMask;
                                referencedShards.Add(shardIndex);
                                if (entry.Address > maxAddrPerShard[shardIndex])
                                {
                                    maxAddrPerShard[shardIndex] = entry.Address;
                                }

                                collected++;
                            }

                            if (batch.Count == 0)
                            {
                                continue;
                            }

                            // Ensure all referenced shards have committed their entries to disk
                            // This guarantees that when we write an index entry pointing to address X,
                            // the data at address X is already durable
                            foreach (int shardIndex in referencedShards)
                            {
                                // Sync-over-async is okay here because the worker thread can block without impacting the main write path.
                                _shards[shardIndex]
                                    .CommitAndWait(maxAddrPerShard[shardIndex])
                                    .AsTask()
                                    .GetAwaiter()
                                    .GetResult();
                            }

                            // Now write to SQLite - entries are guaranteed to be durable in FasterLog
                            using PooledConnection pooled = GetConnection();
                            using SqliteTransaction tx = pooled.Connection.BeginTransaction();
                            using SqliteCommand cmd = pooled.Connection.CreateCommand();
                            cmd.Transaction = tx;
                            cmd.CommandText = "INSERT INTO stream_index (device_id, timestamp, log_address) VALUES ($did, $ts, $addr)";
                            SqliteParameter pDid = cmd.Parameters.Add("$did", SqliteType.Integer);
                            SqliteParameter pTs = cmd.Parameters.Add("$ts", SqliteType.Integer);
                            SqliteParameter pAddr = cmd.Parameters.Add("$addr", SqliteType.Integer);
                            cmd.Prepare();

                            int written = 0;
                            foreach ((int DeviceId, long Timestamp, long Address) entry in batch)
                            {
                                pDid.Value = entry.DeviceId;
                                pTs.Value = entry.Timestamp;
                                pAddr.Value = entry.Address;
                                cmd.ExecuteNonQuery();
                                written++;
                            }

                            tx.Commit();
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, "FlushWorker: error writing index batch");
                            // Continue processing despite errors
                        }
                    }
                    finally
                    {
                        // Always release read lock, even on error
                        _indexWriteLock.ExitReadLock();
                    }
                }
            }
        }

        /// <summary>
        /// Dynamically adjusts IndexEveryN and FlushBatchSize based on queue depth.
        /// High queue depth (backpressure) → decrease indexing frequency (increase N) and increase batch size
        /// Low queue depth → increase indexing frequency (decrease N) and decrease batch size for lower latency
        /// Uses hysteresis (batch counter) to prevent rapid oscillation.
        /// </summary>
        private void AdaptivelyTuneParameters()
        {
            int queueDepth = Volatile.Read(ref _pendingIndexCount);
            int currAdaptiveIdx = Volatile.Read(ref _adaptiveIdx);

            // Increment batch counter (hysteresis)
            int batchesSinceAdj = Interlocked.Increment(ref _batchesSinceLastAdjustment);

            // Only adjust if we've processed enough batches since last adjustment
            if (batchesSinceAdj < MinBatchesBetweenAdjustments)
            {
                return;
            }

            if (queueDepth > QueueHighWaterMark)
            {
                Volatile.Write(ref _adaptiveIdx, Math.Min(currAdaptiveIdx + 1, AdaptiveTuning.Length - 1));
                Interlocked.Increment(ref _scaleUpCount);
                Interlocked.Exchange(ref _batchesSinceLastAdjustment, 0);
            }
            else if (queueDepth < QueueLowWaterMark)
            {
                // Low backpressure: increase indexing frequency and decrease batch size for lower latency
                Volatile.Write(ref _adaptiveIdx, Math.Max(currAdaptiveIdx - 1, 0));
                Interlocked.Increment(ref _scaleDownCount);
                Interlocked.Exchange(ref _batchesSinceLastAdjustment, 0);
            }
            // else: normal range, maintain current settings
        }

        /// <summary>
        /// Append a record to the stream. The record is stored with a 16-byte header
        /// followed by the raw payload bytes. The secondary index is used for shard selection
        /// and sparse indexing; the timestamp is the primary index for range queries.
        /// </summary>
        public void Append(int secondaryIndex, ReadOnlySpan<byte> payload, long timestamp, ushort version)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            LogShard shard = GetShard(secondaryIndex);
            int count = _writeCounts.AddOrUpdate(secondaryIndex, 1, static (_, prev) => prev + 1);

            // Use adaptive indexing frequency
            (int indexSpacing, int batchSize, int indexSpacingMask) = AdaptiveTuning[Volatile.Read(ref _adaptiveIdx)];
            bool shouldIndex = (count & indexSpacingMask) == 0;

            long address = shard.Enqueue(secondaryIndex, payload, timestamp, version);

            if (shouldIndex)
            {
                // Soft-bounded queue: check count for backpressure (eventually consistent)
                // Race condition is acceptable - queue may briefly exceed capacity before settling
                // Increment before enqueue to reduce drift window (decrement happens immediately on dequeue)
                int currentCount = Volatile.Read(ref _pendingIndexCount);
                if (currentCount < QueueCapacity)
                {
                    Interlocked.Increment(ref _pendingIndexCount);
                    _pendingIndexInserts.Enqueue((secondaryIndex, timestamp, address));
                    // in aggressive scenarios as index frequency goes down we want to make sure the worker is signaled to process the larger batches in a timely manner
                    _flushSignal.Set();
                }
                else
                {
                    // Queue is full - drop this index entry
                    // This is acceptable because we have sparse indexing and can always scan from a previous entry.
                    Interlocked.Increment(ref _droppedIndexEntries);
                }
            }
        }

        /// <summary>
        /// For testing: ensures all pending index entries have been written to SQLite.
        /// Signals the worker and waits for the queue to be drained.
        /// </summary>
        public void WaitForPendingWrites()
        {
            if (_pendingIndexInserts.IsEmpty)
                return;

            _flushSignal.Set();

            // Wait for worker to process all entries
            Stopwatch sw = Stopwatch.StartNew();
            while (!_pendingIndexInserts.IsEmpty && sw.Elapsed < TimeSpan.FromSeconds(10))
            {
                Thread.Sleep(50);
            }

            // Give worker a bit more time to complete any in-flight batch
            Thread.Sleep(100);
        }

        /// <summary>
        /// Get statistics about adaptive tuning and index queue behavior.
        /// </summary>
        public StreamDbStats GetStats() => new StreamDbStats(
                Volatile.Read(ref _scaleUpCount),
                Volatile.Read(ref _scaleDownCount),
                Volatile.Read(ref _droppedIndexEntries),
                Volatile.Read(ref _adaptiveIdx),
                Volatile.Read(ref _pendingIndexCount)
            );

        #endregion

        #region Read Methods

        public List<StreamEntry> ReadRange(int secondaryIndex, long startTs, long endTs, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _logger?.LogDebug("ReadRange: secondaryIndex={SecondaryIndex}, startTs={StartTs}, endTs={EndTs}, limit={Limit}",
                secondaryIndex, startTs, endTs, limit);

            long scanFrom = LookupNearestAddress(secondaryIndex, startTs);
            _logger?.LogDebug("ReadRange: secondaryIndex={SecondaryIndex} -> scanFrom={ScanFrom} (shard={Shard})",
                secondaryIndex, scanFrom, secondaryIndex & ShardMask);

            LogShard shard = GetShard(secondaryIndex);
            List<StreamEntry> results = shard.ScanRange(secondaryIndex, scanFrom, startTs, endTs, limit);

            _logger?.LogDebug("ReadRange: secondaryIndex={SecondaryIndex} -> returned {Count} items",
                secondaryIndex, results.Count);

            return results;
        }

        /// <summary>
        /// Multi-device overload of ReadRange. Groups devices by shard to minimize redundant scanning.
        /// For each shard, looks up the nearest address ≤ startTs across all devices in that shard, 
        /// then scans once per shard to collect results for all devices. Returns a dictionary keyed by secondary index.
        /// </summary>
        public Dictionary<int, List<StreamEntry>> ReadRange(IEnumerable<int> secondaryIndexes, long startTs, long endTs, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            List<int> indexList = secondaryIndexes.ToList();
            _logger?.LogDebug("ReadRange (multi-device): secondaryIndexes=[{Indexes}], startTs={StartTs}, endTs={EndTs}, limit={Limit}",
                string.Join(", ", indexList), startTs, endTs, limit);

            // Group devices by shard so we can scan each shard at most once.
            Dictionary<int, (HashSet<int> Indexes, long MinAddress)> shardGroups = new Dictionary<int, (HashSet<int> Indexes, long MinAddress)>();

            foreach (int idx in indexList)
            {
                int shardIndex = idx & ShardMask;
                long addr = LookupNearestAddress(idx, startTs);

                if (shardGroups.TryGetValue(shardIndex, out (HashSet<int> Indexes, long MinAddress) group))
                {
                    group.Indexes.Add(idx);
                    if (addr < group.MinAddress)
                        shardGroups[shardIndex] = (group.Indexes, addr);
                }
                else
                {
                    shardGroups[shardIndex] = (new HashSet<int> { idx }, addr);
                }
            }

            Dictionary<int, List<StreamEntry>> result = new Dictionary<int, List<StreamEntry>>();

            _logger?.LogDebug("ReadRange (multi-device): grouped into {ShardCount} shards", shardGroups.Count);

            foreach ((int shardIndex, (HashSet<int>? indexes, long minAddress)) in shardGroups)
            {
                _logger?.LogDebug("ReadRange (multi-device): scanning shard {ShardIndex} with {Count} indexes from address {MinAddress}",
                    shardIndex, indexes.Count, minAddress);

                Dictionary<int, List<StreamEntry>> shardResults = _shards[shardIndex].ScanRange(indexes, minAddress, startTs, endTs, limit);

                foreach ((int idx, List<StreamEntry>? items) in shardResults)
                {
                    result[idx] = items;
                }
            }

            var totalItems = result.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRange (multi-device): returned {TotalItems} items across {Count} indexes",
                totalItems, result.Count);

            return result;
        }

        /// <summary>
        /// All-devices overload of ReadRange. Scans all shards and returns data for every secondary index
        /// that has data in the specified time range.
        /// </summary>
        public Dictionary<int, List<StreamEntry>> ReadRange(long startTs, long endTs, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _logger?.LogDebug("ReadRange (all-devices): startTs={StartTs}, endTs={EndTs}, limit={Limit}",
                startTs, endTs, limit);

            Dictionary<int, List<StreamEntry>> result = new Dictionary<int, List<StreamEntry>>();

            // Scan all shards
            for (int shardIndex = 0; shardIndex < ShardCount; shardIndex++)
            {
                long addr = LookupNearestAddressForShard(shardIndex, startTs);

                _logger?.LogDebug("ReadRange (all-devices): scanning shard {ShardIndex} from address {Address}",
                    shardIndex, addr);

                Dictionary<int, List<StreamEntry>> shardResults = _shards[shardIndex].ScanRangeAllDevices(addr, startTs, endTs, limit);

                foreach ((int idx, List<StreamEntry>? items) in shardResults)
                {
                    result[idx] = items;
                }
            }

            var totalItems = result.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRange (all-devices): returned {TotalItems} items across {Count} indexes",
                totalItems, result.Count);

            return result;
        }

        /// <summary>
        /// Returns the minimum earliest timestamp across all requested secondary indexes by scanning
        /// the FasterLog from the sparse index pointer. Returns null if no data exists.
        /// </summary>
        public long? GetEarliestTimestamp(IEnumerable<int> secondaryIndexes, long fromTs)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            List<int> indexList = secondaryIndexes.ToList();
            _logger?.LogDebug("GetEarliestTimestamp: secondaryIndexes=[{Indexes}], fromTs={FromTs}",
                string.Join(", ", indexList), fromTs);

            Dictionary<int, (HashSet<int> Indexes, long MinAddress)> shardGroups = new Dictionary<int, (HashSet<int> Indexes, long MinAddress)>();

            foreach (int idx in indexList)
            {
                int shardIndex = idx & ShardMask;
                long addr = LookupNearestAddress(idx, fromTs);

                if (shardGroups.TryGetValue(shardIndex, out (HashSet<int> Indexes, long MinAddress) group))
                {
                    group.Indexes.Add(idx);
                    if (addr < group.MinAddress)
                        shardGroups[shardIndex] = (group.Indexes, addr);
                }
                else
                {
                    shardGroups[shardIndex] = (new HashSet<int> { idx }, addr);
                }
            }

            long? globalMin = null;

            _logger?.LogDebug("GetEarliestTimestamp: grouped into {ShardCount} shards", shardGroups.Count);

            foreach ((int shardIndex, (HashSet<int>? indexes, long minAddress)) in shardGroups)
            {
                _logger?.LogDebug("GetEarliestTimestamp: searching shard {ShardIndex} with {Count} indexes from address {MinAddress}",
                    shardIndex, indexes.Count, minAddress);

                long? shardMin = _shards[shardIndex].FindFirstTimestamp(indexes, minAddress, fromTs);

                _logger?.LogDebug("GetEarliestTimestamp: shard {ShardIndex} returned {ShardMin}",
                    shardIndex, shardMin?.ToString() ?? "null");

                if (shardMin.HasValue && (!globalMin.HasValue || shardMin.Value < globalMin.Value))
                    globalMin = shardMin.Value;
            }

            _logger?.LogDebug("GetEarliestTimestamp: final result = {GlobalMin}",
                globalMin?.ToString() ?? "null");

            return globalMin;
        }

        /// <summary>
        /// Combined availability-check and range-read in a single operation. Scans forward
        /// from <paramref name="fromTs"/>, discovers the first matching entry, then collects
        /// a window of <paramref name="windowMs"/> milliseconds of data from that point.
        ///
        /// Returns <c>rangeEndMs = -1</c> when no data exists for any secondary index.
        /// </summary>
        public (long RangeEndMs, Dictionary<int, List<StreamEntry>> Data) ReadRangeFromAvailable(
            IEnumerable<int> secondaryIndexes, long fromTs, long windowMs, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _logger?.LogDebug("ReadRangeFromAvailable: secondaryIndexes=[{Indexes}], fromTs={FromTs}, windowMs={WindowMs}, limit={Limit}",
                string.Join(", ", secondaryIndexes), fromTs, windowMs, limit);

            // Group by shard
            Dictionary<int, (HashSet<int> Indexes, long MinAddress)> shardGroups = new Dictionary<int, (HashSet<int> Indexes, long MinAddress)>();

            foreach (int idx in secondaryIndexes)
            {
                int shardIndex = idx & ShardMask;
                long addr = LookupNearestAddress(idx, fromTs);

                if (shardGroups.TryGetValue(shardIndex, out (HashSet<int> Indexes, long MinAddress) group))
                {
                    group.Indexes.Add(idx);
                    if (addr < group.MinAddress)
                        shardGroups[shardIndex] = (group.Indexes, addr);
                }
                else
                {
                    shardGroups[shardIndex] = (new HashSet<int> { idx }, addr);
                }
            }

            // Single scan per shard: find first entry and collect window in one pass.
            long? globalMin = null;
            List<(long? FirstTs, Dictionary<int, List<StreamEntry>> Data)> shardResults = new List<(long? FirstTs, Dictionary<int, List<StreamEntry>> Data)>(shardGroups.Count);

            foreach ((int shardIndex, (HashSet<int>? indexes, long minAddress)) in shardGroups)
            {
                _logger?.LogDebug("ReadRangeFromAvailable: scanning shard {ShardIndex} with {Count} indexes from address {MinAddress}",
                    shardIndex, indexes.Count, minAddress);

                (long? FirstTimestamp, Dictionary<int, List<StreamEntry>> Data) result = _shards[shardIndex].FindAndScanRange(indexes, minAddress, fromTs, windowMs, limit);
                shardResults.Add(result);

                if (result.FirstTimestamp.HasValue && (!globalMin.HasValue || result.FirstTimestamp.Value < globalMin.Value))
                    globalMin = result.FirstTimestamp;
            }

            if (!globalMin.HasValue)
            {
                _logger?.LogDebug("ReadRangeFromAvailable: no data found");
                return (-1, new Dictionary<int, List<StreamEntry>>());
            }

            long rangeEndMs = globalMin.Value + windowMs;

            // Merge shard results, trimming entries beyond the canonical window.
            Dictionary<int, List<StreamEntry>> merged = new Dictionary<int, List<StreamEntry>>();
            foreach ((long? firstTs, Dictionary<int, List<StreamEntry>>? data) in shardResults)
            {
                foreach ((int idx, List<StreamEntry>? items) in data)
                {
                    if (items.Count == 0)
                        continue;

                    // Trim entries that fall beyond rangeEndMs
                    if (firstTs.HasValue && firstTs.Value != globalMin.Value && items[^1].Timestamp > rangeEndMs)
                    {
                        int lo = 0, hi = items.Count;
                        while (lo < hi)
                        {
                            int mid = lo + (hi - lo) / 2;
                            if (items[mid].Timestamp <= rangeEndMs)
                                lo = mid + 1;
                            else
                                hi = mid;
                        }
                        if (lo < items.Count)
                            items.RemoveRange(lo, items.Count - lo);
                    }
                    merged[idx] = items;
                }
            }

            var totalItems = merged.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRangeFromAvailable: returned {TotalItems} items across {Count} indexes, rangeEndMs={RangeEndMs}",
                totalItems, merged.Count, rangeEndMs);

            return (rangeEndMs, merged);
        }

        /// <summary>
        /// Find the largest indexed timestamp ≤ startTs for this device.
        /// Returns the corresponding FasterLog address, or 0 if no index entry exists (scan from beginning).
        /// </summary>
        private long LookupNearestAddress(int deviceId, long startTs)
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "SELECT log_address FROM stream_index WHERE device_id = $did AND timestamp <= $ts ORDER BY timestamp DESC LIMIT 1";
            cmd.Parameters.AddWithValue("$did", deviceId);
            cmd.Parameters.AddWithValue("$ts", startTs);
            object? result = cmd.ExecuteScalar();
            long addr = result is long a ? a : 0L;

            _logger?.LogDebug("LookupNearestAddress: deviceId={DeviceId}, startTs={StartTs} -> address={Address}",
                deviceId, startTs, addr);

            return addr;
        }

        /// <summary>
        /// Returns all distinct secondary indexes that have at least one indexed entry in the stream.
        /// </summary>
        private List<int> GetAllSecondaryIndexes()
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "SELECT DISTINCT device_id FROM stream_index";
            List<int> ids = new List<int>();
            using SqliteDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                ids.Add(reader.GetInt32(0));
            }
            return ids;
        }

        /// <summary>
        /// Returns the single latest record for each secondary index that has data in the stream.
        /// Uses the sparse index to efficiently locate the latest data region per index,
        /// then scans forward from there to find the actual latest record.
        /// </summary>
        public Dictionary<int, StreamEntry> ReadLatest()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            List<int> indexIds = GetAllSecondaryIndexes();
            _logger?.LogDebug("ReadLatest: found {Count} index(es) in index", indexIds.Count);

            Dictionary<int, StreamEntry> result = new Dictionary<int, StreamEntry>(indexIds.Count);

            foreach (int idx in indexIds)
            {
                // Find the latest sparse index entry
                long scanFrom = LookupNearestAddress(idx, long.MaxValue);
                LogShard shard = GetShard(idx);

                // Scan from latest index entry to end of shard with no timestamp filter
                List<StreamEntry> records = shard.ScanRange(idx, scanFrom, 0, long.MaxValue, 0);
                if (records.Count > 0)
                {
                    result[idx] = records[^1]; // Keep only the latest record
                }
            }

            _logger?.LogDebug("ReadLatest: returned latest data for {Count} index(es)", result.Count);
            return result;
        }

        #endregion

        #region Maintenance Methods

        /// <summary>
        /// Periodic checkpoint with write barrier to prevent SQLite write contention.
        /// Disables index writes for the checkpoint window, allowing FasterLog writes to continue.
        /// Guarantees that ongoing transactions complete before checkpointing.
        /// </summary>
        internal void RunCheckpoint()
        {
            // Ensure only one maintenance operation runs at a time (checkpoint or retention)
            if (!_maintenanceLock.TryEnter())
            {
                _logger?.LogWarning("RunCheckpoint: skipping - another maintenance operation in progress");
                return;
            }

            try
            {
                _logger?.LogInformation("RunCheckpoint: starting checkpoint window");

                // Acquire write lock - blocks until all ongoing index writes (read locks) complete
                // and prevents new index writes from starting
                _indexWriteLock.EnterWriteLock();
                try
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    using PooledConnection pooled = GetConnection();
                    using SqliteCommand cmd = pooled.Connection.CreateCommand();
                    // This mode blocks (invokes the busy-handler callback) until there is no database writer and 
                    // all readers are reading from the most recent database snapshot. 
                    // It then checkpoints all frames in the log file and syncs the database file. 
                    // FULL blocks concurrent writers while it is running, but readers can proceed.
                    cmd.CommandText = "PRAGMA wal_checkpoint(FULL)";

                    // PRAGMA wal_checkpoint returns (busy, log, checkpointed)
                    // busy: number of frames that couldn't be checkpointed (0 = success)
                    // log: total frames in WAL before checkpoint
                    // checkpointed: frames actually checkpointed
                    using SqliteDataReader reader = cmd.ExecuteReader();
                    if (reader.Read())
                    {
                        int busy = reader.GetInt32(0);
                        int log = reader.GetInt32(1);
                        int checkpointed = reader.GetInt32(2);

                        sw.Stop();

                        if (busy > 0)
                        {
                            _logger?.LogWarning("RunCheckpoint: checkpoint incomplete in {Elapsed}s - {Busy} frames busy, {Log} total frames, {Checkpointed} checkpointed",
                                sw.Elapsed.TotalSeconds, busy, log, checkpointed);
                        }
                        else
                        {
                            _logger?.LogInformation("RunCheckpoint: checkpoint complete in {Elapsed}s - {Checkpointed} frames checkpointed from {Log} total frames",
                                sw.Elapsed.TotalSeconds, checkpointed, log);
                        }
                    }
                    else
                    {
                        sw.Stop();
                        _logger?.LogWarning("RunCheckpoint: checkpoint returned no result after {Elapsed}s", sw.Elapsed.TotalSeconds);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "RunCheckpoint: error during checkpoint");
                }
                finally
                {
                    // Release write lock - allows index writes to resume
                    _indexWriteLock.ExitWriteLock();

                    // Signal the flush worker to process any accumulated entries
                    _flushSignal.Set();
                }
            }
            finally
            {
                _maintenanceLock.Exit();
            }
        }

        /// <summary>
        /// Runs the retention policy: deletes index entries older than the retention cutoff
        /// and truncates each FasterLog shard up to the minimum surviving address.
        /// Does not block index writes - WAL mode allows concurrent DELETE and INSERT operations.
        /// </summary>
        internal void RunRetention()
        {
            // Ensure only one maintenance operation runs at a time (checkpoint or retention)
            if (!_maintenanceLock.TryEnter())
            {
                _logger?.LogWarning("RunRetention: skipping - another maintenance operation in progress");
                return;
            }

            try
            {
                _logger?.LogInformation("RunRetention: starting retention");

                long cutoffTs = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - (long)_retentionPeriod.TotalSeconds;
                PurgeIndexBefore(cutoffTs);
                TruncateShards(cutoffTs);

                _logger?.LogInformation("RunRetention: retention complete");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "RunRetention: error during retention");
            }
            finally
            {
                _maintenanceLock.Exit();
            }
        }


        /// <summary>
        /// Delete all sparse-index rows whose timestamp is strictly before <paramref name="cutoffTs"/>.
        /// </summary>
        private void PurgeIndexBefore(long cutoffTs)
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM stream_index WHERE timestamp < $cutoff";
            cmd.Parameters.AddWithValue("$cutoff", cutoffTs);
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// For each shard, find the minimum surviving log_address across all devices that map
        /// to that shard, then truncate the FasterLog up to that address.
        /// </summary>
        private void TruncateShards(long cutoffTs)
        {
            // For each shard, determine the safe truncation address.
            // Query the minimum log_address still alive in the index per shard.
            using PooledConnection pooled = GetConnection();

            for (int shardIndex = 0; shardIndex < ShardCount; shardIndex++)
            {
                // Find the minimum log_address among index entries that survived the purge
                // and belong to devices assigned to this shard.
                // Since we can't easily filter by shard in SQL (no shard column),
                // we use the global minimum address for this shard from all devices.
                using SqliteCommand cmd = pooled.Connection.CreateCommand();
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

        #endregion

        #region Utils

        /// <summary>
        /// Find the minimum log_address across all devices in a specific shard where timestamp ≤ startTs.
        /// This provides an efficient starting point for wildcard scans that need to read all devices in a shard.
        /// Returns 0 if no index entry exists (scan from beginning).
        /// </summary>
        private long LookupNearestAddressForShard(int shardIndex, long startTs)
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = @"
                SELECT MIN(log_address) 
                FROM stream_index 
                WHERE (device_id & $mask) = $shard 
                  AND timestamp <= $ts";
            cmd.Parameters.AddWithValue("$mask", ShardMask);
            cmd.Parameters.AddWithValue("$shard", shardIndex);
            cmd.Parameters.AddWithValue("$ts", startTs);
            object? result = cmd.ExecuteScalar();
            long addr = result is long a ? a : 0L;

            _logger?.LogDebug("LookupNearestAddressForShard: shardIndex={ShardIndex}, startTs={StartTs} -> address={Address}",
                shardIndex, startTs, addr);

            return addr;
        }

        private PooledConnection GetConnection()
        {
            if (_connPool.TryDequeue(out SqliteConnection? conn))
                return new PooledConnection(conn, _connPool);

            conn = new SqliteConnection(_connectionString);
            conn.Open();

            using SqliteCommand pragma = conn.CreateCommand();
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;";
            pragma.ExecuteNonQuery();

            return new PooledConnection(conn, _connPool);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private LogShard GetShard(int secondaryIndex) => _shards[secondaryIndex & ShardMask];
        #endregion

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // Stop timers first to prevent new maintenance operations
            _retentionTimer?.Dispose();
            _checkpointTimer?.Dispose();

            // Signal the flush worker to wake up and exit
            _flushSignal.Set();

            // Give the background worker a short time to exit cleanly
            // Use Task.Wait with timeout to avoid indefinite hang
            try
            {
                if (!_bgFlushRunner.Wait(TimeSpan.FromSeconds(2)))
                {
                    _logger?.LogWarning("Dispose: flush worker did not exit within 2 seconds, proceeding with cleanup");
                }
            }
            catch (AggregateException ex)
            {
                _logger?.LogWarning(ex, "Dispose: error waiting for flush worker to exit, proceeding with cleanup");
            }

            // Dispose synchronization primitives
            _flushSignal.Dispose();
            _indexWriteLock.Dispose();

            // Dispose shards (commits FasterLog)
            foreach (LogShard shard in _shards)
            {
                shard.Dispose();
            }

            // Dispose connection pool
            while (_connPool.TryDequeue(out SqliteConnection? conn))
            {
                conn.Dispose();
            }

            // Note: We intentionally do NOT flush pending index entries on dispose.
            // The sparse index is a performance optimization and missing a few entries
            // at shutdown is acceptable - reads will still work by scanning from the
            // previous indexed entry.
        }

        /// <summary>
        /// A single FasterLog shard shared by multiple secondary indexes.
        /// Entries are assigned to shards via <c>secondaryIndex &amp; ShardMask</c>.
        /// </summary>
        private sealed class LogShard : IDisposable
        {
            private readonly FasterLog _log;

            public long BeginAddress => _log.BeginAddress;
            public long TailAddress => _log.TailAddress;

            public LogShard(int shardIndex, string baseDir)
            {
                string logPath = Path.Combine(baseDir, shardIndex.ToString(), string.Format(ShardLogFmt, shardIndex));
                FasterLogSettings settings = new FasterLogSettings
                {
                    LogDevice = Devices.CreateLogDevice(logPath, deleteOnClose: false),
                    TryRecoverLatest = true,
                    AutoRefreshSafeTailAddress = true,
                };
                _log = new FasterLog(settings);
            }

            /// <summary>
            /// Write a record with header + payload to the log.
            /// Header: [8B timestamp][4B secondaryIndex][2B version][2B payloadLength]
            /// </summary>
            public long Enqueue(int secondaryIndex, ReadOnlySpan<byte> payload, long timestamp, ushort version)
            {
                int totalSize = StreamHeader.Size + payload.Length;
                Span<byte> buffer = totalSize <= 256
                    ? stackalloc byte[totalSize]
                    : new byte[totalSize];

                StreamHeader.Write(buffer, timestamp, secondaryIndex, version, (ushort)payload.Length);
                payload.CopyTo(buffer[StreamHeader.Size..]);

                return _log.Enqueue(buffer);
            }

            /// <summary>
            /// Single-index scan. Delegates to the multi-index overload.
            /// </summary>
            public List<StreamEntry> ScanRange(int secondaryIndex, long fromAddress, long startTs, long endTs, int limit = 0)
            {
                Dictionary<int, List<StreamEntry>> results = ScanRange(new HashSet<int>(1) { secondaryIndex }, fromAddress, startTs, endTs, limit);
                return results[secondaryIndex];
            }

            /// <summary>
            /// Scan the shard once from <paramref name="fromAddress"/> and collect entries for all
            /// secondary indexes in <paramref name="indexes"/> whose timestamps fall within [startTs, endTs].
            /// Timestamps are read from the header (primary index), secondary indexes from the header.
            /// </summary>
            public Dictionary<int, List<StreamEntry>> ScanRange(HashSet<int> indexes, long fromAddress, long startTs, long endTs, int limit = 0)
            {
                Dictionary<int, List<StreamEntry>> results = new Dictionary<int, List<StreamEntry>>(indexes.Count);
                foreach (int id in indexes)
                    results[id] = new List<StreamEntry>();

                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return results;

                HashSet<int> finished = new HashSet<int>();
                bool hasLimit = limit > 0;

                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < StreamHeader.Size)
                        continue;

                    // Read secondary index from header for fast-path filtering
                    int entryIdx = StreamHeader.ReadSecondaryIndex(entry.AsSpan());
                    if (!indexes.Contains(entryIdx) || finished.Contains(entryIdx))
                        continue;

                    long ts = StreamHeader.ReadTimestamp(entry.AsSpan());

                    if (ts > endTs)
                    {
                        finished.Add(entryIdx);
                        if (finished.Count == indexes.Count)
                            break;
                        continue;
                    }

                    if (ts >= startTs)
                    {
                        ushort version = StreamHeader.ReadVersion(entry.AsSpan());
                        ushort payloadLen = StreamHeader.ReadPayloadLength(entry.AsSpan());
                        byte[] payload = new byte[payloadLen];
                        entry.AsSpan(StreamHeader.Size, payloadLen).CopyTo(payload);

                        results[entryIdx].Add(new StreamEntry(ts, entryIdx, version, payload));

                        if (hasLimit && results[entryIdx].Count >= limit)
                        {
                            finished.Add(entryIdx);
                            if (finished.Count == indexes.Count)
                                break;
                        }
                    }
                }

                return results;
            }

            /// <summary>
            /// Scan the shard from <paramref name="fromAddress"/> and collect entries for all secondary indexes
            /// whose timestamps fall within [startTs, endTs]. Lazily creates result lists.
            /// </summary>
            public Dictionary<int, List<StreamEntry>> ScanRangeAllDevices(long fromAddress, long startTs, long endTs, int limit = 0)
            {
                Dictionary<int, List<StreamEntry>> results = new Dictionary<int, List<StreamEntry>>();
                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return results;

                HashSet<int> finished = new HashSet<int>();
                bool hasLimit = limit > 0;

                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < StreamHeader.Size)
                        continue;

                    int entryIdx = StreamHeader.ReadSecondaryIndex(entry.AsSpan());
                    if (finished.Contains(entryIdx))
                        continue;

                    long ts = StreamHeader.ReadTimestamp(entry.AsSpan());

                    if (ts > endTs)
                    {
                        finished.Add(entryIdx);
                        continue;
                    }

                    if (ts >= startTs)
                    {
                        if (!results.ContainsKey(entryIdx))
                            results[entryIdx] = new List<StreamEntry>();

                        ushort version = StreamHeader.ReadVersion(entry.AsSpan());
                        ushort payloadLen = StreamHeader.ReadPayloadLength(entry.AsSpan());
                        byte[] payload = new byte[payloadLen];
                        entry.AsSpan(StreamHeader.Size, payloadLen).CopyTo(payload);

                        results[entryIdx].Add(new StreamEntry(ts, entryIdx, version, payload));

                        if (hasLimit && results[entryIdx].Count >= limit)
                        {
                            finished.Add(entryIdx);
                        }
                    }
                }

                return results;
            }

            /// <summary>
            /// Scan the shard and find the first entry for any index in <paramref name="indexes"/>
            /// whose timestamp is ≥ <paramref name="fromTs"/>. Returns null if none found.
            /// </summary>
            public long? FindFirstTimestamp(HashSet<int> indexes, long fromAddress, long fromTs)
            {
                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return null;

                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < StreamHeader.Size)
                        continue;

                    int entryIdx = StreamHeader.ReadSecondaryIndex(entry.AsSpan());
                    if (!indexes.Contains(entryIdx))
                        continue;

                    long ts = StreamHeader.ReadTimestamp(entry.AsSpan());
                    if (ts >= fromTs)
                        return ts;
                }

                return null;
            }

            /// <summary>
            /// Single-pass combination of FindFirstTimestamp and ScanRange. Scans forward, skips
            /// entries below <paramref name="fromTs"/>, and once the first matching entry is found,
            /// collects all entries within [firstTs, firstTs + windowMs].
            /// </summary>
            public (long? FirstTimestamp, Dictionary<int, List<StreamEntry>> Data) FindAndScanRange(
                HashSet<int> indexes, long fromAddress, long fromTs, long windowMs, int limit = 0)
            {
                Dictionary<int, List<StreamEntry>> results = new Dictionary<int, List<StreamEntry>>(indexes.Count);
                foreach (int id in indexes)
                    results[id] = new List<StreamEntry>();

                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return (null, results);

                long? firstTs = null;
                long endTs = long.MaxValue;
                HashSet<int> finished = new HashSet<int>();
                bool hasLimit = limit > 0;

                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < StreamHeader.Size)
                        continue;

                    int entryIdx = StreamHeader.ReadSecondaryIndex(entry.AsSpan());
                    if (!indexes.Contains(entryIdx) || finished.Contains(entryIdx))
                        continue;

                    long ts = StreamHeader.ReadTimestamp(entry.AsSpan());

                    if (ts < fromTs)
                        continue;

                    // First matching entry defines the window
                    if (!firstTs.HasValue)
                    {
                        firstTs = ts;
                        endTs = ts + windowMs;
                    }

                    if (ts > endTs)
                    {
                        finished.Add(entryIdx);
                        if (finished.Count == indexes.Count)
                            break;
                        continue;
                    }

                    ushort version = StreamHeader.ReadVersion(entry.AsSpan());
                    ushort payloadLen = StreamHeader.ReadPayloadLength(entry.AsSpan());
                    byte[] payload = new byte[payloadLen];
                    entry.AsSpan(StreamHeader.Size, payloadLen).CopyTo(payload);

                    results[entryIdx].Add(new StreamEntry(ts, entryIdx, version, payload));

                    if (hasLimit && results[entryIdx].Count >= limit)
                    {
                        finished.Add(entryIdx);
                        if (finished.Count == indexes.Count)
                            break;
                    }
                }

                return (firstTs, results);
            }

            /// <summary>
            /// Commit all pending writes to disk and wait until the specified address is durable.
            /// </summary>
            public ValueTask CommitAndWait(long untilAddress)
            {
                _log.Commit(spinWait: false);
                return _log.WaitForCommitAsync(untilAddress);
            }

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

    public record class StreamDbStats(long ScaleUp, long ScaleDown, long Dropped, int AdaptiveIdx, long PendingIdxQueueLen);
}