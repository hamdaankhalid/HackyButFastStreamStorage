using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;
using Microsoft.Data.Sqlite;

namespace WebServer.Storage
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
    /// <typeparam name="TDeviceId">The type of the device ID field stored at offset 0 in telemetry structs (ushort or uint).</typeparam>
    /// <typeparam name="T">The type of telemetry struct stored in this StreamDB instance.</typeparam>
    public sealed class StreamDB<TDeviceId, T> : IDisposable where TDeviceId : unmanaged where T : unmanaged
    {
        #region member variables and Consts

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
            (1 << 7, 1 << 6, (1 << 7) - 1),   // idx 3 (default)
            (1 << 8, 1 << 7, (1 << 8) - 1),  // idx 4
            (1 << 9, 1 << 8, (1 << 9) - 1),  // idx 5
            (1 << 10, 1 << 9, (1 << 10) - 1), // idx 6: low frequency, big batches
            (1 << 11, 1 << 10, (1 << 11) - 1) // idx 7: very low frequency, very big batches
        };

        // Now I only need to move this slider which points to readonly data.
        // Since int is atomic I can do this without even interlocked kewl
        private int _adaptiveIdx = 3;

        // Bounded queue parameters
        private const int QueueCapacity = 2048;       // Maximum queue size
        private const int QueueHighWaterMark = 1536;  // 75% - start reducing index frequency
        private const int QueueLowWaterMark = 512;    // 25% - start increasing index frequency

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
        private readonly ILogger<StreamDB<TDeviceId, T>>? _logger;
        private readonly Task _bgFlushRunner;
        private readonly ManualResetEventSlim _flushSignal = new(false);
        private readonly ManualResetEventSlim _flushWorkerIdle = new(true); // Set when idle, reset when active
        private readonly Lock _maintenanceLock = new(); // Ensures only one maintenance operation at a time
        private int _indexWritesDisabled = 0; // 0 = enabled, 1 = disabled; use Interlocked for memory barriers
        private bool _disposed;

        // Statistics tracking
        private long _scaleUpCount = 0;    // Number of times we scaled up (reduced indexing frequency)
        private long _scaleDownCount = 0;  // Number of times we scaled down (increased indexing frequency)
        private long _droppedIndexEntries = 0; // Number of index entries dropped due to queue full

        #endregion

        public StreamDB(string? baseDir = null, TimeSpan? retentionPeriod = null, TimeSpan? checkpointInterval = null, ILogger<StreamDB<TDeviceId, T>>? logger = null)
        {
            _logger = logger;
            _baseDir = baseDir ?? "streams";
            Directory.CreateDirectory(_baseDir);

            _retentionPeriod = retentionPeriod ?? TimeSpan.FromDays(60);
            _checkpointInterval = checkpointInterval ?? TimeSpan.FromHours(1);

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
            while (!_disposed)
            {
                // Signal that we're idle before waiting
                _flushWorkerIdle.Set();

                // Wait for signal or timeout (5 seconds) to check for pending work.
                // During checkpointing even if this timesout and proceeds the indexWritesDisabled CAS
                // protects any sqlite transactions from starting till checkpointing completes and resets
                // the _indexWritesDisabled
                _flushSignal.Wait(TimeSpan.FromSeconds(5));
                _flushSignal.Reset();

                if (_disposed)
                    break;

                // Signal that we're now active (processing)
                _flushWorkerIdle.Reset();

                try
                {
                    // Process all pending batches until queue is empty
                    while (!_pendingIndexInserts.IsEmpty && !_disposed)
                    {
                        // Check if writes are disabled - use Interlocked.CompareExchange for memory barrier
                        if (Interlocked.CompareExchange(ref _indexWritesDisabled, 0, 0) != 0)
                        {
                            _logger?.LogDebug("FlushWorker: skipping batch during checkpoint window");
                            break;
                        }

                        // Check tuning on every batch for responsiveness
                        AdaptivelyTuneParameters();

                        try
                        {
                            using var pooled = GetConnection();
                            using var tx = pooled.Connection.BeginTransaction();
                            using var cmd = pooled.Connection.CreateCommand();
                            cmd.Transaction = tx;
                            cmd.CommandText = "INSERT INTO stream_index (device_id, timestamp, log_address) VALUES ($did, $ts, $addr)";
                            var pDid = cmd.Parameters.Add("$did", SqliteType.Integer);
                            var pTs = cmd.Parameters.Add("$ts", SqliteType.Integer);
                            var pAddr = cmd.Parameters.Add("$addr", SqliteType.Integer);
                            cmd.Prepare();

                            int adaptiveIdx = Volatile.Read(ref _adaptiveIdx);
                            (int _, int batchSize, int _) = AdaptiveTuning[adaptiveIdx];

                            int flushed = 0;
                            while (flushed < batchSize && _pendingIndexInserts.TryDequeue(out var entry))
                            {
                                // Re-check the flag before each dequeue to avoid processing during checkpoint
                                // This handles the following race condition:
                                /*
                                Worker is at step 2 (blocked, waiting) - _flushWorkerIdle IS SET (shows "idle")
                                Checkpoint sets _indexWritesDisabled = 1
                                Checkpoint signals the worker to wake up
                                Checkpoint calls Wait() on _flushWorkerIdle
                                    Since the worker is still at step 2, the idle signal is STILL SET
                                    Wait() returns IMMEDIATELY Checkpoint thinks worker is idle
                                Worker wakes up from step 2
                                Worker does step 3 - Reset() the idle signal (NOW shows "active")
                                Worker enters the try block and starts the while loop
                                WITHOUT the check, worker would start dequeuing and writing to SQLite DURING THE CHECKPOINT!

                                The fallacy that would lead one to running into this bug is thinking that there is a ping-pong 
                                like communication between calling Wait and set across threads. No the scheduler does not guarantee
                                that be careful kids.
                                */
                                if (Interlocked.CompareExchange(ref _indexWritesDisabled, 0, 0) != 0)
                                {
                                    // Re-enqueue the entry we just dequeued
                                    _pendingIndexInserts.Enqueue(entry);
                                    break;
                                }

                                pDid.Value = entry.DeviceId;
                                pTs.Value = entry.Timestamp;
                                pAddr.Value = entry.Address;
                                cmd.ExecuteNonQuery();
                                flushed++;
                            }

                            tx.Commit();
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, "FlushWorker: error writing index batch");
                            // Continue processing despite errors
                        }
                    }
                }
                finally
                {
                    // Always signal idle when done processing, even if we skipped work
                    // This ensures checkpoint can proceed
                    _flushWorkerIdle.Set();
                }
            }
        }

        /// <summary>
        /// Dynamically adjusts IndexEveryN and FlushBatchSize based on queue depth.
        /// High queue depth (backpressure) → decrease indexing frequency (increase N) and increase batch size
        /// Low queue depth → increase indexing frequency (decrease N) and decrease batch size for lower latency
        /// </summary>
        private void AdaptivelyTuneParameters()
        {
            int queueDepth = _pendingIndexInserts.Count;
            int currAdaptiveIdx = Volatile.Read(ref _adaptiveIdx);

            // More aggressive scaling: consider medium pressure zone
            if (queueDepth > (QueueHighWaterMark + QueueLowWaterMark) / 2)
            {
                _adaptiveIdx = Math.Min(currAdaptiveIdx + 1, AdaptiveTuning.Length - 1);
                _scaleUpCount++;
            }
            else if (queueDepth < QueueLowWaterMark)
            {
                // Low backpressure: increase indexing frequency and decrease batch size for lower latency
                _adaptiveIdx = Math.Max(currAdaptiveIdx - 1, 0);
                _scaleDownCount++;
            }
            // else: normal range, maintain current settings
        }

        /*
        The invariant this needs to maintain is that writes are arranged monotonically by timestamp on the FasterLog.
        This is guaranteed by the fact that each the upstream processor serializes these calls per device.
        That means the timestamp is always monotonically increasing for each device, so we can safely index every Nth write without worrying about out-of-order timestamps.
        While multiple devices may be out of order, each device's timeline is strictly ordered, 
        so readers can safely scan forward from the nearest indexed timestamp and stop when they pass the endTs without missing any relevant entries.
        Any group/multiple device id will first have to find the minimum address to start from and then scan forward, so as long as the index entries are not too sparse, 
        the scan distance is bounded and performance is good.
        */
        public void Append(int deviceId, in T item, long timestamp)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            LogShard shard = GetShard(deviceId);
            int count = _writeCounts.AddOrUpdate(deviceId, 1, static (_, prev) => prev + 1);

            // Use adaptive indexing frequency
            (int indexSpacing, int batchSize, int indexSpacingMask) = AdaptiveTuning[Volatile.Read(ref _adaptiveIdx)];
            bool shouldIndex = (count & indexSpacingMask) == 0;

            // Read _indexWritesDisabled once with memory barrier to ensure consistency
            bool indexingDisabled = Interlocked.CompareExchange(ref _indexWritesDisabled, 0, 0) != 0;

            // During checkpoint window, skip the commit wait to avoid blocking log writes
            bool waitForCommit = shouldIndex && !indexingDisabled;
            long address = shard.Enqueue(item, waitForCommit: waitForCommit);

            if (shouldIndex && !indexingDisabled)
            {
                // Soft-bounded queue: check count for backpressure (eventually consistent)
                // Race condition is acceptable - queue may briefly exceed capacity before settling
                int currentCount = _pendingIndexInserts.Count;
                if (currentCount < QueueCapacity)
                {
                    _pendingIndexInserts.Enqueue((deviceId, timestamp, address));
                    // in aggressive scenarios as index frequency goes down we want to make sure the worker is signaled to process the larger batches in a timely manner
                    _flushSignal.Set();
                }
                else
                {
                    // Queue is full - drop this index entry
                    // This is acceptable because we have sparse indexing and can always scan from a previous entry
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
            var sw = Stopwatch.StartNew();
            while (!_pendingIndexInserts.IsEmpty && sw.Elapsed < TimeSpan.FromSeconds(10))
            {
                Thread.Sleep(50);
            }

            // Wait for worker to signal idle
            _flushWorkerIdle.Wait(TimeSpan.FromSeconds(5));
        }

        /// <summary>
        /// Get statistics about adaptive tuning and index queue behavior.
        /// </summary>
        public (long ScaleUpCount, long ScaleDownCount, long DroppedIndexEntries, int CurrentAdaptiveIdx, int CurrentQueueDepth) GetStats()
        {
            return (
                Volatile.Read(ref _scaleUpCount),
                Volatile.Read(ref _scaleDownCount),
                Volatile.Read(ref _droppedIndexEntries),
                Volatile.Read(ref _adaptiveIdx),
                _pendingIndexInserts.Count
            );
        }

        #endregion

        #region Read Methods

        public List<T> ReadRange(int deviceId, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _logger?.LogDebug("ReadRange: deviceId={DeviceId}, startTs={StartTs}, endTs={EndTs}, limit={Limit}",
                deviceId, startTs, endTs, limit);

            long scanFrom = LookupNearestAddress(deviceId, startTs);
            _logger?.LogDebug("ReadRange: deviceId={DeviceId} -> scanFrom={ScanFrom} (shard={Shard})",
                deviceId, scanFrom, deviceId & ShardMask);

            LogShard shard = GetShard(deviceId);
            var results = shard.ScanRange(deviceId, scanFrom, startTs, endTs, getTimestamp, limit);

            _logger?.LogDebug("ReadRange: deviceId={DeviceId} -> returned {Count} items",
                deviceId, results.Count);

            return results;
        }

        /// <summary>
        /// Multi-device overload of ReadRange. Groups devices by shard to minimize redundant scanning.
        /// For each shard, looks up the nearest address ≤ startTs across all devices in that shard, 
        /// then scans once per shard to collect results for all devices. Returns a dictionary keyed by deviceId.
        /// </summary>
        /// <param name="deviceIds">Collection of device IDs to query.</param>
        /// <param name="startTs">Start timestamp (inclusive) in Unix seconds.</param>
        /// <param name="endTs">End timestamp (inclusive) in Unix seconds.</param>
        /// <param name="getTimestamp">Function to extract the timestamp from a deserialized item.</param>
        /// <param name="limit">Maximum number of items to return per device. 0 means unlimited.</param>
        /// <returns>Dictionary mapping device IDs to their respective lists of items within the time range.</returns>
        public Dictionary<int, List<T>> ReadRange(IEnumerable<int> deviceIds, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var deviceIdList = deviceIds.ToList();
            _logger?.LogDebug("ReadRange (multi-device): deviceIds=[{DeviceIds}], startTs={StartTs}, endTs={EndTs}, limit={Limit}",
                string.Join(", ", deviceIdList), startTs, endTs, limit);

            // Group devices by shard so we can scan each shard at most once.
            var shardGroups = new Dictionary<int, (HashSet<int> Devices, long MinAddress)>();

            foreach (int deviceId in deviceIdList)
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
                    shardGroups[shardIndex] = (new HashSet<int> { deviceId }, addr);
                }
            }

            var result = new Dictionary<int, List<T>>();

            _logger?.LogDebug("ReadRange (multi-device): grouped into {ShardCount} shards", shardGroups.Count);

            foreach (var (shardIndex, (devices, minAddress)) in shardGroups)
            {
                _logger?.LogDebug("ReadRange (multi-device): scanning shard {ShardIndex} with {DeviceCount} devices from address {MinAddress}",
                    shardIndex, devices.Count, minAddress);

                var shardResults = _shards[shardIndex].ScanRange(devices, minAddress, startTs, endTs, getTimestamp, limit);

                foreach (var (deviceId, items) in shardResults)
                {
                    result[deviceId] = items;
                }
            }

            var totalItems = result.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRange (multi-device): returned {TotalItems} items across {DeviceCount} devices",
                totalItems, result.Count);

            return result;
        }

        /// <summary>
        /// All-devices overload of ReadRange. Scans all shards and returns data for every device
        /// that has data in the specified time range. More efficient than querying individual devices
        /// when you need a complete timeline across the entire system.
        /// </summary>
        /// <param name="startTs">Start timestamp (inclusive) in Unix seconds.</param>
        /// <param name="endTs">End timestamp (inclusive) in Unix seconds.</param>
        /// <param name="getTimestamp">Function to extract the timestamp from a deserialized item.</param>
        /// <param name="limit">Maximum number of items to return per device. 0 means unlimited.</param>
        /// <returns>Dictionary mapping device IDs to their respective lists of items within the time range.</returns>
        public Dictionary<int, List<T>> ReadRange(long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _logger?.LogDebug("ReadRange (all-devices): startTs={StartTs}, endTs={EndTs}, limit={Limit}",
                startTs, endTs, limit);

            var result = new Dictionary<int, List<T>>();

            // Scan all shards
            for (int shardIndex = 0; shardIndex < ShardCount; shardIndex++)
            {
                long addr = LookupNearestAddressForShard(shardIndex, startTs);

                _logger?.LogDebug("ReadRange (all-devices): scanning shard {ShardIndex} from address {Address}",
                    shardIndex, addr);

                var shardResults = _shards[shardIndex].ScanRangeAllDevices(addr, startTs, endTs, getTimestamp, limit);

                foreach (var (deviceId, items) in shardResults)
                {
                    result[deviceId] = items;
                }
            }

            var totalItems = result.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRange (all-devices): returned {TotalItems} items across {DeviceCount} devices",
                totalItems, result.Count);

            return result;
        }

        /// <summary>
        /// Returns the minimum earliest timestamp across all requested devices by scanning
        /// the FasterLog from the sparse index pointer. For each shard, looks up the nearest
        /// address and scans forward until it finds the first entry ≥ <paramref name="fromTs"/>
        /// for any of the requested devices. Returns null if no data exists.
        /// </summary>
        public long? GetEarliestTimestamp(IEnumerable<int> deviceIds, long fromTs, Func<T, long> getTimestamp)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            var deviceIdList = deviceIds.ToList();
            _logger?.LogDebug("GetEarliestTimestamp: deviceIds=[{DeviceIds}], fromTs={FromTs}",
                string.Join(", ", deviceIdList), fromTs);

            var shardGroups = new Dictionary<int, (HashSet<int> Devices, long MinAddress)>();

            foreach (int deviceId in deviceIdList)
            {
                int shardIndex = deviceId & ShardMask;
                long addr = LookupNearestAddress(deviceId, fromTs);

                if (shardGroups.TryGetValue(shardIndex, out var group))
                {
                    group.Devices.Add(deviceId);
                    if (addr < group.MinAddress)
                        shardGroups[shardIndex] = (group.Devices, addr);
                }
                else
                {
                    shardGroups[shardIndex] = (new HashSet<int> { deviceId }, addr);
                }
            }

            long? globalMin = null;

            _logger?.LogDebug("GetEarliestTimestamp: grouped into {ShardCount} shards", shardGroups.Count);

            foreach (var (shardIndex, (devices, minAddress)) in shardGroups)
            {
                _logger?.LogDebug("GetEarliestTimestamp: searching shard {ShardIndex} with {DeviceCount} devices from address {MinAddress}",
                    shardIndex, devices.Count, minAddress);

                long? shardMin = _shards[shardIndex].FindFirstTimestamp(devices, minAddress, fromTs, getTimestamp);

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
        /// Find the largest indexed timestamp ≤ startTs for this device.
        /// Returns the corresponding FasterLog address, or 0 if no index entry exists (scan from beginning).
        /// </summary>
        private long LookupNearestAddress(int deviceId, long startTs)
        {
            using var pooled = GetConnection();
            using var cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "SELECT log_address FROM stream_index WHERE device_id = $did AND timestamp <= $ts ORDER BY timestamp DESC LIMIT 1";
            cmd.Parameters.AddWithValue("$did", deviceId);
            cmd.Parameters.AddWithValue("$ts", startTs);
            object? result = cmd.ExecuteScalar();
            long addr = result is long a ? a : 0L;

            _logger?.LogDebug("LookupNearestAddress: deviceId={DeviceId}, startTs={StartTs} -> address={Address}",
                deviceId, startTs, addr);

            return addr;
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

                // Disable index writes atomically with memory barrier - new writes won't queue index entries
                Interlocked.Exchange(ref _indexWritesDisabled, 1);

                // Signal flush worker to wake up and process any pending work
                _flushSignal.Set();

                // Wait for worker to reach idle state (not actively writing).
                // If it has reached an IDLE state it will create anymore SQLITE transactions
                _flushWorkerIdle.Wait();

                // Additional safety: wait for pending queue to be fully drained
                var sw = Stopwatch.StartNew();
                while (!_pendingIndexInserts.IsEmpty && sw.Elapsed < TimeSpan.FromSeconds(10))
                {
                    Thread.Sleep(10);
                }

                if (!_pendingIndexInserts.IsEmpty)
                {
                    _logger?.LogWarning("RunCheckpoint: proceeding with {Count} pending index entries still queued", _pendingIndexInserts.Count);
                }

                try
                {
                    sw.Restart();
                    using var pooled = GetConnection();
                    using var cmd = pooled.Connection.CreateCommand();
                    // This mode blocks (invokes the busy-handler callback) until there is no database writer and 
                    // all readers are reading from the most recent database snapshot. 
                    // It then checkpoints all frames in the log file and syncs the database file. 
                    // FULL blocks concurrent writers while it is running, but readers can proceed.
                    cmd.CommandText = "PRAGMA wal_checkpoint(FULL)";

                    // PRAGMA wal_checkpoint returns (busy, log, checkpointed)
                    // busy: number of frames that couldn't be checkpointed (0 = success)
                    // log: total frames in WAL before checkpoint
                    // checkpointed: frames actually checkpointed
                    using var reader = cmd.ExecuteReader();
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
                    // Re-enable index writes atomically with memory barrier
                    Interlocked.Exchange(ref _indexWritesDisabled, 0);

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
            using var pooled = GetConnection();
            using var cmd = pooled.Connection.CreateCommand();
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

        #endregion

        #region Utils

        /// <summary>
        /// Find the minimum log_address across all devices in a specific shard where timestamp ≤ startTs.
        /// This provides an efficient starting point for wildcard scans that need to read all devices in a shard.
        /// Returns 0 if no index entry exists (scan from beginning).
        /// </summary>
        private long LookupNearestAddressForShard(int shardIndex, long startTs)
        {
            using var pooled = GetConnection();
            using var cmd = pooled.Connection.CreateCommand();
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
            if (_connPool.TryDequeue(out var conn))
                return new PooledConnection(conn, _connPool);

            conn = new SqliteConnection(_connectionString);
            conn.Open();

            using var pragma = conn.CreateCommand();
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;";
            pragma.ExecuteNonQuery();

            return new PooledConnection(conn, _connPool);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private LogShard GetShard(int deviceId) => _shards[deviceId & ShardMask];

        /// <summary>
        /// Reads a device ID from raw bytes and converts it to int for shard selection and indexing.
        /// Supports ushort (regular telemetry) and uint (ADSB data).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ReadDeviceIdAsInt(ReadOnlySpan<byte> data)
        {
            if (typeof(TDeviceId) == typeof(ushort))
                return MemoryMarshal.Read<ushort>(data);
            if (typeof(TDeviceId) == typeof(uint))
                return (int)MemoryMarshal.Read<uint>(data);

            throw new NotSupportedException($"Device ID type {typeof(TDeviceId)} is not supported. Only ushort and uint are supported.");
        }
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
            _flushWorkerIdle.Dispose();

            // Dispose shards (commits FasterLog)
            foreach (var shard in _shards)
            {
                shard.Dispose();
            }

            // Dispose connection pool
            while (_connPool.TryDequeue(out var conn))
            {
                conn.Dispose();
            }

            // Note: We intentionally do NOT flush pending index entries on dispose.
            // The sparse index is a performance optimization and missing a few entries
            // at shutdown is acceptable - reads will still work by scanning from the
            // previous indexed entry.
        }

        /// <summary>
        /// A single FasterLog shard shared by multiple devices.
        /// Devices are assigned to shards via <c>deviceId &amp; ShardMask</c>.
        /// </summary>
        private sealed class LogShard : IDisposable
        {
            private readonly FasterLog _log;

            public long BeginAddress => _log.BeginAddress;
            public long TailAddress => _log.TailAddress;

            public LogShard(int shardIndex, string baseDir)
            {
                string logPath = Path.Combine(baseDir, shardIndex.ToString(), string.Format(Constants.ShardLogFmt, shardIndex));
                var settings = new FasterLogSettings
                {
                    LogDevice = Devices.CreateLogDevice(logPath, deleteOnClose: false),
                    PageSizeBits = 22,   // 4 MB pages
                    MemorySizeBits = 24, // 16 MB in-memory
                    TryRecoverLatest = true,
                    AutoRefreshSafeTailAddress = true,
                    AutoCommit = true,
                    // LogCommitPolicy = LogCommitPolicy.RateLimit(1_000, 1024 * 1024 * 1024) // 1 mb/s or 1 second, whichever comes first
                };
                _log = new FasterLog(settings);
            }

            public long Enqueue(in T item, bool waitForCommit)
            {
                ReadOnlySpan<byte> payload = MemoryMarshal.AsBytes(new ReadOnlySpan<T>(in item));
                return waitForCommit ? _log.EnqueueAndWaitForCommit(payload) : _log.Enqueue(payload);
            }

            /// <summary>
            /// Scan the shard from <paramref name="fromAddress"/> and collect items belonging to
            /// <paramref name="deviceId"/> whose timestamp falls within [startTs, endTs].
            /// Delegates to the multi-device overload with a single-element set.
            /// </summary>
            public List<T> ScanRange(int deviceId, long fromAddress, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0)
            {
                var results = ScanRange(new HashSet<int>(1) { deviceId }, fromAddress, startTs, endTs, getTimestamp, limit);
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
            public Dictionary<int, List<T>> ScanRange(HashSet<int> deviceIds, long fromAddress, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0)
            {
                var results = new Dictionary<int, List<T>>(deviceIds.Count);
                foreach (int id in deviceIds)
                    results[id] = new List<T>();

                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return results;

                // Track which devices have passed endTs or hit the limit so we can stop early when all are done.
                var finished = new HashSet<int>();
                bool hasLimit = limit > 0;

                int itemSize = Unsafe.SizeOf<T>();

                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < itemSize)
                        continue;

                    // All telemetry structs have a device ID field at offset 0 (ushort for regular telemetry, uint for ADSB).
                    // Fast-path filter: skip entries from other devices without deserializing the full struct.
                    int entryDeviceId = ReadDeviceIdAsInt(entry.AsSpan(0, Unsafe.SizeOf<TDeviceId>()));
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

            /// <summary>
            /// Scan the shard from <paramref name="fromAddress"/> and collect items for all devices
            /// whose timestamps fall within [startTs, endTs]. Returns a dictionary keyed by deviceId,
            /// lazily creating entries as devices are discovered during the scan.
            /// </summary>
            public Dictionary<int, List<T>> ScanRangeAllDevices(long fromAddress, long startTs, long endTs, Func<T, long> getTimestamp, int limit = 0)
            {
                var results = new Dictionary<int, List<T>>();
                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return results;

                var finished = new HashSet<int>();
                bool hasLimit = limit > 0;

                int itemSize = Unsafe.SizeOf<T>();
                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < itemSize)
                        continue;

                    int entryDeviceId = ReadDeviceIdAsInt(entry.AsSpan(0, Unsafe.SizeOf<TDeviceId>()));
                    if (finished.Contains(entryDeviceId))
                        continue;

                    T item = MemoryMarshal.Read<T>(entry.AsSpan(0, entryLength));
                    long ts = getTimestamp(item);

                    if (ts > endTs)
                    {
                        finished.Add(entryDeviceId);
                        continue;
                    }

                    if (ts >= startTs)
                    {
                        // Lazily create result lists only when we have data to add
                        if (!results.ContainsKey(entryDeviceId))
                            results[entryDeviceId] = new List<T>();

                        results[entryDeviceId].Add(item);

                        if (hasLimit && results[entryDeviceId].Count >= limit)
                        {
                            finished.Add(entryDeviceId);
                        }
                    }
                }

                return results;
            }

            /// <summary>
            /// Scan the shard from <paramref name="fromAddress"/> and find the first entry
            /// for any device in <paramref name="deviceIds"/> whose timestamp is ≥ <paramref name="fromTs"/>.
            /// Returns the minimum such timestamp, or null if none found.
            /// </summary>
            public long? FindFirstTimestamp(HashSet<int> deviceIds, long fromAddress, long fromTs, Func<T, long> getTimestamp)
            {
                long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
                long tailAddr = _log.SafeTailAddress;

                if (beginAddr >= tailAddr)
                    return null;

                int itemSize = Unsafe.SizeOf<T>();
                using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
                while (iter.GetNext(out byte[] entry, out int entryLength, out _))
                {
                    if (entryLength < itemSize)
                        continue;

                    int entryDeviceId = ReadDeviceIdAsInt(entry.AsSpan(0, Unsafe.SizeOf<TDeviceId>()));
                    if (!deviceIds.Contains(entryDeviceId))
                        continue;

                    T item = MemoryMarshal.Read<T>(entry.AsSpan(0, entryLength));
                    long ts = getTimestamp(item);

                    if (ts >= fromTs)
                        return ts;
                }

                return null;
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
}
