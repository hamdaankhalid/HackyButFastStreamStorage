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

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace StreamDB
{
    /// <summary>
    /// Schema-on-read stream storage engine. Shards writes across a fixed pool of FasterLog
    /// instances and uses a shared SQLite database for sparse primary index→address index lookups.
    ///
    /// Records are stored with a 16-byte header:
    /// [8B primary index] [4B secondary index] [2B version] [2B payload length] [payload bytes]
    ///
    /// The secondary index is used for sharding (<c>secondaryIndex &amp; ShardMask</c>) and scan filtering.
    /// For telemetry data the secondary index typically stores the device ID, but it can represent
    /// any grouping key (sensor ID, user ID, region, etc.).
    ///
    /// Write path: Append header + payload bytes to the shard, every Nth write insert (primary index, address) into SQLite.
    /// Read path:  Query SQLite for the closest address ≤ startPrimaryIndex, scan the shard filtering by secondary index.
    ///
    /// Retention: A background timer periodically removes index entries older than the retention
    /// period and truncates each FasterLog shard up to the minimum surviving address.
    /// </summary>
    public sealed class StreamDB : IStreamDb
    {
        #region member variables and Consts

        private const string StreamIndexDbName = "stream_index.db";

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
        // Bit-mask for fast shard selection: secondaryIndex & ShardMask == secondaryIndex % ShardCount.
        private const int ShardMask = ShardCount - 1;

        private readonly string _baseDir;
        private readonly LogShard[] _shards;
        private readonly ConcurrentDictionary<int, int> _writeCounts = new();
        private readonly string _connectionString;
        private readonly ConcurrentQueue<SqliteConnection> _connPool = new();
        private readonly ConcurrentQueue<(int SecondaryIndex, long PrimaryIndex, long Address)> _pendingIndexInserts = new();
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

        // Late arrivals: out-of-order write support
        private readonly ConcurrentDictionary<int, long> _maxPrimaryIndexes = new();
        private readonly LateArrivalsStore _lateArrivals;
        private long _lateArrivalCount = 0;

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
                    secondary_index INTEGER NOT NULL,
                    primary_index INTEGER NOT NULL,
                    log_address INTEGER NOT NULL,
                    PRIMARY KEY (secondary_index, primary_index)
                ) WITHOUT ROWID;
                """;
            cmd.ExecuteNonQuery();

            _lateArrivals = new LateArrivalsStore(GetConnection);

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
                WHERE (secondary_index & $mask) = $shard
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
            List<(int SecondaryIndex, long PrimaryIndex, long Address)> batch = new List<(int SecondaryIndex, long PrimaryIndex, long Address)>(capacity: AdaptiveTuning[^1].batchSize);
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
                            while (collected < batchSize && _pendingIndexInserts.TryDequeue(out (int SecondaryIndex, long PrimaryIndex, long Address) entry))
                            {
                                // Successfully dequeued - decrement count immediately
                                Interlocked.Decrement(ref _pendingIndexCount);

                                batch.Add(entry);
                                var shardIndex = entry.SecondaryIndex & ShardMask;
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
                            cmd.CommandText = "INSERT INTO stream_index (secondary_index, primary_index, log_address) VALUES ($sidx, $pi, $addr)";
                            SqliteParameter pDid = cmd.Parameters.Add("$sidx", SqliteType.Integer);
                            SqliteParameter pPi = cmd.Parameters.Add("$pi", SqliteType.Integer);
                            SqliteParameter pAddr = cmd.Parameters.Add("$addr", SqliteType.Integer);
                            cmd.Prepare();

                            int written = 0;
                            foreach ((int SecondaryIndex, long PrimaryIndex, long Address) entry in batch)
                            {
                                pDid.Value = entry.SecondaryIndex;
                                pPi.Value = entry.PrimaryIndex;
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
        /// followed by the raw payload bytes. The primary index is used for
        /// range queries; the secondary index is used for shard selection and sparse indexing.
        ///
        /// If the primary index is lower than the maximum previously seen for this secondary index
        /// (a late arrival), the entry is routed to a SQLite side store instead of FasterLog.
        /// Reads merge both sources transparently.
        /// </summary>
        /// <param name="primaryIndex">Primary index – monotonic key used for range queries (e.g. Unix epoch timestamp, sequence number).</param>
        /// <param name="secondaryIndex">Grouping key used for shard selection and sparse indexing.</param>
        /// <param name="version">Schema version tag stored in the record header.</param>
        /// <param name="payload">Raw payload bytes appended after the 16-byte header.</param>
        public void Append(long primaryIndex, int secondaryIndex, ushort version, ReadOnlySpan<byte> payload)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // Detect out-of-order (late arrival) writes.
            // AddOrUpdate returns the NEW value: max(prev, primaryIndex).
            // If primaryIndex < returned value, a higher primary index was already seen → late arrival.
            long maxPi = _maxPrimaryIndexes.AddOrUpdate(
                secondaryIndex,
                static (_, arg) => arg,
                static (_, prev, arg) => Math.Max(prev, arg),
                primaryIndex);
            if (primaryIndex < maxPi)
            {
                Interlocked.Increment(ref _lateArrivalCount);
                // Acquire read lock so late arrival writes are blocked during checkpoint (write lock)
                _indexWriteLock.EnterReadLock();
                try
                {
                    _lateArrivals.Insert(secondaryIndex, primaryIndex, version, payload);
                }
                finally
                {
                    _indexWriteLock.ExitReadLock();
                }
                return;
            }

            // Normal (monotonic) path: append to FasterLog
            LogShard shard = GetShard(secondaryIndex);
            int count = _writeCounts.AddOrUpdate(secondaryIndex, 1, static (_, prev) => prev + 1);

            // Use adaptive indexing frequency
            (int indexSpacing, int batchSize, int indexSpacingMask) = AdaptiveTuning[Volatile.Read(ref _adaptiveIdx)];
            bool shouldIndex = (count & indexSpacingMask) == 0;

            long address = shard.Enqueue(secondaryIndex, payload, primaryIndex, version);

            if (shouldIndex)
            {
                // Soft-bounded queue: check count for backpressure (eventually consistent)
                // Race condition is acceptable - queue may briefly exceed capacity before settling
                // Increment before enqueue to reduce drift window (decrement happens immediately on dequeue)
                int currentCount = Volatile.Read(ref _pendingIndexCount);
                if (currentCount < QueueCapacity)
                {
                    Interlocked.Increment(ref _pendingIndexCount);
                    _pendingIndexInserts.Enqueue((secondaryIndex, primaryIndex, address));
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
                Volatile.Read(ref _pendingIndexCount),
                Volatile.Read(ref _lateArrivalCount)
            );

        #endregion

        #region Read Methods

        public List<StreamEntry> ReadRange(int secondaryIndex, long startPrimaryIndex, long endPrimaryIndex, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _logger?.LogDebug("ReadRange: secondaryIndex={SecondaryIndex}, start={StartPrimaryIndex}, end={EndPrimaryIndex}, limit={Limit}",
                secondaryIndex, startPrimaryIndex, endPrimaryIndex, limit);

            long scanFrom = LookupNearestAddress(secondaryIndex, startPrimaryIndex);
            _logger?.LogDebug("ReadRange: secondaryIndex={SecondaryIndex} -> scanFrom={ScanFrom} (shard={Shard})",
                secondaryIndex, scanFrom, secondaryIndex & ShardMask);

            LogShard shard = GetShard(secondaryIndex);
            List<StreamEntry> results = shard.ScanRange(secondaryIndex, scanFrom, startPrimaryIndex, endPrimaryIndex, limit);

            // Only query the late arrivals store if there are any late arrivals
            if (Volatile.Read(ref _lateArrivalCount) > 0)
            {
                List<StreamEntry> lateResults = _lateArrivals.QueryRange(secondaryIndex, startPrimaryIndex, endPrimaryIndex, limit);
                if (lateResults.Count > 0)
                    results = MergeByPrimaryIndex(results, lateResults, limit);
            }

            _logger?.LogDebug("ReadRange: secondaryIndex={SecondaryIndex} -> returned {Count} items",
                secondaryIndex, results.Count);

            return results;
        }

        /// <summary>
        /// Zero-allocation range scan for a single secondary index. Invokes <paramref name="handler"/>
        /// for each matching entry with a <see cref="StreamEntryView"/> whose payload references
        /// FasterLog's internal buffer — no <c>byte[]</c> is allocated per entry.
        ///
        /// Late arrivals are merged in primary index order. The handler must process or copy the
        /// payload inline; it is invalid after the callback returns.
        /// Return <c>false</c> from the handler to stop scanning early.
        /// </summary>
        public void ReadRangePooled(int secondaryIndex, long startPrimaryIndex, long endPrimaryIndex, StreamEntryHandler handler)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // Pre-fetch late arrivals (if any) so we can merge them in order during the scan
            List<StreamEntry>? lateEntries = null;
            if (Volatile.Read(ref _lateArrivalCount) > 0)
            {
                var late = _lateArrivals.QueryRange(secondaryIndex, startPrimaryIndex, endPrimaryIndex);
                if (late.Count > 0)
                    lateEntries = late;
            }

            long scanFrom = LookupNearestAddress(secondaryIndex, startPrimaryIndex);
            LogShard shard = GetShard(secondaryIndex);

            if (lateEntries == null)
            {
                // Fast path: no late arrivals, pure zero-allocation scan
                shard.ScanRange(secondaryIndex, scanFrom, startPrimaryIndex, endPrimaryIndex, handler);
                return;
            }

            // Merge path: interleave FasterLog scan with pre-fetched late arrivals in primary index order
            int lateIdx = 0;
            shard.ScanRange(secondaryIndex, scanFrom, startPrimaryIndex, endPrimaryIndex,
                (in StreamEntryView logEntry) =>
                {
                    // Emit any late arrivals that come before this log entry
                    while (lateIdx < lateEntries.Count && lateEntries[lateIdx].PrimaryIndex <= logEntry.PrimaryIndex)
                    {
                        StreamEntry le = lateEntries[lateIdx++];
                        var lateView = new StreamEntryView
                        {
                            PrimaryIndex = le.PrimaryIndex,
                            SecondaryIndex = le.SecondaryIndex,
                            Version = le.Version,
                            Payload = le.Payload
                        };
                        if (!handler(in lateView))
                            return false;
                    }
                    return handler(in logEntry);
                });

            // Emit remaining late arrivals that come after all log entries
            while (lateIdx < lateEntries.Count)
            {
                StreamEntry le = lateEntries[lateIdx++];
                var lateView = new StreamEntryView
                {
                    PrimaryIndex = le.PrimaryIndex,
                    SecondaryIndex = le.SecondaryIndex,
                    Version = le.Version,
                    Payload = le.Payload
                };
                if (!handler(in lateView))
                    break;
            }
        }

        /// <summary>
        /// Multi-index overload of ReadRange. Groups secondary indexes by shard to minimize redundant scanning.
        /// For each shard, looks up the nearest address ≤ startPrimaryIndex across all indexes in that shard, 
        /// then scans once per shard to collect results for all indexes. Returns a dictionary keyed by secondary index.
        /// </summary>
        public Dictionary<int, List<StreamEntry>> ReadRange(IEnumerable<int> secondaryIndexes, long startPrimaryIndex, long endPrimaryIndex, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            List<int> indexList = secondaryIndexes.ToList();
            _logger?.LogDebug("ReadRange (multi-index): secondaryIndexes=[{Indexes}], start={StartPrimaryIndex}, end={EndPrimaryIndex}, limit={Limit}",
                string.Join(", ", indexList), startPrimaryIndex, endPrimaryIndex, limit);

            // Group secondary indexes by shard so we can scan each shard at most once.
            Dictionary<int, (HashSet<int> Indexes, long MinAddress)> shardGroups = new Dictionary<int, (HashSet<int> Indexes, long MinAddress)>();

            foreach (int idx in indexList)
            {
                int shardIndex = idx & ShardMask;
                long addr = LookupNearestAddress(idx, startPrimaryIndex);

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

            _logger?.LogDebug("ReadRange (multi-index): grouped into {ShardCount} shards", shardGroups.Count);

            foreach ((int shardIndex, (HashSet<int>? indexes, long minAddress)) in shardGroups)
            {
                _logger?.LogDebug("ReadRange (multi-index): scanning shard {ShardIndex} with {Count} indexes from address {MinAddress}",
                    shardIndex, indexes.Count, minAddress);

                Dictionary<int, List<StreamEntry>> shardResults = _shards[shardIndex].ScanRange(indexes, minAddress, startPrimaryIndex, endPrimaryIndex, limit);

                foreach ((int idx, List<StreamEntry>? items) in shardResults)
                {
                    result[idx] = items;
                }
            }

            // Only merge with late arrivals if there are any
            if (Volatile.Read(ref _lateArrivalCount) > 0)
                MergeLateArrivals(result, _lateArrivals.QueryRange(indexList, startPrimaryIndex, endPrimaryIndex, limit), limit);

            var totalItems = result.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRange (multi-index): returned {TotalItems} items across {Count} indexes",
                totalItems, result.Count);

            return result;
        }

        /// <summary>
        /// All-indexes overload of ReadRange. Scans all shards and returns data for every secondary index
        /// that has data in the specified primary index range.
        /// </summary>
        public Dictionary<int, List<StreamEntry>> ReadRange(long startPrimaryIndex, long endPrimaryIndex, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _logger?.LogDebug("ReadRange (all-indexes): start={StartPrimaryIndex}, end={EndPrimaryIndex}, limit={Limit}",
                startPrimaryIndex, endPrimaryIndex, limit);

            Dictionary<int, List<StreamEntry>> result = new Dictionary<int, List<StreamEntry>>();

            // Scan all shards
            for (int shardIndex = 0; shardIndex < ShardCount; shardIndex++)
            {
                long addr = LookupNearestAddressForShard(shardIndex, startPrimaryIndex);

                _logger?.LogDebug("ReadRange (all-indexes): scanning shard {ShardIndex} from address {Address}",
                    shardIndex, addr);

                Dictionary<int, List<StreamEntry>> shardResults = _shards[shardIndex].ScanRangeAll(addr, startPrimaryIndex, endPrimaryIndex, limit);

                foreach ((int idx, List<StreamEntry>? items) in shardResults)
                {
                    result[idx] = items;
                }
            }

            // Only merge with late arrivals if there are any
            if (Volatile.Read(ref _lateArrivalCount) > 0)
                MergeLateArrivals(result, _lateArrivals.QueryRangeAll(startPrimaryIndex, endPrimaryIndex, limit), limit);

            var totalItems = result.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRange (all-indexes): returned {TotalItems} items across {Count} indexes",
                totalItems, result.Count);

            return result;
        }

        /// <summary>
        /// Returns the minimum earliest primary index across all requested secondary indexes by scanning
        /// the FasterLog from the sparse index pointer and checking the late arrivals store.
        /// Returns null if no data exists.
        /// </summary>
        public long? GetEarliestPrimaryIndex(IEnumerable<int> secondaryIndexes, long fromPrimaryIndex)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            List<int> indexList = secondaryIndexes.ToList();
            _logger?.LogDebug("GetEarliestPrimaryIndex: secondaryIndexes=[{Indexes}], from={FromPrimaryIndex}",
                string.Join(", ", indexList), fromPrimaryIndex);

            Dictionary<int, (HashSet<int> Indexes, long MinAddress)> shardGroups = new Dictionary<int, (HashSet<int> Indexes, long MinAddress)>();

            foreach (int idx in indexList)
            {
                int shardIndex = idx & ShardMask;
                long addr = LookupNearestAddress(idx, fromPrimaryIndex);

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

            _logger?.LogDebug("GetEarliestPrimaryIndex: grouped into {ShardCount} shards", shardGroups.Count);

            foreach ((int shardIndex, (HashSet<int>? indexes, long minAddress)) in shardGroups)
            {
                _logger?.LogDebug("GetEarliestPrimaryIndex: searching shard {ShardIndex} with {Count} indexes from address {MinAddress}",
                    shardIndex, indexes.Count, minAddress);

                long? shardMin = _shards[shardIndex].FindFirstPrimaryIndex(indexes, minAddress, fromPrimaryIndex);

                _logger?.LogDebug("GetEarliestPrimaryIndex: shard {ShardIndex} returned {ShardMin}",
                    shardIndex, shardMin?.ToString() ?? "null");

                if (shardMin.HasValue && (!globalMin.HasValue || shardMin.Value < globalMin.Value))
                    globalMin = shardMin.Value;
            }

            // Also check late arrivals if any exist
            if (Volatile.Read(ref _lateArrivalCount) > 0)
            {
                long? lateMin = _lateArrivals.GetEarliestPrimaryIndex(indexList, fromPrimaryIndex);
                if (lateMin.HasValue && (!globalMin.HasValue || lateMin.Value < globalMin.Value))
                    globalMin = lateMin.Value;
            }

            _logger?.LogDebug("GetEarliestPrimaryIndex: final result = {GlobalMin}",
                globalMin?.ToString() ?? "null");

            return globalMin;
        }

        /// <summary>
        /// Combined availability-check and range-read in a single operation. Finds the earliest
        /// available entry ≥ <paramref name="fromPrimaryIndex"/> across both FasterLog and late arrivals,
        /// then reads a window of <paramref name="window"/> units of data from that point.
        ///
        /// Returns <c>RangeEnd = -1</c> when no data exists for any secondary index.
        /// </summary>
        public (long RangeEnd, Dictionary<int, List<StreamEntry>> Data) ReadRangeFromAvailable(
            IEnumerable<int> secondaryIndexes, long fromPrimaryIndex, long window, int limit = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            List<int> indexList = secondaryIndexes.ToList();

            _logger?.LogDebug("ReadRangeFromAvailable: secondaryIndexes=[{Indexes}], from={FromPrimaryIndex}, window={Window}, limit={Limit}",
                string.Join(", ", indexList), fromPrimaryIndex, window, limit);

            // Find the earliest available primary index across both sources
            long? earliest = GetEarliestPrimaryIndex(indexList, fromPrimaryIndex);
            if (!earliest.HasValue)
            {
                _logger?.LogDebug("ReadRangeFromAvailable: no data found");
                return (-1, new Dictionary<int, List<StreamEntry>>());
            }

            long rangeEnd = earliest.Value + window;

            // ReadRange already merges FasterLog + late arrivals
            Dictionary<int, List<StreamEntry>> data = ReadRange(indexList, earliest.Value, rangeEnd, limit);

            var totalItems = data.Values.Sum(list => list.Count);
            _logger?.LogDebug("ReadRangeFromAvailable: returned {TotalItems} items across {Count} indexes, rangeEnd={RangeEnd}",
                totalItems, data.Count, rangeEnd);

            return (rangeEnd, data);
        }

        /// <summary>
        /// Find the largest indexed primary index ≤ startPrimaryIndex for this secondary index.
        /// Returns the corresponding FasterLog address, or 0 if no index entry exists (scan from beginning).
        /// </summary>
        private long LookupNearestAddress(int secondaryIndex, long startPrimaryIndex)
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "SELECT log_address FROM stream_index WHERE secondary_index = $sidx AND primary_index <= $pi ORDER BY primary_index DESC LIMIT 1";
            SqliteParameter pSidx = cmd.Parameters.Add("$sidx", SqliteType.Integer);
            SqliteParameter pPi = cmd.Parameters.Add("$pi", SqliteType.Integer);
            cmd.Prepare();

            pSidx.Value = secondaryIndex;
            pPi.Value = startPrimaryIndex;
            object? result = cmd.ExecuteScalar();
            long addr = result is long a ? a : 0L;

            _logger?.LogDebug("LookupNearestAddress: secondaryIndex={SecondaryIndex}, start={StartPrimaryIndex} -> address={Address}",
                secondaryIndex, startPrimaryIndex, addr);

            return addr;
        }

        /// <summary>
        /// Returns all distinct secondary indexes that have at least one indexed entry in the stream.
        /// </summary>
        private List<int> GetAllSecondaryIndexes()
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "SELECT DISTINCT secondary_index FROM stream_index";
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
        /// Also checks the late arrivals store for entries that may be more recent.
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

                // Scan from latest index entry to end of shard with no primary index filter
                List<StreamEntry> records = shard.ScanRange(idx, scanFrom, 0, long.MaxValue, 0);
                if (records.Count > 0)
                {
                    result[idx] = records[^1]; // Keep only the latest record
                }
            }

            // Merge with latest from late arrivals — keep whichever has the higher primary index
            if (Volatile.Read(ref _lateArrivalCount) > 0)
            {
                Dictionary<int, StreamEntry> lateLatest = _lateArrivals.GetLatest();
                foreach ((int idx, StreamEntry lateEntry) in lateLatest)
                {
                    if (!result.TryGetValue(idx, out StreamEntry existing) || lateEntry.PrimaryIndex > existing.PrimaryIndex)
                    {
                        result[idx] = lateEntry;
                    }
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

                long cutoffPi = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - (long)_retentionPeriod.TotalSeconds;
                PurgeIndexBefore(cutoffPi);
                _lateArrivals.PurgeBefore(cutoffPi);
                TruncateShards(cutoffPi);

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
        /// Delete all sparse-index rows whose primary index is strictly before <paramref name="cutoffPi"/>.
        /// </summary>
        private void PurgeIndexBefore(long cutoffPi)
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = "DELETE FROM stream_index WHERE primary_index < $cutoff";
            cmd.Parameters.AddWithValue("$cutoff", cutoffPi);
            cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// For each shard, find the minimum surviving log_address across all secondary indexes that map
        /// to that shard, then truncate the FasterLog up to that address.
        /// </summary>
        private void TruncateShards(long cutoffPi)
        {
            // For each shard, determine the safe truncation address.
            // Query the minimum log_address still alive in the index per shard.
            using PooledConnection pooled = GetConnection();

            for (int shardIndex = 0; shardIndex < ShardCount; shardIndex++)
            {
                // Find the minimum log_address among index entries that survived the purge
                // and belong to secondary indexes assigned to this shard.
                using SqliteCommand cmd = pooled.Connection.CreateCommand();
                cmd.CommandText =
                    """
                    SELECT MIN(log_address) FROM stream_index
                    WHERE (secondary_index & $mask) = $shard
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
        /// Find the minimum log_address across all secondary indexes in a specific shard where primary_index ≤ startPrimaryIndex.
        /// This provides an efficient starting point for wildcard scans that need to read all indexes in a shard.
        /// Returns 0 if no index entry exists (scan from beginning).
        /// </summary>
        private long LookupNearestAddressForShard(int shardIndex, long startPrimaryIndex)
        {
            using PooledConnection pooled = GetConnection();
            using SqliteCommand cmd = pooled.Connection.CreateCommand();
            cmd.CommandText = @"
                SELECT MIN(log_address) 
                FROM stream_index 
                WHERE (secondary_index & $mask) = $shard 
                  AND primary_index <= $pi";
            cmd.Parameters.AddWithValue("$mask", ShardMask);
            cmd.Parameters.AddWithValue("$shard", shardIndex);
            cmd.Parameters.AddWithValue("$pi", startPrimaryIndex);
            object? result = cmd.ExecuteScalar();
            long addr = result is long a ? a : 0L;

            _logger?.LogDebug("LookupNearestAddressForShard: shardIndex={ShardIndex}, start={StartPrimaryIndex} -> address={Address}",
                shardIndex, startPrimaryIndex, addr);

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

        /// <summary>
        /// Merge two primary-index-sorted lists into one, applying an optional per-index limit.
        /// Both input lists must be sorted by primary index ascending.
        /// </summary>
        private static List<StreamEntry> MergeByPrimaryIndex(List<StreamEntry> a, List<StreamEntry> b, int limit = 0)
        {
            if (b.Count == 0) return a;
            if (a.Count == 0) return b;

            var merged = new List<StreamEntry>(a.Count + b.Count);
            int i = 0, j = 0;
            while (i < a.Count && j < b.Count)
            {
                if (limit > 0 && merged.Count >= limit) break;
                if (a[i].PrimaryIndex <= b[j].PrimaryIndex)
                    merged.Add(a[i++]);
                else
                    merged.Add(b[j++]);
            }
            while (i < a.Count && (limit <= 0 || merged.Count < limit)) merged.Add(a[i++]);
            while (j < b.Count && (limit <= 0 || merged.Count < limit)) merged.Add(b[j++]);

            return merged;
        }

        /// <summary>
        /// Merge late arrivals into a multi-index result dictionary. For each secondary index,
        /// merges the primary-index-sorted lists and applies the per-index limit.
        /// </summary>
        private static void MergeLateArrivals(Dictionary<int, List<StreamEntry>> result, Dictionary<int, List<StreamEntry>> lateResults, int limit)
        {
            foreach ((int idx, List<StreamEntry> lateEntries) in lateResults)
            {
                if (result.TryGetValue(idx, out List<StreamEntry>? existing))
                {
                    result[idx] = MergeByPrimaryIndex(existing, lateEntries, limit);
                }
                else
                {
                    result[idx] = limit > 0 && lateEntries.Count > limit
                        ? lateEntries.GetRange(0, limit)
                        : lateEntries;
                }
            }
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

    }

    public record class StreamDbStats(long ScaleUp, long ScaleDown, long Dropped, int AdaptiveIdx, long PendingIdxQueueLen, long LateArrivals);
}