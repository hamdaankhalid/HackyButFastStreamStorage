/*
Holds a skiplist that stores key-value pairs in-memory.
Whenever flush is called it does 2 things:
1. Swap the skiplist with double buffering.
2. Flush the previous skiplist to disk as a segment file and clear it after flushing completes.

Thread-Safety:
- Put() is not really thread-safe: It expects writes to be single-threaded such that they respect monotonicity
- Session tracking via Interlocked ensures all in-flight Put() operations complete before flush
- Each Put() increments/decrements _skipListSessions counter using Interlocked
- Commit() waits for _skipListSessions[oldIndex] to reach zero before flushing
- TryGet() uses binary search to locate segments and handles three states:
  1. In-memory: reads from active or inactive skiplist with session tracking
  2. Being flushed: waits for _flushingSegmentIndex to clear before reading
  3. Already flushed: reads directly from segment file
- Commit() uses Monitor.TryEnter to prevent concurrent flushes (non-blocking)
- Double-buffered design allows writes to continue during flush (writes go to new skiplist)
- Reads (TryGet/QueryRange) can happen concurrently with writes/flushes
- Keys are strictly monotonic, so no deduplication is needed during queries
*/
namespace StreamDB.LiteLsm;

public class LiteLsm<TKey, TValue> where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
{
    private readonly IMonotonicSkipList<TKey, TValue>[] _skipLists;
    private readonly SegmentManager _segmentManager;
    private readonly int _memTableCapacity;
    private int _activeSkipListIndex = 0;
    private readonly int[] _skipListSessions = new int[2];  // Track active sessions per skiplist
    private int _flushingSegmentIndex = -1;                 // Segment currently being flushed (-1 = none)
    private readonly object _commitLock = new object();     // Uncontended lock for flush synchronization with other flush calls
    private Task? _pendingFlush;                            // Background flush task

    /// <summary>
    /// Creates a new LiteLsm with a custom skip list factory.
    /// Useful for testing or custom implementations.
    /// </summary>
    /// <param name="factory">Factory function that creates skip list instances.</param>
    /// <param name="segmentDirectory">Directory for segment files.</param>
    /// <param name="memTableCapacity">Number of entries before flushing to disk. Must match skip list capacity.</param>
    public LiteLsm(Func<IMonotonicSkipList<TKey, TValue>> factory, string segmentDirectory = "segments", int memTableCapacity = SegmentManager.SegmentSize)
    {
        _memTableCapacity = memTableCapacity;
        _skipLists = new IMonotonicSkipList<TKey, TValue>[2];
        _skipLists[0] = factory();
        _skipLists[1] = factory();
        _segmentManager = new SegmentManager(segmentDirectory);
    }

    public void Put(TKey key, TValue value)
    {
        int activeIndex = EnterSkipListSession();
        bool needsFlush;
        try
        {
            _skipLists[activeIndex].InsertMonotonic(key, value);
            needsFlush = _skipLists[activeIndex].Count >= _memTableCapacity;
        }
        finally
        {
            ExitSkipListSession(activeIndex);
        }

        // Auto-flush must happen outside session tracking to avoid self-deadlock:
        // Commit() waits for _skipListSessions to drain to zero, so the calling
        // thread must not be holding a session count.
        if (needsFlush)
        {
            Commit();
        }
    }

    private void Commit()
    {
        // Try to acquire the lock; if someone else is already flushing, just return. This is not a common case so this lock will be very much uncontended
        if (!Monitor.TryEnter(_commitLock))
        {
            return;
        }

        try
        {
            // If a previous flush is still running, we must wait — both buffers are in use
            _pendingFlush?.Wait();

            int currentIndex = Volatile.Read(ref _activeSkipListIndex);
            var skipListToFlush = _skipLists[currentIndex];

            if (skipListToFlush.Count == 0)
            {
                return;
            }

            int nextIndex = (currentIndex + 1) % 2;

            // Switch to the other skiplist atomically
            // After this point, new writes go to the other skiplist
            Volatile.Write(ref _activeSkipListIndex, nextIndex);

            // Memory barrier to ensure all threads see the index switch
            Thread.MemoryBarrier();

            // Wait for all active sessions on the old skiplist to complete
            // This ensures no thread is in the middle of Put() using the old skiplist
            while (Volatile.Read(ref _skipListSessions[currentIndex]) > 0)
            {
                Thread.Yield();
            }

            // Launch flush on background thread so the writer is not blocked by disk I/O
            int segmentIndex = _segmentManager.NumSegments;
            _pendingFlush = Task.Run(() =>
            {
                Interlocked.Exchange(ref _flushingSegmentIndex, segmentIndex);

                _segmentManager.Flush<TKey, TValue>(skipListToFlush);

                Interlocked.Exchange(ref _flushingSegmentIndex, -1);

                // Wait for any TryGet readers still scanning this skiplist before clearing
                while (Volatile.Read(ref _skipListSessions[currentIndex]) > 0)
                {
                    Thread.Yield();
                }

                skipListToFlush.Clear();
            });
        }
        finally
        {
            Monitor.Exit(_commitLock);
        }
    }

    public void WaitForPendingFlush() => _pendingFlush?.Wait();

    // used by iterators to read batches
    internal (bool _hasMoreBatchesToRead, int _bufferedBatchCount)
        ReadBatchInto(TKey readFrom, TKey endKey, Span<(TKey, TValue)> bufferedBatch, bool excludeStart = false)
    {
        int totalRead = 0;

        // Step 1: Try on-disk segments first (contain older/lower keys due to monotonic ordering)
        var segResult = _segmentManager.ReadRangeFromSegment<TKey, TValue>(readFrom, endKey, bufferedBatch, excludeStart);
        totalRead = segResult.EntriesRead;

        // If buffer is full or segments signaled more data within range, return
        if (totalRead >= bufferedBatch.Length || segResult.HasMore)
        {
            return (true, totalRead);
        }

        // Step 2: Segments exhausted or partially filled — try active memtable for continuation.
        // MemCpyRawRange handles the case where readFrom < memMinKey by starting from the first valid node.
        // If we already read segment data, the memtable keys are strictly greater (monotonic),
        // so excludeStart only applies if no segment data was read.
        bool memExcludeStart = excludeStart && totalRead == 0;
        int activeIdx = EnterSkipListSession();
        try
        {
            IMonotonicSkipList<TKey, TValue> skipList = _skipLists[activeIdx];
            if (skipList.Count > 0)
            {
                var remaining = bufferedBatch.Slice(totalRead);
                var memResult = skipList.MemCpyRawRange(readFrom, endKey, remaining, memExcludeStart);
                totalRead += memResult.Item2;
                return (memResult.Item1, totalRead);
            }
        }
        finally
        {
            ExitSkipListSession(activeIdx);
        }

        return (false, totalRead);
    }


    /// <summary>
    /// Create an iterator to read ranges of keys and values via IEnumerable interface.
    /// This allows you to read large ranges. The iterator will read in batches of the specified size to minimize memory usage.
    /// </summary>
    /// <param name="fromKey"></param>
    /// <param name="endKey"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public LiteLsmIterator<TKey, TValue> GetIterator(TKey fromKey, TKey endKey, int batchSize = 256) => new LiteLsmIterator<TKey, TValue>(this, fromKey, endKey, batchSize);

    /// <summary>
    /// Tries to get a value by key. Uses binary search to find the correct segment,
    /// then handles in-memory, flushing, or flushed states.
    /// </summary>
    public bool TryGet(TKey key, out TValue value)
    {
        value = default;

        // Case 0: key in memory. Search in memory via fast ProbablyContainsKey first.
        int activeIdx = EnterSkipListSession();
        bool foundInActive = TryGetKeyFromMemory(_skipLists[activeIdx], key, out value);
        ExitSkipListSession(activeIdx);
        if (foundInActive)
        {
            return true;
        }

        // No active session lock is held since we are searching for segments on disk at this point
        int segmentIdx = _segmentManager.FindSegmentForKey(key);
        // TODO: Above segment manager method uses binary search but it won't be able to read when a segment file is actively being flushed.
        // This means we cannot really get to checking if we are reading from a segment mid-flush... This is a bug I will fix later

        // Case 1: The segment is actively being flushed. Wait for the flush to complete. Then read from disk
        if (IsFlushingSegment(segmentIdx))
        {
            WaitForPendingFlush();
        }

        // Case 2: Key falls within a segment, try to read from disk
        return _segmentManager.TryGetFromSegment<TKey, TValue>(segmentIdx, key, out value);
    }

    private bool TryGetKeyFromMemory(IMonotonicSkipList<TKey, TValue> skipList, TKey key, out TValue value)
    {
        value = default;
        if (!skipList.ProbablyContainsKey(key))
        {
            return false;
        }
        return skipList.TryGetValue(key, out value);
    }

    private bool IsFlushingSegment(int segmentIndex) => Volatile.Read(ref _flushingSegmentIndex) == segmentIndex;

    /// <summary>
    /// Gets statistics about the LSM tree.
    /// </summary>
    public LsmStats GetStats()
    {
        int activeIndex = Volatile.Read(ref _activeSkipListIndex);
        return new LsmStats
        {
            ActiveMemTableCount = _skipLists[activeIndex].Count,
            InactiveMemTableCount = _skipLists[(activeIndex + 1) % 2].Count,
            NumSegments = _segmentManager.NumSegments - _segmentManager.BeginSegmentIndex,
            EarliestSegmentIndex = _segmentManager.BeginSegmentIndex,
            LatestSegmentIndex = _segmentManager.NumSegments - 1
        };
    }

    /// <summary>
    /// Truncates all segments whose keys are strictly less than the given key.
    /// Frees disk space by deleting fully-truncated segment files.
    /// Only affects on-disk segments; in-memory data is not modified.
    /// </summary>
    public int Truncate(TKey beforeKey)
    {
        WaitForPendingFlush();

        int newBegin = _segmentManager.BeginSegmentIndex;

        // Walk segments from oldest to newest, truncating those entirely below beforeKey
        for (int i = _segmentManager.BeginSegmentIndex; i < _segmentManager.NumSegments; i++)
        {
            if (_segmentManager.TryGetSegmentMaxKey<TKey>(i, out TKey maxKey) && maxKey.CompareTo(beforeKey) < 0)
            {
                newBegin = i + 1;
            }
            else
            {
                break; // Keys are monotonic, so all subsequent segments have higher keys
            }
        }

        int truncated = newBegin - _segmentManager.BeginSegmentIndex;
        if (truncated > 0)
        {
            _segmentManager.TruncateBefore(newBegin);
        }
        return truncated;
    }

    // Every method that accesses the skiplist for reading or writing must call TryEnter() at the beginning to ensure it doesn't run concurrently with a flush that is clearing the skiplist. 
    private int EnterSkipListSession()
    {
        while (true)
        {
            int activeIndex = Volatile.Read(ref _activeSkipListIndex);
            Interlocked.Increment(ref _skipListSessions[activeIndex]);
            // if the active skipist got switched between our read and increment then 
            // we need to redo the increment on the new active skiplist after we decrement the old one
            if (Volatile.Read(ref _activeSkipListIndex) == activeIndex)
            {
                return activeIndex;
            }
            else
            {
                Interlocked.Decrement(ref _skipListSessions[activeIndex]);
            }
        }
    }

    private void ExitSkipListSession(int sessionIdx) => Interlocked.Decrement(ref _skipListSessions[sessionIdx]);
}