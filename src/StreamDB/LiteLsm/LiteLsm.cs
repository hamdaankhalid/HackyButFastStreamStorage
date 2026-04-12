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

/// <summary>
/// Specifies which skip list implementation to use.
/// </summary>
public enum SkipListType
{
  /// <summary>
  /// Class-based skip list with ArrayPool optimization.
  /// Good general-purpose choice with ~90% GC reduction.
  /// </summary>
  ClassBased,
  
  /// <summary>
  /// Struct-based skip list with zero allocations.
  /// Use for ultra-low latency / zero-GC requirements.
  /// </summary>
  StructBased
}

class LiteLsm<TKey, TValue> where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
{
  private readonly IMonotonicSkipList<TKey, TValue>[] _skipLists;
  private readonly SegmentManager _segmentManager;
  private readonly int _memTableCapacity;
  private int _activeSkipListIndex = 0;
  private readonly int[] _skipListSessions = new int[2];  // Track active sessions per skiplist
  private int _flushingSegmentIndex = -1;                 // Segment currently being flushed (-1 = none)
  private readonly object _commitLock = new object();
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

  private static IMonotonicSkipList<TKey, TValue> CreateSkipList(SkipListType type)
  {
    return type switch
    {
      SkipListType.ClassBased => new SkipList<TKey, TValue>(maxCapacity: SegmentManager.SegmentSize),
      SkipListType.StructBased => new StructSkipList<TKey, TValue>(maxCapacity: SegmentManager.SegmentSize),
      _ => throw new ArgumentException($"Unknown skip list type: {type}", nameof(type))
    };
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

  /// <summary>
  /// Tries to get a value by key. Uses binary search to find the correct segment,
  /// then handles in-memory, flushing, or flushed states.
  /// </summary>
  public bool TryGet(TKey key, out TValue value)
  {
    value = default;

    // Check active skiplist first
    int activeIndex = Volatile.Read(ref _activeSkipListIndex);
    Interlocked.Increment(ref _skipListSessions[activeIndex]);
    try
    {
      if (_skipLists[activeIndex].TryGetValue(key, out value))
      {
        return true;
      }
    }
    finally
    {
      ExitSkipListSession(activeIndex);
    }

    // Check the other skiplist (might be in the process of flushing)
    int otherIndex = (activeIndex + 1) % 2;
    Interlocked.Increment(ref _skipListSessions[otherIndex]);
    try
    {
      if (_skipLists[otherIndex].TryGetValue(key, out value))
      {
        return true;
      }
    }
    finally
    {
      Interlocked.Decrement(ref _skipListSessions[otherIndex]);
    }

    // Not in memory, find the segment on disk that might contain the key
    if (_segmentManager.NumSegments == _segmentManager.BeginSegmentIndex)
    {
      return false; // No segments on disk
    }

    // Use SegmentManager to find the segment that might contain the key
    int targetSegment = _segmentManager.FindSegmentForKey(key);
    
    if (targetSegment == -1)
    {
      return false;
    }

    // Check if this segment is currently being flushed
    int flushingSegment = Interlocked.CompareExchange(ref _flushingSegmentIndex, -1, -1);
    if (targetSegment == flushingSegment)
    {
      // Wait for flush to complete
      while (Interlocked.CompareExchange(ref _flushingSegmentIndex, -1, -1) == targetSegment)
      {
        Thread.Yield();
      }
    }

    // Read from the segment file
    return _segmentManager.TryGetFromSegment(targetSegment, key, out value);
  }

  /// <summary>
  /// Iterator for range queries. Yields entries in sorted order across memory and disk.
  /// Since keys are strictly monotonic, no deduplication is needed.
  /// </summary>
  public IEnumerable<KeyValuePair<TKey, TValue>> QueryRange(TKey startKey, TKey endKey)
  {
    // Snapshot the active index to ensure consistency during iteration
    int activeIndex = Volatile.Read(ref _activeSkipListIndex);
    int otherIndex = (activeIndex + 1) % 2;

    // see where the startKey falls in segments
    int segmentIndex = _segmentManager.FindSegmentForKey(startKey);

    if (segmentIndex == -1)
    {
      // No segments, just yield from memory
      foreach (var kvp in _skipLists[activeIndex].GetRange(startKey, endKey))
      {
        yield return kvp;
      }
      foreach (var kvp in _skipLists[otherIndex].GetRange(startKey, endKey))
      {
        yield return kvp;
      }
    }
    else
    {
      // Yield from segments first, then memory
      foreach (var kvp in _segmentManager.QuerySegmentRange(segmentIndex, startKey, endKey))
      {
        yield return kvp;
      }

      foreach (var kvp in _skipLists[activeIndex].GetRange(startKey, endKey))
      {
        yield return kvp;
      }
      
      foreach (var kvp in _skipLists[otherIndex].GetRange(startKey, endKey))
      {
        yield return kvp;
      }
    }
  }

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