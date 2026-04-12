/*
Holds a skiplist and a reference to an AOF on disk.
Skiplist maintains a sparse index of keys in-memory. Whenever flush is called it does 2 things.
1. Enqueue a commit and wait for the commit to be written to the AOF. This ensures durability of the data.
2. Swap the skiplist, flush the previous skiplist to disk and clear it after flushing completes.

Thread-Safety:
- Put() is thread-safe: multiple threads can write concurrently to the active skiplist
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
/// Statistics about the LSM tree state.
/// </summary>
public readonly struct LsmStats
{
  public int ActiveMemTableCount { get; init; }
  public int InactiveMemTableCount { get; init; }
  public int NumSegments { get; init; }
  public int EarliestSegmentIndex { get; init; }
  public int LatestSegmentIndex { get; init; }
}