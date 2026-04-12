using System.IO.MemoryMappedFiles;

namespace StreamDB.LiteLsm;

/// <summary>
/// LRU cache for open memory-mapped segment accessors.
/// Avoids repeated open/close of MemoryMappedFile per read — the dominant cost in the read path.
/// Thread-safe via a simple lock (reads are fast once cached).
/// </summary>
internal sealed class SegmentCache : IDisposable
{
  private readonly int _capacity;
  private readonly Lock _lock = new();

  // LRU tracking: most recently used at the end
  private readonly LinkedList<int> _lruOrder = new();
  private readonly Dictionary<int, (LinkedListNode<int> Node, CachedSegment Segment)> _cache = new();

  internal sealed class CachedSegment : IDisposable
  {
    public MemoryMappedFile Mmf { get; }
    public MemoryMappedViewAccessor Accessor { get; }
    public int Count { get; }

    public CachedSegment(MemoryMappedFile mmf, MemoryMappedViewAccessor accessor, int count)
    {
      Mmf = mmf;
      Accessor = accessor;
      Count = count;
    }

    public void Dispose()
    {
      Accessor.Dispose();
      Mmf.Dispose();
    }
  }

  public SegmentCache(int capacity = 32)
  {
    _capacity = capacity;
  }

  /// <summary>
  /// Gets a cached segment accessor, or creates and caches a new one.
  /// Returns null if the segment cannot be opened.
  /// </summary>
  public CachedSegment? GetOrCreate(int segmentIndex, Func<int, CachedSegment?> factory)
  {
    lock (_lock)
    {
      if (_cache.TryGetValue(segmentIndex, out var entry))
      {
        // Move to end (most recently used)
        _lruOrder.Remove(entry.Node);
        _lruOrder.AddLast(entry.Node);
        return entry.Segment;
      }
    }

    // Create outside lock to avoid holding it during I/O
    var segment = factory(segmentIndex);
    if (segment == null)
      return null;

    lock (_lock)
    {
      // Double-check: another thread may have cached it
      if (_cache.TryGetValue(segmentIndex, out var existing))
      {
        segment.Dispose();
        _lruOrder.Remove(existing.Node);
        _lruOrder.AddLast(existing.Node);
        return existing.Segment;
      }

      // Evict LRU if at capacity
      while (_cache.Count >= _capacity && _lruOrder.First != null)
      {
        var evictKey = _lruOrder.First.Value;
        _lruOrder.RemoveFirst();
        if (_cache.Remove(evictKey, out var evicted))
        {
          evicted.Segment.Dispose();
        }
      }

      var node = _lruOrder.AddLast(segmentIndex);
      _cache[segmentIndex] = (node, segment);
      return segment;
    }
  }

  /// <summary>
  /// Invalidates a specific segment entry (e.g., after truncation).
  /// </summary>
  public void Invalidate(int segmentIndex)
  {
    lock (_lock)
    {
      if (_cache.Remove(segmentIndex, out var entry))
      {
        _lruOrder.Remove(entry.Node);
        entry.Segment.Dispose();
      }
    }
  }

  public void Dispose()
  {
    lock (_lock)
    {
      foreach (var (_, entry) in _cache)
      {
        entry.Segment.Dispose();
      }
      _cache.Clear();
      _lruOrder.Clear();
    }
  }
}
