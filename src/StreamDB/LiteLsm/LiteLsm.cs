namespace StreamDB.LiteLsm;

public class LiteLsm<TKey, TValue> : ILiteLsm<TKey, TValue> where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
{
  private readonly SegmentManager _segmentManager;
  private readonly SkipList<TKey, TValue> _skipList;

  public LiteLsm(string segmentDirectory = "segments", int memTableCapacity = SegmentManager.SegmentSize)
  {
    _segmentManager = new SegmentManager(segmentDirectory);
    _skipList = new SkipList<TKey, TValue>(memTableCapacity);
  }

  /// <inheritdoc/>
  public void Put(TKey key, TValue value)
  {
    while (!_skipList.InsertMonotonic(ref key, ref value))
    {
      EvictSegmentToDisk(0.25);
    }
  }

  private void EvictSegmentToDisk(double evictionThreshold)
  {
    int evictCount = Math.Max(1, (int)(_skipList.Count * evictionThreshold));
    FileStream buf = _segmentManager.PrepareForFlush(evictCount, entrySize: _skipList.EntrySize, out SegmentBlockInfo blockInfo);
    _skipList.CopyPrefixToBuffer(evictCount, buf);
    _segmentManager.CommitFlush(blockInfo);
    _skipList.EvictPrefix(evictCount);
  }

  // used by iterators to read batches
  internal (bool _hasMoreBatchesToRead, int _bufferedBatchCount)
      ReadBatchInto(in TKey readFrom, in TKey endKey, Span<KeyValuePair<TKey, TValue>> bufferedBatch, bool excludeStart = false)
  {
    (bool HasMore, int EntriesRead) = _segmentManager.ReadRangeFromSegment<TKey, TValue>(readFrom, endKey, bufferedBatch, excludeStart);
    int totalRead = EntriesRead;

    if (totalRead >= bufferedBatch.Length || HasMore)
    {
      return (true, totalRead);
    }

    bool memExcludeStart = excludeStart && totalRead == 0;
    if (_skipList.Count > 0)
    {
      Span<KeyValuePair<TKey, TValue>> remaining = bufferedBatch.Slice(totalRead);
      (bool, int) memResult = _skipList.MemCpyRawRange(readFrom, endKey, remaining, memExcludeStart);
      totalRead += memResult.Item2;
      return (memResult.Item1, totalRead);
    }

    return (false, totalRead);
  }

  public LiteLsmIterator<TKey, TValue> GetIterator(TKey fromKey, TKey endKey, int batchSize = 256) => new LiteLsmIterator<TKey, TValue>(this, fromKey, endKey, batchSize);

  public bool TryGet(in TKey key, out TValue value)
  {
    bool foundInActive = TryGetKeyFromMemory(_skipList, in key, out value);
    if (foundInActive)
    {
      return true;
    }
    int segmentIdx = _segmentManager.FindSegmentForKey(key);
    return _segmentManager.TryGetFromSegment<TKey, TValue>(segmentIdx, key, out value);
  }

  /// <inheritdoc/>
  public bool Delete(in TKey key)
  {
    // Try in-memory first
    if (_skipList.Delete(in key))
    {
      return true;
    }

    // Try on-disk segments
    int segmentIdx = _segmentManager.FindSegmentForKey(key);
    if (segmentIdx < 0)
      return false;

    return _segmentManager.DeleteInSegment<TKey, TValue>(segmentIdx, key);
  }

  private static bool TryGetKeyFromMemory(SkipList<TKey, TValue> skipList, in TKey key, out TValue value)
  {
    value = default;
    if (!skipList.ProbablyContainsKey(in key))
    {
      return false;
    }
    return skipList.TryGetValue(in key, out value);
  }

  public LsmStats GetStats() => new LsmStats
  {
    ActiveMemTableCount = _skipList.Count,
    NumSegments = _segmentManager.NumSegments - _segmentManager.BeginSegmentIndex,
    EarliestSegmentIndex = _segmentManager.BeginSegmentIndex,
    LatestSegmentIndex = _segmentManager.NumSegments - 1
  };


  /// <inheritdoc/>
  public int Truncate(TKey beforeKey)
  {
    // Phase 1: Disk truncation -- drop whole blocks where maxKey < beforeKey
    int newBegin = _segmentManager.BeginSegmentIndex;

    for (int i = _segmentManager.BeginSegmentIndex; i < _segmentManager.NumSegments; i++)
    {
      if (_segmentManager.TryGetSegmentMaxKey<TKey>(i, out TKey maxKey) && maxKey.CompareTo(beforeKey) < 0)
      {
        newBegin = i + 1;
      }
      else
      {
        break;
      }
    }

    int diskTruncated = 0;
    if (newBegin > _segmentManager.BeginSegmentIndex)
    {
      // Sum entry counts from truncated blocks
      for (int i = _segmentManager.BeginSegmentIndex; i < newBegin; i++)
      {
        diskTruncated += _segmentManager.GetSegmentEntryCount(i);
      }
      _segmentManager.TruncateBefore(newBegin);
    }

    // Phase 2: SkipList truncation -- discard entries < beforeKey
    int skipListTruncated = _skipList.TruncatePrefix(beforeKey);

    return diskTruncated + skipListTruncated;
  }
}