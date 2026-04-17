using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace StreamDB.LiteLsm;

public class SegmentManager
{
  #region Consts and Readonlys
  // When skiplists grow to this size we flush them to disk and clear them from memory.
  public const int SegmentSize = 1024;

  // Maximum size for a segment file before rotating to a new one (1GB)
  private const long MaxSegmentFileSize = 1024 * 1024 * 1024;

  // Size of each metadata entry in bytes (BlockId + FileId + FileOffset + BlockSize + EntryCount)
  private const int MetadataEntrySize = 24;

  // Initial metadata file capacity (supports 1M blocks = 16MB file)
  private const long InitialMetadataCapacity = 1024 * 1024 * MetadataEntrySize;

  private readonly string _segmentDirectory;
  private readonly string _metadataFilePath;

  #endregion

  #region Mutable state

  // Num of commit blocks already flushed to disk completely.
  public int NumSegments { get; private set; }

  // Earliest commit block index still on disk. Aka not truncated away
  public int BeginSegmentIndex { get; private set; }

  // Memory-mapped metadata file
  private FileStream _metadataFileStream = null!;
  private MemoryMappedFile _metadataFile = null!;
  private long _metadataFileCapacity;

  // Current active segment file for appending
  private int _currentFileId;
  private FileStream? _currentFile;
  private long _currentFileOffset;

  // LRU cache for open segment accessors
  private readonly SegmentCache _segmentCache = new(capacity: 64);

  private const int HEADER_SIZE = 8;

  #endregion

  #region Init and recovery
  public SegmentManager(string segmentDirectory)
  {
    _segmentDirectory = segmentDirectory;
    _metadataFilePath = Path.Combine(_segmentDirectory, "metadata.dat");
    _currentFileId = 0;
    _currentFileOffset = 0;

    Directory.CreateDirectory(_segmentDirectory);
    Recover();
  }

  private void Recover()
  {
    if (File.Exists(_metadataFilePath))
    {
      // Recovery path: read existing metadata
      var fileInfo = new FileInfo(_metadataFilePath);
      long capacity = Math.Max(fileInfo.Length, InitialMetadataCapacity);

      _metadataFileStream = new FileStream(_metadataFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
      InitializeMetadataFile(capacity);

      NumSegments = (int)(fileInfo.Length / MetadataEntrySize);
      BeginSegmentIndex = 0;

      if (NumSegments > 0)
      {
        ReadMetadataEntry(NumSegments - 1, out SegmentBlockInfo lastBlock);
        _currentFileId = lastBlock.FileId;
        _currentFileOffset = lastBlock.FileOffset + lastBlock.BlockSize;
      }

      // Open the last segment file for appending if it's not too large
      if (_currentFileOffset < MaxSegmentFileSize && _currentFileOffset > 0)
      {
        string lastFilePath = GetSegmentFilePath(_currentFileId);
        if (File.Exists(lastFilePath))
        {
          _currentFile = new FileStream(lastFilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
        }
      }
      else if (_currentFileOffset >= MaxSegmentFileSize)
      {
        _currentFileId++;
        _currentFileOffset = 0;
      }
    }
    else
    {
      // Fresh start: create empty metadata file
      _metadataFileStream = new FileStream(_metadataFilePath, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
      _metadataFileStream.SetLength(InitialMetadataCapacity);
      InitializeMetadataFile(InitialMetadataCapacity);

      NumSegments = 0;
      BeginSegmentIndex = 0;
    }
  }

  #endregion

  #region Write Path

  public FileStream PrepareForFlush(int entryCount, int entrySize, out SegmentBlockInfo info)
  {
    // Block format: [header 8B][flags[entryCount]][KV entries]
    var bytesNeeded = HEADER_SIZE + entryCount + (entryCount * entrySize);
    if (_currentFile == null || _currentFileOffset + bytesNeeded > MaxSegmentFileSize)
    {
      _currentFile?.Flush();
      _currentFile?.Dispose();

      _currentFileId++;
      _currentFileOffset = 0;

      string newFilePath = GetSegmentFilePath(_currentFileId);
      _currentFile = new FileStream(newFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);
    }

    // Write the commit block to the current file
    long blockStartOffset = _currentFileOffset;

    // Record metadata for this block
    info = new SegmentBlockInfo(
      NumSegments,
      _currentFileId,
      blockStartOffset,
      bytesNeeded,
      entryCount);

    return _currentFile;
  }

  public void CommitFlush(SegmentBlockInfo info)
  {
    _currentFile!.Flush(flushToDisk: true);

    WriteMetadataEntry(NumSegments, ref info);

    _currentFileOffset += info.BlockSize;
    NumSegments++;
  }

  /// <summary>
  /// Writes pre-built raw segment bytes to disk. Used by both full flush and partial eviction.
  /// rawData must include the 8-byte header (blockSize + count).
  /// </summary>
  public void FlushRaw(ReadOnlySpan<byte> rawData, int entryCount)
  {
    int blockSize = rawData.Length;

    // Check if we need to rotate to a new file
    if (_currentFile == null || _currentFileOffset + blockSize > MaxSegmentFileSize)
    {
      _currentFile?.Flush();
      _currentFile?.Dispose();

      _currentFileId++;
      _currentFileOffset = 0;

      string newFilePath = GetSegmentFilePath(_currentFileId);
      _currentFile = new FileStream(newFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);
    }

    // Write the commit block to the current file
    long blockStartOffset = _currentFileOffset;
    _currentFile.Write(rawData);
    _currentFile.Flush(flushToDisk: true);

    // Record metadata for this block
    var info = new SegmentBlockInfo(
      NumSegments,
      _currentFileId,
      blockStartOffset,
      blockSize,
      entryCount);
    WriteMetadataEntry(NumSegments, ref info);

    _currentFileOffset += blockSize;
    NumSegments++;
  }

  /// <summary>
  /// Writes a metadata entry to the memory-mapped file.
  /// </summary>
  private void WriteMetadataEntry(int segmentIndex, ref SegmentBlockInfo info)
  {
    EnsureMetadataCapacity(segmentIndex + 1);
    long offset = segmentIndex * MetadataEntrySize;
    using (MemoryMappedViewAccessor accessor = _metadataFile.CreateViewAccessor(offset, MetadataEntrySize, MemoryMappedFileAccess.Write))
    {
      accessor.Write<SegmentBlockInfo>(0, ref info);
    }
    // fsync metadata to survive OS crash — this is the commit point
    _metadataFileStream.Flush(flushToDisk: true);
  }

  /// <summary>
  /// Ensures the metadata file has capacity for the specified number of entries.
  /// Doubles the file size when more space is needed.
  /// </summary>
  private void EnsureMetadataCapacity(int requiredEntries)
  {
    long requiredCapacity = requiredEntries * MetadataEntrySize;

    if (requiredCapacity > _metadataFileCapacity)
    {
      // Double the capacity
      long newCapacity = _metadataFileCapacity * 2;
      while (newCapacity < requiredCapacity)
      {
        newCapacity *= 2;
      }

      // Close and remap with new capacity
      _metadataFile?.Dispose();
      _metadataFileStream.SetLength(newCapacity);
      InitializeMetadataFile(newCapacity);
    }
  }

  /// <summary>
  /// Initializes or reinitializes the metadata memory-mapped file.
  /// </summary>
  private void InitializeMetadataFile(long capacity)
  {
    _metadataFileCapacity = capacity;
    _metadataFile = MemoryMappedFile.CreateFromFile(
      _metadataFileStream,
      null,
      _metadataFileCapacity,
      MemoryMappedFileAccess.ReadWrite,
      HandleInheritability.None,
      leaveOpen: true);
  }

  #endregion

  #region Read Path

  /// <summary>
  /// Tries to find a key in a specific segment using binary search.
  /// Returns true if found and sets the value.
  /// </summary>
  public bool TryGetFromSegment<TKey, TValue>(int segmentIndex, TKey key, out TValue value)
    where TKey : unmanaged, IComparable<TKey>
    where TValue : unmanaged
  {
    value = default;

    SegmentCache.CachedSegment? cached = GetCachedSegment(segmentIndex);
    if (cached == null)
      return false;

    (int keySize, int _, int entrySize) = GetEntrySizes<TKey, TValue>();

    int index = BinarySearchSegment(cached.Accessor, key, cached.Count, entrySize, cached.DataOffset, exactMatch: true);
    if (index == -1)
      return false;

    // Check if entry is deleted
    byte flag = 0;
    cached.Accessor.Read(cached.FlagsOffset + index, out flag);
    if (flag != 0)
      return false;

    long offset = cached.DataOffset + (index * entrySize);
    cached.Accessor.Read(offset + keySize, out value);
    return true;
  }

  /// <summary>
  /// Reads all entries from segments within a key range.
  /// Returns (hasMore, entriesRead) where hasMore indicates if additional data exists beyond what was read.
  /// </summary>
  public (bool HasMore, int EntriesRead) ReadRangeFromSegment<TKey, TValue>(
      TKey startKey,
      TKey endKey,
      Span<KeyValuePair<TKey, TValue>> buffer,
      bool excludeStart = false,
      int segmentIdxHint = -1)
      where TKey : unmanaged, IComparable<TKey>
      where TValue : unmanaged
  {
    // Find starting segment
    int currentSegment = segmentIdxHint >= 0
        ? segmentIdxHint
        : FindStartingSegment(startKey);

    if (currentSegment >= NumSegments)
    {
      return (false, 0); // No data available
    }

    (int keySize, int _, int entrySize) = GetEntrySizes<TKey, TValue>();
    int totalEntriesRead = 0;

    // Read across segments until buffer is full or we exceed endKey
    bool isFirstSegment = true;
    while (currentSegment < NumSegments && totalEntriesRead < buffer.Length)
    {
      (int EntriesRead, bool ExceededEndKey) result = ReadFromSingleSegment(
          currentSegment,
          startKey,
          endKey,
          buffer.Slice(totalEntriesRead),
          keySize,
          entrySize,
          excludeStart: excludeStart && isFirstSegment);
      isFirstSegment = false;

      totalEntriesRead += result.EntriesRead;

      // Stop if we hit endKey
      if (result.ExceededEndKey)
      {
        return (true, totalEntriesRead);
      }

      // Stop if buffer is full
      if (totalEntriesRead >= buffer.Length)
      {
        return (true, totalEntriesRead);
      }

      // Move to next segment
      currentSegment++;
    }

    // We exhausted all segments without filling buffer or hitting endKey
    return (false, totalEntriesRead);
  }

  private int FindStartingSegment<TKey>(TKey startKey) where TKey : unmanaged, IComparable<TKey>
  {
    // Finds the first segment that might contain entries >= startKey.
    int segmentIndex = FindSegmentForKey(startKey);
    // If exact segment found, use it
    if (segmentIndex >= 0)
    {
      return segmentIndex;
    }
    // Not found - use insertion point (first segment after the key)
    return ~segmentIndex;
  }

  /// <summary>
  /// Reads entries from a single segment within the key range.
  /// </summary>
  private (int EntriesRead, bool ExceededEndKey) ReadFromSingleSegment<TKey, TValue>(
      int segmentIndex,
      TKey startKey,
      TKey endKey,
      Span<KeyValuePair<TKey, TValue>> buffer,
      int keySize,
      int entrySize,
      bool excludeStart = false)
      where TKey : unmanaged, IComparable<TKey>
      where TValue : unmanaged
  {
    SegmentCache.CachedSegment? cached = GetCachedSegment(segmentIndex);
    if (cached == null)
    {
      return (0, false);
    }

    // Find first entry >= startKey in this segment
    int startIndex = BinarySearchSegment(
        cached.Accessor,
        startKey,
        cached.Count,
        entrySize,
        cached.DataOffset,
        exactMatch: false);

    // If excludeStart and exact match, advance past the matched entry
    if (excludeStart && startIndex < cached.Count)
    {
      long checkOffset = cached.DataOffset + (startIndex * entrySize);
      cached.Accessor.Read(checkOffset, out TKey foundKey);
      if (foundKey.CompareTo(startKey) == 0)
      {
        startIndex++;
      }
    }

    // All entries in this segment are before startKey
    if (startIndex >= cached.Count)
    {
      return (0, false);
    }

    // Single-pass: read entries, skip deleted ones
    int entriesRead = 0;
    bool exceededEndKey = false;
    int entriesToScan = cached.Count - startIndex;

    for (int i = 0; i < entriesToScan && entriesRead < buffer.Length; i++)
    {
      // Check deleted flag
      byte flag = 0;
      cached.Accessor.Read(cached.FlagsOffset + startIndex + i, out flag);
      if (flag != 0)
        continue; // skip deleted entry

      long offset = cached.DataOffset + ((startIndex + i) * entrySize);

      // Read key and value separately from memory-mapped file
      cached.Accessor.Read(offset, out TKey key);
      cached.Accessor.Read(offset + keySize, out TValue val);

      // Check if we've exceeded endKey
      if (key.CompareTo(endKey) > 0)
      {
        exceededEndKey = true;
        break;
      }

      buffer[entriesRead] = new KeyValuePair<TKey, TValue>(key, val);
      entriesRead++;
    }

    return (entriesRead, exceededEndKey);
  }

  /// <summary>
  /// Finds which segment might contain the given key by checking segment key ranges.
  /// Returns the segment index, or bitwise complement of the insertion point if not found (like Array.BinarySearch).
  /// </summary>
  public int FindSegmentForKey<TKey>(TKey key) where TKey : unmanaged, IComparable<TKey>
  {
    int left = BeginSegmentIndex;
    int right = NumSegments - 1;
    while (left <= right)
    {
      int mid = left + (right - left) / 2;
      if (!TryGetSegmentKeyRange(mid, out TKey minKey, out TKey maxKey))
      {
        throw new InvalidOperationException($"Segment file containing {mid} is missing or corrupted.");
      }

      if (key.CompareTo(minKey) < 0)
      {
        right = mid - 1;
      }
      else if (key.CompareTo(maxKey) > 0)
      {
        left = mid + 1;
      }
      else
      {
        return mid;
      }
    }
    return ~left;
  }


  /// <summary>
  /// Gets the key range for a specific segment (min and max keys).
  /// </summary>
  private bool TryGetSegmentKeyRange<TKey>(int segmentIndex, out TKey minKey, out TKey maxKey)
    where TKey : unmanaged, IComparable<TKey>
  {
    minKey = default;
    maxKey = default;

    SegmentCache.CachedSegment? cached = GetCachedSegment(segmentIndex);
    if (cached == null)
      return false;

    // Use GetEntrySizes to get correct entry size
    (int _, int _, int entrySize) = GetEntrySizes<TKey, long>();

    cached.Accessor.Read(cached.DataOffset, out minKey);
    long lastOffset = cached.DataOffset + ((cached.Count - 1) * entrySize);
    cached.Accessor.Read(lastOffset, out maxKey);

    return true;
  }

  /// <summary>
  /// Gets the maximum key in a segment. Used by truncation to determine which segments are fully below a threshold.
  /// </summary>
  public bool TryGetSegmentMaxKey<TKey>(int segmentIndex, out TKey maxKey)
    where TKey : unmanaged, IComparable<TKey> => TryGetSegmentKeyRange(segmentIndex, out _, out maxKey);

  /// <summary>
  /// Gets the entry count for a segment block from metadata.
  /// </summary>
  public int GetSegmentEntryCount(int segmentIndex)
  {
    ReadMetadataEntry(segmentIndex, out SegmentBlockInfo info);
    return info.EntryCount;
  }

  /// <summary>
  /// Marks a key as deleted in a specific on-disk segment.
  /// Returns true if the key was found and marked deleted.
  /// </summary>
  public bool DeleteInSegment<TKey, TValue>(int segmentIndex, TKey key)
    where TKey : unmanaged, IComparable<TKey>
    where TValue : unmanaged
  {
    var cached = GetCachedSegment(segmentIndex);
    if (cached == null)
      return false;

    (int _, int _, int entrySize) = GetEntrySizes<TKey, TValue>();
    int index = BinarySearchSegment(cached.Accessor, key, cached.Count, entrySize, cached.DataOffset, exactMatch: true);
    if (index == -1)
      return false;

    // Check if already deleted
    cached.Accessor.Read(cached.FlagsOffset + index, out byte flag);
    if (flag != 0)
      return false;

    // Mark as deleted and flush to disk
    cached.Accessor.Write(cached.FlagsOffset + index, (byte)1);
    cached.Accessor.Flush();
    return true;
  }

  /// <summary>
  /// Gets or creates a cached segment accessor.
  /// </summary>
  private SegmentCache.CachedSegment? GetCachedSegment(int segmentIndex)
  {
    return _segmentCache.GetOrCreate(segmentIndex, idx =>
    {
      if (idx < 0 || idx >= NumSegments)
        return null;

      ReadMetadataEntry(idx, out SegmentBlockInfo block);
      string filePath = GetSegmentFilePath(block.FileId);

      if (!File.Exists(filePath))
        return null;

      var fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
      var mmf = MemoryMappedFile.CreateFromFile(fs, null, 0, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: false);
      MemoryMappedViewAccessor accessor = mmf.CreateViewAccessor(block.FileOffset, block.BlockSize, MemoryMappedFileAccess.ReadWrite);

      int count = accessor.ReadInt32(4);
      if (count == 0)
      {
        accessor.Dispose();
        mmf.Dispose();
        return null;
      }

      // Block format: [header 8B][flags[count]][KV entries]
      long flagsOffset = HEADER_SIZE;
      long dataOffset = HEADER_SIZE + count;

      return new SegmentCache.CachedSegment(mmf, accessor, count, flagsOffset, dataOffset);
    });
  }

  /// <summary>
  /// Calculates entry size and offsets for key-value pairs.
  /// </summary>
  private static (int keySize, int valueSize, int entrySize) GetEntrySizes<TKey, TValue>()
    where TKey : unmanaged
    where TValue : unmanaged
  {
    int keySize = Marshal.SizeOf<TKey>();
    int valueSize = Marshal.SizeOf<TValue>();
    return (keySize, valueSize, keySize + valueSize);
  }

  /// <summary>
  /// Finds the index of a key using binary search.
  /// Returns -1 if not found (exact match), or the index of first entry >= key (lower bound).
  /// </summary>
  private static int BinarySearchSegment<TKey>(MemoryMappedViewAccessor accessor, TKey key, int count, int entrySize, long dataOffset, bool exactMatch)
    where TKey : unmanaged, IComparable<TKey>
  {
    int left = 0;
    int right = count - 1;
    int resultIndex = exactMatch ? -1 : count;

    while (left <= right)
    {
      int mid = left + (right - left) / 2;
      long offset = dataOffset + (mid * entrySize);

      accessor.Read(offset, out TKey midKey);

      int comparison = midKey.CompareTo(key);

      if (comparison == 0)
      {
        return mid; // Exact match found
      }
      else if (comparison < 0)
      {
        left = mid + 1;
      }
      else
      {
        if (!exactMatch)
          resultIndex = mid; // Track first entry >= key
        right = mid - 1;
      }
    }

    return resultIndex;
  }

  #endregion

  #region Truncation

  /// <summary>
  /// Truncates all segments before the given segment index.
  /// Invalidates cached accessors and deletes segment files that are fully truncated.
  /// </summary>
  internal void TruncateBefore(int newBeginSegmentIndex)
  {
    if (newBeginSegmentIndex <= BeginSegmentIndex || newBeginSegmentIndex > NumSegments)
      return;

    // Collect fileIds that are being truncated to check if we can delete them
    var fileIdsToCheck = new HashSet<int>();

    SegmentBlockInfo block;
    for (int i = BeginSegmentIndex; i < newBeginSegmentIndex; i++)
    {
      _segmentCache.Invalidate(i);
      ReadMetadataEntry(i, out block);
      fileIdsToCheck.Add(block.FileId);
    }

    BeginSegmentIndex = newBeginSegmentIndex;

    // For each file that had truncated segments, check if ALL its segments are now truncated.
    // If so, delete the file.
    foreach (int fileId in fileIdsToCheck)
    {
      bool fileStillInUse = false;
      for (int i = BeginSegmentIndex; i < NumSegments; i++)
      {
        ReadMetadataEntry(i, out block);
        if (block.FileId == fileId)
        {
          fileStillInUse = true;
          break;
        }
      }

      if (!fileStillInUse)
      {
        string filePath = GetSegmentFilePath(fileId);
        try { File.Delete(filePath); } catch { /* best effort */ }
      }
    }
  }

  #endregion

  #region Shared utilities

  private string GetSegmentFilePath(int fileId) => Path.Combine(_segmentDirectory, $"segment_file_{fileId}.dat");

  /// <summary>
  /// Reads a metadata entry from the memory-mapped file.
  /// </summary>
  private void ReadMetadataEntry(int segmentIndex, out SegmentBlockInfo info)
  {
    long offset = segmentIndex * MetadataEntrySize;
    using (MemoryMappedViewAccessor accessor = _metadataFile.CreateViewAccessor(offset, MetadataEntrySize, MemoryMappedFileAccess.Read))
    {
      accessor.Read<SegmentBlockInfo>(0, out info);
    }
  }

  #endregion

  /// <summary>
  /// Closes all open files and flushes all pending writes.
  /// </summary>
  public void Dispose()
  {
    _segmentCache.Dispose();

    _currentFile?.Flush();
    _currentFile?.Dispose();
    _currentFile = null;

    _metadataFile?.Dispose();
    _metadataFileStream?.Dispose();
  }
}