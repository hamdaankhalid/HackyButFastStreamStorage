using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace StreamDB.LiteLsm;

public class SegmentManager
{
  #region Consts and Readonlys
  // When skiplists grow to this size we flush them to disk and clear them from memory.
  public const int SegmentSize = 1024;

  // Maximum size for a segment file before rotating to a new one (64MB)
  private const long MaxSegmentFileSize = 64 * 1024 * 1024;

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

  /// <summary>
  /// Recovers segment metadata from the memory-mapped metadata file.
  /// Falls back to scanning segment files if metadata file doesn't exist (migration path).
  /// </summary>
  private void Recover()
  {
    if (File.Exists(_metadataFilePath))
    {
      // Fast path: read from metadata file
      var fileInfo = new FileInfo(_metadataFilePath);
      long capacity = Math.Max(fileInfo.Length, InitialMetadataCapacity);

      _metadataFileStream = new FileStream(_metadataFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
      InitializeMetadataFile(capacity);

      NumSegments = (int)(fileInfo.Length / MetadataEntrySize);
      BeginSegmentIndex = 0;

      // Determine current file and offset from last metadata entry
      if (NumSegments > 0)
      {
        var lastBlock = ReadMetadataEntry(NumSegments - 1);
        _currentFileId = lastBlock.FileId;
        _currentFileOffset = lastBlock.FileOffset + lastBlock.BlockSize;
      }
      else
      {
        _currentFileId = 0;
        _currentFileOffset = 0;
      }
    }
    else
    {
      // Migration path: scan segment files and build metadata file
      _metadataFileStream = new FileStream(_metadataFilePath, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
      _metadataFileStream.SetLength(InitialMetadataCapacity);
      InitializeMetadataFile(InitialMetadataCapacity);

      string[] segmentFiles = Directory.GetFiles(_segmentDirectory, "segment_file_*.dat")
                                  .OrderBy(f => f)
                                  .ToArray();

      if (segmentFiles.Length == 0)
      {
        NumSegments = 0;
        BeginSegmentIndex = 0;
        _currentFileId = 0;
        _currentFileOffset = 0;
        return;
      }

      int blockId = 0;
      Span<byte> header = stackalloc byte[8];

      foreach (string? filePath in segmentFiles)
      {
        string fileName = Path.GetFileNameWithoutExtension(filePath);
        if (!fileName.StartsWith("segment_file_") ||
            !int.TryParse(fileName.AsSpan(13), out int fileId))
        {
          continue;
        }

        using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        long fileOffset = 0;

        while (fileOffset < fs.Length)
        {
          fs.Position = fileOffset;
          int bytesRead = fs.Read(header);

          if (bytesRead < 8)
            break;

          int blockSize = MemoryMarshal.Read<int>(header);
          int entryCount = MemoryMarshal.Read<int>(header.Slice(4));

          if (blockSize <= 0 || entryCount <= 0)
            break;

          WriteMetadataEntry(blockId, new SegmentBlockInfo(
            blockId,
            fileId,
            fileOffset,
            blockSize,
            entryCount));

          blockId++;
          fileOffset += blockSize;
        }

        _currentFileId = fileId;
        _currentFileOffset = fileOffset;
      }

      NumSegments = blockId;
      BeginSegmentIndex = 0;
    }

    // Open the last segment file for appending if it's not too large
    if (_currentFileOffset < MaxSegmentFileSize && _currentFileOffset > 0)
    {
      string lastFilePath = GetSegmentFilePath(_currentFileId);
      if (File.Exists(lastFilePath))
      {
        _currentFile = new FileStream(lastFilePath, FileMode.Append, FileAccess.Write);
      }
    }
    else if (_currentFileOffset >= MaxSegmentFileSize)
    {
      _currentFileId++;
      _currentFileOffset = 0;
    }
  }

  #endregion

  #region Write Path

  /// <summary>
  /// Commits a skiplist to disk as a new segment block.
  /// Handles file rotation and metadata updates.
  /// </summary>
  internal void Flush<TKey, TValue>(IMonotonicSkipList<TKey, TValue> skipListToFlush) where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
  {
    ReadOnlySpan<byte> rawData = skipListToFlush.GetRawDataLayer();
    int blockSize = rawData.Length;

    // Check if we need to rotate to a new file
    if (_currentFile == null || _currentFileOffset + blockSize > MaxSegmentFileSize)
    {
      _currentFile?.Flush();
      _currentFile?.Dispose();

      _currentFileId++;
      _currentFileOffset = 0;

      string newFilePath = GetSegmentFilePath(_currentFileId);
      _currentFile = new FileStream(newFilePath, FileMode.Create, FileAccess.Write);
    }

    // Write the commit block to the current file
    long blockStartOffset = _currentFileOffset;
    _currentFile.Write(rawData);
    _currentFile.Flush(flushToDisk: true);

    // Record metadata for this block
    WriteMetadataEntry(NumSegments, new SegmentBlockInfo(
      NumSegments,
      _currentFileId,
      blockStartOffset,
      blockSize,
      skipListToFlush.Count));

    _currentFileOffset += blockSize;
    NumSegments++;
  }

  /// <summary>
  /// Writes a metadata entry to the memory-mapped file.
  /// </summary>
  private void WriteMetadataEntry(int segmentIndex, SegmentBlockInfo info)
  {
    EnsureMetadataCapacity(segmentIndex + 1);

    long offset = segmentIndex * MetadataEntrySize;

    using (var accessor = _metadataFile.CreateViewAccessor(offset, MetadataEntrySize, MemoryMappedFileAccess.Write))
    {
      accessor.Write(0, info.BlockId);
      accessor.Write(4, info.FileId);
      accessor.Write(8, info.FileOffset);
      accessor.Write(16, info.BlockSize);
      accessor.Write(20, info.EntryCount);
    }
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

    var cached = GetCachedSegment(segmentIndex);
    if (cached == null)
      return false;

    var (keySize, _, entrySize) = GetEntrySizes<TKey, TValue>();

    int index = BinarySearchSegment(cached.Accessor, key, cached.Count, entrySize, exactMatch: true);
    if (index == -1)
      return false;

    long offset = 8 + (index * entrySize);
    cached.Accessor.Read(offset + keySize, out value);
    return true;
  }

  /// <summary>
  /// Reads all entries from a segment within a key range using binary search to find start position.
  /// </summary>
  public List<KeyValuePair<TKey, TValue>> ReadRangeFromSegment<TKey, TValue>(int segmentIndex, TKey startKey, TKey endKey)
    where TKey : unmanaged, IComparable<TKey>
    where TValue : unmanaged
  {
    var results = new List<KeyValuePair<TKey, TValue>>();

    var cached = GetCachedSegment(segmentIndex);
    if (cached == null)
      return results;

    var (keySize, _, entrySize) = GetEntrySizes<TKey, TValue>();

    int startIndex = BinarySearchSegment(cached.Accessor, startKey, cached.Count, entrySize, exactMatch: false);
    if (startIndex >= cached.Count)
      return results;

    return ReadEntriesForward<TKey, TValue>(cached.Accessor, startIndex, cached.Count, endKey, keySize, entrySize);
  }

  /// <summary>
  /// Finds which segment might contain the given key by checking segment key ranges.
  /// Returns the segment index, or -1 if key is outside all segment ranges.
  /// Searches from newest to oldest segment.
  /// </summary>
  public int FindSegmentForKey<TKey>(TKey key) where TKey : unmanaged, IComparable<TKey>
  {
    for (int segmentIndex = NumSegments - 1; segmentIndex >= BeginSegmentIndex; segmentIndex--)
    {
      if (TryGetSegmentKeyRange<TKey>(segmentIndex, out TKey minKey, out TKey maxKey))
      {
        if (key.CompareTo(minKey) >= 0 && key.CompareTo(maxKey) <= 0)
        {
          return segmentIndex;
        }
      }
    }

    return -1;
  }

  /// <summary>
  /// Gets the key range for a specific segment (min and max keys).
  /// </summary>
  private bool TryGetSegmentKeyRange<TKey>(int segmentIndex, out TKey minKey, out TKey maxKey)
    where TKey : unmanaged, IComparable<TKey>
  {
    minKey = default;
    maxKey = default;

    var cached = GetCachedSegment(segmentIndex);
    if (cached == null)
      return false;

    int keySize = Marshal.SizeOf<TKey>();
    int valueSize = sizeof(long);
    int entrySize = keySize + valueSize;

    cached.Accessor.Read(8, out minKey);
    long lastOffset = 8 + ((cached.Count - 1) * entrySize);
    cached.Accessor.Read(lastOffset, out maxKey);

    return true;
  }

  /// <summary>
  /// Gets the maximum key in a segment. Used by truncation to determine which segments are fully below a threshold.
  /// </summary>
  internal bool TryGetSegmentMaxKey<TKey>(int segmentIndex, out TKey maxKey)
    where TKey : unmanaged, IComparable<TKey>
  {
    maxKey = default;

    var cached = GetCachedSegment(segmentIndex);
    if (cached == null)
      return false;

    int keySize = Marshal.SizeOf<TKey>();
    int valueSize = sizeof(long);
    int entrySize = keySize + valueSize;

    long lastOffset = 8 + ((cached.Count - 1) * entrySize);
    cached.Accessor.Read(lastOffset, out maxKey);

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

      var block = ReadMetadataEntry(idx);
      string filePath = GetSegmentFilePath(block.FileId);

      if (!File.Exists(filePath))
        return null;

      var mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
      var accessor = mmf.CreateViewAccessor(block.FileOffset, block.BlockSize, MemoryMappedFileAccess.Read);

      int count = accessor.ReadInt32(4);
      if (count == 0)
      {
        accessor.Dispose();
        mmf.Dispose();
        return null;
      }

      return new SegmentCache.CachedSegment(mmf, accessor, count);
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
  private static int BinarySearchSegment<TKey>(MemoryMappedViewAccessor accessor, TKey key, int count, int entrySize, bool exactMatch)
    where TKey : unmanaged, IComparable<TKey>
  {
    int left = 0;
    int right = count - 1;
    int resultIndex = exactMatch ? -1 : count;

    while (left <= right)
    {
      int mid = left + (right - left) / 2;
      long offset = 8 + (mid * entrySize);

      TKey midKey = default;
      accessor.Read(offset, out midKey);

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

  /// <summary>
  /// Reads entries from startIndex to endKey (inclusive).
  /// </summary>
  private static List<KeyValuePair<TKey, TValue>> ReadEntriesForward<TKey, TValue>(
    MemoryMappedViewAccessor accessor,
    int startIndex,
    int count,
    TKey endKey,
    int keySize,
    int entrySize)
    where TKey : unmanaged, IComparable<TKey>
    where TValue : unmanaged
  {
    var results = new List<KeyValuePair<TKey, TValue>>();

    for (int i = startIndex; i < count; i++)
    {
      long offset = 8 + (i * entrySize);

      TKey key = default;
      TValue value = default;
      accessor.Read(offset, out key);

      if (key.CompareTo(endKey) > 0)
        break;

      accessor.Read(offset + keySize, out value);
      results.Add(new KeyValuePair<TKey, TValue>(key, value));
    }

    return results;
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
    for (int i = BeginSegmentIndex; i < newBeginSegmentIndex; i++)
    {
      _segmentCache.Invalidate(i);
      var block = ReadMetadataEntry(i);
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
        var block = ReadMetadataEntry(i);
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

  private string GetSegmentFilePath(int fileId)
  {
    return Path.Combine(_segmentDirectory, $"segment_file_{fileId}.dat");
  }

  /// <summary>
  /// Reads a metadata entry from the memory-mapped file.
  /// </summary>
  private SegmentBlockInfo ReadMetadataEntry(int segmentIndex)
  {
    long offset = segmentIndex * MetadataEntrySize;

    using (var accessor = _metadataFile.CreateViewAccessor(offset, MetadataEntrySize, MemoryMappedFileAccess.Read))
    {
      int blockId = accessor.ReadInt32(0);
      int fileId = accessor.ReadInt32(4);
      long fileOffset = accessor.ReadInt64(8);
      int blockSize = accessor.ReadInt32(16);
      int entryCount = accessor.ReadInt32(20);

      return new SegmentBlockInfo(blockId, fileId, fileOffset, blockSize, entryCount);
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