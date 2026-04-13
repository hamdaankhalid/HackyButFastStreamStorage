using System.Diagnostics;
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
                _currentFile = new FileStream(lastFilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
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
            _currentFile = new FileStream(newFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);
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

        long offset = HEADER_SIZE + (index * entrySize);
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
        Span<(TKey, TValue)> buffer,
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

        var (keySize, _, entrySize) = GetEntrySizes<TKey, TValue>();
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
        Span<(TKey, TValue)> buffer,
        int keySize,
        int entrySize,
        bool excludeStart = false)
        where TKey : unmanaged, IComparable<TKey>
        where TValue : unmanaged
    {
        var cached = GetCachedSegment(segmentIndex);
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
            exactMatch: false);

        // If excludeStart and exact match, advance past the matched entry
        if (excludeStart && startIndex < cached.Count)
        {
            long checkOffset = HEADER_SIZE + (startIndex * entrySize);
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

        // Single-pass: read entries directly into buffer while checking endKey
        int entriesRead = 0;
        bool exceededEndKey = false;
        int maxEntries = Math.Min(cached.Count - startIndex, buffer.Length);

        for (int i = 0; i < maxEntries; i++)
        {
            long offset = HEADER_SIZE + ((startIndex + i) * entrySize);

            // Read the full tuple directly from memory-mapped file
            cached.Accessor.Read(offset, out (TKey, TValue) entry);

            // Check if we've exceeded endKey
            if (entry.Item1.CompareTo(endKey) > 0)
            {
                exceededEndKey = true;
                break;
            }

            buffer[entriesRead] = entry;
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

        var cached = GetCachedSegment(segmentIndex);
        if (cached == null)
            return false;

        // Use GetEntrySizes to get correct entry size (works for any key type)
        // We use a dummy long for TValue since we only need to calculate offsets based on key positions
        var (_, _, entrySize) = GetEntrySizes<TKey, long>();

        cached.Accessor.Read(HEADER_SIZE, out minKey);
        long lastOffset = HEADER_SIZE + ((cached.Count - 1) * entrySize);
        cached.Accessor.Read(lastOffset, out maxKey);

        return true;
    }

    /// <summary>
    /// Gets the maximum key in a segment. Used by truncation to determine which segments are fully below a threshold.
    /// </summary>
    public bool TryGetSegmentMaxKey<TKey>(int segmentIndex, out TKey maxKey)
      where TKey : unmanaged, IComparable<TKey> => TryGetSegmentKeyRange(segmentIndex, out _, out maxKey);

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

            var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var mmf = MemoryMappedFile.CreateFromFile(fs, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, leaveOpen: false);
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
            long offset = HEADER_SIZE + (mid * entrySize);

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