using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace StreamDB.LiteLsm;

public sealed unsafe partial class SkipList<TKey, TValue> : IDisposable
  where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
{
  #region Serialized Data Layer
  private byte[] _deletedFlags;
  private CircularBufferAllocator<KeyValuePair<TKey, TValue>> _serializedDataAllocator;

  public int Count => _serializedDataAllocator.Count;

  private long WriteToDataLayer(ref TKey key, ref TValue value)
  {
    if (Count >= _maxEntries)
    {
      throw new InvalidOperationException($"Data layer capacity exceeded: {_maxEntries} entries (Count={Count})");
    }

    int logicalAddr = _serializedDataAllocator.TryAllocate(out KeyValuePair<TKey, TValue>* slot);
    *slot = new KeyValuePair<TKey, TValue>(key, value);
    _deletedFlags[logicalAddr % _maxEntries] = 0; // alive
    return logicalAddr;
  }

  private ref TKey GetKeyFromDataLayer(long logicalIdx)
  {
    int physicalIdx = (int)(logicalIdx % _maxEntries);
    ref KeyValuePair<TKey, TValue> pair = ref _serializedDataAllocator.Dereference(physicalIdx);
    byte* pairPtr = (byte*)Unsafe.AsPointer(ref pair);
    return ref Unsafe.AsRef<TKey>(pairPtr);
  }

  private ref TValue GetValueFromDataLayer(long logicalIdx)
  {
    int physicalIdx = (int)(logicalIdx % _maxEntries);
    ref KeyValuePair<TKey, TValue> pair = ref _serializedDataAllocator.Dereference(physicalIdx);
    byte* pairPtr = (byte*)Unsafe.AsPointer(ref pair);
    return ref Unsafe.AsRef<TValue>(pairPtr + _keySize);
  }

  internal bool IsDeleted(long logicalIdx) => _deletedFlags[(int)(logicalIdx % _maxEntries)] != 0;

  internal void MarkDeleted(long logicalIdx) => _deletedFlags[(int)(logicalIdx % _maxEntries)] = 1;

  // Writes to stream: [header 8B][flags byte[count]][KV entries]
  private void CopyRingToBuffer(int count, Stream destination)
  {
    int entryByteSize = Unsafe.SizeOf<KeyValuePair<TKey, TValue>>();
    int dataBytes = count * entryByteSize;
    int totalSize = 8 + count + dataBytes; // header + flags + KV data
    // header = [totalSize (4 bytes), count (4 bytes)]
    destination.Write(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref totalSize, 1)));
    destination.Write(MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref count, 1)));

    int startPhysical = _serializedDataAllocator.FirstLogicalAddress;
    int capacity = _maxEntries;
    int contiguous = capacity - startPhysical;

    // Write flags section (handles wrap)
    if (count <= contiguous)
    {
      destination.Write(_deletedFlags.AsSpan(startPhysical, count));
    }
    else
    {
      destination.Write(_deletedFlags.AsSpan(startPhysical, contiguous));
      destination.Write(_deletedFlags.AsSpan(0, count - contiguous));
    }

    // Write KV data (handles wrap)
    if (count <= contiguous)
    {
      KeyValuePair<TKey, TValue>* startPtr = _serializedDataAllocator.GetRawPointer(startPhysical);
      destination.Write(new ReadOnlySpan<byte>(startPtr, dataBytes));
    }
    else
    {
      int firstPartBytes = contiguous * entryByteSize;
      int secondPartBytes = dataBytes - firstPartBytes;
      destination.Write(new ReadOnlySpan<byte>(_serializedDataAllocator.GetRawPointer(startPhysical), firstPartBytes));
      destination.Write(new ReadOnlySpan<byte>(_serializedDataAllocator.GetRawPointer(0), secondPartBytes));
    }
  }

  /// <summary>
  /// Takes the first N entries. Copies raw segment bytes to the provided Stream.
  /// </summary>
  public void CopyPrefixToBuffer(int count, Stream destination)
  {
    if (count <= 0 || count > Count)
      throw new ArgumentOutOfRangeException(nameof(count), $"Cannot evict {count} entries (Count={Count})");

    CopyRingToBuffer(count, destination);
  }

  /// <summary>Disconnects evicted nodes and frees them inline.</summary>
  public void EvictPrefix(int count)
  {
    // Walk level-0 to find the first retained node (the node at position 'count')
    Node* walker = (_dummyHead->levelPtrs[0] != 0) ? (Node*)_dummyHead->levelPtrs[0] : null;
    Node* firstRetained = null;
    for (int n = 0; n < count; n++)
    {
      if (walker == null) break;
      walker = (walker->levelPtrs[0] != 0) ? (Node*)walker->levelPtrs[0] : null;
    }
    firstRetained = walker; // null if all evicted

    // For each level, patch Head to skip past evicted nodes.
    // Walk from head at each level until we find firstRetained or a node that comes after it.
    for (int lvl = 0; lvl < MaxLevel; lvl++)
    {
      if (_dummyHead->levelPtrs[lvl] == 0)
        continue;

      if (firstRetained == null)
      {
        // All nodes evicted at this level
        _dummyHead->levelPtrs[lvl] = 0;
        _tailsPerLevel[lvl] = null;
        continue;
      }

      // Walk this level to find first node >= firstRetained (by pointer identity or key order)
      Node* current = (Node*)_dummyHead->levelPtrs[lvl];
      Node* retainedAtLevel = null;

      while (current != null)
      {
        if (current->key.CompareTo(firstRetained->key) >= 0)
        {
          retainedAtLevel = current;
          break;
        }
        current = (current->levelPtrs[lvl] != 0) ? (Node*)current->levelPtrs[lvl] : null;
      }

      _dummyHead->levelPtrs[lvl] = (retainedAtLevel != null) ? (long)retainedAtLevel : 0;

      if (_tailsPerLevel[lvl] != null && _tailsPerLevel[lvl]->key.CompareTo(firstRetained->key) < 0)
      {
        _tailsPerLevel[lvl] = null;
      }
    }

    // Bulk free both node and data allocators
    _nodeAllocator.Free(count);
    _serializedDataAllocator.Free(count);
  }


  #endregion

  #region Node Memory Management (Circular Buffer)

  private CircularBufferAllocator<Node> _nodeAllocator;

  struct Node
  {
    public TKey key;
    public fixed long levelPtrs[16];
    public long DataIdx;
  }

  #endregion

  private bool disposedValue;

  internal void InitializeAllocator()
  {
    // One node per data entry
    _nodeAllocator = new CircularBufferAllocator<Node>(_maxEntries);
    _serializedDataAllocator = new CircularBufferAllocator<KeyValuePair<TKey, TValue>>(_maxEntries);
    _deletedFlags = new byte[_maxEntries];
  }

  private void Dispose(bool disposing)
  {
    if (!disposedValue)
    {
      disposedValue = true;
      _serializedDataAllocator.Dispose();
      _nodeAllocator.Dispose();
    }
  }

  public void Dispose()
  {
    Dispose(disposing: true);
    GC.SuppressFinalize(this);
  }
}