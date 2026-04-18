using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace StreamDB.LiteLsm;

/// <summary>
/// Zero-allocation struct-based skiplist using index-based linking.
/// All nodes stored in pre-allocated arrays - no heap allocations on insert.
/// Data layer is a circular buffer with logical indices for O(1) prefix eviction.
/// </summary>
/// <typeparam name="TKey">The type of keys, must be unmanaged and comparable</typeparam>
/// <typeparam name="TValue">The type of values, must be unmanaged</typeparam>
public unsafe sealed partial class SkipList<TKey, TValue> : IDisposable
    where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
{
    private const int MaxLevel = 16;
    private const double Probability = 0.25;
    private const int HeaderSize = 8; // 4 bytes for block size + 4 bytes for count

    // raw ptr for headAddr node
    private readonly Node* _dummyHead;

    // track for O(1) monotonic insertions
    private readonly Node*[] _tailsPerLevel;

    private readonly Random _random = new Random();
    public readonly int EntrySize;
    private readonly int _keySize;
    private readonly int _valueSize;
    private readonly int _maxEntries;

    public SkipList(int maxCapacity)
    {
        _maxEntries = maxCapacity;
        InitializeAllocator();

        _keySize = Marshal.SizeOf<TKey>();
        _valueSize = Marshal.SizeOf<TValue>();
        EntrySize = _keySize + _valueSize;

        // create dummy head node. Create this outside the allocator since it doesn't represent a real entry 
        // and we don't want it to be freed up by our arena allocator.
        _dummyHead = (Node*)NativeMemory.AllocZeroed(1, (nuint)sizeof(Node));
        _dummyHead->key = default;
        _dummyHead->DataIdx = -1;

        _tailsPerLevel = new Node*[MaxLevel];
    }

    public bool Insert(ref TKey key, ref TValue value, out Reason reason)
    {
        // check if key being inserted is greater than current max key for O(1) insert path optimization
        if (Count == 0 || key.CompareTo(GetKeyFromDataLayer(_serializedDataAllocator.LastLogicalAddress)) > 0)
        {
            reason = Reason.Full; // can't fail because something already exists if we go down monotonic path
            // if it does fail it will be because we hit capacity ONLY so just set the reason to full.
            return InsertMonotonic(ref key, ref value);
        }

        // Non hotpath fallback to general insert path with O(log n) search
        return InsertGeneral(ref key, ref value, out reason);
    }

    private bool InsertGeneral(ref TKey key, ref TValue value, out Reason reason)
    {
        throw new NotImplementedException("Not done yet, a little hard tbh");
        reason = Reason.None;
        value = default;
        if (_nodeAllocator.TryAllocate(out Node* node) == -1)
        {
            reason = Reason.Full;
            return false;
        }

        for (int i = 0; i < MaxLevel; i++)
            node->levelPtrs[i] = 0;

        // search for where the key should be inserted...
        // when writing to data layer here we need to move the data forward.
        long targetIdx = GetKeyIdxInDataLayer(in key);
        if (targetIdx >= 0)
        {
            // not supporting updates for now
            reason = Reason.KeyExists;
            return false; 
        }

        // now we need to do an insertion at ~targetIdx, which means we need to shift all data starting from that point forward by 1 to make room for the new entry.
        // Issue is that all previous inserts the serialized data layer is kept in sync with skiplist nodes.
        // This would be migrated if whenever we had an out-of-order insert we, would write it to a side store.
        // and then at the time of a flush we would merge the side store entries  
         return false;
    }

    /// <summary>
    /// Inserts a key-value pair assuming the key is greater than all existing keys.
    /// </summary>
    public bool InsertMonotonic(ref TKey key, ref TValue value)
    {
        if (_nodeAllocator.TryAllocate(out Node* node) == -1)
        {
            return false; // skiplist is full - caller should flush do eviction and retry
        }

        // Zero levelPtrs (may have stale data from previous allocation cycle)
        for (int i = 0; i < MaxLevel; i++)
            node->levelPtrs[i] = 0;

        // write to raw data layer first
        long dataAddr = WriteToDataLayer(ref key, ref value);

        node->key = key;
        node->DataIdx = dataAddr;

        int level = RandomLevel();

        for (int i = 0; i < level; i++)
        {
            if (_tailsPerLevel[i] != default)
            {
                Node* currTailNodePtr = _tailsPerLevel[i];
                // Volatile.Write ensures readers see consistent node data before the pointer
                Volatile.Write(ref currTailNodePtr->levelPtrs[i], (long)node);
            }
            else
            {
                Volatile.Write(ref _dummyHead->levelPtrs[i], (long)node);
            }
            _tailsPerLevel[i] = node;
        }
        return true;
    }

    /// <summary>
    /// Searches for a key in the skiplist
    /// </summary>
    public bool TryGetValue(in TKey key, out TValue value)
    {
        value = default;
        long targetIdx = GetKeyIdxInDataLayer(in key);
        if (targetIdx < 0)
        {
            return false;
        }
        if (IsDeleted(targetIdx))
        {
            return false;
        }
        value = GetValueFromDataLayer(targetIdx);
        return true;
    }

    /// <summary>
    /// Marks a key as deleted in the skiplist. Returns true if found and deleted.
    /// </summary>
    public bool Delete(in TKey key)
    {
        long targetIdx = GetKeyIdxInDataLayer(in key);
        if (targetIdx < 0)
        {
            return false;
        }
        if (IsDeleted(targetIdx))
        {
            return false; // already deleted
        }
        MarkDeleted(targetIdx);
        return true;
    }

    // Returns logical index if found, or bitwise complement of logical insertion point.
    private long GetKeyIdxInDataLayer(in TKey key)
    {
        Node* currNode = _dummyHead;
        for (int i = MaxLevel - 1; i > -1; i--)
        {
            while (currNode != default)
            {
                long nextPtr = Volatile.Read(ref currNode->levelPtrs[i]);
                if (nextPtr == 0) break;
                Node* next = (Node*)nextPtr;
                if (next->key.CompareTo(key) < 0)
                {
                    currNode = next;
                }
                else if (next->key.CompareTo(key) == 0)
                {
                    return next->DataIdx;
                }
                else
                {
                    break;
                }
            }
        }

        // Key not found - return bitwise complement of insertion point
        {
            long nextPtr = Volatile.Read(ref currNode->levelPtrs[0]);
            if (nextPtr != 0)
            {
                Node* next = (Node*)nextPtr;
                return ~next->DataIdx;
            }
        }

        return ~(long)_serializedDataAllocator.TailAddress;
    }

    private int RandomLevel()
    {
        int level = 1;
        while (_random.NextDouble() < Probability && level < MaxLevel)
        {
            level++;
        }
        return level;
    }

    public bool ProbablyContainsKey(in TKey key)
    {
        TKey minKey_ = default;
        TKey* minKey = &minKey_;
        if (!TryGetMinKey(out minKey) || key.CompareTo(Unsafe.AsRef<TKey>(minKey)) < 0)
            return false;
        if (!TryGetMaxKey(out minKey) || key.CompareTo(Unsafe.AsRef<TKey>(minKey)) > 0)
            return false;
        return true;
    }

    private bool TryGetMinKey(out TKey* key)
    {
        if (Count == 0)
        {
            key = default;
            return false;
        }

        key = (TKey*)Unsafe.AsPointer(ref GetKeyFromDataLayer(_serializedDataAllocator.FirstLogicalAddress));
        return true;
    }

    private bool TryGetMaxKey(out TKey* key)
    {
        if (Count == 0)
        {
            key = default;
            return false;
        }

        key = (TKey*)Unsafe.AsPointer(ref GetKeyFromDataLayer(_serializedDataAllocator.LastLogicalAddress));
        return true;
    }

    /// <summary>
    /// Discards all entries with keys strictly less than beforeKey.
    /// Returns the number of entries truncated.
    /// </summary>
    public int TruncatePrefix(in TKey beforeKey)
    {
        int count = CountPrefix(beforeKey);
        if (count > 0)
        {
            EvictPrefix(count);
        }
        return count;
    }

    /// <summary>
    /// Counts entries with keys strictly less than beforeKey. Does not mutate.
    /// </summary>
    public int CountPrefix(in TKey beforeKey)
    {
        if (Count == 0)
            return 0;

        ref TKey minKey = ref GetKeyFromDataLayer(_serializedDataAllocator.FirstLogicalAddress);
        if (minKey.CompareTo(beforeKey) >= 0)
            return 0;

        int truncateCount = 0;
        int capacity = _maxEntries;
        int idx = _serializedDataAllocator.FirstLogicalAddress;
        for (int i = 0; i < Count; i++)
        {
            ref TKey k = ref GetKeyFromDataLayer(idx);
            if (k.CompareTo(beforeKey) >= 0)
                break;
            truncateCount++;
            idx = (idx + 1) % capacity;
        }

        return truncateCount;
    }

    // return true if more entries to read. Skips deleted entries.
    public (bool, int) MemCpyRawRange(in TKey fromKey, in TKey endKey, Span<KeyValuePair<TKey, TValue>> buffer, bool excludeStart = false)
    {
        if (Count == 0)
            return (false, 0);

        long fromIdx = GetKeyIdxInDataLayer(in fromKey);
        if (fromIdx < 0)
        {
            fromIdx = ~fromIdx;
        }
        else if (excludeStart)
        {
            fromIdx = (fromIdx + 1) % _maxEntries;
        }

        int capacity = _maxEntries;
        int head = _serializedDataAllocator.FirstLogicalAddress;
        int totalEntries = Count;

        // Calculate how far fromIdx is from head in the ring
        int startOffset = ((int)fromIdx - head + capacity) % capacity;
        if (startOffset >= totalEntries)
            return (false, 0);

        int emitted = 0;
        bool hasMore = false;
        for (int i = startOffset; i < totalEntries && emitted < buffer.Length; i++)
        {
            int physicalIdx = (head + i) % capacity;
            ref TKey key = ref GetKeyFromDataLayer(physicalIdx);
            if (key.CompareTo(endKey) > 0)
            {
                hasMore = true;
                break;
            }

            if (!IsDeleted(physicalIdx))
            {
                ref KeyValuePair<TKey, TValue> pair = ref _serializedDataAllocator.Dereference(physicalIdx);
                byte* pairPtr = (byte*)Unsafe.AsPointer(ref pair);
                buffer[emitted] = new KeyValuePair<TKey, TValue>(
                    Unsafe.AsRef<TKey>(pairPtr),
                    Unsafe.AsRef<TValue>(pairPtr + _keySize));
                emitted++;
            }
        }

        if (!hasMore && emitted == buffer.Length)
        {
            hasMore = true; // buffer full, may have more entries
        }

        return (hasMore, emitted);
    }
}

public enum Reason
{
    None = 0,
    Full,
    KeyExists
}