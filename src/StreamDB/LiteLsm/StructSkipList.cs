using System.Runtime.InteropServices;

namespace StreamDB.LiteLsm;

/// <summary>
/// Zero-allocation struct-based skiplist using index-based linking.
/// All nodes stored in pre-allocated arrays - no heap allocations on insert.
/// </summary>
/// <typeparam name="TKey">The type of keys, must be unmanaged and comparable</typeparam>
/// <typeparam name="TValue">The type of values, must be unmanaged</typeparam>
public class StructSkipList<TKey, TValue> : IMonotonicSkipList<TKey, TValue> where TKey : unmanaged, IComparable<TKey>
    where TValue : unmanaged
{
    private const int MaxLevel = 32;
    private const double Probability = 0.5;
    private const int NullIndex = -1;
    private const int HeaderSize = 8; // 4 bytes for block size + 4 bytes for count

    /// <summary>
    /// A node stored as a struct in an array. No heap allocation.
    /// Uses integer indices instead of object references.
    /// </summary>
    private struct Node
    {
        public TKey Key;
        public int DataOffset;      // Offset into _rawDataLayer
        public int Level;           // Number of forward pointers
        public int ForwardOffset;   // Start index in _forwardLinks array

        // No Forward array here - stored separately in _forwardLinks
    }

    // Node storage - pre-allocated array
    private readonly Node[] _nodes;
    private int _nodeCount;

    // Forward link storage - all forward arrays stored contiguously
    // Layout: [node0_links...][node1_links...][node2_links...]
    private readonly int[] _forwardLinks;
    private int _forwardLinksUsed;

    // Head node forward links (stored separately at start of _forwardLinks)
    private const int HeadForwardOffset = 0;

    private readonly Random _random;
    private int _level;
    private int _count;

    // Pre-serialized data layer for zero-copy flush
    private readonly byte[] _rawDataLayer;
    private int _rawDataOffset;
    private readonly int _entrySize;
    private readonly int _keySize;
    private readonly int _valueSize;

    // Pre-allocated update array
    private readonly int[] _updateArray;

    public StructSkipList(int maxCapacity)
    {
        _nodes = new Node[maxCapacity];
        _nodeCount = 0;

        // Worst case: all nodes at MaxLevel, so need maxCapacity * (MaxLevel + 1) forward links
        // Plus (MaxLevel + 1) for the head node
        _forwardLinks = new int[(maxCapacity * (MaxLevel + 1)) + (MaxLevel + 1)];

        // Initialize head forward links (first MaxLevel+1 entries)
        for (int i = 0; i <= MaxLevel; i++)
        {
            _forwardLinks[i] = NullIndex;
        }
        _forwardLinksUsed = MaxLevel + 1;  // Head takes first MaxLevel+1 slots

        _level = 0;
        _count = 0;
        _random = new Random();

        _keySize = Marshal.SizeOf<TKey>();
        _valueSize = Marshal.SizeOf<TValue>();
        _entrySize = _keySize + _valueSize;

        // Allocate raw data layer: HeaderSize (block size + count) + entries
        int dataSize = HeaderSize + (_entrySize * maxCapacity);
        _rawDataLayer = new byte[dataSize];
        _rawDataOffset = HeaderSize;

        // Pre-allocate update array
        _updateArray = new int[MaxLevel + 1];
    }

    public int Count => _count;

    /// <summary>
    /// Gets the raw data layer for zero-copy flush to disk.
    /// </summary>
    public ReadOnlySpan<byte> GetRawDataLayer()
    {
        int blockSize = _rawDataOffset;

        // Write block header: size and count
        MemoryMarshal.Write(_rawDataLayer.AsSpan(0), blockSize);
        MemoryMarshal.Write(_rawDataLayer.AsSpan(4), _count);

        return new ReadOnlySpan<byte>(_rawDataLayer, 0, blockSize);
    }

    /// <summary>
    /// Inserts a key-value pair assuming the key is greater than all existing keys.
    /// ZERO heap allocations - just writes to pre-allocated arrays.
    /// </summary>
    public void InsertMonotonic(TKey key, TValue value)
    {
        // Write key-value to raw data layer
        int dataOffset = _rawDataOffset;
        MemoryMarshal.Write(_rawDataLayer.AsSpan(dataOffset), in key);
        MemoryMarshal.Write(_rawDataLayer.AsSpan(dataOffset + _keySize), in value);
        _rawDataOffset += _entrySize;

        // Find tail update path (reuse pre-allocated array)
        FindTailUpdatePath(_updateArray);

        int newLevel = RandomLevel();
        EnsureLevel(newLevel, _updateArray);

        // Allocate forward links for this node
        int forwardOffset = _forwardLinksUsed;
        _forwardLinksUsed += newLevel + 1;

        // Initialize new node's forward links
        for (int i = 0; i <= newLevel; i++)
        {
            _forwardLinks[forwardOffset + i] = NullIndex;
        }

        // Create node struct (no allocation - just write to array)
        int nodeIndex = _nodeCount++;
        _nodes[nodeIndex] = new Node
        {
            Key = key,
            DataOffset = dataOffset,
            Level = newLevel,
            ForwardOffset = forwardOffset
        };

        // Link node using indices
        LinkNode(nodeIndex, _updateArray, newLevel);
    }

    /// <summary>
    /// Searches for a key in the skiplist
    /// </summary>
    public bool TryGetValue(TKey key, out TValue value)
    {
        value = default;
        int targetIdx = GetKeyIdxInDataLayer(key);
        if (targetIdx < 0)
        {
            return false;
        }

        value = MemoryMarshal.Read<TValue>(
            _rawDataLayer.AsSpan(_nodes[targetIdx].DataOffset + _keySize, _valueSize));
        return true;
    }

    private int GetKeyIdxInDataLayer(TKey key)
    {
        int currentIdx = NullIndex;  // Start from head
        int currentForwardOffset = HeadForwardOffset;

        // Search from top level down
        for (int i = _level; i >= 0; i--)
        {
            int nextIdx = _forwardLinks[currentForwardOffset + i];

            while (nextIdx != NullIndex && _nodes[nextIdx].Key.CompareTo(key) < 0)
            {
                currentIdx = nextIdx;
                currentForwardOffset = _nodes[currentIdx].ForwardOffset;
                nextIdx = _forwardLinks[currentForwardOffset + i];
            }
        }

        // Move to level 0 next node (first node >= key)
        int targetIdx = _forwardLinks[currentForwardOffset + 0];

        if (targetIdx != NullIndex && _nodes[targetIdx].Key.CompareTo(key) == 0)
        {
            return targetIdx;
        }

        // Return bitwise complement of successor index (first node >= key, or _nodeCount if none)
        int successorIdx = targetIdx == NullIndex ? _nodeCount : targetIdx;
        return ~successorIdx;
    }

    /// <summary>
    /// Clears all elements from the skiplist
    /// </summary>
    public void Clear()
    {
        // Just reset counters - no deallocation needed
        _nodeCount = 0;
        _forwardLinksUsed = MaxLevel + 1;  // Reset to just head's links

        // Clear head forward links
        for (int i = 0; i <= MaxLevel; i++)
        {
            _forwardLinks[i] = NullIndex;
        }

        _level = 0;
        _count = 0;
        _rawDataOffset = HeaderSize;
    }

    /// <summary>
    /// Enumerates all key-value pairs in sorted order
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
    {
        int currentIdx = _forwardLinks[HeadForwardOffset + 0];  // Head's level-0 forward

        while (currentIdx != NullIndex)
        {
            Node node = _nodes[currentIdx]; // Can't use ref in iterator
            TKey key = node.Key;
            TValue value = MemoryMarshal.Read<TValue>(
                _rawDataLayer.AsSpan(node.DataOffset + _keySize, _valueSize));

            yield return new KeyValuePair<TKey, TValue>(key, value);

            currentIdx = _forwardLinks[node.ForwardOffset + 0];
        }
    }

    private int RandomLevel()
    {
        int level = 0;
        while (_random.NextDouble() < Probability && level < MaxLevel)
        {
            level++;
        }
        return level;
    }

    /// <summary>
    /// Finds the tail nodes at each level (for monotonic insert).
    /// Stores node indices in the update array.
    /// </summary>
    private void FindTailUpdatePath(int[] update)
    {
        int currentIdx = NullIndex;  // -1 represents head
        int currentForwardOffset = HeadForwardOffset;

        for (int i = _level; i >= 0; i--)
        {
            int nextIdx = _forwardLinks[currentForwardOffset + i];

            while (nextIdx != NullIndex)
            {
                currentIdx = nextIdx;
                currentForwardOffset = _nodes[currentIdx].ForwardOffset;
                nextIdx = _forwardLinks[currentForwardOffset + i];
            }

            update[i] = currentIdx;  // Store index, not reference
        }
    }

    private void EnsureLevel(int newLevel, int[] update)
    {
        if (newLevel > _level)
        {
            for (int i = _level + 1; i <= newLevel; i++)
            {
                update[i] = NullIndex;  // Head node
            }
            _level = newLevel;
        }
    }

    /// <summary>
    /// Links a new node into the skiplist using indices instead of references.
    /// </summary>
    private void LinkNode(int nodeIndex, int[] update, int level)
    {
        ref Node newNode = ref _nodes[nodeIndex];

        for (int i = 0; i <= level; i++)
        {
            int prevIdx = update[i];

            if (prevIdx == NullIndex)  // Connecting from head
            {
                _forwardLinks[newNode.ForwardOffset + i] = _forwardLinks[HeadForwardOffset + i];
                _forwardLinks[HeadForwardOffset + i] = nodeIndex;
            }
            else
            {
                ref Node prevNode = ref _nodes[prevIdx];
                _forwardLinks[newNode.ForwardOffset + i] = _forwardLinks[prevNode.ForwardOffset + i];
                _forwardLinks[prevNode.ForwardOffset + i] = nodeIndex;
            }
        }

        _count++;
    }

    public bool ProbablyContainsKey(TKey key)
    {
        if (!TryGetMinKey(out TKey minKey) || key.CompareTo(minKey) < 0)
            return false;
        if (!TryGetMaxKey(out TKey maxKey) || key.CompareTo(maxKey) > 0)
            return false;
        return true;
    }

    public bool TryGetMinKey(out TKey key)
    {
        if (Count == 0)
        {
            key = default;
            return false;
        }

        key = MemoryMarshal.Read<TKey>(_rawDataLayer.AsSpan(HeaderSize, _keySize)); // First entry's key is at offset HeaderSize
        return true;
    }

    public bool TryGetMaxKey(out TKey key)
    {
        if (Count == 0)
        {
            key = default;
            return false;
        }

        // Look at the last node at last entry in _rawDataLayer
        int lastNodeDataOffset = _rawDataOffset - _entrySize;
        key = MemoryMarshal.Read<TKey>(_rawDataLayer.AsSpan(lastNodeDataOffset, _keySize));
        return true;
    }


    // return true if more entries to read
    public (bool, int) MemCpyRawRange(TKey fromKey, TKey endKey, Span<(TKey, TValue)> buffer, bool excludeStart = false)
    {
        // Get the starting node index
        int fromIdx = GetKeyIdxInDataLayer(fromKey);
        if (fromIdx < 0)
        {
            fromIdx = ~fromIdx; // Start from the next greater key
        }
        else if (excludeStart)
        {
            // Exact match found but caller wants to start after this key — advance one node via level-0 link
            fromIdx = _forwardLinks[_nodes[fromIdx].ForwardOffset + 0];
            if (fromIdx == NullIndex)
            {
                return (false, 0);
            }
        }

        if (fromIdx < 0 || fromIdx >= _nodeCount)
        {
            return (false, 0);
        }

        int startOffset = _nodes[fromIdx].DataOffset;

        // Scan linearly to find the index we want to copy till
        int entriesToCopy = 0;
        int currentIdx = fromIdx;

        bool fullScanned = false;
        while (currentIdx != NullIndex && entriesToCopy < buffer.Length)
        {
            ref Node node = ref _nodes[currentIdx];
            if (node.Key.CompareTo(endKey) > 0)
            {
                fullScanned = true;
                break;
            }

            entriesToCopy++;
            currentIdx = _forwardLinks[node.ForwardOffset + 0];
        }

        if (entriesToCopy == 0)
        {
            return (false, 0);
        }

        if (entriesToCopy > buffer.Length)
        {
            throw new ArgumentException("Buffer too small for requested range");
        }

        // Single memory copy call
        int byteCount = entriesToCopy * _entrySize;
        _rawDataLayer.AsSpan(startOffset, byteCount).CopyTo(MemoryMarshal.AsBytes(buffer.Slice(0, entriesToCopy)));

        return (!fullScanned, entriesToCopy);
    }
}
