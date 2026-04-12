using System.Buffers;
using System.Runtime.InteropServices;

namespace StreamDB.LiteLsm;

/// <summary>
/// A skiplist optimized for monotonic inserts with zero-copy disk flush.
/// Nodes store keys for search; values only exist in the raw data layer.
/// </summary>
/// <typeparam name="TKey">The type of keys, must be unmanaged and comparable</typeparam>
/// <typeparam name="TValue">The type of values, must be unmanaged</typeparam>
public class SkipList<TKey, TValue> : IMonotonicSkipList<TKey, TValue> 
  where TKey : unmanaged, IComparable<TKey>
  where TValue : unmanaged
{
    private const int MaxLevel = 32;
    private const double Probability = 0.5;

    private readonly Node _head;
    private readonly Random _random;
    private int _level;
    private int _count;
    
    // Pre-serialized data layer for zero-copy flush
    private readonly byte[] _rawDataLayer;
    private int _rawDataOffset;
    private readonly int _entrySize;
    private readonly int _keySize;
    private readonly int _valueSize;
    
    // Pre-allocated update array to avoid allocation on every insert (hot path optimization)
    private readonly Node[] _updateArray;

    /// <summary>
    /// Represents a node in the skiplist
    /// </summary>
    class Node
    {
        public TKey Key { get; }           // Key for searching
        public int DataOffset { get; }      // Offset into _rawDataLayer where key+value are stored
        public Node?[] Forward { get; }
        public bool IsHeader { get; }
        public int Level { get; }          // Store level for cleanup

        public Node(int level, bool isHeader = false)
        {
            Level = level;
            // Use ArrayPool for Forward arrays to reduce GC pressure
            Forward = ArrayPool<Node?>.Shared.Rent(level + 1);
            Array.Clear(Forward, 0, level + 1);
            IsHeader = isHeader;
            DataOffset = -1;
        }

        public Node(TKey key, int dataOffset, int level)
        {
            Key = key;
            DataOffset = dataOffset;
            Level = level;
            // Use ArrayPool for Forward arrays to reduce GC pressure
            Forward = ArrayPool<Node?>.Shared.Rent(level + 1);
            Array.Clear(Forward, 0, level + 1);
            IsHeader = false;
        }
        
        public void ReturnToPool()
        {
            // Return Forward array to pool when node is cleared
            if (!IsHeader)
            {
                ArrayPool<Node?>.Shared.Return(Forward, clearArray: true);
            }
        }
    }

    public SkipList(int maxCapacity)
    {
        _head = new Node(MaxLevel, isHeader: true);
        _level = 0;
        _count = 0;
        _random = new Random();
        
        _keySize = Marshal.SizeOf<TKey>();
        _valueSize = Marshal.SizeOf<TValue>();
        _entrySize = _keySize + _valueSize;
        
        // Allocate raw data layer: 4 bytes for block size + 4 bytes for count + entries
        int dataSize = 8 + (_entrySize * maxCapacity);  // 8 bytes for header (size + count)
        _rawDataLayer = new byte[dataSize];
        _rawDataOffset = 8; // Skip block size and count header
        
        // Pre-allocate update array to avoid allocation on every insert
        _updateArray = new Node[MaxLevel + 1];
    }

    /// <summary>
    /// Gets the number of elements in the skiplist
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Gets the raw data layer for zero-copy flush to disk.
    /// Includes block header: [4B block size][4B entry count][entries...]
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
    /// Reads a key from the raw data layer at the given offset
    /// </summary>
    private TKey ReadKey(int offset)
    {
        return MemoryMarshal.Read<TKey>(_rawDataLayer.AsSpan(offset, _keySize));
    }

    /// <summary>
    /// Reads a value from the raw data layer at the given offset
    /// </summary>
    private TValue ReadValue(int offset)
    {
        return MemoryMarshal.Read<TValue>(_rawDataLayer.AsSpan(offset + _keySize, _valueSize));
    }

    /// <summary>
    /// Inserts a key-value pair assuming the key is greater than all existing keys.
    /// Writes directly to the raw data layer for zero-copy flush.
    /// </summary>
    public void InsertMonotonic(TKey key, TValue value)
    {
        // Write key-value to raw data layer
        int dataOffset = _rawDataOffset;
        MemoryMarshal.Write(_rawDataLayer.AsSpan(dataOffset), in key);
        MemoryMarshal.Write(_rawDataLayer.AsSpan(dataOffset + _keySize), in value);
        _rawDataOffset += _entrySize;

        // Reuse pre-allocated update array (avoids 264 bytes allocation per insert)
        FindTailUpdatePath(_updateArray);

        int newLevel = RandomLevel();
        EnsureLevel(newLevel, _updateArray);
        
        Node newNode = new Node(key, dataOffset, newLevel);
        LinkNode(newNode, _updateArray, newLevel);
    }

    /// <summary>
    /// Searches for a key in the skiplist
    /// </summary>
    public bool TryGetValue(TKey key, out TValue value)
    {
        Node current = _head;

        for (int i = _level; i >= 0; i--)
        {
            while (current.Forward[i] != null && 
                   current.Forward[i]!.Key.CompareTo(key) < 0)
            {
                current = current.Forward[i]!;
            }
        }

        current = current.Forward[0]!;

        if (current != null && current.Key.CompareTo(key) == 0)
        {
            value = ReadValue(current.DataOffset);
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Checks if the skiplist contains a key
    /// </summary>
    public bool ContainsKey(TKey key)
    {
        return TryGetValue(key, out _);
    }

    /// <summary>
    /// Clears all elements from the skiplist
    /// </summary>
    public void Clear()
    {
        // Return Forward arrays to pool before clearing
        Node? current = _head.Forward[0];
        while (current != null)
        {
            Node? next = current.Forward[0];
            current.ReturnToPool();
            current = next;
        }
        
        for (int i = 0; i <= _level; i++)
        {
            _head.Forward[i] = null;
        }
        _level = 0;
        _count = 0;
        _rawDataOffset = 8; // Reset to skip block header (4B size + 4B count)
    }

    /// <summary>
    /// Enumerates all key-value pairs in sorted order
    /// </summary>
    public IEnumerable<KeyValuePair<TKey, TValue>> GetAll()
    {
        Node? current = _head.Forward[0];
        while (current != null)
        {
            TKey key = current.Key;
            TValue value = ReadValue(current.DataOffset);
            yield return new KeyValuePair<TKey, TValue>(key, value);
            current = current.Forward[0];
        }
    }

    /// <summary>
    /// Generates a random level for a new node
    /// </summary>
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
    /// Returns the update array pointing to the last node at each level.
    /// </summary>
    private void FindTailUpdatePath(Node[] update)
    {
        Node current = _head;

        for (int i = _level; i >= 0; i--)
        {
            while (current.Forward[i] != null)
            {
                current = current.Forward[i]!;
            }
            update[i] = current;
        }
    }

    /// <summary>
    /// Ensures the skiplist can accommodate the given level, expanding if necessary.
    /// Updates the update array for new levels.
    /// </summary>
    private void EnsureLevel(int newLevel, Node[] update)
    {
        if (newLevel > _level)
        {
            for (int i = _level + 1; i <= newLevel; i++)
            {
                update[i] = _head;
            }
            _level = newLevel;
        }
    }

    /// <summary>
    /// Links a new node into the skiplist using the update path.
    /// </summary>
    private void LinkNode(Node newNode, Node[] update, int level)
    {
        for (int i = 0; i <= level; i++)
        {
            newNode.Forward[i] = update[i].Forward[i];
            update[i].Forward[i] = newNode;
        }
        _count++;
    }
}