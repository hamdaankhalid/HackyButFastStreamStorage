/*
Holds a skiplist and a reference to an AOF on disk.
Skiplist maintains a sparse index of keys in-memory. Whenever flush is called it does 2 things.
1. Enqueue a commit and wait for the commit to be written to the AOF. This ensures durability of the data.
2. Swap the skiplist, flush the previous skiplist to disk and clear it after flushing completes.
*/
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

struct LogEntry<TKey, TValue> : ILogEnqueueEntry where TKey : unmanaged where TValue : unmanaged
{
  public TKey Key;
  public TValue Value;

  public int SerializedLength => GetLength();

  public LogEntry(TKey key, TValue value)
  {
    Key = key;
    Value = value;
  }

  public int GetLength() => Unsafe.SizeOf<TKey>() + Unsafe.SizeOf<TValue>();

  public void Dispose() { }

  public void SerializeTo(Span<byte> dest)
  {
    MemoryMarshal.Write(dest, in Key);
    MemoryMarshal.Write(dest.Slice(Unsafe.SizeOf<TKey>()), in Value);
  }
}
