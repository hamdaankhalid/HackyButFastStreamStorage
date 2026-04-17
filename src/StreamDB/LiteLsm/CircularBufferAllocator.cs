using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace StreamDB.LiteLsm;

internal unsafe class CircularBufferAllocator<T> : IDisposable where T : unmanaged
{
  private int _head;
  private int _tail;
  private int _count;
  private bool disposedValue;
  private readonly T* _buffer;
  private readonly int _capacity;

  public int Count => _count;

  public CircularBufferAllocator(int capacity)
  {
    _head = 0;
    _tail = 0;
    _count = 0;
    _capacity = capacity;
    _buffer = (T*)NativeMemory.AllocZeroed((nuint)capacity, (nuint)Unsafe.SizeOf<T>());
  }

  public int FirstLogicalAddress => _head;
  public int LastLogicalAddress => (_tail - 1 + _capacity) % _capacity;
  public int TailAddress => _tail;

  public ref T Dereference(int logicalAddr)
  {
    if (logicalAddr < 0 || logicalAddr >= _capacity)
      throw new ArgumentOutOfRangeException(nameof(logicalAddr), $"Logical address {logicalAddr} is out of bounds for capacity {_capacity}");

    int physicalAddr = logicalAddr % _capacity;
    return ref _buffer[physicalAddr];
  }

  public int TryAllocate(out T* item)
  {
    item = default;
    if (_count >= _capacity)
      return -1;

    int addr = _tail;
    item = &_buffer[_tail];
    _tail = (_tail + 1) % _capacity;
    _count++;
    return addr;
  }

  public void Free(int count)
  {
    if (count > _count)
      throw new ArgumentException($"Cannot free {count} items (only {_count} allocated)");

    _head = (_head + count) % _capacity;
    _count -= count;
  }

  internal T* GetRawPointer(int physicalIndex) => &_buffer[physicalIndex];

  protected virtual void Dispose(bool disposing)
  {
    if (!disposedValue)
    {
      if (disposing)
      {
        // TODO: dispose managed state (managed objects)
      }

      // TODO: free unmanaged resources (unmanaged objects) and override finalizer
      NativeMemory.Free(_buffer);
      // TODO: set large fields to null
      disposedValue = true;
    }
  }

  ~CircularBufferAllocator()
  {
      // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
      Dispose(disposing: false);
  }

  public void Dispose()
  {
    // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    Dispose(disposing: true);
    GC.SuppressFinalize(this);
  }
}
