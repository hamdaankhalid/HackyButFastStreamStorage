// Copyright 2025 Hamdaan Khalid
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Concurrent;

namespace StreamDB;

/// <summary>
/// LIMITATION: Can only have one instance of this lock at the moment due to the use of thread-static variables... at some point I will 
/// use a cache line aligned array indexed by thread id to allow multiple instances, but this is sufficient for our use case in StreamDB and avoids the complexity of managing a cache line aligned array.
/// 
/// For cases where we have lots of readers, and only one writer. We want to make sure the reader path is as cheap as possible.
/// In the hotpath you only pay the price of an Interlocked.Increment and Interlocked.Decrement, and the writer will wait until all readers are done before it can acquire the lock. 
/// Once the writer has acquired the lock, all readers will bounce back to using a kernel level lock until the writer releases the lock. This helps readers not churn the CPU while waiting.
/// 
/// 1. If _writerActive is false, then all readers are using the "unsafe" path.
/// 2. If _writerActive is true, then all readers are using the "safe" path, and the writer has acquired the write lock.
/// 
/// Invariant:
/// 1. If the writer is active, then a reader must observe it and use safe path.
/// 2. If a reader is active then writer must observe it and wait until it finishes before acquiring the safe lock.
/// </summary>
public class DeferrableRwLock : IDisposable
{
  private sealed class ThreadSessionCounter
  {
    public int Count;
  }

  // Per-instance registry of per-thread session counters for writer to observe
  private readonly ConcurrentDictionary<int, ThreadSessionCounter> _threadUnsafeSessionRefs = new();

  // Thread local session variables - cached reference to avoid dictionary lookup on each call
  [ThreadStatic]
  private static ThreadSessionCounter? _threadCounter;
  [ThreadStatic]
  private static int _registeredInstanceId;

  private readonly int _instanceId;
  private static int _nextInstanceId;

  // Flag used by writer to signal readers to defer to kernel lock.
  private volatile bool _writerActive;
  
  // Cold path lock
  private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);

  public DeferrableRwLock()
  {
    _instanceId = Interlocked.Increment(ref _nextInstanceId);
  }

  /// <summary>
  /// Returns true if the caller successfully acquired the read lock, 
  /// false if the caller is proceeding without acquiring the lock (because a writer is active). 
  /// The caller must call <see cref="ExitReadLock"/> with the same value to ensure proper cleanup.
  /// </summary>
  /// <returns>Whether the caller acquired a cold path read lock.</returns>
  public bool EnterReadLock()
  {
    EnsureThreadRegistered();

    if (!_writerActive)
    {
      Interlocked.Increment(ref _threadCounter!.Count);
      if (_writerActive)
      {
        Interlocked.Decrement(ref _threadCounter.Count);
        _rwLock.EnterReadLock();
        return true;
      }
      return false;
    }

    _rwLock.EnterReadLock();
    return true;
  }

  /// <summary>
  /// If the caller successfully acquired the a kernel lock in <see cref="EnterReadLock"/>, then this will release it. Else it will decrement the hot path counter
  /// </summary>
  /// <param name="actualLockAcquired"></param>
  public void ExitReadLock(bool actualLockAcquired)
  {
    if (actualLockAcquired)
    {
      _rwLock.ExitReadLock();
    }
    else
    {
      Interlocked.Decrement(ref _threadCounter!.Count);
    }
  }

  /// <summary>
  /// Acquires an exclusive lock for the writer. 
  /// It first sets the _writerActive flag to signal readers to use the safe path, then waits for any active readers in the unsafe path to finish before acquiring the write lock.
  /// </summary>
  public void EnterWriteLock()
  {
    _writerActive = true;

    // Small spin loop to wait for active readers to finish before acquiring the write lock. 
    // This is to avoid unnecessary context switches if the writer comes in while there are still active readers, but they will finish soon.
    while (IsUnsafeSessionActive())
      Thread.Yield();

    _rwLock.EnterWriteLock();
  }

  public void ExitWriteLock()
  {
    _rwLock.ExitWriteLock();
    _writerActive = false;
  }

  private void EnsureThreadRegistered()
  {
    // Check if thread is already registered for this specific instance
    if (_registeredInstanceId == _instanceId && _threadCounter != null)
      return;

    int threadId = Environment.CurrentManagedThreadId;
    _threadCounter = _threadUnsafeSessionRefs.GetOrAdd(threadId, _ => new ThreadSessionCounter());
    _registeredInstanceId = _instanceId;
  }

  private bool IsUnsafeSessionActive()
  {
    foreach (var counter in _threadUnsafeSessionRefs.Values)
    {
      if (Volatile.Read(ref counter.Count) > 0)
        return true;
    }
    return false;
  }
  
  #region IDisposable Support
  private bool _disposedValue;

  protected virtual void Dispose(bool disposing)
  {
    if (!_disposedValue)
    {
      if (disposing)
      {
        _rwLock.Dispose();
      }

      // TODO: free unmanaged resources (unmanaged objects) and override finalizer
      // TODO: set large fields to null
      _disposedValue = true;
    }
  }

  // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
  // ~DeferrableRwLock()
  // {
  //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
  //     Dispose(disposing: false);
  // }

  public void Dispose()
  {
    // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    Dispose(disposing: true);
    GC.SuppressFinalize(this);
  }
  #endregion
}
