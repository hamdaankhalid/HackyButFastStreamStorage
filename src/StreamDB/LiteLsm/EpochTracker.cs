namespace StreamDB.LiteLsm;

/// <summary>
/// Epoch-based reader tracking for single-writer, multi-reader concurrency.
/// Readers call Enter/Exit (one Interlocked op each, no locks).
/// The writer calls AdvanceAndDrain before reclaiming resources (eviction, truncation)
/// to ensure no reader is using stale data.
/// </summary>
internal sealed class EpochTracker
{
  // Two counters, alternating by epoch parity. Readers increment the counter
  // matching the epoch they entered. The writer advances the epoch and waits
  // for the OLD counter to reach zero.
  private readonly int[] _activeReaders = new int[2];
  private long _currentEpoch;

  /// <summary>
  /// Called by a reader before accessing shared data. Returns an epoch token
  /// that must be passed to Exit.
  /// </summary>
  public long Enter()
  {
    long epoch = Volatile.Read(ref _currentEpoch);
    Interlocked.Increment(ref _activeReaders[epoch & 1]);

    // Double-check: if epoch advanced between read and increment, we incremented
    // the wrong slot. Fix up.
    long currentEpoch = Volatile.Read(ref _currentEpoch);
    if (currentEpoch != epoch)
    {
      Interlocked.Decrement(ref _activeReaders[epoch & 1]);
      epoch = currentEpoch;
      Interlocked.Increment(ref _activeReaders[epoch & 1]);
    }

    return epoch;
  }

  /// <summary>
  /// Called by a reader after finishing access to shared data.
  /// </summary>
  public void Exit(long epochToken)
  {
    Interlocked.Decrement(ref _activeReaders[epochToken & 1]);
  }

  /// <summary>
  /// Called by the writer before reclaiming resources. Advances the epoch
  /// and waits until all readers from the previous epoch have exited.
  /// After this returns, no reader holds references to data from before the advance.
  /// </summary>
  public void AdvanceAndDrain()
  {
    long oldEpoch = Volatile.Read(ref _currentEpoch);
    Interlocked.Increment(ref _currentEpoch);

    // Wait for all readers in the old epoch to finish
    SpinWait spin = default;
    while (Volatile.Read(ref _activeReaders[oldEpoch & 1]) > 0)
    {
      spin.SpinOnce();
    }
  }
}
