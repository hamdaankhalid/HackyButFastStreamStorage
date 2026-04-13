using System.Buffers;

namespace StreamDB;

/// <summary>
/// A sized, disposable view over a rented buffer of <see cref="StreamEntry"/> values.
/// Disposing this instance returns the underlying buffer to the pool.
/// </summary>
public readonly struct PooledStreamEntries : IDisposable
{
    private readonly IMemoryOwner<StreamEntry> _owner;

    /// <summary>Number of valid entries in the buffer.</summary>
    public int Count { get; }

    /// <summary>
    /// The valid entries. Backed by pooled memory — do not use after <see cref="Dispose"/>.
    /// </summary>
    public ReadOnlySpan<StreamEntry> Entries => _owner.Memory.Span[..Count];

    internal PooledStreamEntries(IMemoryOwner<StreamEntry> owner, int count)
    {
        _owner = owner;
        Count = count;
    }

    /// <summary>Returns the rented buffer to the pool.</summary>
    public void Dispose() => _owner.Dispose();
}