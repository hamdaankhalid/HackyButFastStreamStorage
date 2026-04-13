using System.Buffers;

namespace StreamDB.LiteLsm;

public sealed class LiteLsmIterator<TKey, TValue> : IDisposable where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
{
    private const int DEFAULT_BATCH_SIZE = 256;
    private readonly LiteLsm<TKey, TValue> _storage;

    private readonly TKey _endKey;

    private TKey _readFrom;

    private bool _hasMoreBatchesToRead = true;
    private readonly int _batchSize;
    private bool _isSubsequentBatch = false;

    private readonly (TKey, TValue)[] _bufferedBatch;
    private int _bufferedBatchCount = 0;
    private int _bufferedBatchOffset = 0;

    private bool IsBufferExhausted() => _bufferedBatchOffset >= _bufferedBatchCount;

    public LiteLsmIterator(LiteLsm<TKey, TValue> storage, TKey fromKey, TKey endKey, int batchSize)
    {
        _storage = storage;
        _endKey = endKey;
        _readFrom = fromKey;
        _batchSize = batchSize;
        _bufferedBatch = ArrayPool<(TKey, TValue)>.Shared.Rent(batchSize);
    }

    /// <summary>
    /// Yields all key-value pairs in the range [fromKey, endKey] in sorted order.
    /// Reads in batches to minimize memory usage.
    /// </summary>
    public IEnumerable<(TKey Key, TValue Value)> ReadAll()
    {
        while (true)
        {
            if (IsBufferExhausted())
            {
                if (!_hasMoreBatchesToRead)
                    yield break;

                ReadNextBatch();

                if (IsBufferExhausted())
                    yield break;
            }

            var (key, value) = _bufferedBatch[_bufferedBatchOffset++];
            yield return (key, value);
        }
    }

    private void ReadNextBatch()
    {
        Array.Clear(_bufferedBatch, 0, _bufferedBatchCount);
        _bufferedBatchOffset = 0;
        _bufferedBatchCount = 0;

        var fullBuffer = new Span<(TKey, TValue)>(_bufferedBatch, 0, _batchSize);
        (_hasMoreBatchesToRead, _bufferedBatchCount) = _storage.ReadBatchInto(_readFrom, _endKey, fullBuffer, excludeStart: _isSubsequentBatch);

        // Advance _readFrom to last key for next batch's starting point
        if (_bufferedBatchCount > 0)
        {
            _readFrom = _bufferedBatch[_bufferedBatchCount - 1].Item1;
            _isSubsequentBatch = true;
        }
    }

    public void Dispose() => ArrayPool<(TKey, TValue)>.Shared.Return(_bufferedBatch);
}