namespace StreamDB.LiteLsm;

public interface IMonotonicSkipList<TKey, TValue> where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged
{
    int Count { get; }
    //bool TryGetMinKey(out TKey key);
    //bool TryGetMaxKey(out TKey key);
    bool ProbablyContainsKey(TKey key);
    void Clear();
    IEnumerable<KeyValuePair<TKey, TValue>> GetAll();
    ReadOnlySpan<byte> GetRawDataLayer();
    void InsertMonotonic(TKey key, TValue value);
    bool TryGetValue(TKey key, out TValue value);
    (bool, int) MemCpyRawRange(TKey fromKey, TKey endKey, Span<(TKey, TValue)> buffer, bool excludeStart = false);
}
