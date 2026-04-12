namespace StreamDB.LiteLsm;

public interface IMonotonicSkipList<TKey, TValue>
where TKey : unmanaged, IComparable<TKey>
where TValue : unmanaged
{
  int Count { get; }

  void Clear();
  IEnumerable<KeyValuePair<TKey, TValue>> GetAll();
  ReadOnlySpan<byte> GetRawDataLayer();
  void InsertMonotonic(TKey key, TValue value);
  bool TryGetValue(TKey key, out TValue value);
}
