namespace StreamDB.LiteLsm;

public interface ILiteLsm<TKey, TValue>
where TKey : unmanaged, IComparable<TKey>
where TValue : unmanaged
{
  /// <summary> Create a reusable iterator for scanning a range of keys </summary>
  LiteLsmIterator<TKey, TValue> GetIterator(TKey fromKey, TKey endKey, int batchSize = 256);
  /// <summary> Get current stats about the skiplist and segment files </summary>
  LsmStats GetStats();
  /// <summary> O(1) </summary>
  void Put(TKey key, TValue value);
  /// <summary> Delete a key. Returns true if found and deleted. </summary>
  bool Delete(in TKey key);
  /// <summary> Truncate data before the specified key </summary>
  int Truncate(TKey beforeKey);
  /// <summary> Try to get the value associated with the specified key </summary>
  bool TryGet(in TKey key, out TValue value);
}
