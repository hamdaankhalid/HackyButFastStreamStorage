namespace StreamDB;

/// <summary>
/// Common binary search utilities for sorted data structures.
/// </summary>
public static class BinarySearchHelper
{
    /// <summary>
    /// Binary search to find an exact value in a sorted collection.
    /// </summary>
    /// <typeparam name="T">The type of elements, must be comparable</typeparam>
    /// <param name="getValue">Function to get value at a given index</param>
    /// <param name="target">The target value to search for</param>
    /// <param name="startIndex">The starting index (inclusive)</param>
    /// <param name="endIndex">The ending index (inclusive)</param>
    /// <returns>The index where the target is found, or -1 if not found</returns>
    public static int BinarySearch<T>(Func<int, T> getValue, T target, int startIndex, int endIndex) 
        where T : IComparable<T>
    {
        while (startIndex <= endIndex)
        {
            int mid = startIndex + (endIndex - startIndex) / 2;
            T midValue = getValue(mid);
            
            int comparison = midValue.CompareTo(target);
            
            if (comparison == 0)
            {
                return mid;
            }
            else if (comparison < 0)
            {
                startIndex = mid + 1;
            }
            else
            {
                endIndex = mid - 1;
            }
        }
        
        return -1;
    }

    /// <summary>
    /// Binary search to find the first index where the value is >= target.
    /// </summary>
    /// <typeparam name="T">The type of elements, must be comparable</typeparam>
    /// <param name="getValue">Function to get value at a given index</param>
    /// <param name="target">The target value to search for</param>
    /// <param name="startIndex">The starting index (inclusive)</param>
    /// <param name="endIndex">The ending index (inclusive)</param>
    /// <param name="count">Total number of elements</param>
    /// <returns>The first index where value >= target, or count if all values are less than target</returns>
    public static int LowerBound<T>(Func<int, T> getValue, T target, int startIndex, int endIndex, int count) 
        where T : IComparable<T>
    {
        int result = count;
        
        while (startIndex <= endIndex)
        {
            int mid = startIndex + (endIndex - startIndex) / 2;
            T midValue = getValue(mid);
            
            if (midValue.CompareTo(target) >= 0)
            {
                result = mid;
                endIndex = mid - 1;
            }
            else
            {
                startIndex = mid + 1;
            }
        }
        
        return result;
    }

    /// <summary>
    /// Binary search to find the first index where the value is > target.
    /// </summary>
    /// <typeparam name="T">The type of elements, must be comparable</typeparam>
    /// <param name="getValue">Function to get value at a given index</param>
    /// <param name="target">The target value to search for</param>
    /// <param name="startIndex">The starting index (inclusive)</param>
    /// <param name="endIndex">The ending index (inclusive)</param>
    /// <param name="count">Total number of elements</param>
    /// <returns>The first index where value > target, or count if all values are <= target</returns>
    public static int UpperBound<T>(Func<int, T> getValue, T target, int startIndex, int endIndex, int count) 
        where T : IComparable<T>
    {
        int result = count;
        
        while (startIndex <= endIndex)
        {
            int mid = startIndex + (endIndex - startIndex) / 2;
            T midValue = getValue(mid);
            
            if (midValue.CompareTo(target) > 0)
            {
                result = mid;
                endIndex = mid - 1;
            }
            else
            {
                startIndex = mid + 1;
            }
        }
        
        return result;
    }
}
