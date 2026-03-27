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

namespace StreamDB
{
    /// <summary>
    /// Primary abstraction for StreamDB — a schema-on-read stream storage engine.
    /// Program against this interface to enable dependency injection and unit testing
    /// without requiring real FasterLog/SQLite infrastructure.
    /// </summary>
    public interface IStreamDb : IDisposable
    {
        /// <summary>
        /// Append a record to the stream with the given primary index, secondary index,
        /// schema version, and raw payload bytes. Late-arriving entries (out-of-order
        /// primary indexes) are handled transparently.
        /// </summary>
        void Append(long primaryIndex, int secondaryIndex, ushort version, ReadOnlySpan<byte> payload);

        /// <summary>
        /// Block until all pending sparse-index entries have been flushed to SQLite.
        /// Useful for deterministic reads in tests and demos.
        /// </summary>
        void WaitForPendingWrites();

        /// <summary>
        /// Get statistics about adaptive tuning, index queue behavior, and late arrivals.
        /// </summary>
        StreamDbStats GetStats();

        /// <summary>
        /// Read entries for a single secondary index whose primary indexes fall within
        /// [<paramref name="startPrimaryIndex"/>, <paramref name="endPrimaryIndex"/>].
        /// Results are ordered by primary index and merged with any late arrivals.
        /// </summary>
        List<StreamEntry> ReadRange(int secondaryIndex, long startPrimaryIndex, long endPrimaryIndex, int limit = 0);

        /// <summary>
        /// Read entries for multiple secondary indexes whose primary indexes fall within
        /// the specified range. Groups indexes by shard to minimize redundant scanning.
        /// </summary>
        Dictionary<int, List<StreamEntry>> ReadRange(IEnumerable<int> secondaryIndexes, long startPrimaryIndex, long endPrimaryIndex, int limit = 0);

        /// <summary>
        /// Read entries for all secondary indexes whose primary indexes fall within
        /// the specified range. Scans all shards.
        /// </summary>
        Dictionary<int, List<StreamEntry>> ReadRange(long startPrimaryIndex, long endPrimaryIndex, int limit = 0);

        /// <summary>
        /// Returns the minimum earliest primary index ≥ <paramref name="fromPrimaryIndex"/>
        /// across the requested secondary indexes, checking both FasterLog and late arrivals.
        /// Returns null if no data exists.
        /// </summary>
        long? GetEarliestPrimaryIndex(IEnumerable<int> secondaryIndexes, long fromPrimaryIndex);

        /// <summary>
        /// Combined availability-check and range-read. Finds the earliest available entry
        /// ≥ <paramref name="fromPrimaryIndex"/>, then reads a window of data from that point.
        /// Returns <c>RangeEnd = -1</c> when no data exists.
        /// </summary>
        (long RangeEnd, Dictionary<int, List<StreamEntry>> Data) ReadRangeFromAvailable(IEnumerable<int> secondaryIndexes, long fromPrimaryIndex, long window, int limit = 0);

        /// <summary>
        /// Returns the single latest record for each secondary index that has data in the stream.
        /// Merges FasterLog and late arrivals, keeping whichever has the higher primary index.
        /// </summary>
        Dictionary<int, StreamEntry> ReadLatest();
    }
}
