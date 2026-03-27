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

using FASTER.core;

namespace StreamDB
{
    /// <summary>
    /// A single FasterLog shard shared by multiple secondary indexes.
    /// Entries are assigned to shards via <c>secondaryIndex &amp; ShardMask</c>.
    /// </summary>
    internal sealed class LogShard : IDisposable
    {
        private const string ShardLogFmt = "shard_{0}.log";
        private readonly FasterLog _log;

        public long BeginAddress => _log.BeginAddress;
        public long TailAddress => _log.TailAddress;

        public LogShard(int shardIndex, string baseDir)
        {
            string logPath = Path.Combine(baseDir, shardIndex.ToString(), string.Format(ShardLogFmt, shardIndex));
            FasterLogSettings settings = new FasterLogSettings
            {
                LogDevice = Devices.CreateLogDevice(logPath, deleteOnClose: false),
                TryRecoverLatest = true,
                AutoRefreshSafeTailAddress = true,
            };
            _log = new FasterLog(settings);
        }

        /// <summary>
        /// Write a record with header + payload to the log.
        /// Header: [8B primary_index][4B secondaryIndex][2B version][2B payloadLength]
        /// </summary>
        public long Enqueue(int secondaryIndex, ReadOnlySpan<byte> payload, long primaryIndex, ushort version)
        {
            if (payload.Length > ushort.MaxValue)
                throw new ArgumentException($"Payload size {payload.Length} exceeds the maximum of {ushort.MaxValue} bytes (≈64 KB).", nameof(payload));

            int totalSize = StreamHeader.Size + payload.Length;
            Span<byte> buffer = totalSize <= 256
                ? stackalloc byte[totalSize]
                : new byte[totalSize];

            StreamHeader.Write(buffer, primaryIndex, secondaryIndex, version, (ushort)payload.Length);
            payload.CopyTo(buffer[StreamHeader.Size..]);

            return _log.Enqueue(buffer);
        }

        /// <summary>
        /// Single-index scan. Optimized path that avoids HashSet/Dictionary allocation.
        /// </summary>
        public List<StreamEntry> ScanRange(int secondaryIndex, long fromAddress, long startPrimaryIndex, long endPrimaryIndex, int limit = 0)
        {
            var results = new List<StreamEntry>();

            long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
            long tailAddr = _log.SafeTailAddress;

            if (beginAddr >= tailAddr)
                return results;

            bool hasLimit = limit > 0;

            using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
            while (iter.GetNext(out byte[] entry, out int entryLength, out _))
            {
                if (entryLength < StreamHeader.Size)
                    continue;

                ReadOnlySpan<byte> header = entry.AsSpan();
                int entryIdx = StreamHeader.ReadSecondaryIndex(header);
                if (entryIdx != secondaryIndex)
                    continue;

                long pi = StreamHeader.ReadPrimaryIndex(header);

                if (pi > endPrimaryIndex)
                    break;

                if (pi >= startPrimaryIndex)
                {
                    ushort version = StreamHeader.ReadVersion(header);
                    ushort payloadLen = StreamHeader.ReadPayloadLength(header);
                    byte[] payload = new byte[payloadLen];
                    entry.AsSpan(StreamHeader.Size, payloadLen).CopyTo(payload);

                    results.Add(new StreamEntry(pi, entryIdx, version, payload));

                    if (hasLimit && results.Count >= limit)
                        break;
                }
            }

            return results;
        }

        /// <summary>
        /// Zero-allocation single-index scan. Invokes <paramref name="handler"/> for each matching entry
        /// with a <see cref="StreamEntryView"/> whose payload points directly into FasterLog's buffer.
        /// No <c>byte[]</c> is allocated per entry — the handler must process or copy the payload inline.
        /// </summary>
        public void ScanRange(int secondaryIndex, long fromAddress, long startPrimaryIndex, long endPrimaryIndex, StreamEntryHandler handler)
        {
            long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
            long tailAddr = _log.SafeTailAddress;

            if (beginAddr >= tailAddr)
                return;

            using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
            while (iter.GetNext(out byte[] entry, out int entryLength, out _))
            {
                if (entryLength < StreamHeader.Size)
                    continue;

                ReadOnlySpan<byte> header = entry.AsSpan();
                int entryIdx = StreamHeader.ReadSecondaryIndex(header);
                if (entryIdx != secondaryIndex)
                    continue;

                long pi = StreamHeader.ReadPrimaryIndex(header);

                if (pi > endPrimaryIndex)
                    break;

                if (pi >= startPrimaryIndex)
                {
                    var view = new StreamEntryView
                    {
                        PrimaryIndex = pi,
                        SecondaryIndex = entryIdx,
                        Version = StreamHeader.ReadVersion(header),
                        Payload = entry.AsSpan(StreamHeader.Size, StreamHeader.ReadPayloadLength(header))
                    };

                    if (!handler(in view))
                        break;
                }
            }
        }

        /// <summary>
        /// Scan the shard once from <paramref name="fromAddress"/> and collect entries for all
        /// secondary indexes in <paramref name="indexes"/> whose primary indexes fall within [startPrimaryIndex, endPrimaryIndex].
        /// Primary indexes are read from the header, secondary indexes from the header.
        /// </summary>
        public Dictionary<int, List<StreamEntry>> ScanRange(HashSet<int> indexes, long fromAddress, long startPrimaryIndex, long endPrimaryIndex, int limit = 0)
        {
            Dictionary<int, List<StreamEntry>> results = new Dictionary<int, List<StreamEntry>>(indexes.Count);
            foreach (int id in indexes)
                results[id] = new List<StreamEntry>();

            long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
            long tailAddr = _log.SafeTailAddress;

            if (beginAddr >= tailAddr)
                return results;

            HashSet<int> finished = new HashSet<int>();
            bool hasLimit = limit > 0;

            using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
            while (iter.GetNext(out byte[] entry, out int entryLength, out _))
            {
                if (entryLength < StreamHeader.Size)
                    continue;

                ReadOnlySpan<byte> header = entry.AsSpan();

                // Read secondary index from header for fast-path filtering
                int entryIdx = StreamHeader.ReadSecondaryIndex(header);
                if (!indexes.Contains(entryIdx) || finished.Contains(entryIdx))
                    continue;

                long pi = StreamHeader.ReadPrimaryIndex(header);

                if (pi > endPrimaryIndex)
                {
                    finished.Add(entryIdx);
                    if (finished.Count == indexes.Count)
                        break;
                    continue;
                }

                if (pi >= startPrimaryIndex)
                {
                    ushort version = StreamHeader.ReadVersion(header);
                    ushort payloadLen = StreamHeader.ReadPayloadLength(header);
                    byte[] payload = new byte[payloadLen];
                    entry.AsSpan(StreamHeader.Size, payloadLen).CopyTo(payload);

                    results[entryIdx].Add(new StreamEntry(pi, entryIdx, version, payload));

                    if (hasLimit && results[entryIdx].Count >= limit)
                    {
                        finished.Add(entryIdx);
                        if (finished.Count == indexes.Count)
                            break;
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Scan the shard from <paramref name="fromAddress"/> and collect entries for all secondary indexes
        /// whose primary indexes fall within [startPrimaryIndex, endPrimaryIndex]. Lazily creates result lists.
        /// </summary>
        public Dictionary<int, List<StreamEntry>> ScanRangeAll(long fromAddress, long startPrimaryIndex, long endPrimaryIndex, int limit = 0)
        {
            Dictionary<int, List<StreamEntry>> results = new Dictionary<int, List<StreamEntry>>();
            long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
            long tailAddr = _log.SafeTailAddress;

            if (beginAddr >= tailAddr)
                return results;

            HashSet<int> finished = new HashSet<int>();
            bool hasLimit = limit > 0;

            using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
            while (iter.GetNext(out byte[] entry, out int entryLength, out _))
            {
                if (entryLength < StreamHeader.Size)
                    continue;

                ReadOnlySpan<byte> header = entry.AsSpan();
                int entryIdx = StreamHeader.ReadSecondaryIndex(header);
                if (finished.Contains(entryIdx))
                    continue;

                long pi = StreamHeader.ReadPrimaryIndex(header);

                if (pi > endPrimaryIndex)
                {
                    finished.Add(entryIdx);
                    continue;
                }

                if (pi >= startPrimaryIndex)
                {
                    if (!results.ContainsKey(entryIdx))
                        results[entryIdx] = new List<StreamEntry>();

                    ushort version = StreamHeader.ReadVersion(header);
                    ushort payloadLen = StreamHeader.ReadPayloadLength(header);
                    byte[] payload = new byte[payloadLen];
                    entry.AsSpan(StreamHeader.Size, payloadLen).CopyTo(payload);

                    results[entryIdx].Add(new StreamEntry(pi, entryIdx, version, payload));

                    if (hasLimit && results[entryIdx].Count >= limit)
                    {
                        finished.Add(entryIdx);
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Scan the shard and find the first entry for any index in <paramref name="indexes"/>
        /// whose primary index is ≥ <paramref name="fromPrimaryIndex"/>. Returns null if none found.
        /// </summary>
        public long? FindFirstPrimaryIndex(HashSet<int> indexes, long fromAddress, long fromPrimaryIndex)
        {
            long beginAddr = Math.Max(fromAddress, _log.BeginAddress);
            long tailAddr = _log.SafeTailAddress;

            if (beginAddr >= tailAddr)
                return null;

            using FasterLogScanIterator iter = _log.Scan(beginAddr, tailAddr, scanUncommitted: true);
            while (iter.GetNext(out byte[] entry, out int entryLength, out _))
            {
                if (entryLength < StreamHeader.Size)
                    continue;

                ReadOnlySpan<byte> header = entry.AsSpan();
                int entryIdx = StreamHeader.ReadSecondaryIndex(header);
                if (!indexes.Contains(entryIdx))
                    continue;

                long pi = StreamHeader.ReadPrimaryIndex(header);
                if (pi >= fromPrimaryIndex)
                    return pi;
            }

            return null;
        }

        /// <summary>
        /// Commit all pending writes to disk and wait until the specified address is durable.
        /// </summary>
        public ValueTask CommitAndWait(long untilAddress)
        {
            _log.Commit(spinWait: false);
            return _log.WaitForCommitAsync(untilAddress);
        }

        /// <summary>
        /// Truncate the log up to the given address, freeing disk space for old entries.
        /// </summary>
        public void TruncateUntil(long untilAddress) => _log.TruncateUntil(untilAddress);

        public void Dispose()
        {
            _log.Commit(true);
            _log.Dispose();
        }
    }
}
