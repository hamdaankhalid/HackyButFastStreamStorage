using System.Runtime.InteropServices;

namespace StreamDB
{
    /// <summary>
    /// Header layout for records stored in FasterLog.
    /// <code>
    /// ┌──────────────┬────────────────┬──────────────┬─────────────────┬──────────────────┐
    /// │ 8B: long     │ 4B: int        │ 2B: ushort   │ 2B: ushort      │ N bytes: payload │
    /// │ primary_idx  │ secondary_idx  │ version      │ payload length  │ (opaque bytes)   │
    /// │ PRIMARY IDX  │ SECONDARY IDX  │              │                 │                  │
    /// └──────────────┴────────────────┴──────────────┴─────────────────┴──────────────────┘
    /// </code>
    /// </summary>
    public static class StreamHeader
    {
        /// <summary>Total header size in bytes: primaryIndex(8) + secondaryIndex(4) + version(2) + payloadLength(2) = 16.</summary>
        public const int Size = 16;

        public const int PrimaryIndexOffset = 0;
        public const int SecondaryIndexOffset = 8;
        public const int VersionOffset = 12;
        public const int PayloadLengthOffset = 14;

        /// <summary>
        /// Writes the header fields into the target span at the appropriate offsets.
        /// The caller must ensure <paramref name="target"/> has at least <see cref="Size"/> bytes.
        /// </summary>
        public static void Write(Span<byte> target, long primaryIndex, int secondaryIndex, ushort version, ushort payloadLength)
        {
            MemoryMarshal.Write(target[PrimaryIndexOffset..], in primaryIndex);
            MemoryMarshal.Write(target[SecondaryIndexOffset..], in secondaryIndex);
            MemoryMarshal.Write(target[VersionOffset..], in version);
            MemoryMarshal.Write(target[PayloadLengthOffset..], in payloadLength);
        }

        /// <summary>Reads the primary index from the header.</summary>
        public static long ReadPrimaryIndex(ReadOnlySpan<byte> header) =>
            MemoryMarshal.Read<long>(header[PrimaryIndexOffset..]);

        /// <summary>Reads the secondary index from the header.</summary>
        public static int ReadSecondaryIndex(ReadOnlySpan<byte> header) =>
            MemoryMarshal.Read<int>(header[SecondaryIndexOffset..]);

        /// <summary>Reads the schema version from the header.</summary>
        public static ushort ReadVersion(ReadOnlySpan<byte> header) =>
            MemoryMarshal.Read<ushort>(header[VersionOffset..]);

        /// <summary>Reads the payload length from the header.</summary>
        public static ushort ReadPayloadLength(ReadOnlySpan<byte> header) =>
            MemoryMarshal.Read<ushort>(header[PayloadLengthOffset..]);
    }

    /// <summary>
    /// A single entry read from a StreamDB. Contains header metadata and raw payload bytes.
    /// Callers deserialize the payload using the version field to select the appropriate struct type.
    /// </summary>
    public readonly record struct StreamEntry(
        /// <summary>Primary index — monotonic key used for range queries and ordering (e.g. timestamp, sequence number).</summary>
        long PrimaryIndex,
        /// <summary>Secondary index — used for sharding and filtering (e.g., device ID, sensor ID, user ID).</summary>
        int SecondaryIndex,
        /// <summary>Schema version of the payload, enabling format evolution.</summary>
        ushort Version,
        /// <summary>Raw payload bytes (header excluded). Deserialize according to Version.</summary>
        byte[] Payload
    );
}
