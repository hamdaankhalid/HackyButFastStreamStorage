using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace WebServer.Storage
{
    /// <summary>
    /// Caller-side utility for deserializing <see cref="StreamEntry"/> payloads into typed structs.
    /// Each version maps to an expected payload size. The caller knows which type to deserialize
    /// at their call site and uses this registry for validation and convenience.
    ///
    /// <example>
    /// Setup (once at startup):
    /// <code>
    /// var registry = new StreamVersionRegistry();
    /// registry.Register&lt;GpsPayload&gt;(StreamVersions.GpsV1);
    /// // Later when GpsPayload evolves:
    /// registry.Register&lt;GpsPayloadV2&gt;(2);
    /// </code>
    ///
    /// Deserializing entries:
    /// <code>
    /// List&lt;StreamEntry&gt; entries = streamDb.ReadRange(deviceId, startTs, endTs);
    /// foreach (var entry in entries)
    /// {
    ///     if (registry.CanDeserialize&lt;GpsPayloadV2&gt;(entry))
    ///     {
    ///         var gps = registry.Deserialize&lt;GpsPayloadV2&gt;(entry);
    ///     }
    ///     else
    ///     {
    ///         var gps = registry.Deserialize&lt;GpsPayload&gt;(entry);
    ///     }
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public sealed class StreamVersionRegistry
    {
        private readonly Dictionary<ushort, int> _versionSizes = new();

        /// <summary>
        /// Register a version with its expected payload struct type.
        /// The payload size is inferred from <c>Unsafe.SizeOf&lt;T&gt;()</c>.
        /// </summary>
        public void Register<T>(ushort version) where T : unmanaged
        {
            _versionSizes[version] = Unsafe.SizeOf<T>();
        }

        /// <summary>
        /// Deserialize a <see cref="StreamEntry"/> payload into the expected struct type.
        /// Throws if the version is not registered or the payload size doesn't match.
        /// </summary>
        public T Deserialize<T>(in StreamEntry entry) where T : unmanaged
        {
            if (!_versionSizes.TryGetValue(entry.Version, out int expectedSize))
                throw new InvalidOperationException($"Version {entry.Version} is not registered.");

            if (entry.Payload.Length < expectedSize)
                throw new InvalidOperationException(
                    $"Payload size {entry.Payload.Length} is smaller than expected {expectedSize} for version {entry.Version}.");

            return MemoryMarshal.Read<T>(entry.Payload.AsSpan(0, expectedSize));
        }

        /// <summary>
        /// Check whether the entry's version is registered and the payload is large enough to deserialize.
        /// </summary>
        public bool CanDeserialize<T>(in StreamEntry entry) where T : unmanaged
        {
            return _versionSizes.TryGetValue(entry.Version, out int expectedSize)
                   && entry.Payload.Length >= expectedSize;
        }
    }
}
