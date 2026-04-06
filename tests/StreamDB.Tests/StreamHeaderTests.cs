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

using NUnit.Framework;

namespace StreamDB.Tests;

[TestFixture]
public class StreamHeaderTests
{
    [Test]
    public void Size_Is16Bytes()
    {
        Assert.That(StreamHeader.Size, Is.EqualTo(16));
    }

    [Test]
    public void WriteAndRead_RoundTrips()
    {
        Span<byte> buffer = stackalloc byte[StreamHeader.Size];
        long ts = 1234567890L;
        int secondaryIdx = 42;
        ushort version = 3;
        ushort payloadLen = 128;

        StreamHeader.Write(buffer, ts, secondaryIdx, version, payloadLen);

        Assert.That(StreamHeader.ReadPrimaryIndex(buffer), Is.EqualTo(ts));
        Assert.That(StreamHeader.ReadSecondaryIndex(buffer), Is.EqualTo(secondaryIdx));
        Assert.That(StreamHeader.ReadVersion(buffer), Is.EqualTo(version));
        Assert.That(StreamHeader.ReadPayloadLength(buffer), Is.EqualTo(payloadLen));
    }

    [Test]
    public void WriteAndRead_NegativePrimaryIndex()
    {
        Span<byte> buffer = stackalloc byte[StreamHeader.Size];
        StreamHeader.Write(buffer, -999L, 0, 1, 0);
        Assert.That(StreamHeader.ReadPrimaryIndex(buffer), Is.EqualTo(-999L));
    }

    [Test]
    public void WriteAndRead_MaxValues()
    {
        Span<byte> buffer = stackalloc byte[StreamHeader.Size];
        StreamHeader.Write(buffer, long.MaxValue, int.MaxValue, ushort.MaxValue, ushort.MaxValue);

        Assert.That(StreamHeader.ReadPrimaryIndex(buffer), Is.EqualTo(long.MaxValue));
        Assert.That(StreamHeader.ReadSecondaryIndex(buffer), Is.EqualTo(int.MaxValue));
        Assert.That(StreamHeader.ReadVersion(buffer), Is.EqualTo(ushort.MaxValue));
        Assert.That(StreamHeader.ReadPayloadLength(buffer), Is.EqualTo(ushort.MaxValue));
    }
}
