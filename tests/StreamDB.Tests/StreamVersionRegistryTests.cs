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

using System.Runtime.InteropServices;
using NUnit.Framework;

namespace StreamDB.Tests;

[TestFixture]
public class StreamVersionRegistryTests
{
    [Test]
    public void Register_And_Deserialize_RoundTrips()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        var payload = new TestPayload { Value = 3.14f, Counter = 42 };
        byte[] bytes = MemoryMarshal.AsBytes(new ReadOnlySpan<TestPayload>(in payload)).ToArray();

        var entry = new StreamEntry(100, 1, 1, bytes);
        TestPayload result = registry.Deserialize<TestPayload>(entry);

        Assert.That(result.Value, Is.EqualTo(3.14f).Within(0.001f));
        Assert.That(result.Counter, Is.EqualTo(42));
    }

    [Test]
    public void CanDeserialize_ReturnsTrueForRegistered()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        byte[] bytes = new byte[8];
        var entry = new StreamEntry(100, 1, 1, bytes);
        Assert.That(registry.CanDeserialize<TestPayload>(entry), Is.True);
    }

    [Test]
    public void CanDeserialize_ReturnsFalseForUnregistered()
    {
        var registry = new StreamVersionRegistry();
        byte[] bytes = new byte[8];
        var entry = new StreamEntry(100, 1, 99, bytes);
        Assert.That(registry.CanDeserialize<TestPayload>(entry), Is.False);
    }

    [Test]
    public void Deserialize_ThrowsForUnregisteredVersion()
    {
        var registry = new StreamVersionRegistry();
        byte[] bytes = new byte[8];
        var entry = new StreamEntry(100, 1, 99, bytes);
        Assert.Throws<InvalidOperationException>(() => registry.Deserialize<TestPayload>(entry));
    }

    [Test]
    public void Deserialize_ThrowsForTooSmallPayload()
    {
        var registry = new StreamVersionRegistry();
        registry.Register<TestPayload>(version: 1);

        byte[] tooSmall = new byte[2]; // TestPayload is 8 bytes
        var entry = new StreamEntry(100, 1, 1, tooSmall);
        Assert.Throws<InvalidOperationException>(() => registry.Deserialize<TestPayload>(entry));
    }
}
