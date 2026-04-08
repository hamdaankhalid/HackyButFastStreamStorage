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
public class StreamDBDisposeTests
{
    [Test]
    public void Append_AfterDispose_Throws()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        var db = new StreamDB(baseDir: dataDir, initialAdaptiveIdx: 0);
        db.Dispose();

        try
        {
            byte[] payloadBytes = new byte[8];
            Assert.Throws<ObjectDisposedException>(() => db.Append(100, 1, 1, payloadBytes));
        }
        finally
        {
            if (Directory.Exists(dataDir))
                Directory.Delete(dataDir, recursive: true);
        }
    }

    [Test]
    public void ReadRange_AfterDispose_Throws()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        var db = new StreamDB(baseDir: dataDir, initialAdaptiveIdx: 0);
        db.Dispose();

        try
        {
            Assert.Throws<ObjectDisposedException>(() => db.ReadRange(1, 0, 100));
        }
        finally
        {
            if (Directory.Exists(dataDir))
                Directory.Delete(dataDir, recursive: true);
        }
    }

    [Test]
    public void DoubleDispose_DoesNotThrow()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"streamdb-test-{Guid.NewGuid():N}");
        var db = new StreamDB(baseDir: dataDir, initialAdaptiveIdx: 0);
        try
        {
            db.Dispose();
            Assert.DoesNotThrow(() => db.Dispose());
        }
        finally
        {
            if (Directory.Exists(dataDir))
                Directory.Delete(dataDir, recursive: true);
        }
    }
}
