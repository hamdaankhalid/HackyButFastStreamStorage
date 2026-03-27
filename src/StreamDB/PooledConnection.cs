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

using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;

namespace StreamDB;

/// <summary>
/// A lightweight wrapper that returns a <see cref="SqliteConnection"/> to a
/// <see cref="ConcurrentQueue{T}"/> pool on <see cref="Dispose"/> instead of closing it.
/// Shared by all SQLite-backed stores (<see cref="SqliteMetadataStore"/>,
/// <see cref="StreamDB"/>, etc.) to avoid duplicating the pool-return pattern.
/// </summary>
internal readonly struct PooledConnection : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly ConcurrentQueue<SqliteConnection> _pool;

    public PooledConnection(SqliteConnection connection, ConcurrentQueue<SqliteConnection> pool)
    {
        _connection = connection;
        _pool = pool;
    }

    public SqliteConnection Connection => _connection;
    public void Dispose() => _pool.Enqueue(_connection);
}
