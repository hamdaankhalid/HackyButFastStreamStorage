using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;

namespace WebServer.Storage;

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
