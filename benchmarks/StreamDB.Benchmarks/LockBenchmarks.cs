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

using BenchmarkDotNet.Attributes;
using StreamDB;

[MemoryDiagnoser]
[SimpleJob(iterationCount: 5, warmupCount: 2)]
public class LockBenchmarks
{
    private ReaderWriterLockSlim _rwLockSlim = null!;
    private DeferrableRwLock _deferrableRwLock = null!;

    [Params(1, 4, 8, 16, 32)]
    public int ReaderThreads { get; set; }

    private const int ReadsPerThread = 100_000;

    [IterationSetup]
    public void IterationSetup()
    {
        _rwLockSlim = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        _deferrableRwLock = new DeferrableRwLock();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _rwLockSlim?.Dispose();
        _deferrableRwLock?.Dispose();
    }

    // ── Read-only (no writer contention) ──────────────────────────

    [Benchmark(Baseline = true)]
    public void ReaderWriterLockSlim_ReadOnly()
    {
        Parallel.For(0, ReaderThreads, _ =>
        {
            for (int i = 0; i < ReadsPerThread; i++)
            {
                _rwLockSlim.EnterReadLock();
                _rwLockSlim.ExitReadLock();
            }
        });
    }

    [Benchmark]
    public void DeferrableRwLock_ReadOnly()
    {
        Parallel.For(0, ReaderThreads, _ =>
        {
            for (int i = 0; i < ReadsPerThread; i++)
            {
                bool acquired = _deferrableRwLock.EnterReadLock();
                _deferrableRwLock.ExitReadLock(acquired);
            }
        });
    }

    // ── Read-heavy with rare writer (1 write per 10,000 reads) ───

    [Benchmark]
    public void ReaderWriterLockSlim_ReadHeavyRareWriter()
    {
        int writerDone = 0;
        var writerTask = Task.Run(() =>
        {
            for (int w = 0; w < ReadsPerThread / 10_000; w++)
            {
                _rwLockSlim.EnterWriteLock();
                Thread.SpinWait(10);
                _rwLockSlim.ExitWriteLock();
                Thread.Sleep(1);
            }
            Volatile.Write(ref writerDone, 1);
        });

        Parallel.For(0, ReaderThreads, _ =>
        {
            for (int i = 0; i < ReadsPerThread; i++)
            {
                _rwLockSlim.EnterReadLock();
                _rwLockSlim.ExitReadLock();
            }
        });

        writerTask.Wait();
    }

    [Benchmark]
    public void DeferrableRwLock_ReadHeavyRareWriter()
    {
        int writerDone = 0;
        var writerTask = Task.Run(() =>
        {
            for (int w = 0; w < ReadsPerThread / 10_000; w++)
            {
                _deferrableRwLock.EnterWriteLock();
                Thread.SpinWait(10);
                _deferrableRwLock.ExitWriteLock();
                Thread.Sleep(1);
            }
            Volatile.Write(ref writerDone, 1);
        });

        Parallel.For(0, ReaderThreads, _ =>
        {
            for (int i = 0; i < ReadsPerThread; i++)
            {
                bool acquired = _deferrableRwLock.EnterReadLock();
                _deferrableRwLock.ExitReadLock(acquired);
            }
        });

        writerTask.Wait();
    }
}
