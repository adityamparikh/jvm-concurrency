# §4 Dispatchers ↔ Executors

A `CoroutineDispatcher` is literally an executor with coroutine plumbing (`asCoroutineDispatcher()` / `asExecutor()` convert both ways).

| Kotlin dispatcher | Sized for | Java executor analog |
|---|---|---|
| `Dispatchers.Default` | CPU-bound · threads = #cores | `ForkJoinPool.commonPool()` / fixed pool of #cores |
| `Dispatchers.IO` | Blocking I/O · elastic, up to 64 threads (or #cores if larger), created on demand · shares one pool with Default (a blocking-flag is the only distinction) | `newCachedThreadPool()` — or with Loom: `newVirtualThreadPerTaskExecutor()`, which makes the whole distinction obsolete |
| `Dispatchers.IO.limitedParallelism(n)` | A view that **bypasses** the 64 cap — genuinely n threads, additive across views. This is the modern replacement for `newFixedThreadPoolContext(n)` | `Semaphore(n)` around submits, or a fixed pool of n |
| `Dispatchers.Unconfined` | Resume on whatever thread resumed you (tests, edge cases) | Caller-runs / same-thread executor (`Runnable::run`) |
| `Dispatchers.Loom`-style: `Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()` | Run coroutines on VTs — lets blocking calls inside coroutines park cheaply | — (that *is* the Java side) |

> **SIZING:** `Dispatchers.IO`'s 64-thread default is a classic prod ceiling (raise via `kotlinx.coroutines.io.parallelism`). Virtual threads have no pool to exhaust — but your **JDBC pool** becomes the new bottleneck either way. Keep Hikari sized deliberately; a million VTs queueing on 10 connections is still 10-wide.
