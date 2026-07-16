# §5 Cancellation & timeouts

Both models are **cooperative** — nothing dies mid-instruction. Coroutines check at suspension points; Java threads check at interruption-aware calls. The failure modes are symmetric: a tight CPU loop ignores both.

| Kotlin | Java (threads / VT) | Watch out |
|---|---|---|
| `job.cancel()` → `CancellationException` at next suspension point | `thread.interrupt()` → `InterruptedException` at next blocking call | CPU loops: poll `isActive`/`ensureActive()` ↔ `Thread.interrupted()` |
| `withTimeout(2.seconds) { }` / `withTimeoutOrNull` | `StructuredTaskScope` deadline: `open(joiner, cf -> cf.withTimeout(ofSeconds(2)))` — cancels the whole task tree · `future.get(2, SECONDS)` · `orTimeout()` | `orTimeout` completes the CF but doesn't stop the underlying work |
| `withContext(NonCancellable) { cleanup() }` | `finally` block (interrupt flag survives; re-set it if you swallow) | Never call suspend fns in `finally` without `NonCancellable` — the coroutine is already cancelled |
| `CancellationException` is "normal" — never swallow it in a broad `catch (e: Exception)` without rethrowing | Same sin: `catch (InterruptedException e) {}` — restore via `Thread.currentThread().interrupt()` | Both silently break structured cancellation upstream |

> **THE STS TRAP:** `StructuredTaskScope` cancellation is delivered **as an interrupt**. A subtask that never blocks at an interruptible point — a tight CPU loop, or uninterruptible native I/O — will not notice, and `scope.close()` (the implicit end of the try-with-resources) will **block indefinitely** waiting for it. Same shape as a coroutine that never hits a suspension point, but the consequence is worse: the scope can't exit. In CPU loops, poll `Thread.currentThread().isInterrupted()`.
