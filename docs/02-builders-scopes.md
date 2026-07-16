# §2 Builders & scopes

You can't call a suspend fn from nowhere — a builder creates the coroutine and ties it to a `CoroutineScope` whose `Job` owns its lifecycle. Java's counterpart is an `ExecutorService` — except an executor doesn't propagate cancellation or failure between tasks; that's §3.

| Kotlin | Semantics | Java equivalent |
|---|---|---|
| `runBlocking { }` | Bridge sync→suspend: installs an event loop and *runs* coroutines on the caller thread until done (not merely a park) | Nearest is `CompletableFuture...join()`, but that only parks — it doesn't run a scheduler. No exact analog |
| `scope.launch { }` | Fire-and-manage; returns `Job`, no value | `executor.submit(runnable)` → `Future<?>` |
| `scope.async { }` | Concurrent value; returns `Deferred<T>`, get via `.await()` | `executor.submit(callable)` → `Future<T>.get()`, or `CompletableFuture.supplyAsync` |
| `coroutineScope { }` | Suspend fn that waits for all children; any failure cancels the siblings and rethrows | `StructuredTaskScope.open()` (default all-or-fail policy) — see §3 |
| `supervisorScope { }` | Child failure is isolated — siblings keep running; the scope still joins them | No clean analog: `StructuredTaskScope` (STS) couples the failure policy to the `Joiner`. Closest is a custom `Joiner` that records failures without cancelling (cf. the JEP 525 partial-collector), *not* `awaitAll()` |
| `withContext(ctx) { }` | Switch dispatcher/context for a block, sequential | No direct analog — VT code doesn't switch threads; nearest: submit to another executor and join |
| `GlobalScope.launch` | Unstructured, app-lifetime coroutine — **avoid** | `new Thread(...).start()`, or `@Async` with no configured executor — orphaned work nobody joins or cancels |

> **SPRING:** In Spring, don't hand-roll scopes per request: WebFlux and WebMVC (Framework 6.x) both accept `suspend fun` controller methods and manage the scope for you. For app-lifecycle scopes, create one `CoroutineScope(SupervisorJob() + Dispatchers.Default)` bean and cancel it in `@PreDestroy` — the analog of a Spring-managed `ExecutorService` with `shutdown()`.
