# §11 Server pipeline patterns

Six patterns for high-throughput request pipelines — the shapes behind streaming APIs and fan-out backends (as catalogued in Bowen Feng's *"Concurrency Patterns for Modern High Performance Kotlin Servers"*) — each with its Java translation. They compose: a real endpoint is typically a generator over a fan-in, with hedged calls, timeouts at two granularities, and backpressure at the edge.

| Pattern | Problem it solves | Kotlin | Java |
|---|---|---|---|
| **Generator** | Cut time-to-first-byte: emit results as they're ready, not when everything is done | function returning `Flow` | `Flux` / SSE; MVC: `SseEmitter` on a VT |
| **Fan-in** | Run N independent calls in parallel, merge into one stream | producers → one `Channel`; or `merge()` / `flatMapMerge` | VTs → one `BlockingQueue`; or `Flux.merge` |
| **Ordering** | Concurrent results arrive out of order | server-side: `awaitAll()` · client-side: sequence numbers | ordered `fork` list + `join` · same seq-number scheme |
| **Timeout control** | A slow backend must not hang the request | `withTimeout`, coarse and fine | scope deadline (`StructuredTaskScope`) · `orTimeout` · Reactor `.timeout` |
| **Hedging** | Tail latency: p99 dominated by one slow replica | race two `async`, first wins, loser cancelled | `Joiner.anySuccessfulResultOrThrow()` (JDK 26: `anySuccessfulOrThrow`) · `Mono.firstWithValue` |
| **Backpressure** | A slow client must be able to slow the server | built into `Flow`; `buffer(n)` to stay ahead | Reactive-Streams `request(n)`; MVC+VT: the blocking socket write *is* the backpressure |

## §11.1Generator + fan-in — the streaming aggregator

> **THE GOAL:** An endpoint calls three independent backends (or tools, or RPCs) and streams each result to the client **the moment it arrives**. Total latency ≈ max(backends), not sum(backends) — and the client's time-to-first-byte ≈ min(backends).

#### KOTLIN · channelFlow (fan-in) returned as a Flow (generator) — see §7.0

```kotlin
// GENERATOR: the return type is a Flow — the framework streams it
// (SSE / chunked) instead of buffering a full response.
fun answers(q: Query): Flow<Part> = channelFlow {
    // FAN-IN: three child coroutines send into ONE implicit channel.
    launch { send(search(q))  }        // each send() happens as soon as
    launch { send(ratings(q)) }        //   that backend responds —
    launch { send(news(q))    }        //   arrival order, not call order
}   // channelFlow closes when all three children complete (structured!)

// Same shape with operators, when the sources are already Flows:
val merged: Flow<Part> = merge(searchF, ratingsF, newsF)
// flatMapMerge = fan-in over a DYNAMIC set of inner flows (§7.1);
// flatMapConcat = same, but strictly sequential emission.
```

#### JAVA · MVC + virtual threads

```java
@GetMapping("/answers")
SseEmitter answers(Query q) {
    var sse  = new SseEmitter(30_000L);
    var exec = Executors.newVirtualThreadPerTaskExecutor();
    var left = new AtomicInteger(3);
    var done = new AtomicBoolean(false);   // single-completion guard

    // MUST shut the executor down, or you leak one per request.
    // Can't use try-with-resources: close() blocks; return now.
    sse.onCompletion(exec::shutdownNow);   // fires on complete OR error
    sse.onTimeout(() -> finish(sse, done, null));

    for (Supplier<Part> call :
            List.of(() -> search(q), () -> ratings(q), () -> news(q))) {
        exec.submit(() -> {              // FAN-IN: 3 VTs, one emitter
            try {
                sse.send(call.get());        // GENERATOR: flush per result
                if (left.decrementAndGet() == 0) finish(sse, done, null);
            } catch (Exception e) {
                finish(sse, done, e);    // end the stream on any failure
            }
            return null;
        });
    }
    return sse;
}

// Complete AT MOST ONCE. Two subtasks failing near-simultaneously
// would otherwise both call complete*/(), and the 2nd throws
// IllegalStateException ("already completed") silently inside a VT.
private static void finish(SseEmitter sse, AtomicBoolean done, Exception err) {
    if (!done.compareAndSet(false, true)) return;   // someone already ended it
    if (err == null) sse.complete(); else sse.completeWithError(err);
}
```

> Three bugs this shape invites: **leaking the executor** (nothing closes it — hence `onCompletion`), **hanging the client** when a subtask throws and the countdown never reaches zero, and — subtlest — **double completion**: two near-simultaneous failures both calling `completeWithError`, the second throwing `IllegalStateException` unobserved inside a VT. The `AtomicBoolean` serialises it. Contrast `channelFlow`, where completion and cancellation are structural and none of the three is expressible.

#### JAVA · WebFlux

```java
@GetMapping(value = "/answers", produces = TEXT_EVENT_STREAM_VALUE)
Flux<Part> answers(Query q) {
    return Flux.merge(          // FAN-IN, arrival order
        search(q), ratings(q), news(q));  // each a Mono<Part>
}   // the Flux return type IS the generator
```

## §11.2Ordering — when the merge must not shuffle

#### KOTLIN · server-side vs client-side ordering

```kotlin
// SERVER-SIDE: run concurrently, deliver in ORIGINAL order.
// awaitAll preserves list order regardless of completion order —
// simple, but the client waits for the slowest item before item 1.
suspend fun enrichAll(ids: List<Id>): List<Doc> = coroutineScope {
    ids.map { async { enrich(it) } }.awaitAll()
}

// CLIENT-SIDE: stream in ARRIVAL order, tagged with a sequence
// number; the client reassembles. Best time-to-first-byte.
data class Chunk(val seq: Int, val doc: Doc)
fun enrichStream(ids: List<Id>): Flow<Chunk> = channelFlow {
    ids.forEachIndexed { i, id ->
        launch { send(Chunk(seq = i, doc = enrich(id))) }
    }
}
```

#### JAVA · the same two choices

```java
// SERVER-SIDE: fork in order, get() in order. Completion order
// is irrelevant — the subtask LIST carries the sequence.
List<Doc> enrichAll(List<Id> ids) throws Exception {
    try (var scope = StructuredTaskScope.open()) {
        var tasks = ids.stream()
            .map(id -> scope.fork(() -> enrich(id)))
            .toList();
        scope.join();
        return tasks.stream().map(Subtask::get).toList();  // get() only valid AFTER join()
    }
}

// CLIENT-SIDE: identical idea — a record with a seq field,
// emitted through the SseEmitter/Flux fan-in from §11.1.
record Chunk(int seq, Doc doc) {}
```

> Pre-Loom equivalent of the server-side variant: `invokeAll`, or a list of `CompletableFuture` + `allOf().join()` then `map(CompletableFuture::join)` — order comes from the list, minus the cancellation guarantees.

## §11.3Resiliency — timeouts at two granularities, and hedging

#### KOTLIN

```kotlin
// TIMEOUTS: coarse around the whole request, fine per call.
suspend fun handle(q: Query): Answer =
    withTimeout(2.seconds) {              // COARSE: whole request
        val fast = withTimeoutOrNull(300.milliseconds) {
            personalize(q)                   // FINE: optional extra —
        }                                    // null on timeout, keep going
        answer(q, fast)
    }

// HEDGING: fire the same call at two replicas; FIRST SUCCESS wins.
// A hedge must survive a FAILING replica, so each branch yields a
// Result rather than throwing. CRITICAL: use runSuspendCatching,
// NOT stdlib runCatching — runCatching catches CancellationException
// too, so when the winner cancels the loser, the loser would swallow
// its own cancellation and break structured concurrency (KT-#1814).
suspend inline fun <T> runSuspendCatching(block: () -> T): Result<T> =
    try { Result.success(block()) }
    catch (c: CancellationException) { throw c }   // let cancellation propagate
    catch (e: Throwable)            { Result.failure(e) }

suspend fun hedged(q: Query): Px = coroutineScope {
    val ch = Channel<Result<Px>>(capacity = 2)
    launch { ch.send(runSuspendCatching { replicaA.quote(q) }) }
    launch {
        delay(50.milliseconds)          // hedge only AFTER a grace period,
        ch.send(runSuspendCatching { replicaB.quote(q) })  // most calls stay single
    }
    var last: Throwable = IllegalStateException("no replica ran")
    repeat(2) {                       // take the first SUCCESS and return —
        val r = ch.receive()          // leaving coroutineScope cancels the
        r.getOrNull()?.let { return@coroutineScope it }   // loser automatically
        r.exceptionOrNull()?.let { last = it }
    }
    throw last                       // both replicas failed (non-null accumulator)
}
```

#### JAVA

```java
// TIMEOUTS — coarse: a deadline on the whole scope cancels the
// entire task tree at once (JDK 25 preview API):
try (var scope = StructuredTaskScope.open(
        Joiner.<Part>allSuccessfulOrThrow(),   // JDK 25: join() yields a Stream
        cf -> cf.withTimeout(Duration.ofSeconds(2)))) {  // (List in JDK 26)
    scope.fork(() -> search(q));
    scope.fork(() -> ratings(q));
    var parts = scope.join().map(Subtask::get).toList();  // timeout ⇒ TimeoutException
}
// NOTE: awaitAll() also exists but returns Void — you'd read results
// via Subtask::get; use allSuccessfulOrThrow() when you want them
// back from join(). fine, per call (CompletableFuture):
//   call.orTimeout(300, TimeUnit.MILLISECONDS)  // TimeUnit, not ChronoUnit
//   — but orTimeout completes the CF; the underlying work keeps
//   running unless it is itself cancellable. Reactor: .timeout(...)

// HEDGING — here Java is genuinely nicer: the Joiner encodes exactly
// the "first SUCCESS wins, a failing replica does not sink the call"
// policy, and cancels the loser for you.
Px hedged(Query q) throws Exception {
    try (var scope = StructuredTaskScope.open(
            Joiner.<Px>anySuccessfulResultOrThrow())) {   // JDK 25 spelling
        scope.fork(() -> replicaA.quote(q));
        scope.fork(() -> {
            Thread.sleep(50);              // grace period — cheap to
            return replicaB.quote(q);      //   park: it's a virtual thread
        });
        return scope.join();               // first SUCCESS (failures are
    }                                       //   ignored unless ALL fail);
}                                           //   the loser is interrupted
// JDK 26 (JEP 525) renames it: Joiner.anySuccessfulOrThrow()
// Reactor: Mono.firstWithValue(a, b) — same "first value, tolerate
// a failing source" semantics; the losing subscription is disposed.
```

## §11.4Backpressure at the edge

#### KOTLIN · Flow has it built in

```kotlin
// collect() drives emission: the server does not compute response
// N+1 until the client has consumed response N. A slow client
// therefore slows the SERVER — for free.
fun results(q: Query): Flow<Part> = flow {
    while (hasMore(q)) emit(nextPart(q))   // suspends on a slow client
}
// BUFFERED backpressure: let the server run k steps AHEAD, so it
// isn't idle while the client digests — bounded, so still safe:
.buffer(8)
```

#### JAVA · protocol vs parked thread

```java
// WebFlux: backpressure is the request(n) protocol. The framework
// translates the client's TCP window / SSE consumption into demand;
// bound the gap explicitly:
flux.onBackpressureBuffer(8)     // == buffer(8)
    .onBackpressureLatest();      // or: drop stale instead (== conflate)

// WebMVC + VT: no protocol needed — writing to a slow client's
// socket simply BLOCKS the request's virtual thread. The parked VT
// IS the backpressure signal, and it costs ~nothing. The TCP send
// buffer (~64KB) plays the role of buffer(k).
```

> **COMPOSITION:** These aren't alternatives — a production streaming endpoint is typically **generator(fan-in(hedged(timeout(call))))** with buffered backpressure at the edge. The reason to prefer coroutines or StructuredTaskScope over hand-rolled futures for this: cancellation composes. When the client disconnects, the generator's scope is cancelled, which cancels the fan-in children, which cancels the hedges — the whole tree, automatically (§3).
