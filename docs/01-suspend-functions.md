# §1 Suspend functions

`suspend` marks a function that may **pause and resume**. You write it as if it were sequential blocking code; the compiler rewrites it into a state machine. No callbacks, no futures in signatures.

Relationship to coroutines: **a suspend function is not a coroutine.** A coroutine is a running *instance* — created by a builder like `launch`/`async` (§2), owning a `Job` and a context. Suspend functions are the composable *units of work* that execute inside one; they relate to a coroutine roughly as a method relates to the thread executing it. That's why a suspend fn can only be called from a coroutine (or another suspend fn): it needs a coroutine's continuation to suspend against (§1.1). The three Java-side translations below differ radically in ergonomics.

#### KOTLIN · suspend

```kotlin
suspend fun quote(id: String): Quote =
    withContext(Dispatchers.IO) { repo.find(id) }  // blocking JDBC, moved off the CPU pool

suspend fun enrich(id: String): Enriched {
    val q = quote(id)            // thread is RELEASED while this waits
    val r = rating(q.issuer)     // resumes on the SAME dispatcher thread
    return Enriched(q, r)
}
```

> The signature is honest: `suspend` ⇒ may pause. Callable only from a coroutine or another suspend fn — the compiler enforces the "color".

#### JAVA · with virtual threads (JDK 21+)

```java
// Write ordinary blocking code. The VT unmounts during I/O,
// so the OS thread underneath is free to run other work.
Quote quote(String id) { return repo.find(id); }

Enriched enrich(String id) {
    var q = quote(id);           // "blocks" — but only a VT: cheap
    var r = rating(q.issuer());
    return new Enriched(q, r);
}
```

> No function coloring at all — any method may park. The cost: nothing in the signature warns you it does I/O.

#### JAVA · without VT (CompletableFuture)

```java
CompletableFuture<Quote> quote(String id) {
    return CompletableFuture.supplyAsync(() -> repo.find(id), ioPool);
}
CompletableFuture<Enriched> enrich(String id) {
    return quote(id)
        .thenCompose(q -> rating(q.issuer())
            .thenApply(r -> new Enriched(q, r)));  // nested, to keep q in scope
}
```

> Monadic coloring: futures infect every signature, intermediate values force nesting, and stack traces show executor frames instead of your call chain. And a CF is **eager** — work starts the moment `supplyAsync` runs.

#### REACTOR · Mono — the reactive equivalent of one suspend call

```java
Mono<Quote> quote(String id) {
    return Mono.fromCallable(() -> repo.find(id))     // wrap blocking call
               .subscribeOn(Schedulers.boundedElastic()); // ≈ Dispatchers.IO
}
Mono<Enriched> enrich(String id) {
    return quote(id)
        .flatMap(q -> rating(q.issuer())          // == thenCompose
            .map(r -> new Enriched(q, r)));
}   // NOTHING has run yet — a Mono is a lazy recipe; work starts
    // only when the framework (or you) subscribe()s.
```

> The slot mapping: `Mono<T>` ≈ a `suspend () -> T` (one value, lazy, composable) and `Flux<T>` ≈ `Flow<T>` (§7). Same monadic coloring as CompletableFuture, but **lazy** (CF is eager) and with a far richer operator set (`timeout`, `retryWhen`, backpressure). Combining the stacks — calling a Mono from a suspend fn (`awaitSingle()`) or exposing a suspend fn as a Mono (`mono { }`) — is §9.

> **NUANCE:** Coroutine stack traces are synthetic across suspension points; turn on `-Dkotlinx.coroutines.debug` (or IntelliJ's coroutine agent) to recover creation stack traces. Virtual threads keep **true** stack traces — a real observability win.

## §1.1Under the hood: the CPS transform

**CPS = continuation-passing style.** A continuation is "the rest of the computation" reified as an object. Instead of returning a value, a suspend function accepts a `Continuation` and, if it can't finish immediately, returns the marker `COROUTINE_SUSPENDED` and arranges for that continuation to be called later. The Kotlin compiler generates all of this. Below is the same `enrich` from above, as the compiler conceptually emits it.

#### WHAT THE COMPILER EMITS — conceptually, but complete

```java
// ── 1. The state machine. It IS the continuation, and it IS the
//    coroutine's "stack frame" — one small heap object per call.
class EnrichSM extends ContinuationImpl {
    int    label  = 0;      // WHERE to resume (which case of the switch)
    String id;              // ── spilled locals: any variable that must
    Quote  q;               //    survive across a suspension lives here,
    Object result;          //    not on the JVM stack (the stack unwinds!)

    EnrichSM(Continuation<?> caller) { super(caller); }

    // Called by whoever finishes the async work (e.g. the IO dispatcher
    // thread once the JDBC call returns). This is the "callback".
    @Override public void resumeWith(Object value) {
        this.result = value;
        enrich(null, this);   // re-enter the method; switch jumps to `label`
    }
}

// ── 2. The function. Note the signature change: a hidden Continuation
//    parameter, and Object return (either the real value, or the
//    COROUTINE_SUSPENDED marker).
Object enrich(String id, Continuation<Enriched> cont) {

    EnrichSM sm = (cont instanceof EnrichSM e)
        ? e                          // we are being RESUMED: reuse the frame
        : new EnrichSM(cont);        // first call: allocate the frame

    Object result = sm.result;      // whatever resumeWith() handed back

    switch (sm.label) {

        case 0:                                  // ── entry point
            sm.id    = id;                       // spill: needed after resume
            sm.label = 1;                       // "if I'm re-entered, go to case 1"
            result   = quote(id, sm);            // pass sm AS the continuation
            if (result == COROUTINE_SUSPENDED)
                return COROUTINE_SUSPENDED;      // UNWIND. thread is now free.
            // not suspended? quote() had the value ready — fall straight
            // through with zero scheduling overhead (the fast path).

        case 1:                                  // ── resumed after quote()
            Quote q  = (Quote) result;            // value from fall-through OR resumeWith
            sm.q     = q;                        // spill it: needed in case 2
            sm.label = 2;
            result   = rating(q.getIssuer(), sm);
            if (result == COROUTINE_SUSPENDED)
                return COROUTINE_SUSPENDED;

        case 2:                                  // ── resumed after rating()
            Rating r = (Rating) result;
            return new Enriched(sm.q, r);        // the coroutine's return value
    }
    throw new IllegalStateException();
}
```

> Read it as a loop you re-enter: each suspension point is a `case`, `label` is the program counter, and the `sm` fields are the stack frame — on the heap, so the real thread stack can unwind completely. *Simplified:* the real compiler splits this across a separate `invokeSuspend` method (where the switch and spills actually live), and the sentinel is `IntrinsicsKt.getCOROUTINE_SUSPENDED()`. Shown fused for readability.

| Mechanism in the code above | Why it matters |
|---|---|
| Hidden `Continuation` parameter | A caller can only invoke `enrich` if it has a continuation to pass — which is exactly why suspend functions are callable only from suspend contexts. *That* is the function coloring, and it's the whole mechanism. |
| `label` + spilled fields (`id`, `q`) | The coroutine's frame lives on the heap — one small object. A platform thread instead *reserves* up to ~1 MB of stack address space (committed lazily, so resident memory is less), which is what caps you at thousands, not millions. |
| `return COROUTINE_SUSPENDED` | The method returns immediately and the whole Java call stack unwinds, freeing the thread. Nothing is "waiting" anywhere. |
| `resumeWith(value)` re-enters the `switch` | Resumption is just a normal method call from whichever thread completed the work — hence you can resume on a different thread than you suspended on. |
| Fall-through when not suspended | The fast path: if the callee already had the value, no state is saved and no dispatch happens. Suspension is only paid for when it actually occurs. |
| Compile-time rewrite | Only `suspend` calls become suspension points. Loom does the equivalent frame capture at **runtime, in the JVM**, so *any* blocking call can unmount and no signature changes. Same idea, different layer. It's also why coroutine stack traces are synthetic: the real stack unwound, and what remains is a chain of continuation objects. |

## §1.2Do virtual threads make `suspend` obsolete?

Half-true, and the half that's false is the important half. Urs Peter stages exactly this question in his Spring I/O deck — his skeptical Java developer asks whether virtual threads won't simply solve all of these problems — and the slide answers: **"No (only one, to be precise)."** The one problem they solve is thread scarcity on blocking calls. His practical addendum: virtual threads live on the JVM, so every JVM language gets them — and they *complement* coroutines (and reactive frameworks generally) rather than replace them. The honest split:

#### VTs DO replace `suspend` here

```java
// The blocking-I/O-offload use case — suspend's
// original headline job. On a virtual thread:
User loadUser(long id) {
    return jdbc.queryForObject(...);  // just blocks
}   // no suspend, no withContext, no coloring —
    // the VT unmounts its carrier for free.
```

> On JDK 21+ with `spring.threads.virtual.enabled=true`, "don't tie up a thread on I/O" needs no coroutines. VTs win here on **simplicity**: no function coloring, true stack traces, working ThreadLocals/MDC.

#### VTs DON'T touch these — coroutines' real moat

```kotlin
// Structured concurrency as a LANGUAGE feature:
val r = coroutineScope {
    val a = async { svc.x() }      // typed Deferred<T>
    val b = async { svc.y() }      // auto parent-child
    Combined(a.await(), b.await())  // cancel
}                                     //   propagation

// COLD, backpressured async STREAMS — no VT analog:
prices.conflate().map { ... }.collect { ... }
// plus Channel + select, dispatcher confinement,
// cooperative cancellation at suspension points.
```

> `StructuredTaskScope` (§3) is Java's answer to the left block, but it's a preview API and more verbose. There is **no** VT replacement for `Flow`, `Channel`/`select`, or dispatcher confinement — that's a concurrency model, not a threading trick.

#### KOTLIN · on virtual threads, yet `suspend` is still required

```kotlin
// Whole app runs on virtual threads. This service ALSO needs the
// coroutine model — because it returns a STREAM and fans out with
// cancellation. VTs give neither. So suspend stays; it just runs
// ON a VT-backed dispatcher instead of Dispatchers.IO.
//
// NOTE: kotlinx ships NO built-in VT dispatcher. You wrap a virtual-
// thread executor yourself. Urs Peter's slides expose it as an extension
// property, Dispatchers.VT — same mechanism, nicer call-sites. One care:
// extension properties can't have backing fields, so cache the dispatcher
// in a top-level val or every access builds a NEW executor.
private val vtDispatcher: CoroutineDispatcher =
    Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
val Dispatchers.VT: CoroutineDispatcher get() = vtDispatcher

// (1) Returns a Flow → MUST be built with a flow builder; there is
//     no virtual-thread way to express a cold, backpressured stream.
fun priceStream(sym: String): Flow<Tick> = flow {
    while (true) emit(poll(sym))        // emit() is a suspend fn
}.conflate().flowOn(Dispatchers.VT)   // blocking poll() on a VT

// (2) Fan-out with a typed result + auto-cancellation → async/await.
//     suspend is mandatory to call coroutineScope / await at all.
suspend fun dashboard(id: String): Dashboard = coroutineScope {
    val pos = async(Dispatchers.VT) { blockingPositions(id) }  // blocking
    val pnl = async(Dispatchers.VT) { blockingPnl(id) }        //   JDBC,
    Dashboard(pos.await(), pnl.await())   // either fails ⇒ sibling cancelled
}                                        //   — a guarantee VTs alone don't give
```

> This is the combination Urs Peter's deck names the *winning formula*: virtual threads *with* coroutines/reactive. His framing of why it wins: with a blocking API you'd otherwise need a separate thread pool with spare blockable threads (`Dispatchers.IO` / `Schedulers.boundedElastic()`) — and that pool can get exhausted and degrade performance. On a VT dispatcher, blocking I/O parks a virtual thread instead of a platform thread, so there is no pool to exhaust. But `Flow`, `async`/`await`, and `coroutineScope` are language constructs you can only reach through `suspend`. VTs are the *execution infrastructure*; coroutines are the *abstraction* — here you want both, so `suspend` stays.

> **TAKEAWAY:** Urs Peter's verdict, which this sheet adopts: **"Loom will not make coroutines obsolete — it completes them."** Post-Loom, "escape the thread-per-request ceiling" is no longer a reason to choose coroutines. What survives is what was always coroutines' own: more mature structured concurrency, finer cancellation hierarchies, automatic context propagation (MDC, security context), and `Flow`. And the two compose exactly as he describes — **let virtual threads execute your coroutines**: wrap a VT executor as a dispatcher (`Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()` — there is no built-in `Dispatchers.LOOM`) and the dedicated I/O dispatcher for blocking calls disappears, while the orchestration and streaming features remain. The deck adds two boundary conditions worth quoting in code review: on an already fully-async stack (WebClient, R2DBC), virtual threads add *only overhead, no value* — there is nothing blocking for them to fix; and virtual threads have *no API for parallelism* — fan-out needs structured concurrency, which coroutines ship out of the box (`async`/`await`).
