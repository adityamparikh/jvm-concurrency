# §7 Flow ↔ Reactor — asynchronous streams

A `Flow<T>` is a **cold, suspending stream**: a recipe that produces values over time. Nothing runs until you collect it, each collector gets its own fresh run, and backpressure is implicit — a slow collector simply suspends the emitter. `Flux<T>` occupies the same conceptual slot in Reactor.

`java.util.stream.Stream` is *not* an equivalent: it's synchronous, pull-only, single-use, and has no notion of time. And `java.util.concurrent.Flow` (JEP 266) is only the Reactive-Streams *interfaces* — the JDK ships zero operators; Reactor and RxJava supply them.

## §7.0Channel vs Flow — and `channelFlow`, the bridge

§6 was `Channel`; this section is `Flow`. They are easy to confuse because both carry a sequence of values over time — but they sit on opposite sides of one line: **a channel is *hot* and shared; a flow is *cold* and per-collector.** Getting this distinction is what makes `channelFlow` (used heavily in §11) obvious rather than mysterious.

|  | `Channel<T>` (§6) | `Flow<T>` (§7) |
|---|---|---|
| **Hot or cold** | HOT — exists and buffers whether or not anyone is reading | COLD — inert until `collect`; the body re-runs for *each* collector |
| **Who produces** | One side `send`s, another `receive`s — **decoupled**, often different coroutines | The `flow { }` block *is* the producer; it runs inside the collector |
| **Fan-out** | Each item goes to **exactly one** receiver (competing consumers) | Each collector gets **its own independent run** of every item |
| **Concurrency inside** | Naturally multi-producer / multi-consumer | Sequential by default — you can't `emit` from another coroutine |
| **Reuse** | Single conduit; once closed, done | Re-collectable any number of times |
| **Java analogy** | `BlockingQueue` — a live pipe between threads | A lazy `Supplier`-of-stream — nothing until someone iterates |

Rule of thumb: reach for a **Channel** when independent producers must hand work to consumers (a pipeline, §6). Reach for a **Flow** when you're describing a reusable stream a caller will consume (an API return type, §7). The tension appears when you want a Flow's cold, backpressured, re-collectable *interface* but need several coroutines producing into it concurrently — which a plain `flow { }` forbids.

#### KOTLIN · `channelFlow` — a Flow whose values come from a Channel

```kotlin
// A plain flow { } is sequential: you may only emit() from the
// flow's own coroutine. The naive attempts both fail:
//   flow { launch { emit(x) } }                  // doesn't COMPILE —
//     the flow block has no CoroutineScope, so launch doesn't resolve
//   flow { coroutineScope { launch { emit(x) } } }  // compiles, then
//     throws at RUNTIME: "Flow invariant is violated" (concurrent emit)

// channelFlow gives the block a hidden Channel. Concurrent children
// send() into it; channelFlow drains it and emits downstream. To the
// COLLECTOR it's an ordinary cold Flow with normal backpressure —
// the channel is an internal implementation detail.
fun prices(syms: List<String>): Flow<Tick> = channelFlow {
    syms.forEach { sym ->
        launch {                       // one child coroutine per symbol —
            feed(sym).collect { send(it) }  // all producing CONCURRENTLY
        }
    }
}   // closes when every child completes (structured concurrency)

// send() suspends when the downstream collector is slow → the cold
// Flow's backpressure reaches all the way back to each producer.
```

> So: `flow { }` = cold + sequential producer. `Channel` = hot + concurrent, but not a Flow. `channelFlow { }` = the bridge — a cold Flow on the outside, a concurrent Channel on the inside. That is exactly the fan-in shape §11.1 needs: several backends producing into one streamed response. (`callbackFlow` is the same tool for bridging a callback-based API into a Flow.)

> **THE GOAL:** Both snippets below build the **same live price feed** for one symbol: poll the source repeatedly, normalise each tick, discard bad prices, keep going if the source errors (substituting a stale marker, retrying up to 3×), and decouple the producer from a slow consumer with a 256-element buffer so a slow subscriber can't stall the poll loop.

#### KOTLIN · Flow

```kotlin
fun ticks(sym: String): Flow<Tick> = flow {   // COLD: this block re-runs
    while (currentCoroutineContext().isActive) {  //   for each collector
        emit(poll(sym))                  // emit() can suspend — no callbacks
    }
}
.map { normalize(it) }                  // transform each tick
.filter { it.px > 0 }                  // drop bad prices
.buffer(256)                            // run producer + consumer concurrently,
                                        //   with 256 slots between them
.flowOn(Dispatchers.IO)                 // everything ABOVE this line runs on IO
.catch { e -> emit(Tick.stale(sym)) }   // upstream failed: substitute a value
.retry(3)                              // ...or just re-run the whole flow, 3×

// TERMINAL: nothing above has run yet. collect() starts it and
// SUSPENDS the caller until the flow completes.
ticks("AAPL").collect { publish(it) }
```

#### REACTOR · Flux

```java
Flux<Tick> ticks(String sym) {
    return Flux.<Tick>generate(s -> s.next(poll(sym)))  // COLD too
        .map(this::normalize)               // same
        .filter(t -> t.px() > 0)             // same
        .onBackpressureBuffer(256)          // == buffer(256)
        .subscribeOn(Schedulers.boundedElastic())  // == flowOn(IO)
        .onErrorResume(e -> Flux.just(Tick.stale(sym)))  // == catch{}
        .retry(3);
}

// TERMINAL: subscribe() starts it and RETURNS IMMEDIATELY.
// The work happens on a scheduler thread; results arrive by callback.
ticks("AAPL").subscribe(this::publish);
```

> The one deep difference: `collect` **suspends the caller** — you stay in sequential code and can wrap the whole thing in try/catch. `subscribe` **detaches** — you're in callback land, and errors must be routed through operators. Reactor's demand signal (`request(n)`) is an explicit protocol that can cross a network boundary (RSocket); Kotlin's suspension cannot — it's in-process only.

## §7.1The operators that actually cause bugs

Names alone don't help. These four groups are where people get it wrong; each marble diagram reads left→right as time, and each row is one stream.

**Source stream:** `A` at t=1, `B` at t=3 (each opens an inner stream: A → a1, a2, a3; B → b1, b2)

| Operator | Emission order | Behavior |
|---|---|---|
| `flatMapMerge` (Flow) / `flatMap` (Reactor) | a1, a2, b1, a3, b2 | Interleaved — order NOT preserved, inner streams run concurrently |
| `flatMapConcat` (Flow) / `concatMap` (Reactor) | a1, a2, a3, b1, b2 | B waits for A to finish — order preserved, no concurrency |
| `flatMapLatest` (Flow) / `switchMap` (Reactor) | a1, a2, b1, b2 (a3 dropped) | B's arrival CANCELS A's inner stream — a3 never emitted (search-as-you-type pattern) |



*fig 2a — the same source, three flattening strategies. Picking the wrong one is the classic reactive bug.*

**Source stream:** emits 1, 2, 3, 4, 5, 6 — consumer is slower than the producer.

| Strategy | Emitted | Behavior |
|---|---|---|
| `buffer(n)` | 1, 2, 3, 4, 5, 6 (delayed) | Keep EVERYTHING, deliver late — full buffer ⇒ producer suspends |
| `conflate()` | 1, 3, 5, 6 | Skip stale values, keep only the LATEST |
| `debounce(t)` | 3, 5, 6 | Emit only after a period of SILENCE |
| `sample(t)` | 3, 5, 6 | Emit the latest value on a fixed CLOCK tick |



*fig 2b — `debounce` waits for the typing to stop; `sample` ticks on a timer regardless. They are not interchangeable.*

The full mapping, with what each operator actually does:

| Kotlin Flow | Reactor | What it does |
|---|---|---|
| `map { }` / `filter { }` | `map` / `filter` | Transform each element / drop the ones failing a predicate. 1-to-1 and 1-to-0. |
| `flatMapMerge` | `flatMap` | Each element opens an inner stream; run them **concurrently** and interleave the results. Fastest, order not preserved. *Use for: fan-out calls per element.* |
| `flatMapConcat` | `concatMap` | Same, but each inner stream must **finish before the next starts**. Order preserved, no concurrency. *Use when order matters.* |
| `flatMapLatest` | `switchMap` | **Cancels** the in-flight inner stream when a new element arrives. *Use for: search-as-you-type, "only the newest request counts".* |
| `zip` | `zip` | Pair up the **1st with the 1st, 2nd with the 2nd**. Waits for both sides; a fast stream is throttled by a slow one. |
| `combine` | `combineLatest` | Re-emit whenever **either** side changes, using the latest of the other. *Use for: live dashboards ("price × qty").* |
| `buffer(n)` | `onBackpressureBuffer(n)` | Let producer and consumer run concurrently with n slots between them. Keeps everything; full buffer ⇒ producer suspends. |
| `conflate()` | `onBackpressureLatest()` | Drop intermediate values; the consumer always gets the **newest**. *Use for: prices, positions, any "current state".* |
| `debounce(t)` | `debounce` / `sampleTimeout` | Emit only after **t of silence**. Bursts collapse to their last element. |
| `sample(t)` | `sample(t)` | Emit the latest value **every t**, on a clock. Steady output rate regardless of input rate. |
| `onEach` / `onCompletion` | `doOnNext` / `doFinally` | Side effects (logging, metrics) without changing the stream. |
| `catch { }` / `retry(n)` | `onErrorResume` / `retryWhen(Retry.backoff(..))` | Substitute a fallback stream on failure / re-subscribe from scratch, up to n times. |
| `flowOn(ctx)` — affects everything UPSTREAM of it | `subscribeOn` (the source) + `publishOn` (everything downstream) | Choose which threads run which part of the chain. Kotlin has one operator with one direction; Reactor has two with opposite effects — a frequent source of confusion. |
| `first()` / `toList()` / `collect { }` | `next()` / `collectList()` / `subscribe()` | Terminal operators — these are what actually start the stream. |
