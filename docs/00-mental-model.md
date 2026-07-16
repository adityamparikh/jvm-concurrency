# §0 Mental model

## §0.1Vocabulary: async ≠ parallel ≠ non-blocking

These words get used interchangeably; they name **orthogonal properties**. Every technology in this sheet is a different combination of them — misdiagnosing which property you actually need is how teams end up on WebFlux for a CRUD service.

| Term | A property of… | Meaning | Litmus example |
|---|---|---|---|
| **Blocking / non-blocking** | how the *wait* is implemented | Does an OS thread sit parked inside the call until the result arrives — or is the wait registered with the OS (epoll/NIO) and the thread freed to do other work? | `socket.read()` on a platform thread vs an NIO selector loop |
| **Synchronous / asynchronous** | the *call contract* | Sync: the result is in hand when the call returns. Async: the call returns immediately and the result is delivered later — via callback, future, or resumption. | `Quote quote(id)` vs `CompletableFuture<Quote> quote(id)` |
| **Concurrent** | program *structure* | Multiple tasks in progress over overlapping time windows. Interleaving on a single thread counts — no second core required. | 10k coroutines multiplexed on one dispatcher thread |
| **Parallel** | *hardware* execution | Multiple tasks executing at the same instant on different cores. Parallelism is a subset of concurrency; it buys throughput for CPU-bound work, nothing for waits. | `list.parallelStream().map(…)` |

```mermaid
quadrantChart
    title Call contract vs thread behavior during a wait
    x-axis Sync contract --> Async contract
    y-axis Thread parked --> Thread freed
    quadrant-1 The sweet spot for most services
    quadrant-2 Max control - backpressure and operators
    quadrant-3 Fine until thread count is the ceiling
    quadrant-4 Easy to mistake for non-blocking
    virtual threads / suspend fun: [0.3, 0.8]
    Reactor WebFlux async CF over NIO: [0.8, 0.8]
    platform thread plus JDBC RestTemplate: [0.3, 0.2]
    supplyAsync jdbc query pool: [0.75, 0.25]
```


*fig 0a — contract × wait behavior. Parallelism is a third, independent axis: how many cores execute at once.*

> **THE TRAPS:** **Async ≠ non-blocking**: `supplyAsync` wrapping JDBC is asynchronous to the caller while a pool thread blocks all the same. **Non-blocking ≠ async**: a `suspend fun` or a blocking call on a virtual thread gives a synchronous contract over a freed thread. **Concurrent ≠ parallel**: 100k coroutines can interleave on one thread, while `parallelStream` is parallel yet fully synchronous — the caller waits.

## §0.2Who parks whom

All three models solve the same problem — **don't hold an OS thread hostage during I/O** — at different layers. Coroutines unmount at `suspend` points (compiler-generated state machines, CPS transform). Virtual threads unmount at blocking JDK calls (JVM-managed continuations). Platform threads never unmount; they just block.

```mermaid
flowchart TB
    subgraph PT["Platform threads (classic)"]
        direction LR
        PT1["1 request = 1 OS thread<br/>~1 MB stack · blocks on I/O"]
        PT2["1:1 mapping — caps out at ~thousands"]
    end

    subgraph VT["Virtual threads (JDK 21+)"]
        direction LR
        VT1["M:N · unmounts from carrier<br/>on blocking JDK call"]
        VT2["carriers ≈ #cores<br/>parked VTs live on heap"]
        VT3["unmount at: socket read,<br/>sleep, lock park, JDBC…"]
    end

    subgraph CO["Coroutines (Kotlin)"]
        direction LR
        CO1["M:N · unmounts only<br/>at suspension points"]
        CO2["Dispatchers.IO / Default"]
        CO3["a blocking call NOT marked suspend<br/>still blocks the dispatcher thread"]
    end

    PT --> VT --> CO

    Rule["Rule of thumb: coroutines = structured concurrency + streaming as language features.<br/>Virtual threads = the same scalability for plain Java.<br/>Reactor = only when you need true Reactive-Streams semantics."]
    CO --> Rule
```


*fig 0 — thread mapping. "Parked"/"suspended" work costs a heap object, not an OS thread.*
