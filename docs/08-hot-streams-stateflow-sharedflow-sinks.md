# §8 Hot streams: StateFlow / SharedFlow ↔ Sinks

Cold streams restart per collector; hot streams broadcast one live sequence to all. Kotlin splits the hot case into two purpose-built types instead of Reactor's `Sinks`/`ConnectableFlux` matrix.

| Kotlin | Semantics | Reactor | Plain Java |
|---|---|---|---|
| `MutableStateFlow(initial)` | Always has a value · conflated · new collectors get current value · `.value` readable synchronously | `Sinks.many().replay().latest()` or `Flux.cache(1)` | `volatile` field + listener list (what you'd hand-roll) |
| `MutableSharedFlow(replay=n, extraBufferCapacity=…)` | Broadcast events · configurable replay · slow-collector policy (`SUSPEND`/`DROP_OLDEST`/`DROP_LATEST`) | `Sinks.many().multicast().onBackpressureBuffer()` / `.replay().limit(n)` | `SubmissionPublisher` (JDK — the one operator-less publisher it ships) |
| `flow.shareIn(scope, WhileSubscribed(), replay)` / `stateIn(...)` | Cold→hot promotion with lifecycle | `flux.publish().refCount()` / `replay(1).refCount()` | n/a |

> **PATTERN:** For a "latest snapshot" cache (config, reference data, last quote per key): `StateFlow` beats a channel — reads are free (`.value`), writers conflate, collectors observe changes. Java analog on VT: `AtomicReference` + a condition/phaser for wakeups, or just poll.
