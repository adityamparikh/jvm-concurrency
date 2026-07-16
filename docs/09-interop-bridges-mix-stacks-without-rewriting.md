# §9 Interop bridges — mix stacks without rewriting

You rarely choose one model wholesale. These adapters make Reactor libraries (WebClient, R2DBC, Kafka Reactor) feel native from coroutines, and vice versa.

#### KOTLIN ⇢ consuming Java async APIs

```kotlin
// kotlinx-coroutines-reactor:
val user: User = webClient.get().uri("/u/{id}", id)
    .retrieve().bodyToMono<User>()
    .awaitSingle()                    // Mono<T> → suspend

val events: Flow<Event> = flux.asFlow()   // Flux<T> → Flow<T>

// kotlinx-coroutines-jdk8:
val px: Px = completableFuture.await()      // CF<T> → suspend

// blocking Java API (JDBC, Solr client…):
val docs = withContext(Dispatchers.IO) { solr.query(q) }
```

#### JAVA ⇠ consuming Kotlin suspend APIs

```java
// expose suspend fn to Java as Reactor types:
fun enrichMono(id: String): Mono<Enriched> =
    mono { enrich(id) }               // builder from -reactor

fun ticksFlux(): Flux<Tick> = ticks("AAPL").asFlux()

// or as a CompletableFuture (kotlinx-coroutines-jdk8):
fun enrichCf(id: String): CompletableFuture<Enriched> =
    scope.future { enrich(id) }
```

> Dependency map: `kotlinx-coroutines-reactor` (Mono/Flux ⇄ suspend/Flow, incl. Reactor context propagation), `-jdk8` (CompletableFuture), `-reactive` (raw Reactive Streams `Publisher`).
