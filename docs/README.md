# `docs/` — generated, machine-readable mirror

This directory is a **generated** markdown mirror of `index.html`, one file per section, produced by `extract.py` at the repo root. It exists for AI tools — coding agents, RAG pipelines, `llms.txt`-aware crawlers — that read text more reliably than syntax-highlighted HTML and SVG.

**`index.html` is the source of truth.** Do not hand-edit files in this directory — edits will be overwritten the next time `extract.py` runs. If something here is wrong, fix it in `index.html` and re-run the extraction.

## What changed in the conversion

- Syntax-highlighted `<span>` code becomes plain fenced ```` ```kotlin ```` / ```` ```java ```` blocks.
- Side-by-side comparison cards (Kotlin vs Java vs Reactor) become sequential `####` subsections in reading order.
- Flowcharts and decision trees (thread model, failure propagation, channel pipeline, the WebMVC/WebFlux decision) become [Mermaid](https://mermaid.js.org/) diagrams — validated to render before being committed.
- Marble diagrams (`flatMap` family, backpressure strategies) become markdown tables. Mermaid has no timeline primitive that honestly represents concurrent emission, so a table is more accurate than forcing a diagram type that would misrepresent the timing.
- Quadrant diagrams use Mermaid's native `quadrantChart` type.

## Files

- `00-mental-model.md` … `13-the-source-track-....md` — one per `<section>` in `index.html`, in document order.
- `../llms.txt` — top-level index in the [llms.txt](https://llmstxt.org/) convention, one line per section with a short description.
- `../llms-full.md` — all sections concatenated into a single file, for tools that ingest whole-document context in one shot.

## Regenerating

```
python3 extract.py
```

Requires `beautifulsoup4`. Reads `index.html`, writes `docs/*.md`. Regenerate `llms-full.md` by concatenating the output (see `extract.py` for the exact join logic) after any content change.
