#!/usr/bin/env python3
"""Extract coroutines-cheatsheet.html into per-section markdown files for AI/human consumption."""
import re, os
from bs4 import BeautifulSoup, NavigableString, Tag

SRC = '/home/claude/coroutines-cheatsheet.html'
OUT = '/home/claude/extract/docs'
os.makedirs(OUT, exist_ok=True)

soup = BeautifulSoup(open(SRC), 'html.parser')

# ---------- validated Mermaid diagrams, keyed by the SVG's aria-label ----------
MERMAID = {
"Quadrant of call contract versus thread behavior during a wait": ("quadrantChart", '''quadrantChart
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
    supplyAsync jdbc query pool: [0.75, 0.25]'''),
"Comparison of platform threads, virtual threads, and coroutines": ("flowchart", '''flowchart TB
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
    CO --> Rule'''),
"Structured concurrency failure propagation tree": ("flowchart", '''flowchart TD
    P["coroutineScope / open()"]
    P --> A["child A · positions()"]
    P --> B["child B · pnl() ✕ throws"]
    P --> C["child C · limits()"]
    B -->|cancel sibling| A
    B -->|cancel sibling| C
    B -->|rethrow to parent| P'''),
"Channel producer consumer pipeline with suspension-based backpressure": ("flowchart", '''flowchart LR
    P["producer<br/>ch.send(tick)"] --> BUF["buffer of 1024<br/>full ⇒ send() suspends"]
    BUF --> W1["worker 1"]
    BUF --> W2["worker 2 … 4"]
    W1 -.->|competing consumers: each tick to exactly ONE worker| W2'''),
"Urs Peter's decision tree: language, then throughput needs, then blocking code, resolving to virtual threads or measurement": ("flowchart", '''flowchart TD
    Q{"Need high throughput /<br/>parallelism / streams?"}
    Q -->|Java| JavaB["Java"]
    Q -->|Kotlin| KotlinB["Kotlin"]

    JavaB -->|yes| WF["Spring WebFlux<br/>use a reactive stack ⚠"]
    JavaB -->|no| MVC["Spring Web (MVC)<br/>stay with your blocking model<br/>on virtual threads"]
    KotlinB -->|no| MVC
    KotlinB -->|yes| WFC["Spring WebFlux + coroutines<br/>choose for coroutines"]

    WF --> B1{"Need to use<br/>blocking code?"}
    WFC --> B2{"Need to use<br/>blocking code?"}

    B1 -->|no| M1["Measure<br/>VT vs PT"]
    B1 -->|yes| VT["Use virtual threads<br/>(Dispatchers.VT)"]
    B2 -->|yes| VT
    B2 -->|no| M2["Measure<br/>VT vs PT"]'''),
}

# ---------- marble diagrams -> markdown tables (time is the axis; mermaid has no primitive for this) ----------
MARBLE_TABLES = {
"Marble diagram comparing flatMapMerge, flatMapConcat and flatMapLatest": '''**Source stream:** `A` at t=1, `B` at t=3 (each opens an inner stream: A → a1, a2, a3; B → b1, b2)

| Operator | Emission order | Behavior |
|---|---|---|
| `flatMapMerge` (Flow) / `flatMap` (Reactor) | a1, a2, b1, a3, b2 | Interleaved — order NOT preserved, inner streams run concurrently |
| `flatMapConcat` (Flow) / `concatMap` (Reactor) | a1, a2, a3, b1, b2 | B waits for A to finish — order preserved, no concurrency |
| `flatMapLatest` (Flow) / `switchMap` (Reactor) | a1, a2, b1, b2 (a3 dropped) | B's arrival CANCELS A's inner stream — a3 never emitted (search-as-you-type pattern) |
''',
"Marble diagram of slow-consumer strategies: buffer, conflate, debounce, sample": '''**Source stream:** emits 1, 2, 3, 4, 5, 6 — consumer is slower than the producer.

| Strategy | Emitted | Behavior |
|---|---|---|
| `buffer(n)` | 1, 2, 3, 4, 5, 6 (delayed) | Keep EVERYTHING, deliver late — full buffer ⇒ producer suspends |
| `conflate()` | 1, 3, 5, 6 | Skip stale values, keep only the LATEST |
| `debounce(t)` | 3, 5, 6 | Emit only after a period of SILENCE |
| `sample(t)` | 3, 5, 6 | Emit the latest value on a fixed CLOCK tick |
''',
}

def slugify(num, title):
    t = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
    return f"{num:02d}-{t}"

def inline_md(node):
    """Render inline HTML (within a <p>, <td>, callout, etc.) to markdown text."""
    out = []
    for child in node.children:
        if isinstance(child, NavigableString):
            out.append(str(child))
        elif isinstance(child, Tag):
            if child.name == 'code':
                out.append(f"`{child.get_text()}`")
            elif child.name in ('b', 'strong'):
                out.append(f"**{inline_md(child)}**")
            elif child.name in ('em', 'i'):
                out.append(f"*{inline_md(child)}*")
            elif child.name == 'br':
                out.append("  \n")
            elif child.name == 'a':
                href = child.get('href', '')
                out.append(f"[{inline_md(child)}]({href})")
            elif child.name == 'span':
                # tag labels inside callouts (e.g. <span class="tag ok">takeaway</span>) - bold it
                cls = child.get('class', [])
                if 'tag' in cls:
                    out.append(f"**{child.get_text().strip().upper()}:** ")
                else:
                    out.append(inline_md(child))
            else:
                out.append(inline_md(child))
    text = ''.join(out)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()

def code_block(pre_tag, lang):
    code = pre_tag.get_text()
    code = code.rstrip('\n')
    return f"```{lang}\n{code}\n```"

def card_lang(card_tag):
    cls = card_tag.get('class', [])
    if 'k' in cls:
        return 'kotlin'
    return 'java'  # v (virtual threads) and j (java/reactor) are both Java syntax

def render_table(table_tag):
    rows = table_tag.find_all('tr')
    if not rows:
        return ''
    def cells_of(row, tagname):
        return [inline_md(c).replace('\n', ' ').replace('|','\\|') for c in row.find_all(tagname)]
    header_cells = cells_of(rows[0], 'th') or cells_of(rows[0], 'td')
    lines = ['| ' + ' | '.join(header_cells) + ' |']
    lines.append('|' + '|'.join(['---'] * len(header_cells)) + '|')
    for r in rows[1:]:
        cells = cells_of(r, 'td') or cells_of(r, 'th')
        if len(cells) < len(header_cells):
            cells += [''] * (len(header_cells) - len(cells))
        lines.append('| ' + ' | '.join(cells[:len(header_cells)]) + ' |')
    return '\n'.join(lines)

def render_figure(fig_tag):
    svg = fig_tag.find('svg')
    label = svg.get('aria-label', '') if svg else ''
    figcap = fig_tag.find('figcaption')
    cap_text = inline_md(figcap) if figcap else ''
    parts = []
    if label in MERMAID:
        difftype, code = MERMAID[label]
        parts.append(f"```mermaid\n{code}\n```")
    elif label in MARBLE_TABLES:
        parts.append(MARBLE_TABLES[label])
    else:
        parts.append(f"*[Diagram: {label}]*")
    if cap_text:
        parts.append(f"\n*{cap_text}*")
    return '\n\n'.join(parts)

def render_card(card_tag):
    h3 = card_tag.find('h3')
    title = inline_md(h3) if h3 else ''
    lang = card_lang(card_tag)
    out = [f"#### {title}"] if title else []
    for pre in card_tag.find_all('pre', recursive=False):
        out.append(code_block(pre, lang))
    note = card_tag.find('div', class_='note')
    if note:
        out.append(f"> {inline_md(note)}")
    return '\n\n'.join(out)

def render_container(container_tag):
    """duo / stack: transparent containers holding cards and spines."""
    out = []
    for child in container_tag.find_all(['div'], recursive=False):
        cls = child.get('class', [])
        if 'card' in cls:
            out.append(render_card(child))
        elif 'stack' in cls or 'duo' in cls:
            out.append(render_container(child))
        # spine (separator) skipped
    return '\n\n'.join(out)

def render_block(tag, section_num):
    name = tag.name
    cls = tag.get('class', []) or []

    if name == 'h2':
        # sub-heading within section (main h2 handled separately by caller)
        text = inline_md(tag)
        return f"## {text}"
    if name == 'p':
        return inline_md(tag)
    if name == 'table':
        return render_table(tag)
    if name == 'figure':
        return render_figure(tag)
    if name == 'div' and 'callout' in cls:
        return f"> {inline_md(tag)}"
    if name == 'div' and ('duo' in cls or 'stack' in cls):
        return render_container(tag)
    if name == 'div' and 'card' in cls:
        return render_card(tag)
    if name == 'pre':
        return code_block(tag, 'kotlin')
    return None

def extract_section(section_tag, order_num):
    h2 = section_tag.find('h2', recursive=False)
    num_span = h2.find('span', class_='num')
    sec_id_num = inline_md(num_span) if num_span else ''
    title_text = h2.get_text().replace(sec_id_num, '', 1).strip() if num_span else h2.get_text().strip()
    full_title = f"{sec_id_num} {title_text}".strip()

    blocks = [f"# {full_title}"]
    for child in section_tag.find_all(recursive=False):
        if child is h2:
            continue
        md = render_block(child, order_num)
        if md:
            blocks.append(md)
    body = '\n\n'.join(blocks)
    slug = slugify(order_num, title_text)
    return slug, full_title, body

sections = soup.find_all('section', recursive=True)
index = []
for i, sec in enumerate(sections):
    slug, title, body = extract_section(sec, i)
    path = os.path.join(OUT, slug + '.md')
    with open(path, 'w') as f:
        f.write(body + '\n')
    index.append((slug, title, len(body)))
    print(f"{slug}.md  ({len(body):>6} chars)  {title}")

print(f"\n{len(sections)} sections extracted to {OUT}/")
