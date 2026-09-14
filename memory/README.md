# Memory — rules

This is the **Memory layer**: short, current, working facts an agent should trust
first. It is NOT a second copy of `docs/`.

## The line between docs/ and memory/

- **`docs/`** = Document layer. Source of truth for anything designed/decided:
  architecture, data model, MVP scope, exchange conventions. Stable, versioned in
  git, meant to be read in full when you need the reasoning behind something.
  Agents *cite* it as evidence.
- **`memory/`** = distilled facts an agent should *trust as currently true* without
  re-reading a whole doc — things that are operationally useful right now but
  don't (yet, or ever) belong as a section in a design doc: a discovered gotcha,
  a temporary constraint, a decision made mid-session that hasn't been written
  into `docs/` yet, current state of something that changes (e.g. "Bybit adapter
  blocked on X").

**If a fact is already written in `docs/`, it does not get a memory entry.**
Point at the doc instead (`see docs/04-architecture/exchanges/binance.md`).
Memory exists for what docs don't say yet, or for facts too transient to belong
in a design doc at all.

## Format

One file per topic, e.g. `memory/collector.md`, `memory/exchanges.md`. Each entry:

```
- [YYYY-MM-DD] <fact, 1-4 lines> (source: <session/PR/commit/doc section>)
```

No entry without a source. A memory fact with no traceable origin is a rumor, not
a fact — don't write it.

## Update rules

- **Write when you learn something durable during work** that isn't already in
  `docs/` — a constraint, a gotcha, a decision, a current-state fact. Don't batch
  it for later; add it in the same session you learned it.
- **Before writing, check it isn't already covered by a doc.** If it belongs in
  `docs/` as designed behavior (not a transient fact), it probably shouldn't be a
  memory entry at all — either update the doc directly, or add it to memory only
  if it's genuinely provisional and not yet ready to be canon.
- **On conflict, don't delete — supersede.** Mark the old entry
  `[SUPERSEDED by <date/entry>]` and add the new one. History of what was true
  stays visible.
- **Promote, then remove.** When a memory fact stabilizes and gets written into
  `docs/` (e.g. a workaround becomes the documented approach), delete the memory
  entry and point to the doc instead — don't let the same fact live in both places.
- **Expire what's no longer true.** If a fact was tied to a state that changed
  (e.g. "prototype has no auth" once auth ships), remove or supersede it rather
  than leaving stale facts for the next session to trust.
- **Keep entries compact.** 1-4 lines. If it needs more explanation than that, it
  belongs in `docs/`, not here.

## Retrieval priority

When a fact matters for a task: fresh `memory/` entry → older `memory/` entry →
`docs/`. Memory wins on recency for operational facts; `docs/` wins on depth/reasoning.
