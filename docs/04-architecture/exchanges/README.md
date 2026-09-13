# Exchange Maps — Convention

Each exchange gets its own file here, following the pattern established by
[binance.md](./binance.md) and [bybit.md](./bybit.md):

- Instrument list (with exclusions and open questions noted explicitly, not silently dropped).
- Technical details discovered while checking current documentation, dated.
- Data → source map (per segment: trades, order book, funding/mark price, OI, liquidations).
- Order book bootstrap + reconciliation mechanism.
- Connection strategy and REST rate-limit budget.
- Historical backfill sources.
- A closing "what this requires changing in decisions already made" note, if the new exchange forces a change to shared schemas (see [Incidents schema](../00-overview.md#631-incidents-schema) for the kind of thing that needs to stay exchange-agnostic).

Shared/cross-exchange concerns (collector stack, caching, storage, incidents schema) stay in
[00-overview.md](../00-overview.md) and [01-stack.md](../01-stack.md) — don't duplicate them per exchange.
