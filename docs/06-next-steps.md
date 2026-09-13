# Next Steps (Unresolved)

- ~~Check Binance ToS for data redistribution~~ — resolved, see [Risks](./05-risks.md) item 1: risk confirmed by literal wording, MVP plan unchanged, contact with Binance mandatory before monetization/scaling.
- ~~Detail the incidents schema for the source limitations found~~ — resolved, see [Incidents schema](./04-architecture/00-overview.md#631-incidents-schema).
- ~~Bybit (spot + linear + inverse) — design collection the same way, mirroring Binance~~ — resolved, see [Bybit map](./04-architecture/exchanges/bybit.md).
- ~~Bybit: choose book depth (`orderbook.200` at 100ms or `orderbook.1000` at 200ms)~~ — resolved, see [Bybit map](./04-architecture/exchanges/bybit.md): `orderbook.200` chosen for MVP, uniformly across spot/linear/inverse. Decision is reasoned from documented trade-offs, not empirical traffic measurement (not possible before implementation) — revisit once real traffic data is available.
- ~~Bybit: open a sample file at `public.bybit.com/spot/` to determine whether it contains trades or klines~~ — resolved, see [Bybit map](./04-architecture/exchanges/bybit.md): confirmed raw trades (verified by direct download and column inspection), packaged monthly.
- Bybit: verify whether `markPrice` freezes for XAUUSDT/XAGUSDT outside trading hours — **still open**, see "Still open" section below. Investigated, see [Bybit map](./04-architecture/exchanges/bybit.md): no explicit documentation found either way; best-informed guess is "likely yes" by analogy with Binance's COMEX/LBMA-style reference, but not confirmed.
- ~~Check Bybit's ToS for data redistribution and for scraping the `history-data` portal~~ — resolved, see [Risks](./05-risks.md) item 7: redistribution risk confirmed (comparable to Binance), general anti-scraping clause applies to the portal, written permission needed before scraping at volume.
- ~~Generalize `segment` in the incidents schema across exchanges~~ — resolved, see [Incidents schema](./04-architecture/00-overview.md#631-incidents-schema): added an `exchange` field, kept `segment` exchange-native (not force-unified), normalized `affected_channel` to a shared logical vocabulary with the literal channel name preserved in `details.source_channel`.
- ~~Binance options (BTC/ETH chains)~~ — no action needed now: already explicitly out of MVP scope per [MVP decisions](./03-mvp-scope.md) and [data model](./02-data-model.md) (separate protocol/volume of work). Revisit only when planning a post-MVP options round.
- ~~Establish a convention for adding new exchanges~~ — resolved: see [architecture/exchanges/README.md](./04-architecture/exchanges/README.md) for the required structure of a new exchange map file.

## Still open

1. **Bybit XAUUSDT/XAGUSDT markPrice freeze** — needs an empirical weekend/holiday check against the live API before the `reference_price_freeze` detector can be trusted for these symbols on Bybit (see above).
2. **Storage/egress cost estimate** (tracked in [Risks](./05-risks.md) item 5) — still needs a GB/day-per-instrument-per-exchange estimate.
3. **Next exchange candidate** — no exchange beyond Binance and Bybit is scoped yet; when one is chosen, follow the convention in [architecture/exchanges/README.md](./04-architecture/exchanges/README.md).
