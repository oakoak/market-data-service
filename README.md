# Market Data Service (tardis.dev analog)

A historical/real-time crypto market data service, aimed at closing specific gaps
in tardis.dev and in the market as a whole — not at replicating the product 1:1.

## Local prototype

A deliberately cut-down local Docker Compose stack (Binance spot BTCUSDT only,
trades + L2 order book, no API/MCP yet) is running in `src/`, `infra/`, and
`docker-compose.yml`. See [docs/07-local-dev.md](./docs/07-local-dev.md) to run it.

## Docs

- [Idea and Market Analysis](./docs/01-idea-and-market.md) — the idea, competitive landscape, demand validation.
- [Data Model](./docs/02-data-model.md) — instrument × data-type axes.
- [MVP Scope](./docs/03-mvp-scope.md) — MVP decisions, MCP tools, success criteria.
- Architecture
  - [Overview](./docs/04-architecture/00-overview.md) — data history constraints, collection layer, diagram, incidents schema, caching, storage.
  - [Collector Stack](./docs/04-architecture/01-stack.md) — Python MVP stack (WS client, broker, ClickHouse client, supervision).
  - Exchanges ([convention for adding a new one](./docs/04-architecture/exchanges/README.md))
    - [Binance](./docs/04-architecture/exchanges/binance.md)
    - [Bybit](./docs/04-architecture/exchanges/bybit.md)
- [Risks / Open Issues](./docs/05-risks.md)
- [Next Steps](./docs/06-next-steps.md)
