# Market Data Service (tardis.dev analog)

A historical/real-time crypto market data service, aimed at closing specific gaps
in tardis.dev and in the market as a whole — not at replicating the product 1:1.

## Local prototype

A deliberately cut-down local Docker Compose stack (Binance spot BTCUSDT only,
trades + L2 order book, plus a REST + MCP serving layer) is running in `src/`,
`infra/`, and `docker-compose.yml`. See [docs/07-local-dev.md](./docs/07-local-dev.md)
to run it, and [docs/08-prototype-roadmap.md](./docs/08-prototype-roadmap.md) for
the gap vs. the full MVP scope.

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
- [Local Dev](./docs/07-local-dev.md) — running the prototype stack (Docker Compose, REST/MCP, logging & monitoring).
- [Prototype → MVP Roadmap](./docs/08-prototype-roadmap.md) — gap vs. MVP scope, build phases, open decisions.
- [ADR-001: Hardening — Tests, Load Testing, Authorization](./docs/09-hardening-tests-load-auth.md) — architecture weak points found, plus decisions for the three (index doc).
  - [Test Plan](./docs/10-test-plan.md) — 140 enumerated test cases, directory layout, test-blockers.
  - [Load Testing Plan](./docs/11-load-testing-plan.md) — computed time-to-data-loss numbers, k6 script, Redis-lag harness.
  - [Authorization Spec](./docs/12-authorization-spec.md) — API-key design: storage, middleware code, revocation, rate limiting, rollout.
- [ADR-002: Architecture Validation](./docs/13-architecture-validation.md) — are the documented architecture decisions sound? ReplacingMergeTree for incidents, a process-supervision doc/impl drift, and more.
