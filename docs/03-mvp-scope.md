# MVP Decisions

| Question | Decision |
|---|---|
| Exchange | Binance (MVP launch exchange). Bybit is designed as the second collection slice (see [Bybit map](./04-architecture/exchanges/bybit.md)) but is a post-MVP expansion, not required for the initial 5-10 user launch. |
| Instruments | Spot + USDT-M Futures (perp) + COIN-M Futures (perp), extended list in [Binance exchange map](./04-architecture/exchanges/binance.md) |
| Data types | Trades + L2 order book + funding/OI/liquidations (the latter three — perp only) |
| Access method | Hosted REST/WS API |
| MCP | Yes, MCP tools included in the MVP from day one (key differentiator) |

**Functionality that closes the validated gaps:**
1. **Point-in-time reconstruction**: `GET /orderbook/at?symbol=&ts=` — reconstructed book state at a point in time (snapshot + delta replay), no requirement for the client to replay the stream from scratch.
2. **Machine-readable incidents**: `GET /incidents?symbol=&from=&to=` — structured JSON for gaps/reconnects (full generalized field list, including `exchange` and `segment`, in [Incidents schema](./04-architecture/00-overview.md#631-incidents-schema)).
3. **Dual timestamp** (`ts_exchange` + `ts_received`) on every record without exception.

**MCP tools (minimal set):**
- `get_trades(symbol, from, to, limit)`
- `get_orderbook_at(symbol, timestamp)`
- `get_orderbook_events(symbol, from, to, limit)` — with pagination/cursor
- `get_derivatives_metrics(symbol, from, to)` — funding + OI + liquidations together
- `list_data_incidents(symbol, from, to)`

**Explicitly out of MVP scope:** multi-exchange normalization beyond the exchanges listed under [architecture/exchanges](./04-architecture/exchanges/), multi-region collection, FIX/S3/Snowflake, options, SDKs in multiple languages, self-serve billing.

**Success criterion:** not "features shipped", but 5-10 target users actually using point-in-time query and the MCP tools for their own work, giving substantive feedback on the normalization schema.
