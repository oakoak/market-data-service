# Idea and Market Analysis

## 1. Idea

Build a service similar to tardis.dev — normalized historical and real-time crypto
market data — but close the specific gaps that exist in tardis.dev and in the market
as a whole.

## 2. Competitive Analysis

| Provider | Coverage | Granularity | Access | Pricing model | Strengths | Weaknesses |
|---|---|---|---|---|---|---|
| **Tardis.dev** | ~30-50+ exchanges (spot/perp/options) | Tick-level trades, L2/L3 order book, funding, liquidations | REST, WS replay, tardis-machine (self-host), Python/Node SDK | Tiered subscription, history tied to subscription term | Raw data accuracy, order book depth, local replay server | 1 IP/key on low tiers, API only from Pro+, no FIX, no MCP/AI layer, 300-3000ms reconnect gaps |
| **Kaiko** | 100+ CEX/DEX | L1/L2 normalized, tick + aggregates | REST, WS, S3/warehouse | Enterprise contracts | Derivatives, analytics, ready-made data warehouse integrations | "Smoothed" normalization — raw fidelity is lost |
| **CoinAPI** | 400+ exchanges (claimed) | Trades, order book, dual timestamp | REST, WS, FIX 4.4, S3 flat files | Tiers + pay-per-use | Widest coverage, transparent latency tiers, has MCP integration | Breadth ≠ depth per exchange |
| **Amberdata** | Major CEX + on-chain/DeFi | Tick order book, futures/options/swaps + blockchain data | WS, REST, S3, Snowflake | Enterprise, closed pricing | On-chain + market data together, deep history | High entry barrier, weak self-serve |
| **CryptoCompare / CCData** | 250-300+ exchanges, 6000+ assets | Trade-level tick, L2 (whitelist only) | REST (legacy + new) | Free tier + paid | Broad asset/pair coverage, indices | Manual whitelisting for real-time order book |
| **Coinalyze** | Major derivatives exchanges | OI, funding, long/short ratio, OHLCV | REST, free | Free | Simple, free | Intraday history holds only 1500-2000 points and gets purged, no tick/order book |
| **Databento** (DX benchmark, TradFi→crypto) | Growing in crypto | Unified live/historical schema | Python/C++/Rust SDK, HTTP | Pay-as-you-go | Best documentation/DX in the industry | Crypto coverage still young |

**Observations:**
- The market is split between "broad coverage + smoothed data" (CoinAPI, CryptoCompare, Kaiko) vs "narrow coverage + raw accuracy" (Tardis, Amberdata). No one combines both poles without compromise.
- Onboarding is a common weakness across all players (manual whitelisting, history tied to subscription, closed enterprise pricing).
- No one has genuinely built an MCP/AI layer for historical tick/order-book replay.

## 3. Demand Validation

Source — a real [HN AMA with the tardis.dev founder](https://news.ycombinator.com/item?id=20663766), live comments from professionals (a derivatives data-company employee, an ex-researcher at a trading firm, a latency specialist).

**Key findings:**
- Price was perceived by the audience as **underpriced** relative to data quality — there's headroom on willingness-to-pay.
- The founder's target audience was independent algo traders without large budgets, not top-tier HFT (those more often build their own infrastructure).
- **Validated gap** (requested features that didn't exist then, and largely still don't):
  1. normalized order-book objects, unified across exchanges;
  2. point-in-time reconstruction — book state at an arbitrary point in time, without manual replay from scratch;
  3. a standardized event schema (snapshot/add/delete/trade);
  4. machine-readable incident reports for data gaps.

**Pricing benchmarks of adjacent products:** CoinGlass $29-299/mo, CoinGecko Pro from $129/mo, Glassnode $29-799/mo, Tardis institutional tier ~$700/mo. The market holds a $29-800/mo corridor regardless of data depth.

**Conclusion:** demand is niche, not mass-market — independent/small quant teams, academic researchers, small market makers. The upper institutional segment is partly self-sufficient and isn't the primary buyer.
