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
- ~~No one has genuinely built an MCP/AI layer for historical tick/order-book replay.~~ **[OUTDATED as of Sept 2026 — see Update below]** CoinAPI now ships hosted MCP servers covering L2/L3 order books, trades, quotes, OHLCV, flat files.

## 3. Demand Validation

Source — a real [HN AMA with the tardis.dev founder](https://news.ycombinator.com/item?id=20663766), live comments from professionals (a derivatives data-company employee, an ex-researcher at a trading firm, a latency specialist).

**Key findings:**
- Price was perceived by the audience as **underpriced** relative to data quality — there's headroom on willingness-to-pay.
- The founder's target audience was independent algo traders without large budgets, not top-tier HFT (those more often build their own infrastructure).
- **Validated gap** (requested features that didn't exist then, and largely still don't):
  1. normalized order-book objects, unified across exchanges;
  2. point-in-time reconstruction — book state at an arbitrary point in time, without manual replay from scratch. **[Partially closed by Tardis as of Sept 2026 — see Update below: daily snapshot + replay exists, but as client-side replay with a 300-3000ms gap, not a single-call arbitrary-timestamp reconstruction. Still a defensible difference, just narrower than stated.]**
  3. a standardized event schema (snapshot/add/delete/trade);
  4. machine-readable incident reports for data gaps. **[Partially closed by Tardis as of Sept 2026 — see Update below: `incidentReports` API exists, but per-exchange and not typed/per-symbol like ours.]**

**Pricing benchmarks of adjacent products:** CoinGlass $29-299/mo, CoinGecko Pro from $129/mo, Glassnode $29-799/mo, Tardis institutional tier ~$700/mo. The market holds a $29-800/mo corridor regardless of data depth.

**Conclusion:** demand is niche, not mass-market — independent/small quant teams, academic researchers, small market makers. The upper institutional segment is partly self-sufficient and isn't the primary buyer.


## 4. Update (September 2026) — re-verified against current competitor state

Section 2/3 above were written from a 2019 HN AMA with the Tardis.dev founder and
have not been re-checked against what competitors actually ship today. Re-verified
via their current docs/product pages (sources at the end of this section):

- **Tardis.dev already provides dual timestamps** — `timestamp` (exchange-native)
  + `localTimestamp` (arrival, 100ns precision) on every message. Our "dual
  timestamp" hard rule (`CLAUDE.md`) is hygiene we need, not a differentiator
  versus Tardis specifically.
- **Tardis.dev already has a machine-readable incidents API** (`incidentReports`
  via `/exchanges/:exchange`) for its own collection bugs. Our remaining edge here
  is granularity: typed, per-symbol incident categories, not a per-exchange log.
- **CoinAPI now ships hosted MCP servers** (quotes, trades, L2/L3 order books,
  OHLCV, flat files, WS). The "no one built an MCP/AI layer" observation is no
  longer accurate — see the struck-through line in section 2.
- **What still looks like a real, unmatched difference**, checked against Tardis,
  CoinAPI, Kaiko, and Amberdata's current public docs: (a) single-call order-book
  reconstruction at an arbitrary historical timestamp — every competitor checked
  either doesn't document this or requires client-side replay from a snapshot;
  (b) typed, per-symbol machine-readable incidents — none of the four document
  this at symbol granularity.
- **Positioning implication**: don't claim "we have MCP" (CoinAPI already does) or
  "we have dual timestamps / incident reports" (Tardis already does) as the pitch.
  The defensible claim is narrower: an MCP tool that does exact point-in-time book
  reconstruction *and* typed per-symbol incidents in one call — that combination,
  not either piece alone, is what's unmatched.
- **Caveat**: based on public docs/marketing pages, not each vendor's full API
  reference or a hands-on test — could be more capable (or less) than advertised.
  Worth a hands-on check before finalizing GA positioning, not just before MVP
  validation.

Sources: [Tardis.dev — Historical Data Details](https://docs.tardis.dev/historical-data-details/overview),
[Tardis.dev — Data FAQ](https://docs.tardis.dev/faq/data),
[CoinAPI — MCP](https://www.coinapi.io/mcp),
[CoinAPI — Market Data API](https://www.coinapi.io/products/market-data-api),
[Kaiko — L1/L2 Data](https://www.kaiko.com/products/l1-l2-data),
[Amberdata — Order Book Data](https://www.amberdata.io/order-book).
