# Load Testing Plan — Market Data Service Prototype

Companion document to [ADR-001](./09-hardening-tests-load-auth.md#2-load-testing). Computes concrete numbers from data already in this repo's docs and source rather than leaving the plan as "TBD, define before running." No code was changed to produce this document.

## Facts vs. assumptions (read from repo)

**Facts (exact numbers/quotes from the repo):**
- `src/collector/sink.py::publish()` — `XADD ... maxlen=<REDIS_STREAM_MAXLEN> approximate=True` on every message, unconditionally.
- `.env.example` / `docker-compose.yml` — `REDIS_STREAM_MAXLEN=1000000` (current default).
- `src/normalizer/config.py` — `sink_batch_size=500` (`SINK_BATCH_SIZE`), `sink_flush_interval_s=2` (`SINK_FLUSH_INTERVAL_S`), `book_snapshot_interval_s=60`.
- **Critical architectural fact** (from `config.py`'s `stream_prefix = f"raw:{exchange}:{symbol.lower()}"` on both collector and normalizer): streams are **keyed per (exchange, symbol)**, not shared/aggregated across symbols. Each symbol gets its own `raw:{exchange}:{symbol}:trades` and `raw:{exchange}:{symbol}:depth` stream, each with its own independent 1,000,000-entry maxlen. **This means the full-slice symbol count (23 Binance + 20 Bybit = 43 symbol-streams) does not dilute or accelerate any single stream's trim risk — each stream fills at its own rate, on its own clock.** What full-slice scale does change is how many independent trim clocks are running simultaneously (43 depth + 43 trades = 86 streams), which raises the odds that at least one hot stream (BTCUSDT) is the one that trims first.
- Binance depth cadence: `{symbol}@depth@100ms` on spot/USDT-M/COIN-M → **10 messages/sec/symbol**, uniform across all 23 Binance symbols (`docs/04-architecture/exchanges/binance.md`).
- Bybit depth cadence: `orderbook.200` "arrives every 100ms, like Binance's `@depth@100ms`" → **10 messages/sec/symbol**, uniform across all 20 Bybit symbols (`docs/04-architecture/exchanges/bybit.md`).
- Symbol counts: Binance 8 spot + 13 USDT-M + 2 COIN-M = 23. Bybit 7 spot + 11 linear + 2 inverse = 20 (per `bybit.md`'s own "spot 7×2=14, linear 11×4=44, inverse 2×4=8" breakdown).
- Neither exchange doc gives a trades/sec figure per symbol — that's genuinely absent from the docs.

**Assumptions (estimates, NOT in the docs — flagged explicitly):**
- Trades/sec per symbol scenarios (typical CEX order-of-magnitude for major vs. minor pairs, general market knowledge, not measured here):
  - **Low** (illiquid/minor pair, e.g. `1000PEPEUSDT` off-hours, `XAGUSDT`, inverse contracts): ~0.3 trades/sec
  - **Medium** (liquid major under normal conditions, e.g. `ETHUSDT`/`SOLUSDT`): ~3 trades/sec
  - **High** (BTCUSDT under normal active trading): ~15 trades/sec
  - **Burst/extreme** (BTCUSDT during a volatility spike/news event): ~75 trades/sec, assumed short-lived
- 5-10 researcher query-rate assumptions for the k6 section (below), also explicitly flagged.

## Time-to-maxlen-trim math

Formula: `t = maxlen / rate`. At current `REDIS_STREAM_MAXLEN=1,000,000`:

| Stream | Rate | Time to fill (seconds) | Time to fill (minutes) | Time to fill (hours) |
|---|---|---|---|---|
| **Depth** (any symbol, Binance or Bybit, uniform 100ms cadence) | 10 msg/s | 100,000 | **1,666.7 min** | 27.78 h (1.16 days) |
| Trades — Low | 0.3/s | 3,333,333 | 55,555.6 min | 925.9 h (38.6 days) |
| Trades — Medium | 3/s | 333,333 | 5,555.6 min | 92.6 h (3.86 days) |
| Trades — **High** (BTCUSDT normal-active) | 15/s | 66,667 | **1,111.1 min** | 18.52 h (0.77 days) |
| Trades — Burst | 75/s | 13,333 | 222.2 min | 3.70 h |

**Non-obvious finding:** under the "High" (not even burst) trades assumption, BTCUSDT's *trades* stream fills to maxlen faster (1,111 min / 18.5h) than *any* depth stream (1,667 min / 27.8h) — the ADR's original framing worried primarily about depth-diff loss, but a busy major pair's trade tape is at least as exposed, maybe more, depending on real trade rates that should be measured, not assumed.

**Bottom line:** at today's default, the system has somewhere between **~3.7 hours (burst) and ~28 hours (baseline depth, worst realistic sustained case ~18.5h)** of normalizer downtime before the first stream starts silently trimming unconsumed, unrecoverable order-book/trade history. Given ADR weak point #5 (no health signal for collector/normalizer — just `restart: unless-stopped`), a crash loop or a stuck-but-not-crashed process could plausibly go unnoticed for exactly this long, especially overnight or over a weekend.

## Recommendation

**1. Raise `REDIS_STREAM_MAXLEN` from 1,000,000 to 5,000,000 (5x).**

Reasoning: this stretches the tightest realistic scenario (High trades, 15/s) from 18.5h to **3.86 days**, and the depth-stream baseline from 27.8h to **5.79 days** — comfortably past a weekend outage. Even the burst scenario (75/s) moves from 3.7h to 18.5h, now within reach of a same-day fix rather than "already gone before anyone checks." Caveat: Redis runs with `maxmemory-policy noeviction` and **no `maxmemory` cap set** in `docker-compose.yml` — raising maxlen 5x raises the worst-case per-stream memory footprint proportionally (roughly 1KB/entry for a typical JSON depth-diff/trade envelope → ~5GB/stream if a single stream maxed out; unlikely all 86 streams do so simultaneously, but worth pairing this change with a `maxmemory` limit + eviction/growth monitoring rather than raising maxlen unboundedly).

**2. Add a consumer-lag alert using Redis 7's `XINFO GROUPS <stream>` `lag` field** (exact lag since entries-added tracking, available on the `redis:7.4.1-alpine` image already in use), checked per symbol/stream/group:
- **WARNING at lag ≥ 1,000,000** (20% of the new 5,000,000 maxlen) — leaves ~2.96–4.6 days of runway depending on stream/scenario, ample time to notice and restart a stuck normalizer even unattended over a weekend.
- **CRITICAL/page at lag ≥ 2,500,000** (50% of maxlen) — even in the burst trades scenario this still leaves ~9.3 hours before trimming starts; in the depth/baseline case, ~2.9 days.

This turns weak point #1 from "silent" into "paged with hours to days of lead time," and gives a concrete number for a `consumer_lag` incident type, opened at the WARNING threshold, distinct from `backfill_gap`.

## k6 script for the REST read path

Endpoints and exact params read from `src/api/rest.py`: `/trades` (`symbol, ts_from, ts_to, limit≤10000, exchange, segment`), `/orderbook/at` (`symbol, ts, exchange, segment`), `/orderbook/events` (`symbol, ts_from, ts_to, limit≤10000, cursor, exchange, segment`), `/incidents` (`symbol, ts_from, ts_to, exchange, segment`).

**Assumption (explicit, not in docs):** 5-10 quant researchers doing point-in-time/exploratory analysis (not automated high-frequency polling — MVP explicitly targets human researchers per `03-mvp-scope.md`) generate organic load on the order of **1 query per 5-15s per active user** when working, i.e. ~1.25 req/s sustained at 10 concurrent users, with occasional script-driven bursts (e.g. someone looping `/orderbook/at` over a list of timestamps) up to ~10-20 req/s for short windows. Committing to a **load-test target of 20 req/s sustained** (~16x organic peak, standard headroom multiplier) and a **50 req/s, 30s burst** scenario, rather than leaving this "TBD."

```javascript
// k6 run rest-load-test.js
// Exercises GET /trades, /orderbook/at, /orderbook/events, /incidents
// against the market-data-service REST API (Traefik-routed: api.localhost).
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://api.localhost';

// Symbol pool spanning the full Binance + Bybit slice, mixing hot (BTCUSDT)
// and cold (1000PEPEUSDT, XAGUSDT) pairs since latency/row-scan cost differs.
const SYMBOLS = [
  'BTCUSDT', 'ETHUSDT', 'SOLUSDT', '1000PEPEUSDT', 'XAGUSDT', // binance
  'BTCPERP', 'ETHPERP', 'BTCUSD', // bybit
];

const errorRate = new Rate('errors');
const tradesLatency = new Trend('trades_latency', true);
const obAtLatency = new Trend('orderbook_at_latency', true);
const obEventsLatency = new Trend('orderbook_events_latency', true);
const incidentsLatency = new Trend('incidents_latency', true);

export const options = {
  scenarios: {
    steady_state: {
      executor: 'constant-arrival-rate',
      rate: 20,               // target: 20 req/s sustained (see assumption above)
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 50,
      maxVUs: 100,
      exec: 'mixedQueries',
    },
    burst: {
      executor: 'ramping-arrival-rate',
      startTime: '5m30s',      // runs after steady_state finishes
      startRate: 20,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 200,
      stages: [
        { target: 50, duration: '15s' },
        { target: 50, duration: '30s' },
        { target: 20, duration: '15s' },
      ],
      exec: 'mixedQueries',
    },
  },
  thresholds: {
    // Targets, per endpoint (assumption-driven, committed numbers):
    'trades_latency': ['p(50)<150', 'p(99)<800'],
    'orderbook_at_latency': ['p(50)<300', 'p(99)<1500'], // heavier: snapshot+replay reconstruction
    'orderbook_events_latency': ['p(50)<150', 'p(99)<800'],
    'incidents_latency': ['p(50)<50', 'p(99)<300'],
    'errors': ['rate<0.01'],   // <1% error rate under steady_state
  },
};

function randomSymbol() {
  return SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)];
}

// Random point-in-time within the last ~24h, ISO-8601 (matches queries.parse_ts).
function randomTs(withinHours = 24) {
  const now = Date.now();
  const offsetMs = Math.floor(Math.random() * withinHours * 3600 * 1000);
  return new Date(now - offsetMs).toISOString();
}

export function mixedQueries() {
  const symbol = randomSymbol();
  const roll = Math.random();

  if (roll < 0.35) {
    // GET /trades?symbol=&ts_from=&ts_to=&limit=
    const tsTo = randomTs(1);
    const tsFrom = new Date(new Date(tsTo).getTime() - 5 * 60 * 1000).toISOString();
    const res = http.get(
      `${BASE_URL}/trades?symbol=${symbol}&ts_from=${tsFrom}&ts_to=${tsTo}&limit=500`,
      { tags: { name: 'trades' } }
    );
    tradesLatency.add(res.timings.duration);
    errorRate.add(res.status >= 400);
    check(res, { 'trades status 200': (r) => r.status === 200 });
  } else if (roll < 0.60) {
    // GET /orderbook/at?symbol=&ts= -- the point-in-time reconstruction endpoint,
    // the MVP's headline feature (03-mvp-scope.md item 1).
    const res = http.get(
      `${BASE_URL}/orderbook/at?symbol=${symbol}&ts=${randomTs(6)}`,
      { tags: { name: 'orderbook_at' } }
    );
    obAtLatency.add(res.timings.duration);
    errorRate.add(res.status >= 400 && res.status !== 404); // 404 (no data yet) is expected, not an error
    check(res, { 'orderbook_at status ok': (r) => r.status === 200 || r.status === 404 });
  } else if (roll < 0.90) {
    // GET /orderbook/events?symbol=&ts_from=&ts_to=&limit=&cursor=
    // Simulates a researcher paginating through a window of raw diff events.
    const tsTo = randomTs(1);
    const tsFrom = new Date(new Date(tsTo).getTime() - 2 * 60 * 1000).toISOString();
    const res = http.get(
      `${BASE_URL}/orderbook/events?symbol=${symbol}&ts_from=${tsFrom}&ts_to=${tsTo}&limit=1000`,
      { tags: { name: 'orderbook_events' } }
    );
    obEventsLatency.add(res.timings.duration);
    errorRate.add(res.status >= 400);
    check(res, { 'orderbook_events status 200': (r) => r.status === 200 });

    // Adversarial case per weak point #6: someone hitting the max limit.
    if (Math.random() < 0.1) {
      const wideRes = http.get(
        `${BASE_URL}/orderbook/events?symbol=${symbol}&ts_from=${tsFrom}&ts_to=${tsTo}&limit=10000`,
        { tags: { name: 'orderbook_events_max_limit' } }
      );
      errorRate.add(wideRes.status >= 400);
    }
  } else {
    // GET /incidents?symbol=&ts_from= -- cheap, small-table lookup.
    const res = http.get(
      `${BASE_URL}/incidents?symbol=${symbol}&ts_from=${randomTs(48)}`,
      { tags: { name: 'incidents' } }
    );
    incidentsLatency.add(res.timings.duration);
    errorRate.add(res.status >= 400);
    check(res, { 'incidents status 200': (r) => r.status === 200 });
  }

  sleep(0.1);
}
```

## Redis-lag simulation harness (concrete sketch)

Uses the actual service names from `docker-compose.yml` (`redis`, `normalizer`, `collector`) and the real stream-naming convention from `normalizer/config.py`/`collector/config.py` (`raw:{exchange}:{symbol}:trades` / `:depth`, in Redis logical DB 1 per `REDIS_STREAM_DB=1`), and the real consumer-group name (`normalizer:{exchange}:{segment}:{symbol}`).

```bash
#!/usr/bin/env bash
# lag_sim.sh — measure real time-to-maxlen-trim for one symbol's streams by
# actually pausing the normalizer while the collector keeps producing.
set -euo pipefail

EXCHANGE="${EXCHANGE:-binance}"
SEGMENT="${SEGMENT:-spot}"
SYMBOL_LC="${SYMBOL:-btcusdt}"           # lowercase, matches stream_prefix
GROUP="normalizer:${EXCHANGE}:${SEGMENT}:${SYMBOL_LC}"
DEPTH_STREAM="raw:${EXCHANGE}:${SYMBOL_LC}:depth"
TRADES_STREAM="raw:${EXCHANGE}:${SYMBOL_LC}:trades"
REDIS_DB=1
LOG="lag_sim_$(date +%s).csv"

redis_cli() { docker compose exec -T redis redis-cli -n "$REDIS_DB" "$@"; }

echo "ts,depth_xlen,depth_lag,trades_xlen,trades_lag" > "$LOG"

echo "== baseline before stopping normalizer =="
redis_cli XLEN "$DEPTH_STREAM"
redis_cli XLEN "$TRADES_STREAM"
redis_cli XINFO GROUPS "$DEPTH_STREAM"
redis_cli XINFO GROUPS "$TRADES_STREAM"

echo "== stopping normalizer only; collector + redis + clickhouse stay up =="
docker compose stop normalizer

# Poll XLEN and the group's `lag` field (exact, Redis 7+, entries-added-based)
# every 30s until a stream's XLEN plateaus at/above maxlen (the trim signal)
# or a hard ceiling is reached.
MAXLEN="${REDIS_STREAM_MAXLEN:-1000000}"
CEILING_S=$((48 * 3600))   # don't run forever; 48h ceiling for this manual pass
START=$(date +%s)

while true; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))

  DEPTH_XLEN=$(redis_cli XLEN "$DEPTH_STREAM")
  TRADES_XLEN=$(redis_cli XLEN "$TRADES_STREAM")
  DEPTH_LAG=$(redis_cli XINFO GROUPS "$DEPTH_STREAM" | awk '/lag/{getline; print; exit}')
  TRADES_LAG=$(redis_cli XINFO GROUPS "$TRADES_STREAM" | awk '/lag/{getline; print; exit}')

  echo "$NOW,$DEPTH_XLEN,$DEPTH_LAG,$TRADES_XLEN,$TRADES_LAG" >> "$LOG"
  echo "[+${ELAPSED}s] depth xlen=$DEPTH_XLEN lag=$DEPTH_LAG | trades xlen=$TRADES_XLEN lag=$TRADES_LAG"

  # Trim onset: XLEN stops growing (or hovers near maxlen due to
  # approximate=True's macro-node trimming) while the producer is still
  # writing -- that's the signal, not just "XLEN == maxlen" exactly.
  if [ "$DEPTH_XLEN" -ge "$MAXLEN" ] || [ "$TRADES_XLEN" -ge "$MAXLEN" ]; then
    echo "TRIM THRESHOLD REACHED at +${ELAPSED}s ($(python3 -c "print(${ELAPSED}/60,'min')"))"
    break
  fi
  if [ "$ELAPSED" -ge "$CEILING_S" ]; then
    echo "Ceiling reached without trimming -- stream is safe for at least ${CEILING_S}s at current rate."
    break
  fi
  sleep 30
done

echo "== confirming actual unrecoverable loss (not just XLEN==maxlen) =="
# The real loss event is when the group's pending/last-delivered position is
# now BELOW the stream's current first-entry id -- i.e. entries the
# normalizer hadn't yet read are now gone, not just that the stream is full.
redis_cli XINFO STREAM "$DEPTH_STREAM" FULL | head -50

echo "== restarting normalizer, confirming it resumes and drains backlog =="
docker compose start normalizer
sleep 10
redis_cli XINFO GROUPS "$DEPTH_STREAM"
redis_cli XINFO GROUPS "$TRADES_STREAM"

echo "Log written to $LOG"
```

For the **synthetic/accelerated variant** (to get "High"/"Burst" scenario numbers in minutes instead of waiting real hours/days), replace the "collector stays up" step with a small Python producer that XADDs at a controlled, amplified rate against a disposable test stream, so the measured time-to-trim can be extrapolated back to real rates without waiting out the actual 18-28 hour windows:

```python
# synthetic_producer.py — amplified-rate XADD against a disposable stream,
# for fast, controlled measurement of time-to-trim at chosen rates.
import asyncio
import json
import time
import redis.asyncio as redis

STREAM = "raw:binance:btcusdt:depth:LOADTEST"  # disposable, not the real stream
MAXLEN = 1_000_000
RATE_PER_SEC = 500  # e.g. 50x the real 10/s baseline, to compress 27.8h -> ~33min

async def main():
    r = redis.from_url("redis://localhost:6379", db=1, decode_responses=True)
    interval = 1.0 / RATE_PER_SEC
    n = 0
    start = time.monotonic()
    while True:
        payload = {"type": "depth_diff", "receive_ts": int(time.time() * 1000), "raw": {"synthetic": True, "n": n}}
        await r.xadd(STREAM, {"payload": json.dumps(payload)}, maxlen=MAXLEN, approximate=True)
        n += 1
        if n % 10_000 == 0:
            xlen = await r.xlen(STREAM)
            elapsed = time.monotonic() - start
            print(f"n={n} xlen={xlen} elapsed={elapsed:.1f}s")
            if xlen >= MAXLEN:
                print(f"TRIM THRESHOLD at {elapsed:.1f}s wall clock, "
                      f"implies {elapsed * (RATE_PER_SEC/10):.0f}s (~{elapsed*(RATE_PER_SEC/10)/60:.1f} min) at real 10/s rate")
                break
        await asyncio.sleep(interval)

asyncio.run(main())
```

## Key file references

- `src/collector/sink.py` — unconditional `XADD ... maxlen` trim
- `src/normalizer/consumer.py` — XREADGROUP/XACK/at-most-once loss-window logic
- `src/normalizer/config.py` — `stream_prefix` (per-symbol keying, the key architectural fact), `sink_batch_size=500`, `sink_flush_interval_s=2`
- `src/collector/config.py`, `runner.py` — `stream_maxlen`, per-symbol stream naming confirmed on the producer side too
- `.env.example`, `docker-compose.yml` — `REDIS_STREAM_MAXLEN=1000000`, `REDIS_STREAM_DB=1`, service names (`redis`, `normalizer`, `collector`), no `maxmemory` cap set alongside `noeviction`
- `src/api/rest.py` — exact query params for `/trades`, `/orderbook/at`, `/orderbook/events`, `/incidents`
- [Binance map](./04-architecture/exchanges/binance.md), [Bybit map](./04-architecture/exchanges/bybit.md) — symbol lists, 100ms depth cadence facts
- [MVP Scope](./03-mvp-scope.md) — 5-10 user success criterion
- [ADR-001](./09-hardening-tests-load-auth.md) — the ADR this analysis fills in
