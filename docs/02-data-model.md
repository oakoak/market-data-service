# Data Model: Instrument × Data Type

Two independent axes:
- **Instrument type** (what is traded): spot, perpetual (perp), futures (with expiry), options.
- **Data type** (how the activity is recorded): trades, OHLCV, order book (L2/L3).

Each instrument is an independent, separate order book and trade tape (BTC spot ≠ BTC perp ≠ BTC futures ≠ each options contract).

| | Trades | OHLCV | Order book L2 | Order book L3 | Instrument-specific data |
|---|---|---|---|---|---|
| Spot | ✓ raw tape | derived from trades | ✓ price-level aggregation | practically not published on crypto exchanges | — |
| Perpetual | ✓ | ✓ | ✓ | no | funding rate, open interest, mark/index price, liquidations |
| Futures (with expiry) | ✓ | ✓ | ✓ | no | open interest, mark/index price, liquidations (usually no funding) |
| Options | ✓ (often thin/empty book) | ✓ | ✓ | no | OI by strike, implied volatility, greeks |

**Important notes:**
- OHLCV is not primary data, but an aggregate computed from trades.
- L3 is practically not published publicly anywhere in crypto — the only realistic promise is **L2**.
- Options have an order of magnitude more instruments per underlying (hundreds of strike×expiry×put/call combinations), plus separate derived metrics (IV, greeks). Not "just another instrument" but a separate, sizeable block of work — out of scope for MVP.
