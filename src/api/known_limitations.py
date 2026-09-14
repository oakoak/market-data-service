"""Static list backing `GET /known-limitations` / the `list_known_limitations`
data (docs §6.3.1: "a static config/JSON is sufficient" -- no DB table).

Phase B (docs/08-prototype-roadmap.md) added Binance USDT-M perp `forceOrder`
liquidation collection (src/normalizer/parser.py `parse_liquidation`,
infra/clickhouse/migrations/005_derivatives.sql `market_data.liquidations`).
Binance's `forceOrder` stream is not a full liquidation tape -- it emits only
the single largest liquidation per symbol per 1000ms window; smaller ones in
the same window are permanently lost, and there is no complete REST history
to backfill them from (see docs/04-architecture/exchanges/binance.md and
docs/04-architecture/00-overview.md §6.3.1's "Example -- static record in
/known-limitations"). This is a structural property of the source, not a time-
ranged event, so it's declared here (with `since_ts`, no `end_ts`) rather than
as an `/incidents` record. Kept as its own module (rather than a literal in
queries.py) so a later pass can grow this list without touching query logic.
"""

from __future__ import annotations

from typing import Any

KNOWN_LIMITATIONS: list[dict[str, Any]] = [
    {
        "exchange": "binance",
        "type": "liquidation_partial_coverage",
        "affected_channel": "liquidations",
        "source_channel": "forceOrder",
        "symbol": None,
        "since_ts": "2026-09-14T00:00:00.000Z",
        "description": (
            "forceOrder only emits the largest liquidation per 1000ms per "
            "symbol; smaller ones in the same window are lost permanently, "
            "no complete REST history exists"
        ),
    },
]
