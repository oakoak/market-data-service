"""In-memory order-book state tracker for a single (exchange, segment, symbol).

Responsibilities (task scope, Binance spot only):
- Apply the REST snapshot pushed by the collector as a `snapshot` message.
- Apply depth-diff events on top of it, maintaining a full price->qty book.
- Detect `orderbook_sequence_break` per docs/04-architecture/exchanges/
  binance.md step 5 (spot continuity: current.U == previous.u + 1) and
  docs/04-architecture/00-overview.md §6.3.1.
- Periodically hand back a full-book snapshot row for
  market_data.orderbook_snapshots (docs §6.4: every 1-5 min; interval is
  configurable, see config.book_snapshot_interval_s).

Resync strategy on a detected break (see module docstring in incidents.py
for the incident side): this prototype **waits for the collector's next
`snapshot` message** rather than calling the Binance REST endpoint directly
from the normalizer. Rationale (asked for in the task to be justified):
  1. The collector already owns REST snapshot fetching end-to-end (see
     BinanceSpotAdapter.fetch_snapshot / CollectorRunner._bootstrap_snapshot)
     including the buffer-until-snapshot-ready dance and rate-limit
     awareness (binance.md's REST weight budget). Duplicating that call
     from the normalizer means a second, uncoordinated consumer of
     Binance's per-IP REST weight and a second place that has to know
     `GET /api/v3/depth?limit=1000` semantics.
  2. The collector's reconnect loop already forces a fresh snapshot on
     every WS reconnect (CollectorRunner._apply_snapshot), so restarting
     just the WS connection would also naturally produce a new snapshot
     -- but this prototype doesn't have a channel to ask the collector to
     reconnect. Given the task's own suggested fallback ("wait for the
     next snapshot message OR call REST yourself -- pick whichever is
     architecturally cleaner"), waiting keeps the normalizer a pure
     consumer (matches the collector/normalizer split in
     00-overview.md §6.2: REST snapshot fetching is collector-adjacent
     I/O, not the normalizer's job) and avoids a second REST calling
     path with its own error handling/backoff to maintain.
  Trade-off (documented, not silently ignored): if the collector's WS
  connection is fine and doesn't reconnect on its own, no new `snapshot`
  message will ever arrive, so the book state stays stale/discarded
  until the next reconnect. That's acceptable for this prototype's single-
  instrument scope; a production version would ask the collector for an
  on-demand resync trigger (e.g. a Redis key/command channel) instead.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class SequenceBreak:
    last_valid_update_id: int
    received_update_id_u: int
    received_update_id_U: int


@dataclass
class OrderBookState:
    exchange: str
    segment: str
    symbol: str

    _bids: dict[float, float] = field(default_factory=dict)
    _asks: dict[float, float] = field(default_factory=dict)
    last_update_id: int | None = None
    ready: bool = False  # True once a snapshot has been applied
    awaiting_resync: bool = False  # True after a detected sequence break
    # False right after apply_snapshot() until the anchor event (binance.md
    # step 4: U <= lastUpdateId+1 <= u) has been accepted; True afterwards,
    # at which point the strict per-event continuity check (step 5) applies.
    _bootstrapped: bool = False

    def apply_snapshot(self, row: dict[str, Any]) -> None:
        """`row` is a parser.parse_snapshot(...) output dict."""
        self._bids = {p: q for p, q in row["bids"] if q > 0}
        self._asks = {p: q for p, q in row["asks"] if q > 0}
        self.last_update_id = row["last_update_id"]
        self.ready = True
        self.awaiting_resync = False
        self._bootstrapped = False

    def apply_diff(self, row: dict[str, Any]) -> SequenceBreak | None:
        """`row` is a parser.parse_depth_diff(...) output dict.

        Returns a SequenceBreak if continuity was violated (caller is
        responsible for raising the incident and triggering resync); returns
        None and applies the diff to book state otherwise.

        If the book isn't ready yet (no snapshot applied) or is already
        awaiting resync, the diff is simply not applied to book state (it's
        still stored to ClickHouse by the caller regardless -- raw diff
        history is always persisted per docs §6.5, only *book state*
        reconstruction pauses).

        Bootstrap handling (binance.md steps 3-5): right after a snapshot is
        applied, the first event is not held to the strict `U == prev.u + 1`
        continuity rule (that's a general-purpose comparison to a *previous
        event*, and there is no previous event yet). Instead:
          - events that predate the snapshot (`u <= lastUpdateId`) are stale
            and silently discarded (not a break, not applied);
          - the first non-stale event must straddle the snapshot
            (`U <= lastUpdateId+1 <= u`) to be accepted as the anchor;
          - anything else at this stage (e.g. a gap where even the first
            available event's `U` is already past `lastUpdateId+1`) is a
            genuine sequence break.
        Once the anchor has been accepted, `_bootstrapped` is True and every
        subsequent event goes through the strict `U == prev.u + 1` check.
        """
        if not self.ready or self.awaiting_resync:
            return None

        first_update_id = row["first_update_id"]
        final_update_id = row["final_update_id"]

        assert self.last_update_id is not None

        if not self._bootstrapped:
            # binance.md step 3: discard events that predate the snapshot.
            if final_update_id <= self.last_update_id:
                return None

            # binance.md step 4: the first applied event must straddle the
            # snapshot's lastUpdateId -- checked once, as a range, not with
            # equality.
            if first_update_id <= self.last_update_id + 1 <= final_update_id:
                self._apply_levels(self._bids, row["bids"])
                self._apply_levels(self._asks, row["asks"])
                self.last_update_id = final_update_id
                self._bootstrapped = True
                return None

            # Neither stale nor a valid straddle -- there's a gap between
            # the snapshot and the earliest available diff event.
            brk = SequenceBreak(
                last_valid_update_id=self.last_update_id,
                received_update_id_u=final_update_id,
                received_update_id_U=first_update_id,
            )
            self.awaiting_resync = True
            return brk

        # binance.md step 5, spot: current.U == previous.u + 1
        if first_update_id != self.last_update_id + 1:
            brk = SequenceBreak(
                last_valid_update_id=self.last_update_id,
                received_update_id_u=final_update_id,
                received_update_id_U=first_update_id,
            )
            self.awaiting_resync = True
            return brk

        self._apply_levels(self._bids, row["bids"])
        self._apply_levels(self._asks, row["asks"])
        self.last_update_id = final_update_id
        return None

    @staticmethod
    def _apply_levels(book_side: dict[float, float], levels: list[tuple[float, float]]) -> None:
        for price, qty in levels:
            if qty == 0:
                book_side.pop(price, None)
            else:
                book_side[price] = qty

    def to_snapshot_row(self, ts_ms: int | None = None) -> dict[str, Any]:
        """Materialize current book state as a market_data.orderbook_snapshots
        row (periodic full-book snapshot, docs §6.4)."""
        from datetime import datetime, timezone

        ts_ms = ts_ms if ts_ms is not None else int(time.time() * 1000)
        ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        bids = sorted(self._bids.items(), key=lambda kv: -kv[0])
        asks = sorted(self._asks.items(), key=lambda kv: kv[0])
        return {
            "exchange": self.exchange,
            "segment": self.segment,
            "symbol": self.symbol,
            "last_update_id": self.last_update_id,
            "bids": bids,
            "asks": asks,
            "ts_exchange": ts,
            "ts_received": ts,
        }
