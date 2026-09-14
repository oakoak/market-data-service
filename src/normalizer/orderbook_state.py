"""In-memory order-book state tracker for a single (exchange, segment, symbol).

Responsibilities (Phase B, docs/08-prototype-roadmap.md: spot + USDT-M perp):
- Apply the REST snapshot pushed by the collector as a `snapshot` message.
- Apply depth-diff events on top of it, maintaining a full price->qty book.
- Detect `orderbook_sequence_break` per docs/04-architecture/exchanges/
  binance.md step 5 and docs/04-architecture/00-overview.md §6.3.1, using an
  exchange/segment-aware continuity check (see `_check_continuity` below):
  spot uses `current.U == previous.u + 1`; USDT-M/COIN-M futures use
  `current.pu == previous.u` (the `pu` field -- absent on spot, always
  present on futures diffs per binance.md).
- Periodically hand back a full-book snapshot row for
  market_data.orderbook_snapshots (docs §6.4: every 1-5 min; interval is
  configurable, see config.book_snapshot_interval_s).

Continuity strategy generalization (Phase B): the original prototype
hardcoded the spot `U == previous.u + 1` rule directly in `apply_diff`.
Binance futures uses a structurally different rule (`pu == previous.u`,
via a field spot doesn't even have) per binance.md step 5. Rather than
branching on `segment` string values inline (which would tie this class to
Binance's specific segment names, e.g. 'usdtm' vs a hypothetical
Bybit 'linear'), continuity is expressed as a small strategy selected once
at construction time from a `continuity` parameter: `"spot"` picks the
U==prev.u+1 rule, `"futures"` picks the pu==prev.u rule. The caller
(consumer.py) decides which to pass based on whether `prev_update_id` is
populated on the parsed rows for this deployment (i.e. whether the source
segment is a Binance futures segment) -- this keeps OrderBookState itself
free of Binance-specific segment-name string matching, and ready for a
future Bybit `seq`-based continuity strategy to be added the same way
without touching the two existing ones.

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
from typing import Any, Literal

ContinuityMode = Literal["spot", "futures"]


@dataclass
class SequenceBreak:
    last_valid_update_id: int
    received_update_id_u: int
    received_update_id_U: int
    # Which field the continuity check used -- surfaced so the incident
    # `details` payload can be accurate for both spot (`U`) and futures
    # (`pu`) breaks instead of always describing a spot-shaped break.
    continuity_mode: ContinuityMode = "spot"
    received_prev_update_id: int | None = None


@dataclass
class OrderBookState:
    exchange: str
    segment: str
    symbol: str

    # Which continuity rule this instance enforces -- see module docstring.
    # Defaults to "spot" (the original behavior) so existing spot callers
    # need no change; USDT-M/COIN-M deployments pass continuity="futures".
    continuity: ContinuityMode = "spot"

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
        applied, the first event is not held to the strict continuity rule
        (that's a general-purpose comparison to a *previous event*, and
        there is no previous event yet). Instead:
          - events that predate the snapshot (`u <= lastUpdateId`) are stale
            and silently discarded (not a break, not applied);
          - the first non-stale event must straddle the snapshot
            (`U <= lastUpdateId+1 <= u`) to be accepted as the anchor;
          - anything else at this stage (e.g. a gap where even the first
            available event's `U` is already past `lastUpdateId+1`) is a
            genuine sequence break.
        This bootstrap straddle check is identical for spot and futures
        (binance.md steps 1-4 don't differentiate) -- only step 5's ongoing
        continuity rule differs, see `_check_continuity`.
        Once the anchor has been accepted, `_bootstrapped` is True and every
        subsequent event goes through `_check_continuity`.
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
            # equality. Identical for spot and futures.
            if first_update_id <= self.last_update_id + 1 <= final_update_id:
                self._apply_levels(self._bids, row["bids"])
                self._apply_levels(self._asks, row["asks"])
                self.last_update_id = final_update_id
                self._bootstrapped = True
                return None

            # Neither stale nor a valid straddle -- there's a gap between
            # the snapshot and the earliest available diff event.
            self.awaiting_resync = True
            return SequenceBreak(
                last_valid_update_id=self.last_update_id,
                received_update_id_u=final_update_id,
                received_update_id_U=first_update_id,
                continuity_mode=self.continuity,
                received_prev_update_id=row.get("prev_update_id"),
            )

        brk = self._check_continuity(row)
        if brk is not None:
            self.awaiting_resync = True
            return brk

        self._apply_levels(self._bids, row["bids"])
        self._apply_levels(self._asks, row["asks"])
        self.last_update_id = final_update_id
        return None

    def _check_continuity(self, row: dict[str, Any]) -> SequenceBreak | None:
        """binance.md step 5, exchange/segment-aware:
          - spot:    current.U  == previous.u + 1
          - futures: current.pu == previous.u   (`pu` = Binance's
                     "previous update id" field; absent on spot, always
                     present on USDT-M/COIN-M diffs)
        Returns a SequenceBreak (not yet marked awaiting_resync -- the
        caller does that) on a violation, None if continuity holds.
        """
        assert self.last_update_id is not None
        first_update_id = row["first_update_id"]
        final_update_id = row["final_update_id"]

        if self.continuity == "futures":
            prev_update_id = row.get("prev_update_id")
            # A futures diff with no `pu` at all would itself be a data
            # anomaly (the field is mandatory on futures per binance.md) --
            # treat it as a break rather than silently falling back to the
            # spot rule, so a malformed/mis-routed message surfaces loudly
            # instead of corrupting book state.
            broke = prev_update_id is None or prev_update_id != self.last_update_id
        else:
            broke = first_update_id != self.last_update_id + 1

        if not broke:
            return None
        return SequenceBreak(
            last_valid_update_id=self.last_update_id,
            received_update_id_u=final_update_id,
            received_update_id_U=first_update_id,
            continuity_mode=self.continuity,
            received_prev_update_id=row.get("prev_update_id"),
        )

    @staticmethod
    def _apply_levels(book_side: dict[float, float], levels: list[tuple[float, float]]) -> None:
        for price, qty in levels:
            if qty == 0:
                book_side.pop(price, None)
            else:
                book_side[price] = qty

    def to_snapshot_row(self, ts_ms: int | None = None) -> dict[str, Any]:
        """Materialize current book state as a market_data.orderbook_snapshots
        row (docs §6.4 periodic snapshot)."""
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
