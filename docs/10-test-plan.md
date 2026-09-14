# Test Plan — Market Data Service Prototype

Companion document to [ADR-001](./09-hardening-tests-load-auth.md#1-tests). Produced by a full read of `orderbook_state.py`, `parser.py`, `incidents.py`, `consumer.py`, `clickhouse_sink.py`, `api/queries.py`, the Binance collector adapter, and all four ClickHouse migrations — every test case below cites the specific source lines/behavior it guards. No code was changed to produce this document.

# Test Plan Research — Market Data Service Prototype

Full-source read completed for all modules listed, plus all four ClickHouse migrations, `docs/09-hardening-tests-load-auth.md`, and `docs/05-risks.md` item 8. No files were modified. Below is the exhaustive, no-gray-areas test plan.

---

## 0. Key facts that shape every test below

- **No test infra exists at all**: no `tests/`, no `pytest`/`pytest-asyncio`/`testcontainers` in `requirements.txt`, no `pyproject.toml`/`setup.cfg`. Imports like `from normalizer import incidents` only resolve because `src/` is presumably the container `WORKDIR`/on `PYTHONPATH` at runtime — there is no packaging config that makes this work for a test runner invoked from the repo root. This is a blocker (see §6).
- `trades.quote_qty` is a ClickHouse `DEFAULT price * quantity` **computed server-side on insert** — `clickhouse_sink.py`'s `TRADES_COLUMNS` correctly omits it, and `queries.get_trades` correctly selects it back. Not a bug; a fixture/assertion gotcha (unit tests for `clickhouse_sink.add_trade` must not expect `quote_qty` in the inserted column list; integration tests must expect it in read-back rows).
- `clickhouse_sink._flush_table` pops the buffer **before** calling `.insert()`. On insert failure the batch is logged as "dropping batch" and the exception is **re-raised** — silently swallowed by `_periodic_flush`'s `except Exception` (batch permanently lost) or propagated to `consumer.py`'s `_process_one`, which catches it, logs, and **XACKs anyway** (also lost, and this is a *second*, undocumented data-loss path beyond the two named in `docs/05-risks.md` item 8 and `consumer.py`'s own docstring — worth surfacing to whoever owns the ADR, not just testing).
- All I/O objects (`redis.asyncio.Redis`, `AsyncClient` from `clickhouse_connect`) are constructed inside the classes that use them (`ClickHouseSink.connect()`, `RedisStreamSink.connect()`, `ClickHouseReadClient.connect()`) rather than injected — a real dependency-injection blocker, detailed in §6.

---

## 1. `src/normalizer/orderbook_state.py` — unit tests

File: `tests/unit/test_orderbook_state.py`

1. `test_apply_snapshot_sets_ready_and_clears_awaiting_resync` — construct `OrderBookState`, call `apply_snapshot` with a `parse_snapshot`-shaped dict; assert `ready is True`, `awaiting_resync is False`, `_bootstrapped is False`, `last_update_id` set, bid/ask dicts populated. Guards `apply_snapshot` lines 73-80.
2. `test_apply_snapshot_drops_zero_qty_levels` — snapshot row with a `(price, 0.0)` pair in `bids`; assert that price is absent from `_bids`. Guards line 75-76's `if q > 0` filter (snapshot semantics differ from diff semantics — snapshots should never legitimately contain qty=0, but the state must not crash/store it if they do).
3. `test_apply_diff_before_snapshot_returns_none_and_is_noop` — call `apply_diff` on a fresh `OrderBookState` (`ready=False`); assert return is `None` and book dicts stay empty. Guards line 109-110.
4. `test_apply_diff_while_awaiting_resync_returns_none_and_is_noop` — manually set `awaiting_resync=True` after a snapshot; call `apply_diff`; assert `None` and no book mutation. Guards line 109-110 (second branch of the `or`).
5. `test_bootstrap_discards_stale_event_below_snapshot` — apply snapshot with `last_update_id=100`; apply diff with `final_update_id=99` (any `first_update_id`); assert return `None`, `_bootstrapped` stays `False`, `last_update_id` unchanged, book unmutated. Guards line 118-120 (`final_update_id <= self.last_update_id`).
6. `test_bootstrap_discards_stale_event_equal_to_snapshot` — same as #5 but `final_update_id == last_update_id` exactly (boundary `<=`). Distinct test because off-by-one is the highest-risk bug class here.
7. `test_bootstrap_accepts_straddling_anchor_event` — snapshot `last_update_id=100`; diff `first_update_id=95, final_update_id=105` (straddles: `95 <= 101 <= 105`); assert return `None`, `_bootstrapped becomes True`, `last_update_id == 105`, levels applied. Guards line 125-130.
8. `test_bootstrap_accepts_straddling_anchor_at_exact_boundary_U_eq_lastid_plus_1` — `first_update_id == last_update_id + 1` exactly (`U == 101`), `final_update_id` anything `>= 101`. Boundary case for the `<=` on the left side of the range check.
9. `test_bootstrap_accepts_straddling_anchor_at_exact_boundary_u_eq_lastid_plus_1` — `final_update_id == last_update_id + 1` exactly (`u == 101`), `first_update_id <= 101`. Boundary case for the right side.
10. `test_bootstrap_gap_produces_sequence_break` — snapshot `last_update_id=100`; diff `first_update_id=105, final_update_id=110` (gap: `105 > 101`); assert a `SequenceBreak` is returned with `last_valid_update_id=100, received_update_id_U=105, received_update_id_u=110`; assert `awaiting_resync becomes True`, `_bootstrapped` stays `False`. Guards line 132-140.
11. `test_bootstrap_break_leaves_book_state_unmutated` — same setup as #10; assert `_bids`/`_asks` are unchanged (bootstrap break must not partially apply levels).
12. `test_bootstrapped_happy_path_applies_sequential_diffs` — after accepting an anchor (per #7), apply a second diff with `first_update_id == last_update_id + 1`; assert levels merged, `last_update_id` advances, return `None`. Guards line 143, 152-155.
13. `test_bootstrapped_strict_continuity_break_on_gap` — after bootstrap, apply a diff whose `first_update_id != last_update_id + 1` (both higher and lower than expected, as two sub-cases/parametrized); assert `SequenceBreak` returned, `awaiting_resync becomes True`. Guards line 143-150.
14. `test_bootstrapped_break_does_not_apply_levels` — verify the rejected diff's bid/ask levels are **not** merged into the book (unlike the raw-event-always-persisted behavior in `consumer.py` — this test is scoped to `OrderBookState` only, asserting book *state* isn't corrupted even though the raw row is stored elsewhere).
15. `test_apply_levels_removes_zero_qty_price` — unit-test the static `_apply_levels` directly: a level `(50000.0, 0.0)` on a book side that already has `50000.0: 1.0`; assert the key is removed. Guards line 158-163 (`if qty == 0: pop`).
16. `test_apply_levels_adds_and_updates_nonzero_qty` — a new price gets added; an existing price's qty gets overwritten (not summed — Binance diffs are absolute qty at a price level, not deltas-to-add). Explicitly assert it's *replace*, not *increment*, since that's an easy semantic bug to introduce in a naive reimplementation.
17. `test_apply_levels_zero_qty_on_absent_price_is_noop` — `pop(price, None)` on a price never seen; assert no `KeyError` and dict unchanged. Guards the `None` default in line 161.
18. `test_to_snapshot_row_sorts_bids_descending_and_asks_ascending` — populate `_bids`/`_asks` with several out-of-order prices; call `to_snapshot_row()`; assert `bids` sorted by price descending, `asks` ascending. Guards line 172-173.
19. `test_to_snapshot_row_uses_given_ts_ms` — pass an explicit `ts_ms`; assert both `ts_exchange` and `ts_received` equal `datetime.fromtimestamp(ts_ms/1000, tz=utc)` (both fields identical — guards the "single timestamp source" design). Guards line 170-171, 181-182.
20. `test_to_snapshot_row_defaults_to_wallclock_when_ts_ms_omitted` — freeze/mock `time.time()` (or accept a tolerance window) and assert `ts_exchange`/`ts_received` land within it.
21. `test_to_snapshot_row_reflects_current_exchange_segment_symbol_and_last_update_id` — trivial field-passthrough assertion, but worth having explicitly since it's what feeds `orderbook_snapshots` — a silent field-name typo here breaks `get_orderbook_at`.
22. `test_full_bootstrap_then_break_then_new_snapshot_resets_bootstrapped_flag` — integration-of-the-unit test entirely within this module: snapshot → bootstrap accept → forced break → **new** `apply_snapshot()` call (simulating the resync collector sends) → assert `awaiting_resync` resets to `False`, `_bootstrapped` resets to `False`, and the *next* diff again goes through the anchor-straddle path (not the strict-continuity path) — this is the exact mechanism the module's own docstring (lines 13-40) describes and is worth locking down since it's the resync path the whole "wait for next snapshot" architecture decision depends on.
23. `test_decimal_precision_price_as_float_key_dict_collision` — edge case flagged by the task: prices arrive as `float` (parser converts Binance's string prices via `float()`). Construct two levels whose string forms are distinct but whose float reprs could collide/round (e.g. `"0.1" + "0.2"`-style artifacts are not applicable here since prices come pre-formed as floats from `_levels`, but test that a price like `29374.550000000003` vs `29374.55` — if the exchange ever sends inconsistent trailing-zero formatting — round-trips as the *same* dict key or knowingly as different ones). This test exists to document current behavior (float dict keys, no `Decimal`) as a known precision limitation, not necessarily to demand a fix — see §6 for whether `Decimal` should be adopted.

---

## 2. `src/normalizer/parser.py` — unit tests

File: `tests/unit/test_parser.py`

24. `test_parse_trade_happy_path_maps_all_fields` — full valid envelope (`t,p,q,m,b,a,T` all present); assert every output field name/type/value matches (int for `trade_id`, float for `price`/`quantity`, bool for `is_buyer_maker`, correct `ts_exchange`/`ts_received` from `T`/`receive_ts` respectively). Guards lines 34-46.
25. `test_parse_trade_missing_buyer_or_seller_order_id_is_none` — `data` with `"b"`/`"a"` absent (not just `None`) — via `.get("b")`; assert output `buyer_order_id`/`seller_order_id` are `None`, not `KeyError`. Guards line 42-43's `.get(...) is not None` guard — two sub-cases: key entirely absent vs. key present with JSON `null`.
26. `test_parse_trade_is_buyer_maker_coerces_truthy_falsy` — `data["m"]` as Python bool `True`/`False` (JSON booleans decode to real bools so this is mostly a passthrough check, but assert no silent stringy-truthiness bug if `m` ever arrives as `"true"`/`"false"` strings — test both shapes and document which one Binance actually sends and which one silently "works" via `bool(...)`).
27. `test_parse_trade_missing_required_field_raises_keyerror` — parametrize over dropping each of `t, p, q, m, T` from `data`, and dropping `receive_ts`/`exchange`/`segment`/`symbol` from the envelope; assert a `KeyError` (not a silent `None`/garbage row) — this documents current fail-loud behavior for consumer.py's catch-and-ack wrapper (see #52 below on what happens *after* the exception).
28. `test_parse_trade_non_numeric_price_or_qty_raises_valueerror` — `data["p"] = "not-a-number"`; assert `ValueError` from `float(...)`. Same for `data["t"] = "abc"` → `ValueError` from `int(...)`.
29. `test_parse_depth_diff_happy_path_maps_all_fields` — full valid `U,u,b,a,E,s` envelope; assert `first_update_id`/`final_update_id` ints, `prev_update_id is None` (spot has no `pu` — hardcoded), `bids`/`asks` as `(float, float)` tuple lists, timestamps correct.
30. `test_parse_depth_diff_empty_bids_and_asks_lists` — `data["b"] = []`, `data["a"] = []` (a depth-diff heartbeat/no-op update); assert output has empty lists, no crash. Guards `.get("b", [])` default and `_levels([])` returning `[]`.
31. `test_parse_depth_diff_missing_b_or_a_key_entirely` — Binance's docs say `b`/`a` should always be present, but test the `.get(..., [])` defensive default explicitly in case of a malformed/truncated message.
32. `test_parse_depth_diff_qty_zero_level_passthrough_untouched` — a level `["50000.00", "0.00000000"]`; assert it comes through as `(50000.0, 0.0)` in the output — parser does **not** filter zero-qty (that's `OrderBookState`'s job) — this is an explicit non-filtering contract worth locking down since it's easy to "fix" in the wrong layer later.
33. `test_levels_parses_string_price_qty_pairs` — direct unit test of `_levels()` helper: `[["1.5", "2.25"]]` → `[(1.5, 2.25)]`.
34. `test_levels_malformed_pair_raises` — a pair with wrong arity (`["1.5"]` or `["1.5","2","extra"]`) raises `ValueError` on unpack — document as fail-loud.
35. `test_parse_snapshot_happy_path_maps_all_fields` — full raw REST body; assert `last_update_id` int, `bids`/`asks` tuple lists, and **critically** `ts_exchange == ts_received` (both set to the same `received` value per the module's documented convention — line 90-91). This is a dedicated assertion, not implied by #29, since it's the one place the two dual-timestamp fields are *intentionally* identical and a regression (accidentally deriving `ts_exchange` from something else) would violate the documented invariant silently.
36. `test_parse_snapshot_empty_book` — `raw = {"lastUpdateId": 100, "bids": [], "asks": []}` (a real scenario for a thin/new listing); assert empty lists, no crash.
37. `test_parse_snapshot_missing_bids_asks_keys` — `raw` missing `"bids"`/`"asks"` entirely; assert `.get(..., [])` default kicks in (line 88-89) rather than `KeyError`, unlike `parse_trade`'s required fields — document this asymmetry (snapshot tolerates missing book arrays, trade/diff parsers don't tolerate missing required fields) as intentional.
38. `test_ms_to_dt_produces_utc_aware_datetime` — direct test of `_ms_to_dt`: input `0` → `1970-01-01T00:00:00+00:00`; assert `tzinfo is timezone.utc` explicitly (not just "has a tzinfo") — guards against any future refactor introducing naive datetimes, which would silently break ClickHouse `DateTime64(3,'UTC')` inserts.
39. `test_ms_to_dt_millisecond_precision_preserved` — input `1700000000123` → assert microsecond component is `123000` (float division by 1000.0 can introduce floating-point rounding error at the millisecond boundary — e.g. certain `ms` values could round to `.122999` instead of `.123000`); parametrize a handful of ms values known to be float-imprecise and assert round-trip via `int(dt.timestamp()*1000) == ms`.

---

## 3. `src/normalizer/incidents.py` — unit tests

File: `tests/unit/test_incidents.py`

40. `test_open_sequence_break_incident_field_shapes_match_schema` — call with all kwargs; assert **every** column name in `004_incidents.sql` (`id, exchange, type, severity, segment, symbol, affected_channel, start_ts, end_ts, status, description, detected_by, details`) is a key in the returned dict, with correct types (`id` is a `str` UUID — assert `uuid.UUID(result["id"])` doesn't raise; `type == "orderbook_sequence_break"`; `severity == "warning"`; `affected_channel == "orderbook"`; `end_ts is None`; `status == "open"`). This is the literal "schema round-trip test" the ADR calls for.
41. `test_open_sequence_break_incident_details_contains_source_channel_and_last_valid_update_id` — assert `details == {"source_channel": "depth", "last_valid_update_id": <value>}` exactly (no extra/missing keys) — guards field-name drift called out in the ADR's regression-test row.
42. `test_open_sequence_break_incident_defaults_start_ts_to_now_utc` — call without `start_ts`; assert result's `start_ts` is timezone-aware UTC and within a tolerance window of `datetime.now(timezone.utc)`.
43. `test_open_sequence_break_incident_accepts_explicit_start_ts` — pass a fixed `start_ts`; assert it's used verbatim (not overwritten).
44. `test_resolve_sequence_break_incident_preserves_id_and_sets_resolved_status` — build an open incident, resolve it; assert `id` unchanged, `status == "resolved"`, `end_ts` set, and the **original** dict object is not mutated in place (function returns a new dict — assert `open_incident["status"] == "open"` still after calling resolve, guarding the `{**incident, ...}` spread pattern at line 83-92 against an accidental future switch to in-place mutation, which would corrupt any caller still holding the original reference, e.g. `consumer.py`'s `self._open_sequence_break`).
45. `test_resolve_sequence_break_incident_adds_resync_source_to_details_without_losing_original_keys` — assert `details["source_channel"]` and `details["last_valid_update_id"]` from the open incident survive into the resolved dict's `details`, alongside the new `resync_source` key. Guards `dict(incident["details"])` copy-then-extend at line 81-82.
46. `test_resolve_sequence_break_incident_description_appends_rather_than_replaces` — assert the resolved `description` starts with the original open description's exact text plus the appended resync sentence (string, not reconstructed) — a common place for a rewrite to accidentally drop the original detail.
47. `test_open_disconnect_incident_field_shapes_match_schema` — same style as #40 for `collector_disconnect`: `type == "collector_disconnect"`, `affected_channel == "collector"`, `details == {"source_channel": "depth", "error": "<err>"}` when `error` given.
48. `test_open_disconnect_incident_with_none_error_omits_error_key_from_details` — call with `error=None`; assert `details == {"source_channel": "depth"}` (no `"error"` key at all, not `"error": None`) — guards the `if error:` conditional at line 116-117. Also test `error=""` (empty string, falsy) takes the same omission path — a boundary worth being explicit about since an empty-string error message from a real exception is plausible.
49. `test_open_disconnect_incident_description_uses_unknown_error_fallback_text` — `error=None` → assert `description` contains literally `"unknown error"` (line 129's `error or 'unknown error'`).
50. `test_resolve_disconnect_incident_computes_downtime_ms_from_start_and_end_ts` — construct an open incident with a known `start_ts`; resolve with a fixed `end_ts` exactly N milliseconds later; assert `details["downtime_ms"] == N` exactly (not off-by-rounding). Include a sub-case with sub-millisecond fractional seconds to check the `int(... * 1000)` truncation direction (floor vs round) is deterministic and documented.
51. `test_resolve_disconnect_incident_downtime_ms_is_never_negative_when_end_before_start` — pass an `end_ts` earlier than `start_ts` (a clock skew / bad input scenario); assert current behavior (likely a negative `downtime_ms`) explicitly, so this is a documented known gap rather than an untested surprise — flag as a candidate validation gap for the implementer to decide on (raise vs. clamp to 0).
52. `test_resolve_disconnect_incident_reconnect_attempts_passthrough` — assert `details["reconnect_attempts"]` equals exactly the int passed in (consumer.py always passes `1` — see incidents.py's own docstring caveat at lines 146-150 — but the function itself must not silently override it).
53. `test_details_to_json_produces_valid_json_with_stable_keys` — round-trip: `details_to_json({"a": 1, "b": "x"})` then `json.loads(...)` returns the same dict.
54. `test_details_to_json_serializes_non_jsonable_values_via_default_str` — pass a `details` dict containing a `datetime` or other non-JSON-native value (shouldn't normally happen given current callers, but `default=str` at line 169 implies it's anticipated); assert it doesn't raise and produces a string representation.
55. `test_incidents_schema_field_names_match_004_incidents_sql_columns` (regression test, per ADR's explicit ask) — parse `INCIDENTS_COLUMNS` from `clickhouse_sink.py` (or hardcode the migration's column list as a fixture) and assert it's a superset of/equal to the keys produced by both `open_sequence_break_incident` and `open_disconnect_incident` plus their resolve counterparts. This test is specifically designed to fail if a new field is added to the SQL/docs without updating `incidents.py`, or vice versa — exactly the ADR's stated goal for this test.

---

## 4. `src/normalizer/consumer.py` — unit tests

File: `tests/unit/test_consumer.py` (uses a fake/mock `ClickHouseSink` and, where needed, a fake Redis client — no real Redis/ClickHouse for this layer; that's the integration test's job).

**`StreamConsumer` (generic XREADGROUP loop) — needs a `FakeRedis`/mock `redis.asyncio.Redis`:**

56. `test_ensure_group_creates_group_with_mkstream_and_id_zero` — mock `xgroup_create`; assert called with `id="0", mkstream=True`.
57. `test_ensure_group_swallows_busygroup_error` — mock `xgroup_create` to raise `redis.ResponseError("BUSYGROUP Consumer Group name already exists")`; assert no exception propagates. Guards line 88-91.
58. `test_ensure_group_reraises_non_busygroup_response_error` — mock a different `ResponseError` (e.g. `"NOGROUP"` or a permissions error); assert it propagates. Guards the `if "BUSYGROUP" not in str(exc): raise`.
59. `test_process_one_calls_handler_with_parsed_json_payload_and_acks` — a fake entry with `fields={"payload": '{"type":"trade", ...}'}`; assert the handler coroutine is awaited with the parsed dict, and `xack` is called with the same `entry_id`.
60. `test_process_one_malformed_json_payload_still_acks_no_crash` — `fields={"payload": "{not json"}`; assert `json.loads` raises internally, is caught, logged, and `xack` is **still called** (poison-pill-doesn't-wedge-PEL behavior, explicitly documented at lines 124-130) — this is the single most important behavioral contract in this file to lock down, since it's a deliberate at-most-once trade-off.
61. `test_process_one_handler_exception_still_acks_no_crash` — handler raises `ValueError` (simulating a parser failure bubbling up); assert caught, logged, and acked anyway. Same contract as #60 from the other trigger.
62. `test_process_one_missing_payload_field_still_acks` — `fields={}` (no `"payload"` key at all — a genuinely malformed Redis entry); assert `KeyError` is caught by the broad `except Exception`, and ack still happens.
63. `test_run_calls_ensure_group_once_before_looping` — mock `ensure_group`; assert called exactly once at the top of `run()` (not per-iteration).
64. `test_run_empty_response_continues_without_processing` — mock `xreadgroup` to return `None`/`[]` on first call then raise `asyncio.CancelledError` to break the loop for the test; assert `_process_one` never called for that iteration.
65. `test_run_processes_multiple_entries_across_multiple_streams_in_response` — mock `xreadgroup` to return a response shaped like `[(stream_name, [(id1, fields1), (id2, fields2)])]`; assert `_process_one` called once per entry, in order.
66. `test_run_backs_off_one_second_on_xreadgroup_exception_and_continues` — mock `xreadgroup` to raise a generic `Exception` once then succeed; assert `asyncio.sleep(1.0)` is invoked (patch `asyncio.sleep`) and the loop doesn't crash. Guards line 107-110.
67. `test_run_propagates_cancelled_error_without_backoff` — mock `xreadgroup` to raise `asyncio.CancelledError`; assert it propagates immediately (no sleep, no swallow) — important because this is how `main.py`'s `finally: task.cancel()` shutdown is expected to work; a bug here would make the process unkillable/hang on shutdown.

**`NormalizerContext` — needs a `FakeClickHouseSink` (records calls, no real ClickHouse) and can use real `OrderBookState`/`incidents` functions:**

68. `test_handle_trade_happy_path_calls_sink_add_trade_with_parsed_row` — valid trade envelope; assert `sink.add_trade` called once with output of `parser.parse_trade`.
69. `test_handle_trade_wrong_envelope_type_is_noop` — `envelope["type"] = "depth_diff"` sent to `handle_trade` (routing bug simulation); assert `sink.add_trade` **not** called, just logged. Guards line 152-154.
70. `test_handle_depth_diff_always_persists_raw_row_regardless_of_book_continuity` — feed a depth-diff envelope that causes `apply_diff` to return a `SequenceBreak`; assert `sink.add_orderbook_event` is **still called** with the raw row (the "raw diff history always persisted" contract at line 174-177) — this is the single highest-value integration-of-units test in this file per the ADR's own framing.
71. `test_handle_depth_diff_opens_sequence_break_incident_on_first_break_only` — feed two consecutive depth-diff envelopes that both would trigger a `SequenceBreak` (book already in `awaiting_resync`); assert `sink.add_incident` is called exactly **once** (not once per subsequent broken diff) and `self._open_sequence_break is not None` after the first. Guards the `if brk is not None and self._open_sequence_break is None` gate at line 180.
72. `test_handle_depth_diff_no_incident_opened_when_book_not_yet_ready` — before any snapshot, feed a depth-diff; `apply_diff` returns `None` (book not ready); assert no incident, raw row still persisted (repeat of #70's assertion in this specific pre-snapshot state).
73. `test_handle_snapshot_applies_to_book_and_persists_snapshot_row` — feed a snapshot envelope; assert `sink.add_orderbook_snapshot` called and `ctx._book.ready is True` afterward.
74. `test_handle_snapshot_resolves_open_sequence_break_incident` — pre-seed `ctx._open_sequence_break` with a real open-incident dict (via `incidents.open_sequence_break_incident`); feed a snapshot; assert `sink.add_incident` called with a **resolved** incident (`status == "resolved"`, `resync_last_update_id` matches the new snapshot's `last_update_id`), and `ctx._open_sequence_break becomes None`. Guards lines 203-210.
75. `test_handle_snapshot_no_open_incident_is_noop_for_incident_path` — feed a snapshot with no pre-existing `_open_sequence_break`; assert `sink.add_incident` not called for the incident-resolution path (still called once for the snapshot row itself via `add_orderbook_snapshot` — disambiguate the two call sites in the assertion).
76. `test_handle_disconnect_opens_incident_and_tracks_it` — feed a `disconnect` envelope with `raw={"error": "connection reset"}`; assert `sink.add_incident` called with an open `collector_disconnect` incident and `ctx._open_disconnect is not None`.
77. `test_handle_disconnect_missing_raw_or_error_key_defaults_to_none` — `envelope = {"type": "disconnect"}` (no `"raw"` key at all); assert `.get("raw", {}).get("error")` chain doesn't raise (line 213) and the incident opens with `error=None` behavior (per test #48/#49's contract).
78. `test_handle_reconnect_resolves_tracked_disconnect_incident` — pre-seed `_open_disconnect`; feed a `reconnect` envelope; assert `sink.add_incident` called with a resolved incident, `reconnect_attempts=1` hardcoded (per the documented caveat), `_open_disconnect becomes None`.
79. `test_handle_reconnect_with_no_tracked_disconnect_is_noop` — feed `reconnect` with `ctx._open_disconnect is None` (simulating normalizer restart mid-incident, per the code's own comment at line 226-228); assert `sink.add_incident` not called, no exception.
80. `test_handle_depth_unclassified_type_logs_and_does_nothing` — `envelope["type"] = "something_else"`; assert none of the four `_handle_*` methods' sink calls fire.
81. `test_periodic_book_snapshot_loop_emits_when_book_ready_and_not_awaiting_resync` — bring the book to `ready=True, awaiting_resync=False`; run one iteration of the loop (patch `asyncio.sleep` to return immediately, then cancel after one iteration); assert `sink.add_orderbook_snapshot` called with `_book.to_snapshot_row()`'s shape.
82. `test_periodic_book_snapshot_loop_skips_when_book_not_ready` — book still `ready=False`; run one iteration; assert `add_orderbook_snapshot` **not** called. Guards line 243.
83. `test_periodic_book_snapshot_loop_skips_when_awaiting_resync` — book `ready=True` but `awaiting_resync=True`; assert skipped. Second half of the same guard.
84. `test_full_sequence_break_and_resync_cycle_end_to_end_within_context` — the "no gray areas" capstone test for this module: snapshot → bootstrap diff accepted → a diff that breaks continuity → assert incident opened & raw row persisted → a **new** snapshot arrives → assert incident resolved, book re-bootstraps, and a subsequent diff correctly goes through the anchor-straddle path again (not strict continuity) — mirrors orderbook_state test #22 but exercised through the full `NormalizerContext` handler chain including the incident side, which #22 deliberately doesn't touch.

---

## 5. `src/normalizer/clickhouse_sink.py` — unit tests

File: `tests/unit/test_clickhouse_sink.py`, using a **fake async ClickHouse client** (a small stub object whose `.insert(table, batch, column_names=...)` records calls and can be configured to raise) — do not use `clickhouse_connect.get_async_client` here; that belongs to the integration test.

85. `test_add_trade_buffers_row_in_correct_column_order` — call `add_trade` with a full row dict; before hitting `batch_size`, assert `sink._buffers["trades"][-1]` is a list positionally matching `TRADES_COLUMNS` order exactly (a column-order bug here silently corrupts every row — ClickHouse `insert(..., column_names=...)` trusts positional alignment).
86. `test_add_trade_missing_optional_field_inserts_none_at_that_position` — row dict missing `buyer_order_id`; assert `row.get(col)` yields `None` at that column's index rather than raising `KeyError`. Guards line 168.
87. `test_add_orderbook_event_and_snapshot_column_order` — same style as #85 for the other two tables, parametrized over `(method, columns, sample_row)`.
88. `test_add_incident_serializes_dict_details_to_json_string_before_buffering` — call `add_incident` with `row["details"] = {"a": 1}`; assert the buffered value at the `details` column position is a `str` and `json.loads(...)` round-trips to the original dict. Guards lines 161-165.
89. `test_add_incident_leaves_non_dict_details_untouched` — `row["details"]` already a `str` (e.g. caller pre-serialized); assert `isinstance(details, dict)` guard means it's passed through unchanged, not double-encoded. Guards line 163's `isinstance` check.
90. `test_auto_flush_triggers_exactly_at_batch_size` — construct sink with `batch_size=3`; add 2 rows, assert `client.insert` not called; add a 3rd, assert `client.insert` called once with all 3 rows and the buffer is now empty. Guards lines 167-173 (`should_flush` boundary — off-by-one risk on `>=` vs `>`).
91. `test_auto_flush_does_not_trigger_below_batch_size` — batch_size=5, add 4 rows; assert no flush.
92. `test_flush_table_noop_when_buffer_empty` — call `_flush_table` directly on an untouched table; assert `client.insert` not called (guards the `if not self._buffers[table]: return` early exit at line 178-179 — important because `flush_all`/`_periodic_flush` call this unconditionally on a timer).
93. `test_flush_table_swaps_buffer_before_insert_so_concurrent_adds_go_to_new_buffer` — start a flush (with the fake client's `.insert` an `asyncio.Event`-gated coroutine so the test can pause mid-flush), then call `add_trade` again while the flush is "in flight"; assert the new row lands in a **fresh** buffer list, not the one being flushed (verifies the lock-protected buffer-swap pattern at lines 177-181 has no race — this is the exact mechanism that must not double-buffer or drop rows under concurrent `add_*` calls from both `StreamConsumer` tasks sharing one sink).
94. `test_flush_table_on_insert_exception_drops_batch_and_reraises` — configure the fake client's `.insert` to raise `RuntimeError("chdown")`; assert (a) the exception propagates out of `_flush_table`, (b) the buffer was already emptied beforehand (i.e. the batch is **unrecoverably lost**, not left for retry) — this test exists specifically to document/pin the previously-unflagged data-loss path noted in §0 above, distinct from the two paths already named in `docs/05-risks.md` item 8. Recommend this test's docstring cross-reference that risk note explicitly so a future reader doesn't mistake it for a duplicate of the maxlen/redelivery issues.
95. `test_periodic_flush_exception_is_logged_and_does_not_kill_the_flusher_task` — configure `flush_all`/insert to raise once; run one tick of `_periodic_flush` (patch `asyncio.sleep`); assert the task is still alive/loops again on the next tick rather than dying silently. Guards lines 138-143.
96. `test_periodic_flush_propagates_cancelled_error` — assert `CancelledError` isn't swallowed by the broad `except Exception` (line 140-141 explicitly re-raises `CancelledError` before the generic handler) — important for clean shutdown via `close()`.
97. `test_close_cancels_flusher_flushes_remaining_rows_and_closes_client` — add some unflushed rows (below batch size), call `close()`; assert: flusher task cancelled, `flush_all()` called (remaining rows land in `client.insert`), then `client.close()` called, in that order. Guards lines 123-133.
98. `test_flush_all_flushes_every_table_independently_even_if_one_raises` — put rows in two tables' buffers; configure the fake client to raise only for one table name; assert the *other* table's `insert` was still attempted (i.e. `flush_all`'s `for table in list(self._buffers)` loop isn't short-circuited by one table's exception) — **note**: reading the code, `flush_all` has no per-table try/except, so an exception from `_flush_table("trades")` will actually abort the loop before reaching `"orderbook_events"` — this test should assert the *actual* current behavior (that later tables in the dict's iteration order do NOT get flushed if an earlier one raises) so it's documented as a real gap rather than assumed-fixed. Flag for the implementer: `dict` insertion order is `trades, orderbook_events, orderbook_snapshots, incidents`, so a `trades`-table ClickHouse hiccup currently blocks incident/snapshot flushing on that tick too.
99. `test_connect_creates_flusher_task_and_client` — call `connect()` (with `clickhouse_connect.get_async_client` patched to return the fake async client); assert `self._client is not None` and `self._flusher_task` is a live `asyncio.Task`.

---

## 6. `src/api/queries.py` — unit tests (pure logic, no ClickHouse) + integration coverage

File: `tests/unit/test_queries.py` for the pure-function parts; the SQL-executing functions (`get_trades`, `get_orderbook_events`, `get_orderbook_at`, `list_incidents`) are better covered as **integration tests against real ClickHouse** (§8) since their entire value is in the SQL/parameterization correctness — a mocked `client.query` would just be testing that the mock was called, not that the query is right. Still list the pure-logic unit tests here:

100. `test_parse_ts_none_returns_none` — `parse_ts(None) is None`.
101. `test_parse_ts_z_suffix_converts_to_utc_offset` — `"2026-09-14T12:00:00Z"` → `datetime(2026,9,14,12,0,0, tzinfo=utc)`. Guards lines 37-38.
102. `test_parse_ts_naive_timestamp_assumed_utc` — `"2026-09-14T12:00:00"` (no offset, no Z) → assert result has `tzinfo == timezone.utc` (not local-time-then-converted). Guards line 40-41.
103. `test_parse_ts_explicit_offset_converted_to_utc` — `"2026-09-14T08:00:00-04:00"` → assert equals `2026-09-14T12:00:00+00:00` (astimezone conversion, not just tagging). Guards line 42.
104. `test_parse_ts_whitespace_stripped` — `"  2026-09-14T12:00:00Z  "` → parses successfully (line 36's `.strip()`).
105. `test_parse_ts_invalid_format_raises_valueerror` — `"not-a-date"` → `ValueError` from `datetime.fromisoformat` — this is what `rest.py`'s `_parse_ts` wrapper converts to a `422`; test that the underlying function actually raises `ValueError` (not some other exception type) so that mapping stays correct.
106. `test_to_jsonable_converts_datetime_to_isoformat_string` — a `datetime` → `.isoformat()` string.
107. `test_to_jsonable_recurses_into_nested_dicts_lists_and_tuples` — a structure like `{"a": [(datetime(...), 1.0)]}` → every nested datetime converted, tuple becomes list (tuples aren't JSON-native — assert `to_jsonable` returns a `list`, not a `tuple`, at that position, since MCP tool responses must actually serialize).
108. `test_to_jsonable_passthrough_for_plain_scalars` — `str`/`int`/`float`/`bool`/`None` pass through unchanged.
109. `test_parse_cursor_roundtrips_with_make_cursor` — `_make_cursor(ts, 12345)` then `_parse_cursor(...)` returns `(ts, 12345)` with millisecond-precision `ts` preserved (guard the same float-ms-to-datetime precision concern as parser.py test #39, since `_make_cursor`/`_parse_cursor` do their own independent ms conversion at lines 89-97).
110. `test_parse_cursor_malformed_string_raises` — a cursor missing the `":"` separator raises `ValueError` from the `.split(":", 1)` unpack — this becomes a caller-facing `422`-worthy error in `rest.py`/`mcp_tools.py`, currently **unhandled** there (see §9 test #128 and the blockers note in §10).
111. `test_apply_levels_in_queries_matches_orderbook_state_semantics` — `queries._apply_levels` is a **deliberate reimplementation** of `orderbook_state._apply_levels` (per its own docstring, lines 152-157) — write one parametrized test that feeds identical inputs to both functions and asserts identical outputs, so the two implementations can never silently drift (this is exactly the kind of "not just orderbook_state.py generically" case the task asked for).
112. `test_orderbook_at_dataclass_bids_asks_are_plain_tuples` — construct `OrderBookAt` directly and assert field types match what `rest.py`'s `OrderBookAtResponse` Pydantic model expects (`list[tuple[float,float]]`) — a cheap regression guard against the dataclass and the Pydantic model silently diverging in shape.

---

## 7. `src/collector/adapter.py`, `sink.py`, `src/exchanges/binance/spot.py` — unit tests

File: `tests/unit/test_binance_spot_adapter.py`

113. `test_ws_url_builds_combined_stream_with_lowercase_symbol` — `BinanceSpotAdapter("btcusdt")` → `ws_url()` contains `btcusdt@trade` and `btcusdt@depth@100ms`, joined by `/`. Guards lines 51-60.
114. `test_ws_url_lowercases_uppercase_input_symbol` — `BinanceSpotAdapter("BTCUSDT")` → same stream names lowercased, while `self.symbol` (used for REST) stays uppercase (`self.symbol == "BTCUSDT"`). Guards the asymmetric case handling at lines 46-47.
115. `test_stream_type_classifies_trade` — `{"stream": "btcusdt@trade", "data": {...}}` → `"trade"`.
116. `test_stream_type_classifies_depth_diff` — `{"stream": "btcusdt@depth@100ms", ...}` → `"depth_diff"`.
117. `test_stream_type_unknown_for_unrecognized_stream_name` — `{"stream": "btcusdt@bookTicker"}` → `"unknown"`. Guards the fallthrough at line 69.
118. `test_stream_type_handles_missing_stream_key` — `{}` (no `"stream"` key) → `.get("stream", "")` default → `"unknown"`, no `KeyError`.
119. `test_depth_update_ids_extracts_u_and_uu` — `{"data": {"U": 100, "u": 105}}` → `(100, 105)`.
120. `test_depth_update_ids_raises_keyerror_on_missing_data_or_fields` — malformed envelope; assert fails loud (`KeyError`), matching this adapter's "dumb, no defensive parsing" design philosophy stated in `adapter.py`'s docstring.
121. `test_fetch_snapshot_calls_correct_rest_endpoint_and_params` — mock `httpx.AsyncClient.get` (via `respx` or a monkeypatched transport); assert called with `GET {REST_BASE_URL}/api/v3/depth?symbol=BTCUSDT&limit=1000`.
122. `test_fetch_snapshot_raises_on_http_error_status` — mock a 429/500 response; assert `resp.raise_for_status()` propagates (`httpx.HTTPStatusError`) — this becomes the `except Exception` catch in `runner.py::_apply_snapshot` (line 199-203), so document that any HTTP error here is caught upstream and logged, not retried by the adapter itself.
123. `test_fetch_snapshot_returns_raw_json_untouched` — mock a response body with extra/unexpected keys beyond `lastUpdateId/bids/asks`; assert `fetch_snapshot()` returns the dict verbatim (dumb-collector contract, no schema enforcement at this layer).

File: `tests/unit/test_collector_sink.py` (mock `redis.asyncio.Redis`, no real Redis):

124. `test_publish_xadds_with_json_encoded_payload_field` — call `publish(stream, {"type": "trade", ...})`; assert `xadd` called with `{"payload": json.dumps(...)}` as the field dict (single field, not exploded).
125. `test_publish_passes_maxlen_and_approximate_true` — assert `xadd(..., maxlen=self._stream_maxlen, approximate=True)` — this is the exact call the ADR's weak point #1 is about; a unit test here doesn't validate the *architecture* decision (that needs the load test), but it does pin the current call shape so nobody accidentally drops `approximate=True` (which would change `XADD`'s performance profile) or the `maxlen` param entirely without noticing.
126. `test_publish_json_encodes_non_jsonable_values_via_default_str` — payload containing a value that isn't natively JSON-serializable (shouldn't normally occur, but `default=str` at line 48 implies defensive intent); assert no crash.
127. `test_now_ts_ms_returns_integer_milliseconds` — `now_ts_ms()` returns an `int`; sanity-check it's in a plausible epoch-ms range (catches an accidental `time.time()` vs `time.time_ns()` unit mixup, which given the file literally has both patterns nearby, `time.time()*1000` in `orderbook_state.py` vs `time.time_ns()//1_000_000` here, is worth guarding explicitly against a future copy-paste unit bug).

---

## 8. Integration test — `tests/integration/test_pipeline.py`

**Fixture requirements:**
- `testcontainers-python`'s `RedisContainer` and a ClickHouse container (either `testcontainers`'s generic container wrapping `clickhouse/clickhouse-server:24.8` — pinned to match `docs/04-architecture/01-stack.md`'s pinned server version — or a docker-compose-based CI service, per the ADR's own "Suggested stack" note).
- A **recorded WS session fixture file**: `tests/fixtures/binance_btcusdt_session.jsonl` — one JSON object per line, each shaped exactly like the raw combined-stream envelope Binance sends (`{"stream": "btcusdt@trade", "data": {...}}` / `{"stream": "btcusdt@depth@100ms", "data": {...}}`), **plus** one recorded REST snapshot body as a sibling fixture `tests/fixtures/binance_btcusdt_snapshot.json`. The session file must include, deliberately, at minimum:
  - a stretch of clean sequential depth-diffs,
  - one out-of-order/gapped `U`/`u` pair to exercise sequence-break detection,
  - at least 2 trades sharing the same `T` millisecond (tests trade ordering by `trade_id` tie-break in `get_trades`' `ORDER BY ts_exchange ASC, trade_id ASC`),
  - a `qty=0` depth level (level removal),
  - the REST snapshot arriving *after* some buffered diffs (mirrors `runner.py`'s buffering behavior — though the collector isn't necessarily driven by this fixture; see note below).
  - This file can be captured once by literally running the existing collector against real Binance for a minute and saving `raw:binance:btcusdt:*` stream contents, or hand-built to guarantee the edge cases above are present (hand-built is more reliable for deterministic CI).
- A small **replayer** helper that either (a) feeds fixture lines directly into a real Redis instance via `XADD` (bypassing the collector/adapter entirely — tests normalizer→ClickHouse only), or (b) runs the actual `CollectorRunner` against a local `websockets` test server serving the fixture and a stub REST server returning the snapshot fixture (tests collector→Redis→normalizer→ClickHouse end-to-end, closer to the ADR's stated goal). Recommend **both**, as two test classes in the same file, since they catch different bug classes (the collector's buffering dance vs. the normalizer's consumption).

Test cases:

128. `test_end_to_end_trades_land_in_clickhouse_with_correct_columns` — replay the fixture's trades; run the normalizer against real Redis+ClickHouse; assert `SELECT * FROM trades` rows match expected count and field values, including the ClickHouse-computed `quote_qty == price * quantity`.
129. `test_end_to_end_depth_events_land_with_dual_timestamps` — assert `orderbook_events` rows have distinct `ts_exchange`/`ts_received` (received should be >= exchange time, and both should be UTC).
130. `test_end_to_end_sequence_break_produces_incident_row_and_book_pauses` — the fixture's gapped diff triggers `orderbook_sequence_break`; assert an `incidents` row exists with `status='open'`, then after the (fixture's later) snapshot arrives, assert a **second** `incidents` row with the same `id` and `status='resolved'` exists (documents the append-only/no-in-place-update trade-off called out in `incidents.py`'s own docstring, lines 74-78) — test must query "latest row per id" logic explicitly since that's the documented workaround.
131. `test_end_to_end_normalizer_restart_mid_batch_causes_at_most_once_loss` — the loss-window test the ADR explicitly asks for: publish N messages, let the normalizer ack+buffer some of them (buffer below `sink_batch_size`, so not yet flushed), then kill the normalizer process/task **before** the next flush tick, restart it, and assert the buffered-but-unflushed rows are **genuinely absent** from ClickHouse (proves the documented at-most-once loss window is real and reproducible, not just theoretical) and that no exception surfaces to the caller.
132. `test_end_to_end_redelivery_after_normalizer_crash_before_xack_produces_duplicate_row` — the mirror-image test the ADR/`05-risks.md` item 8 explicitly names: publish a message, kill the normalizer/consumer **before** it XACKs (simulate by starting `_process_one` and cancelling the task mid-handler, or by never calling `xack` in a controlled test double), then let a fresh `StreamConsumer` pick up the same entry via normal PEL redelivery (`XREADGROUP ... ">"` after restart naturally redelivers unacked messages to the same consumer, or use `XCLAIM` semantics if testing a different consumer name); assert the row appears **twice** in ClickHouse (since tables are plain `MergeTree`, no dedup) — this is the single most valuable integration test in the whole plan, since it turns a documented architectural risk into a concrete, always-red-until-fixed regression test that the implementer can point to when deciding whether to add `insert_deduplication_token` (per the ADR's own suggested fix).
133. `test_end_to_end_maxlen_trim_drops_unconsumed_messages_under_normalizer_lag` — configure a **small** `stream_maxlen` (e.g. 10) on a test `RedisStreamSink`; publish more messages than that while the normalizer consumer is paused/not running; start the normalizer afterward; assert it only sees the last `maxlen`-ish entries (ClickHouse row count for that run is less than messages published) — this quantifies weak point #1 at unit-test scale (a real "downtime budget" number needs the load-test harness from the ADR's §2, but this integration test proves the mechanism fires at all, deterministically, in CI).
134. `test_end_to_end_disconnect_reconnect_signals_produce_paired_incident` — drive the collector's runner against a WS test server that closes the connection once mid-stream; assert a `collector_disconnect` incident opens, then resolves on the runner's reconnect, with a plausible `downtime_ms`.
135. `test_end_to_end_get_orderbook_at_matches_live_book_state_reconstruction` — after replaying the full fixture, call `queries.get_orderbook_at(client, ..., ts=<a timestamp partway through the fixture>)` against the real ClickHouse data and assert its result (bids/asks) matches what `OrderBookState` would compute if fed the same events directly in-process up to that timestamp — this is the cross-check that the stored-and-replayed reconstruction path (`queries.py`) and the live in-memory path (`orderbook_state.py`) never diverge, which is exactly the kind of invariant that "no gray areas" implies should be pinned down explicitly rather than assumed from the two modules individually passing their own unit tests.

---

## 9. REST/MCP contract test — `tests/contract/test_rest_mcp_parity.py`

**Exact structure:**

```python
import pytest
from httpx import AsyncClient, ASGITransport
from api.main import app  # real FastAPI app, real MCP mount
# ch_client fixture: points app.state.ch_client at a ClickHouse test container
# pre-seeded with a small, fixed dataset (trades, orderbook_events,
# orderbook_snapshots, incidents rows with known values) via a session-scoped
# fixture that runs the 4 migrations then inserts fixture rows directly.

CASES = [
    # (case_id, rest_method, rest_path, rest_params, mcp_tool_name, mcp_kwargs)
    ("trades_basic", "GET", "/trades", {"symbol": "BTCUSDT", "limit": 50}, "get_trades",
     {"symbol": "BTCUSDT", "limit": 50}),
    ("trades_time_range", "GET", "/trades",
     {"symbol": "BTCUSDT", "ts_from": "2026-09-14T00:00:00Z", "ts_to": "2026-09-14T01:00:00Z"},
     "get_trades", {"symbol": "BTCUSDT", "ts_from": "2026-09-14T00:00:00Z", "ts_to": "2026-09-14T01:00:00Z"}),
    ("orderbook_at", "GET", "/orderbook/at", {"symbol": "BTCUSDT", "ts": "2026-09-14T00:30:00Z"},
     "get_orderbook_at", {"symbol": "BTCUSDT", "ts": "2026-09-14T00:30:00Z"}),
    ("orderbook_events_paginated", "GET", "/orderbook/events", {"symbol": "BTCUSDT", "limit": 20},
     "get_orderbook_events", {"symbol": "BTCUSDT", "limit": 20}),
    ("incidents_all", "GET", "/incidents", {}, "list_data_incidents", {}),
    ("incidents_by_symbol", "GET", "/incidents", {"symbol": "BTCUSDT"}, "list_data_incidents", {"symbol": "BTCUSDT"}),
]

@pytest.mark.parametrize("case_id,method,path,params,tool,kwargs", CASES, ids=[c[0] for c in CASES])
async def test_rest_and_mcp_agree(case_id, method, path, params, tool, kwargs, seeded_client):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http:
        rest_resp = await http.get(path, params=params)
    rest_json = rest_resp.json()

    mcp_result = await call_mcp_tool(app, tool, kwargs)  # helper: drive the FastMCP
    # session-manager in-process (streamable HTTP client against the mounted
    # /mcp app, or FastMCP's own in-process tool-call API if 1.29.1 exposes one)

    assert normalize(rest_json) == normalize(mcp_result)
```

Where `normalize()` handles the one legitimate serialization difference: REST returns Pydantic-serialized JSON (datetimes as ISO strings via FastAPI's default encoder, tuples as JSON arrays), MCP returns `queries.to_jsonable(...)`'s output (also ISO strings, also arrays) — these should already match byte-for-byte after JSON round-trip, so `normalize()` should just be `json.loads(json.dumps(x, sort_keys=True))` on both sides to kill key-ordering noise; if a real difference is found (e.g. float formatting, `None` vs missing key), that itself is a bug worth its own dedicated test, not something to paper over in `normalize()`.

Additional specific cases beyond the happy-path table:

136. `test_orderbook_at_404_vs_mcp_exception_parity` — request a `ts` before any snapshot exists; assert REST returns `404` with the `OrderBookNotFound` message, and separately assert the MCP tool call raises/returns an error surface containing the same message text (MCP error shape differs structurally from an HTTP status, so this test's assertion is on message content parity, not status-code parity — document that distinction explicitly rather than asserting a false equivalence).
137. `test_get_trades_limit_boundary_10000_parity` — `limit=10000` (the documented REST `le=10_000` ceiling) on both surfaces; assert both accept it identically. Note MCP's `get_trades` tool has **no `le` bound at all** on `limit` (`mcp_tools.py` line 34, plain `int`) — assert this discrepancy explicitly as its own test (`test_mcp_get_trades_limit_has_no_upper_bound_unlike_rest`) rather than silently expecting parity, since it's a real, currently-existing gap between the two surfaces that weak point #6 in the ADR (unbounded `limit`) is actually *worse* on the MCP side than REST.
138. `test_default_exchange_segment_applied_identically` — omit `exchange`/`segment` on both surfaces; assert both fall back to `config.default_exchange`/`default_segment` and produce identical results.
139. `test_symbol_case_insensitivity_parity` — pass `symbol="btcusdt"` (lowercase) to both; REST uppercases via `symbol.upper()` in the route handler (rest.py lines 61, 93, 142, 180) and MCP tools do the same internally (`symbol.upper()` in each tool) — assert both produce identical results, and separately unit-test (in `tests/unit/test_rest.py` / `test_mcp_tools.py`, not the contract suite) that each surface does its own uppercasing rather than relying on the other.
140. `test_invalid_timestamp_error_shape_documented_as_asymmetric` — REST's `_parse_ts` wrapper catches `ValueError` and raises `HTTPException(422)` (rest.py lines 19-23); MCP's tools call `queries.parse_ts` **directly with no try/except** (mcp_tools.py lines 45, 61, 98, 121) — an invalid timestamp string given to an MCP tool will raise an unhandled `ValueError` inside the tool call rather than a clean error response. Write this as an explicit asymmetry test (`test_mcp_invalid_timestamp_raises_unhandled_valueerror_rest_returns_422`) documenting a real, currently-existing gap in the "REST and MCP can never drift apart" invariant the module's docstring claims — this belongs in the contract suite precisely because it's the kind of drift that suite exists to catch, and it should be **red** until someone adds the same try/except to the MCP tool layer (flag for the implementer as a one-line fix: wrap each MCP tool's `queries.parse_ts(...)` calls in the same pattern as `rest.py::_parse_ts`).

---

## 10. Proposed directory tree

```
tests/
  conftest.py                      # sys.path shim for src/ (see Blockers §11), shared fixtures
  fixtures/
    binance_btcusdt_session.jsonl  # recorded/hand-built WS envelopes (trades + depth + gap + qty=0 level)
    binance_btcusdt_snapshot.json  # matching REST snapshot body
  unit/
    test_orderbook_state.py        # cases 1-23
    test_parser.py                 # cases 24-39
    test_incidents.py              # cases 40-55
    test_consumer.py               # cases 56-84 (FakeRedis + FakeClickHouseSink doubles live here or in conftest)
    test_clickhouse_sink.py        # cases 85-99 (FakeAsyncClickHouseClient double)
    test_queries.py                # cases 100-112 (pure-logic subset only)
    test_binance_spot_adapter.py   # cases 113-123
    test_collector_sink.py         # cases 124-127
    test_rest.py                   # symbol-uppercasing, timestamp-422-wrapping, default-fallback unit tests (isolated from mcp_tools.py)
    test_mcp_tools.py              # same-shaped isolated unit tests for the MCP tool layer, incl. the missing-try/except gap
  integration/
    test_pipeline.py               # cases 128-135, testcontainers Redis+ClickHouse
  contract/
    test_rest_mcp_parity.py        # cases 136-140 + the parametrized table
  regression/
    test_incidents_schema_matches_migration.py   # case 55, kept separate since it's explicitly a "fails on future drift" guard, not a normal unit test of current behavior
```

`pytest.ini` / `pyproject.toml` additions (described, not applied): `testpaths = tests`, `asyncio_mode = auto` (pytest-asyncio), and either a `[tool.pytest.ini_options] pythonpath = ["src"]` entry (pytest ≥7) or a `conftest.py` `sys.path.insert(0, ...)` shim, per the blocker below.

---

## 11. Blockers found (test-blocking issues, with minimal described fixes)

1. **No package/path configuration for `src/` layout.** There is no `pyproject.toml`, `setup.cfg`, or top-level `src/__init__.py`; every module does `from common import get_logger`, `from normalizer import incidents`, etc., which only resolves at runtime because the Docker image presumably sets `WORKDIR=/app/src` or `PYTHONPATH=/app/src`. A test runner invoked from the repo root will fail on every import. **Minimal fix:** add either a `pyproject.toml` with `[tool.pytest.ini_options] pythonpath = ["src"]` (pytest 7+, zero extra files) or a one-line `tests/conftest.py` doing `sys.path.insert(0, str(Path(__file__).parent.parent / "src"))`. No source code changes needed.

2. **No constructor-level dependency injection for the two ClickHouse/Redis client wrappers.** `ClickHouseSink.connect()` and `ClickHouseReadClient.connect()` both call `clickhouse_connect.get_async_client(...)` internally rather than accepting a pre-built client; `RedisStreamSink.connect()` similarly builds its own `redis.from_url(...)`. This means true unit tests of `ClickHouseSink`'s buffering/flushing logic (§5) must monkeypatch `clickhouse_connect.get_async_client` globally or reach into `sink._client` after construction rather than passing in a test double cleanly. **Minimal fix:** add an optional `client: AsyncClient | None = None` constructor parameter to `ClickHouseSink` and `ClickHouseReadClient` (and `redis_client` to `RedisStreamSink`), defaulting to `None` and only calling the real constructor inside `connect()` when it's still `None` — a ~3-line change per class, fully backward compatible, and it turns "monkeypatch a module-level function" into "pass a fake object," which is both easier to reason about and avoids global monkeypatch leakage between tests.

3. **`clickhouse_sink._flush_table`'s buffer-pop-then-insert ordering makes insert failures untestable as "recoverable."** As found in §0/§5 test #94, a failed insert **always** loses the batch (already removed from `self._buffers[table]` before the `await self._client.insert(...)` call). This isn't strictly a *testing* blocker (it's testable as-is, per test #94) but it does mean no test can assert "failed batches are retried," because they aren't — worth flagging to the ADR owner as a third, previously-undocumented at-most-once loss path (distinct from the two named in `docs/05-risks.md` item 8) since a hardening pass focused on tests will otherwise write a test that merely *confirms* silent data loss rather than catching a bug.

4. **No fixture/builder helpers for the several dict shapes used across modules** (`envelope` dicts consumed by `parser.py`, `row` dicts consumed by `orderbook_state.py`, `incident` dicts produced/consumed by `incidents.py`). Every module accepts "whatever the previous stage produced" with no schema validation (by design, per the "dumb collector"/"keep it simple" philosophy stated repeatedly in the docstrings) — which is fine for production but means test authors will hand-build a lot of nested literal dicts. **Recommendation (not a code blocker, a test-suite-design note):** put small factory functions (`make_trade_envelope(**overrides)`, `make_depth_diff_envelope(**overrides)`, `make_snapshot_envelope(**overrides)`, `make_open_incident(**overrides)`) in `tests/conftest.py` or a `tests/factories.py` up front, since nearly every unit test in §2-§4 needs one.

5. **`mcp_tools.py`'s missing `try/except` around `queries.parse_ts`** (contract test #140) isn't a test-blocker but is the one **source-level** issue this research surfaced that a "no gray areas" test plan would otherwise just quietly encode as expected behavior. Minimal fix once someone implements this plan: wrap each of the four `queries.parse_ts(...)` call sites in `mcp_tools.py` in the same try/except-and-reraise-as-a-clean-error pattern `rest.py::_parse_ts` already uses, so test #140 can be flipped from "documents an asymmetry" to "asserts true parity."
