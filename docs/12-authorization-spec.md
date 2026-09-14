# Authorization Spec — Market Data Service Prototype

Companion document to [ADR-001](./09-hardening-tests-load-auth.md#3-authorization). Pushes the ADR's API-key recommendation to an implementation-ready spec: concrete storage decision, code, and rollout plan — not an options table. Produced from a full read of `src/api/main.py`, `rest.py`, `mcp_tools.py`, `config.py`, `clickhouse_client.py`, `known_limitations.py`, `queries.py`, `src/normalizer/incidents.py`, `infra/clickhouse/migrations/001-004`, `docker-compose.yml`, `.env.example`. No repo files were modified — this is a design document; the artifacts below are proposals for a future implementation pass.

## 1. Storage: static config file, not a ClickHouse table

**Decision: static file (`src/api/api_keys.py`), following the `known_limitations.py` precedent exactly.**

Justification, tied to what's actually in the repo:
- `known_limitations.py`'s docstring gives the exact test this repo already applies: "a static config/JSON is sufficient — no DB table" for small, rarely-changing, non-time-series data. 5-10 user rows is a strictly smaller, less dynamic dataset than the (currently empty) known-limitations list.
- ClickHouse in this stack is purpose-built for append-only time-series (every one of 001-004 is `MergeTree`, ordered by time). A key lookup is a point-read-by-hash on a tiny mutable set — the opposite access pattern, and it drags a ClickHouse round-trip into the hot path of *every single request*, including ones ClickHouse itself hasn't answered yet.
- Revocation/rotation of a ClickHouse row hits the same "MergeTree doesn't do in-place updates" problem `src/normalizer/incidents.py::resolve_sequence_break_incident` already documents and works around by inserting new rows and reading "latest row per id." That workaround is fine for an audit trail (incidents); it's the wrong shape for an auth check that must be correct on every request, not eventually-consistent.
- Operationally: at 5-10 named users issued manually (roadmap's own words), a file that's edited and the `api` container restarted/reloaded is simpler than standing up any create/revoke path against ClickHouse, and it's git-diffable for who-has-access history — a real advantage over a DB table for this scale.

Concretely, `src/api/api_keys.py`:

```python
"""Static API key registry for the hosted MVP (5-10 named users).

Same "don't drag in more than needed" reasoning as known_limitations.py:
this is a small, rarely-changing set edited by hand and shipped with the
service, not a live table. Only key HASHES live here -- never raw keys.
Revoke a user by setting revoked_at (see §5); do not delete the entry
(keeps historical log/rate-limit keys attributable).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ApiKeyRecord:
    key_hash: str          # sha256 hex digest of the raw key, never the raw key itself
    user_id: str            # short stable slug, e.g. "alice" -- used in logs/rate-limit keys
    label: str               # human note, e.g. "alice@example.com - issued 2026-09-14"
    issued_at: datetime
    revoked_at: datetime | None = None


API_KEYS: list[ApiKeyRecord] = [
    # ApiKeyRecord(
    #     key_hash="…sha256 hex…",
    #     user_id="alice",
    #     label="alice@example.com - issued 2026-09-14",
    #     issued_at=datetime(2026, 9, 14, tzinfo=timezone.utc),
    # ),
]
```

Loaded once at startup into a `dict[str, ApiKeyRecord]` keyed by hash (O(1) lookup, no per-request file I/O), stored on `app.state` next to `ch_client`/`config` — same pattern `main.py` already uses.

If the user base later grows past "issued manually," that's exactly the trigger the ADR itself names for revisiting JWT/OAuth — the same threshold applies to moving keys into ClickHouse or a real KV store. Don't build that now.

## 2. Hashing, key format, issuance

**Hash: SHA-256 of the raw key, hex-encoded. Not bcrypt/argon2/scrypt.**

Justification: bcrypt/argon2 exist to slow down brute-forcing a *low-entropy, human-chosen* secret (a password) against an offline dictionary attack. An API key here is a 256-bit CSPRNG-generated token — brute-forcing it by trying candidates is already computationally infeasible regardless of hash speed (2^256 keyspace vs. bcrypt's typical ~2^40-2^60 password-guessing gap it's designed to close). Using a slow KDF here only adds attacker-irrelevant latency to every request's auth check for zero security benefit. This is the standard justification used by Stripe/GitHub/AWS for exactly this token shape, and it matches this repo's own "don't drag in more than needed" ethos — SHA-256 is in Python's stdlib (`hashlib`), no new dependency.

**Key format:** `mds_live_<43 url-safe base64 chars>` (32 raw random bytes via `secrets.token_urlsafe(32)`, which already produces exactly 43 chars). Prefix mirrors Stripe's `sk_live_...` convention — makes keys greppable/identifiable in logs and secret-scanners, and `mds_` reads unambiguously as this project's ("market data service"). No `_test_` variant needed yet (no staging environment in this stack).

```python
import hashlib
import secrets

def generate_api_key() -> tuple[str, str]:
    """Returns (raw_key_to_show_once, sha256_hex_hash_to_store)."""
    raw = f"mds_live_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(raw.encode("ascii")).hexdigest()
    return raw, key_hash
```

**Issuance: a one-off CLI script, `scripts/issue_api_key.py`.** There is no existing `scripts/` directory or admin tooling anywhere in the repo, so this is new, minimal, and deliberately not a service endpoint (no "create API key" REST route — that would need *its own* auth to bootstrap, a chicken-and-egg problem not worth solving at 5-10 manually-issued users).

```python
#!/usr/bin/env python3
"""Issue a new API key for a named user. Run manually, out-of-band, by
whoever operates the hosted instance (matches docs/08-prototype-roadmap.md
Phase E: "API keys ... issued manually to each of the 5-10 target users").

Usage:
    python scripts/issue_api_key.py alice "alice@example.com"

Prints the raw key ONCE (never stored) and the ApiKeyRecord line to paste
into src/api/api_keys.py. Send the raw key to the user out-of-band
(1Password share link, Signal, etc.) -- never Slack/email in plaintext.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

sys.path.insert(0, "src")
from api.api_keys import generate_api_key  # noqa: E402


def main() -> None:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <user_id> <label>", file=sys.stderr)
        raise SystemExit(1)
    user_id, label = sys.argv[1], sys.argv[2]
    raw, key_hash = generate_api_key()
    print(f"\nGive this key to {user_id} now -- it will not be shown again:\n")
    print(f"  {raw}\n")
    print("Add this entry to src/api/api_keys.py's API_KEYS list:\n")
    now = datetime.now(timezone.utc).isoformat()
    print(
        "    ApiKeyRecord(\n"
        f'        key_hash="{key_hash}",\n'
        f'        user_id="{user_id}",\n'
        f'        label="{label}",\n'
        f'        issued_at=datetime.fromisoformat("{now}"),\n'
        "    ),"
    )


if __name__ == "__main__":
    main()
```

Operational flow: operator runs the script → pastes the printed `ApiKeyRecord` into `api_keys.py` → commits (hash only, never the raw key — the raw key never touches disk or git) → redeploys `api` → sends the raw key to the user over a separate out-of-band channel.

## 3. Enforcement point: app-level middleware, not two dependency wirings

**Decision: one `BaseHTTPMiddleware` (or raw ASGI middleware) added to `app` in `main.py`, covering both `app.include_router(rest_router)` and `app.mount("/mcp", mcp_asgi_app)`. Not a FastAPI `Depends()` on the REST router plus a separate MCP-side check.**

Why, concretely, from what `main.py` and FastMCP's mounting actually do:
- `main.py`'s own docstring already establishes that `mcp_asgi_app = mcp_server.streamable_http_app()` is a **separate Starlette app**, mounted via plain `app.mount("/mcp", mcp_asgi_app)` — and that plain ASGI mounts (as that docstring notes for lifespan) do *not* automatically inherit anything from the parent FastAPI app's dependency-injection system. A `Depends()` on `rest_router`'s routes has literally no mechanism to reach requests that go to `mcp_asgi_app` — FastMCP's tool handlers aren't FastAPI path operations at all, they're MCP-protocol-level handlers inside a different Starlette app object. There is no `Depends()`-shaped hook into them.
- A `Depends()` on every individual MCP tool function (`get_trades`, `get_orderbook_at`, etc. in `mcp_tools.py`) doesn't work either — `@mcp.tool()` isn't a FastAPI route decorator, it's FastMCP's own registration mechanism with a different signature contract (arguments become the tool's JSON schema).
- What both `rest_router` and `mcp_asgi_app` genuinely share is one thing only: they are both reached through the outer `app`'s ASGI call stack, since `app.mount()` and `app.include_router()` both ultimately route an incoming request through `app`'s own middleware stack before dispatch. Starlette/FastAPI middleware runs *before* routing decides whether the target is a router path or a mount — so it's the one place that structurally covers both without touching `rest.py` or `mcp_tools.py` at all.

```python
"""Auth middleware -- src/api/auth.py"""
from __future__ import annotations

import hashlib

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

# Paths that don't require a key (health checks, OpenAPI docs for humans browsing).
_PUBLIC_PATHS = {"/openapi.json", "/docs", "/redoc", "/health"}


class ApiKeyAuthMiddleware(BaseHTTPMiddleware):
    """Validates `Authorization: Bearer <key>` against app.state.api_keys
    (built from api_keys.py at startup). Covers BOTH the REST router and
    the /mcp ASGI mount -- see §3 for why this must be app-level
    middleware, not a FastAPI Depends()."""

    async def dispatch(self, request: Request, call_next):
        if request.url.path in _PUBLIC_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse({"detail": "missing or malformed Authorization header"}, status_code=401)

        raw_key = auth_header.removeprefix("Bearer ").strip()
        key_hash = hashlib.sha256(raw_key.encode("ascii")).hexdigest()

        record = request.app.state.api_keys.get(key_hash)
        if record is None or record.revoked_at is not None:
            return JSONResponse({"detail": "invalid or revoked API key"}, status_code=401)

        # Stash identity for downstream handlers/logging -- see §4.
        request.state.user_id = record.user_id
        request.state.api_key_hash = key_hash
        return await call_next(request)
```

Wiring in `main.py` (additive to the existing shape, nothing else changes):

```python
from api.api_keys import API_KEYS
from api.auth import ApiKeyAuthMiddleware

...

app = FastAPI(title="market-data-api", lifespan=lifespan)
app.state.ch_client = ch_client
app.state.config = config
app.state.api_keys = {rec.key_hash: rec for rec in API_KEYS}
app.add_middleware(ApiKeyAuthMiddleware)
app.include_router(rest_router)
app.mount("/mcp", mcp_asgi_app)
```

Two caveats worth stating plainly (no gray area left implicit):
- `BaseHTTPMiddleware` buffers the request; MCP's `streamable_http_app()` uses SSE/streaming responses. `BaseHTTPMiddleware` is known to have historically caused issues with `StreamingResponse` under load in some Starlette versions. Given this stack's pinned `fastapi==0.140.13`, this should be smoke-tested against a real MCP streamable session before relying on it; if it causes problems, fall back to a raw ASGI middleware (`class ApiKeyAuthMiddleware: def __init__(self, app): ...  async def __call__(self, scope, receive, send): ...`) which never buffers — same logic, just written at the ASGI-callable level instead of via Starlette's convenience base class. Either way it's still one app-level middleware, not per-router dependencies.
- The auth check must run before `app.mount("/mcp", ...)`'s inner Starlette app does its own routing — confirmed correct here because Starlette applies `add_middleware()` middleware around the whole app's dispatch, which happens before the mount's sub-routing takes over.

## 4. Identity flow into query functions

**Decision: identity does NOT flow into `queries.py` at all. It flows only as far as a request-logging call site, following the exact `request.app.state.*` pattern already used in `rest.py`.**

Reasoning: `rest.py` already threads `request.app.state.ch_client` and `request.app.state.config` into `queries.py` calls, but `queries.py`'s functions are pure query builders parameterized by `exchange`/`segment`/`symbol`/time range — they have no per-user branching today (all 5-10 users see the same data), and the ADR's stated reason for wanting identity (per-key request logging, a cheap first step toward rate limiting) is a cross-cutting concern, not a query concern. Pushing `user_id` into `queries.py` would mean touching the one module both `rest.py` and `mcp_tools.py` deliberately keep protocol-agnostic and identical (`mcp_tools.py`'s own docstring: "exactly the same functions rest.py's handlers call, so REST and MCP can never drift apart") — for zero query-shaping benefit. Keep `queries.py` untouched.

Instead, the middleware in §3 already sets `request.state.user_id` (Starlette's `request.state`, same request object `rest.py` already receives) — consistent with the existing `request.app.state.config`/`request.app.state.ch_client` convention, just per-request state instead of per-app state (correctly, since identity varies per request, unlike config/ch_client).

Logging call site, added to the same middleware right after the key check (one place, not duplicated per route):

```python
from common import get_logger
log = get_logger("api.access")

...
        request.state.user_id = record.user_id
        request.state.api_key_hash = key_hash
        response = await call_next(request)
        log.info(
            "api request",
            extra={
                "user_id": record.user_id,
                "path": request.url.path,
                "method": request.method,
                "status_code": response.status_code,
            },
        )
        return response
```

This reuses `src/common/logging.py::get_logger` (already imported the same way in `main.py`, `clickhouse_client.py`) and ships to Loki via the existing `docker-compose.yml` logging pipeline with zero new infrastructure — the per-key log lines just show up next to `api`'s existing logs, filterable in Grafana by `user_id`. MCP tool calls get the same coverage for free since they go through the same middleware (the tool name itself isn't visible at the ASGI-path level for the streamable-http transport — `path` will just be `/mcp`; per-tool-name logging would need each `@mcp.tool()` wrapper in `mcp_tools.py` to log context itself, since FastMCP tools don't get the Starlette `Request` automatically — out of scope for this pass, flag as a known limitation if per-tool granularity is needed later).

## 5. Revocation: `revoked_at` nullable field — no hard delete

**Decision: soft-revoke via `revoked_at: datetime | None` on `ApiKeyRecord` (already shown in §1/§2). Never delete the record outright.**

Justification, directly consistent with what `src/normalizer/incidents.py` already established for this codebase: `resolve_sequence_break_incident`'s docstring states the reasoning to avoid in-place mutation in this stack — "ClickHouse's MergeTree doesn't do in-place updates ... relies on querying the latest row per incident id." That's a ClickHouse-cost argument, but the *same principle generalizes* here even though this particular data isn't in ClickHouse: mutating identity/audit records in place destroys history. If a key is hard-deleted from `api_keys.py`, every historical log line and future rate-limit-Redis-key referencing that `user_id`/`key_hash` becomes orphaned and unattributable — exactly the kind of silent, hard-to-reconstruct loss the ADR's weak-points list (#1, #2) calls out as a pattern to avoid elsewhere in this codebase. Setting `revoked_at` instead:
- Makes the middleware's revocation check (`record.revoked_at is not None` → 401) take effect immediately on next deploy/reload — same "instant" revocation property a DB-row-delete design would give, without needing a DB.
- Keeps `user_id`/`label` resolvable for every historical log line and any rate-limit counters keyed by `user_id` (§6) even after the key stops working.
- Costs nothing extra: it's one more field on a dataclass already being hand-edited by the same operator running `issue_api_key.py`.

Revocation operational step (a documented manual edit — at 5-10 users, manual edit is fine and matches "issued manually"): flip `revoked_at=datetime.now(timezone.utc)` in the matching `ApiKeyRecord`, commit, redeploy.

## 6. Rate limiting: Redis sliding-window counter, keyed by `user_id`

**Decision: fixed-window-per-minute counter in Redis (not a full sliding-log), using Redis's existing role as the broker in this stack — no new infra.**

Why fixed-window over a true sliding log: at 5-10 users, the boundary-burst imprecision of fixed windows (a user could in theory do `2×limit` requests spanning a window edge) is a non-issue — this stack's own `REDIS_STREAM_MAXLEN`/broker usage is already the pattern of "good enough, not academically perfect" this repo favors elsewhere (e.g. "redelivery can produce duplicates" being accepted as a known trade-off in the ADR itself, weak point #2). A true sliding-window-log (`ZADD`/`ZREMRANGEBYSCORE`) is one more command round-trip for marginal benefit at this scale.

Concrete Redis key scheme, using `redis.asyncio` (already imported this way in `collector/sink.py`, `normalizer/consumer.py`, `main.py` — same client shape, same DB-numbering convention as `REDIS_STREAM_DB`, but a **separate Redis logical DB** so rate-limit keys never collide with or get scanned alongside the Streams broker data):

```
Key:    ratelimit:{user_id}:{window_epoch_minute}
Value:  request count this minute (integer)
TTL:    120s (auto-expires 2 windows out -- no cleanup job needed)
```

```python
"""src/api/rate_limit.py"""
from __future__ import annotations

import time

import redis.asyncio as redis

RATE_LIMIT_PER_MINUTE = 120  # generous default for 5-10 quant-researcher users; tune from load-test data (11-load-testing-plan.md)


async def check_rate_limit(redis_client: redis.Redis, user_id: str) -> bool:
    """Returns True if the request is allowed, False if the user is over
    RATE_LIMIT_PER_MINUTE requests in the current 60s window. Uses a Lua-free
    INCR+EXPIRE pair -- fine at this request volume; a Lua script would only
    matter to close the INCR/EXPIRE race at much higher QPS than 5-10 users
    produce."""
    window = int(time.time() // 60)
    key = f"ratelimit:{user_id}:{window}"
    count = await redis_client.incr(key)
    if count == 1:
        await redis_client.expire(key, 120)
    return count <= RATE_LIMIT_PER_MINUTE
```

Wiring: instantiate a second `redis.asyncio.Redis` client at startup (pointed at `REDIS_URL`, but `db=` a new number, e.g. `REDIS_RATELIMIT_DB=2`, added to `.env.example` next to the existing `REDIS_STREAM_DB=1` convention — keeps it consistent with how this repo already separates logical Redis DBs by purpose), store it on `app.state.redis_ratelimit` (same `app.state` pattern), and call `check_rate_limit` from the same `ApiKeyAuthMiddleware.dispatch` right after the key lookup succeeds — return `429` with a `Retry-After` header on failure:

```python
        if not await check_rate_limit(request.app.state.redis_ratelimit, record.user_id):
            return JSONResponse(
                {"detail": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": "60"},
            )
```

This directly answers ADR weak point #6 ("no rate limiting ... a single caller can currently issue unlimited full-width queries") using the Redis instance already running in `docker-compose.yml`, no new service.

## 7. Rollout plan: log-only mode first, then enforce

Concrete, staged, matching how `docker-compose.yml`/`.env.example` already use env-var toggles for everything else in this stack:

1. **Stage 0 — ship middleware disabled by default.** Add `API_AUTH_MODE=off|log_only|enforce` to `config.py`/`.env.example`, default `off` (today's behavior, zero risk to the existing 1-day-prototype demo). Deploy this with zero behavior change; confirms the middleware wiring itself doesn't break MCP streaming (the `BaseHTTPMiddleware` caveat in §3) before it does anything meaningful.
2. **Stage 1 — `log_only` mode for ~1 week against real traffic.** Middleware runs the key lookup and rate-limit check, logs the outcome (`valid`/`missing`/`invalid`/`over_limit`) via the same `api.access` logger from §4, but never returns 401/429 — every request still passes through regardless of key validity. Purpose: catch any legitimate current caller/script that has no `Authorization` header yet (no HTTP client anywhere in `src/` calls the API today — confirmed — but any of the 5-10 users' own scripts or notebooks predate this change), see exactly who they are from the logs, and issue them keys via `scripts/issue_api_key.py` before flipping to enforcement.
3. **Stage 2 — issue keys to all 5-10 named users** via `scripts/issue_api_key.py`, out-of-band delivery, while still in `log_only` mode. Watch the access logs for a few days confirming every real caller now presents a valid key (zero `missing`/`invalid` log lines from real IPs, only from the expected background noise of internet scanners hitting the exposed Traefik route).
4. **Stage 3 — flip `API_AUTH_MODE=enforce`.** Middleware now returns 401 for missing/invalid/revoked keys and 429 over the rate limit. This is a single env var flip + redeploy, trivially revertible (`enforce`→`log_only`) if something unexpected breaks, with no code rollback needed.
5. **Stage 4 — remove `log_only`/`off` modes** once `enforce` has run cleanly for a couple weeks, collapsing `API_AUTH_MODE` back out of config entirely (avoids the mode flag becoming permanent unused complexity — matches the instinct behind picking a static file over ClickHouse in §1).

This gives every existing REST/MCP caller a non-breaking window to pick up a key before enforcement starts, without ever needing a maintenance-window cutover.

## Summary of artifacts this spec produces (for a future implementation pass — none written to the repo)

- `src/api/api_keys.py` — static registry (`ApiKeyRecord` dataclass + `API_KEYS` list), `known_limitations.py`-style.
- `src/api/auth.py` — `ApiKeyAuthMiddleware` (Starlette `BaseHTTPMiddleware`, app-level).
- `src/api/rate_limit.py` — `check_rate_limit()` against a dedicated Redis logical DB.
- `scripts/issue_api_key.py` — new directory, one-off CLI, no existing tooling conflicts with it.
- `main.py` — two additive lines (`app.state.api_keys = ...`, `app.add_middleware(ApiKeyAuthMiddleware)`), no change to route/mount structure.
- `.env.example` / `config.py` — `API_AUTH_MODE`, `REDIS_RATELIMIT_DB`, `RATE_LIMIT_PER_MINUTE`.
- No changes to `queries.py`, `rest.py` route signatures, or `mcp_tools.py` tool signatures — identity/rate-limiting is entirely middleware-layer, keeping the REST/MCP-parity guarantee both files' docstrings already assert intact.
