"""API service: read-only REST + MCP serving layer over the ClickHouse tables
the normalizer writes (`trades`, `orderbook_events`, `orderbook_snapshots`,
`incidents`). See docs/04-architecture/00-overview.md §6.3 ("Serving:
REST/WS API + MCP tools") and docs/03-mvp-scope.md for the tool/endpoint
contract this package implements.
"""
