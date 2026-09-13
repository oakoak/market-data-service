"""Normalizer service: consumes raw collector messages from Redis Streams,
parses them into the unified ClickHouse schema, tracks order-book state for
sequence-break detection, and emits incident records.

See docs/04-architecture/00-overview.md §6.2/§6.3.1 and
docs/04-architecture/01-stack.md for the contract this package implements.
"""
