"""Exchange-agnostic collector: connection lifecycle, reconnect/backoff,
writing raw messages to Redis Streams. See runner.py for the core loop and
adapter.py for the contract exchange-specific adapters must satisfy.
"""
