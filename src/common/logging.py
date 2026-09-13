"""Minimal structured (JSON-lines) logging setup.

Stdout JSON logs are the simplest thing that plays well with `docker logs` /
any log shipper later, without pulling in a logging framework dependency.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": round(time.time() * 1000) / 1000,
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Anything passed via logging's `extra={...}` ends up as attributes on
        # the record; surface it so callers can attach structured context
        # (e.g. exchange, symbol, stream) without string-formatting it in.
        reserved = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
            "message",
            "asctime",
        }
        for key, value in record.__dict__.items():
            if key not in reserved:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_CONFIGURED = False


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a module-level logger, configuring JSON-lines-to-stdout once."""
    global _CONFIGURED
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(level)
        _CONFIGURED = True
    return logging.getLogger(name)
