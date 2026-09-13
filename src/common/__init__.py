"""Genuinely shared, exchange-agnostic utilities.

Kept intentionally tiny per the "don't drag in more than needed" principle
(docs/04-architecture/00-overview.md §6.2). Only add things here once at
least two call sites actually need them.
"""

from .logging import get_logger

__all__ = ["get_logger"]
