"""Known official sites for G0 when no ``utilities`` row is at hand.

The golden harness and replay run offline, so they read the curated
``utility_sites.json`` instead of the database. Live callers should build
the context from the Utility row with ``source_type.context_from_utility``.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.services.source_type import UtilitySourceContext, context_from_utility

SITES_PATH = (
    Path(__file__).resolve().parents[3]
    / "tests" / "fixtures" / "pricing_golden" / "utility_sites.json"
)


@lru_cache(maxsize=4)
def _load(path: str) -> dict[str, dict[str, Any]]:
    return json.loads(Path(path).read_text()).get("utilities") or {}


def official_context(
    utility_name: str | None, *, path: Path | None = None,
) -> UtilitySourceContext | None:
    """G0 context for a curated utility, or None when it is not on file."""
    row = _load(str(path or SITES_PATH)).get(utility_name or "")
    return context_from_utility(row) if row else None


__all__ = ["SITES_PATH", "official_context"]
