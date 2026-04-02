from __future__ import annotations

from typing import Any


def choose_fallback(route: dict[str, Any]) -> dict[str, Any] | None:
    fallbacks = route.get("fallbacks", [])
    return fallbacks[0] if fallbacks else None
