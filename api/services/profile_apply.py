from __future__ import annotations

from typing import Any


def apply_profile_to_request(profile: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)

    retrieval_policy = profile.get("retrieval_policy", {})
    if retrieval_policy and not out.get("retrieval"):
        out["retrieval"] = {
            "k": retrieval_policy.get("k", 8),
            "min_score": retrieval_policy.get("min_score", 0.25),
            "scope": retrieval_policy.get("scope", "conversation"),
        }

    return out
