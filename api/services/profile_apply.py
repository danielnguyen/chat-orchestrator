from __future__ import annotations

import re
from typing import Any


def apply_profile_to_request(profile: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)

    retrieval_policy = profile.get("retrieval_policy", {})
    if retrieval_policy and not out.get("retrieval"):
        out["retrieval"] = {
            "k": retrieval_policy.get("k", 8),
            "min_score": retrieval_policy.get("min_score", 0.25),
            "scope": retrieval_policy.get("scope", "conversation"),
            "time_window": retrieval_policy.get("time_window", "all"),
            "retrieval_mode": retrieval_policy.get("retrieval_mode", "balanced"),
        }

    return out


_SPEAKER_REFERENCE = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9_.-]{0,119}):([a-z][a-z0-9_-]{0,63}):([1-9][0-9]{0,8})"
)
# BMS uses dev for its unnamed seed/fallback, not a speaker identity.
_GENERIC_SPEAKERS = {"dev", "default", "generic", "neutral", "assistant", "unresolved"}


def speaker_label(reference: Any) -> str | None:
    """Render only bounded profile identifiers, never profile prose or an owner ID."""
    if not isinstance(reference, str):
        return None
    match = _SPEAKER_REFERENCE.fullmatch(reference)
    if not match or match[2] in _GENERIC_SPEAKERS:
        return None
    name = match[2]
    return {"star_trek_q": "Q", "picard": "Picard", "star_trek_picard": "Picard"}.get(name, name)


def resolved_speaker_reference(profile: dict[str, Any], *, owner_id: str) -> str | None:
    reference = profile.get("effective_profile_ref")
    expected = f"{owner_id}:{profile.get('profile_name')}:{profile.get('profile_version')}"
    if (reference == expected and profile.get("owner_id", owner_id) == owner_id
            and speaker_label(reference) is not None):
        return reference
    return None
