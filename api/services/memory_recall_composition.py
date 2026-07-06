from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

SAFE_STRATEGIES = {"implicit", "light_callback", "explicit_callback", "none"}
SUPPRESSED_STATES = {"suppressed", "demoted", "forgotten_or_demoted", "retracted"}
STALE_STATES = {"stale", "dormant", "parked", "unknown_freshness", "expired"}
CURRENT_STATES = {"active", "promoted", "reinforced", "corrected_replacement"}


@dataclass(frozen=True)
class MemoryRecallComposition:
    retrieval_bundle: dict[str, Any]
    prompt_messages: list[dict[str, str]]
    explicit_callbacks: list[str]
    trace: dict[str, Any]
    brief_grounding: dict[str, Any]


def _clean_text(value: Any, *, limit: int = 500) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.strip().split())
    return cleaned[:limit] if cleaned else None


def _source_ref(value: Any) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    ref_type = _clean_text(value.get("ref_type"), limit=64)
    ref_id = _clean_text(value.get("ref_id"), limit=160)
    if not ref_type or not ref_id:
        return None
    return {"ref_type": ref_type, "ref_id": ref_id}


def _source_refs(value: Any, *, limit: int = 6) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    refs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in value:
        ref = _source_ref(item)
        if ref is None:
            continue
        key = (ref["ref_type"], ref["ref_id"])
        if key not in seen:
            seen.add(key)
            refs.append(ref)
        if len(refs) >= limit:
            break
    return refs


def _item_id(item: dict[str, Any], *, artifact: bool) -> str | None:
    keys = ("artifact_id", "memory_id", "message_id") if artifact else ("memory_id", "message_id")
    for key in keys:
        value = _clean_text(item.get(key), limit=160)
        if value:
            return value
    source = _source_ref(item.get("source_ref"))
    if source:
        return f"{source['ref_type']}:{source['ref_id']}"
    return None


def _truth_state(item: dict[str, Any]) -> str:
    hygiene = item.get("memory_hygiene") if isinstance(item.get("memory_hygiene"), dict) else {}
    for key in ("promotion_state", "durable_status", "status", "freshness_state"):
        value = item.get(key) or hygiene.get(key)
        if isinstance(value, str) and value:
            return value.strip().lower()
    framing = item.get("_truth_framing") or hygiene.get("framing")
    if isinstance(framing, str) and framing != "current":
        return framing.strip().lower()
    return "active"


def _score(item: dict[str, Any], *keys: str, default: float = 0.5) -> float:
    for key in keys:
        value = item.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return max(0.0, min(1.0, float(value)))
    return default


def _candidate(item: dict[str, Any], *, artifact: bool) -> dict[str, Any] | None:
    candidate_id = _item_id(item, artifact=artifact)
    if not candidate_id:
        return None
    summary = item.get("snippet") if artifact else item.get("content")
    source_refs = _source_refs([item.get("source_ref")])
    metadata = {
        "truth_state": _truth_state(item),
        "explicit_callback_allowed": bool(item.get("explicit_callback_allowed")),
    }
    return {
        "candidate_id": candidate_id,
        "candidate_type": "artifact" if artifact else "memory_item",
        "summary": _clean_text(summary, limit=700) or candidate_id,
        "source_refs": source_refs,
        "relevance_score": _score(item, "relevance_score", "score", default=0.55),
        "salience_score": _score(item, "salience_score", "utility_score", default=0.5),
        "recency_score": _score(item, "recency_score", default=0.6),
        "metadata": metadata,
    }


def build_recall_candidates(retrieval_bundle: dict[str, Any]) -> list[dict[str, Any]]:
    bundle = retrieval_bundle.get("bundle") if isinstance(retrieval_bundle, dict) else {}
    bundle = bundle if isinstance(bundle, dict) else {}
    candidates: list[dict[str, Any]] = []
    for item in bundle.get("semantic") or []:
        if isinstance(item, dict):
            candidate = _candidate(item, artifact=False)
            if candidate:
                candidates.append(candidate)
    for item in bundle.get("artifact_refs") or []:
        if isinstance(item, dict):
            candidate = _candidate(item, artifact=True)
            if candidate:
                candidates.append(candidate)
    return candidates[:50]


def _decision_by_id(response: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(response, dict):
        return {}
    decisions = response.get("decisions")
    if not isinstance(decisions, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in decisions:
        if not isinstance(item, dict):
            continue
        candidate_id = _clean_text(item.get("candidate_id"), limit=160)
        strategy = item.get("mention_strategy")
        if candidate_id and strategy in SAFE_STRATEGIES:
            out[candidate_id] = item
    return out


def _format_memory_line(item: dict[str, Any], decision: dict[str, Any]) -> str | None:
    text = _clean_text(item.get("snippet") or item.get("content"), limit=500)
    if not text:
        return None
    strategy = decision.get("mention_strategy")
    prefix = {
        "implicit": "Use implicitly; do not explicitly mention this remembered context:",
        "light_callback": "Brief callback allowed:",
        "explicit_callback": "Direct continuity callback allowed:",
    }.get(strategy)
    if not prefix:
        return None
    return f"- {prefix} {text}"


def _format_episode_line(decision: dict[str, Any]) -> str | None:
    episode = decision.get("episode") if isinstance(decision.get("episode"), dict) else {}
    summary = _clean_text(episode.get("summary"), limit=500)
    title = _clean_text(episode.get("title"), limit=160)
    if not summary and not title:
        return None
    strategy = decision.get("callback_strategy")
    prefix = (
        "Direct episode callback allowed:"
        if strategy == "explicit_callback"
        else "Brief episode callback allowed:"
    )
    body = summary or title
    return f"- {prefix} {body}"


def _brief_source(
    kind: str, source_id: str | None, *, state: str, refs: list[dict[str, str]]
) -> dict[str, Any]:
    out: dict[str, Any] = {"kind": kind, "state": state, "source_refs": refs}
    if source_id:
        out["id"] = source_id
    return out


def compose_memory_recall_context(
    *,
    retrieval_bundle: dict[str, Any],
    recall_response: dict[str, Any] | None,
    episode_response: dict[str, Any] | None,
) -> MemoryRecallComposition:
    working = deepcopy(retrieval_bundle) if isinstance(retrieval_bundle, dict) else {}
    bundle = working.setdefault("bundle", {})
    if not isinstance(bundle, dict):
        bundle = {"recent": [], "semantic": [], "artifact_refs": []}
        working["bundle"] = bundle

    decisions = _decision_by_id(recall_response)
    suppressed_ids: list[str] = []
    prompt_lines: list[str] = []
    explicit_callbacks: list[str] = []
    sources: list[dict[str, Any]] = []
    uncertainty: list[str] = []
    omissions: list[dict[str, str]] = []

    for key, artifact in (("semantic", False), ("artifact_refs", True)):
        retained = []
        for item in bundle.get(key) or []:
            if not isinstance(item, dict):
                continue
            source_id = _item_id(item, artifact=artifact)
            state = _truth_state(item)
            if state in SUPPRESSED_STATES:
                if source_id:
                    suppressed_ids.append(source_id)
                omissions.append(
                    {"reason": f"{state}_memory_suppressed", "source_id": source_id or "unknown"}
                )
                continue
            decision = decisions.get(source_id or "")
            if decision and decision.get("decision") == "suppress":
                if source_id:
                    suppressed_ids.append(source_id)
                omissions.append(
                    {"reason": "recall_selection_suppressed", "source_id": source_id or "unknown"}
                )
                continue
            retained.append(item)
            refs = _source_refs([item.get("source_ref")])
            if state in STALE_STATES:
                uncertainty.append(f"{source_id or key}: {state}")
            sources.append(
                _brief_source(
                    "artifact" if artifact else "memory", source_id, state=state, refs=refs
                )
            )
            if decision and decision.get("mention_strategy") in {
                "implicit",
                "light_callback",
                "explicit_callback",
            }:
                line = _format_memory_line(item, decision)
                if line:
                    prompt_lines.append(line)
                    if decision.get("mention_strategy") in {"light_callback", "explicit_callback"}:
                        explicit_callbacks.append(line.split(":", 1)[-1].strip())
        bundle[key] = retained

    episode_decisions = []
    if isinstance(episode_response, dict) and isinstance(episode_response.get("decisions"), list):
        episode_decisions = [
            item for item in episode_response["decisions"] if isinstance(item, dict)
        ]
    episode_lines: list[str] = []
    for decision in episode_decisions:
        episode = decision.get("episode") if isinstance(decision.get("episode"), dict) else {}
        episode_id = _clean_text(decision.get("episode_id") or episode.get("episode_id"), limit=160)
        reasons = [
            str(item)[:80] for item in decision.get("reasons") or [] if isinstance(item, str)
        ]
        if decision.get("prompt_eligible") is not True or decision.get("decision") != "include":
            omissions.append(
                {
                    "reason": reasons[0] if reasons else "episode_callback_suppressed",
                    "source_id": episode_id or "unknown",
                }
            )
            continue
        line = _format_episode_line(decision)
        if line:
            episode_lines.append(line)
            explicit_callbacks.append(line.split(":", 1)[-1].strip())
        sources.append(
            _brief_source(
                "episode",
                episode_id,
                state="callback_eligible",
                refs=_source_refs(episode.get("source_refs")),
            )
        )

    prompt_messages: list[dict[str, str]] = []
    if prompt_lines or episode_lines:
        prompt_messages.append(
            {
                "role": "system",
                "content": "\n".join(
                    [
                        "Wave 4 memory and episode composition:",
                        *prompt_lines[:8],
                        *episode_lines[:6],
                        (
                            "- Respect implicit items by using them only for better guidance, "
                            "without naming them as remembered context."
                        ),
                        (
                            "- Do not restore suppressed memory, episode, or source context "
                            "in fallback attempts."
                        ),
                    ]
                ),
            }
        )

    trace = {
        "status": "composed",
        "recall": {
            "candidate_count": len(build_recall_candidates(retrieval_bundle)),
            "decision_count": len(decisions),
            "suppressed_ids": suppressed_ids[:20],
            "strategy_counts": {
                strategy: sum(
                    1 for item in decisions.values() if item.get("mention_strategy") == strategy
                )
                for strategy in ("implicit", "light_callback", "explicit_callback", "none")
            },
        },
        "episodes": {
            "decision_count": len(episode_decisions),
            "prompt_eligible_count": sum(
                1 for item in episode_decisions if item.get("prompt_eligible") is True
            ),
            "included_episode_ids": [
                item.get("episode_id")
                for item in episode_decisions
                if item.get("prompt_eligible") is True
            ][:20],
        },
        "omission_count": len(omissions),
    }
    brief_grounding = {
        "source_count": len(sources),
        "sources": sources[:20],
        "uncertainty": uncertainty[:12],
        "omissions": omissions[:20],
        "conflicts": [],
    }
    return MemoryRecallComposition(
        retrieval_bundle=working,
        prompt_messages=prompt_messages,
        explicit_callbacks=explicit_callbacks[:4],
        trace=trace,
        brief_grounding=brief_grounding,
    )
