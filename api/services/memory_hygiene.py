from __future__ import annotations

from dataclasses import dataclass
from typing import Any

RetrievalKey = tuple[str, str]
_NON_CURRENT_FRAMINGS = {
    "parked_or_historical",
    "stale_or_unverified",
    "unknown_or_unverified",
}
_VALID_FRESHNESS_STATES = {
    "active",
    "parked",
    "stale",
    "contradicted",
    "superseded",
    "corrected",
    "invalidated",
    "expired",
    "retracted",
    "forgotten_or_demoted",
    "rebuilding",
    "unknown_freshness",
}
_VALID_RUNTIME_FRAMINGS = {
    "current",
    "parked_or_historical",
    "stale_or_unverified",
    "corrected_replacement",
    "omit",
    "unknown_or_unverified",
}
_VALID_EVIDENCE_ROLES = {"canonical", "derived"}
_VALID_DURABLE_STATUSES = {
    "active",
    "parked",
    "stale",
    "contradicted",
    "corrected",
    "invalidated",
    "superseded",
    "expired",
    "retracted",
    "forgotten_or_demoted",
    "rebuilding",
}
_VALID_SOURCE_AVAILABILITY = {
    "available",
    "missing",
    "malformed",
    "unavailable",
    "owner_mismatch",
    "not_applicable",
}
_DERIVED_OMIT_FRESHNESS_STATES = {
    "contradicted",
    "superseded",
    "invalidated",
    "retracted",
    "forgotten_or_demoted",
    "rebuilding",
}
_DERIVED_OMIT_DURABLE_STATUSES = {
    "contradicted",
    "invalidated",
    "retracted",
    "forgotten_or_demoted",
    "rebuilding",
    "superseded",
}
_CURRENT_FRAMINGS = {"current", "corrected_replacement"}
_MAX_STRUCTURAL_VALUE_LENGTH = 200
_FRESHNESS_FALLBACKS: dict[str, tuple[bool, bool, str, str]] = {
    "active": (True, True, "current", "active_fallback"),
    "parked": (True, False, "parked_or_historical", "parked_fallback"),
    "stale": (True, False, "stale_or_unverified", "stale_fallback"),
    "corrected": (True, False, "unknown_or_unverified", "corrected_unverified_fallback"),
    "contradicted": (False, False, "omit", "contradicted_fallback"),
    "invalidated": (False, False, "omit", "invalidated_fallback"),
    "expired": (True, False, "stale_or_unverified", "expired_fallback"),
    "retracted": (False, False, "omit", "retracted_fallback"),
    "rebuilding": (False, False, "omit", "rebuilding_fallback"),
    "superseded": (False, False, "omit", "superseded_fallback"),
    "forgotten_or_demoted": (
        False,
        False,
        "omit",
        "forgotten_or_demoted_fallback",
    ),
    "unknown_freshness": (
        True,
        False,
        "unknown_or_unverified",
        "unknown_freshness_fallback",
    ),
}


@dataclass(frozen=True)
class NormalizedMemoryHygienePayload:
    memory_id: str | None
    freshness_state: str
    durable_status: str | None
    last_verified_at: str | None
    source_kind: str | None
    confidence: float | None
    supersedes: str | None
    superseded_by: str | None

    def to_runtime_item(self, *, key: RetrievalKey) -> dict[str, Any]:
        return {
            "item_ref": {"ref_type": key[0], "ref_id": key[1]},
            "memory_id": self.memory_id,
            "freshness_state": self.freshness_state,
            "last_verified_at": self.last_verified_at,
            "source_kind": self.source_kind,
            "confidence": self.confidence,
            "supersedes": self.supersedes,
            "superseded_by": self.superseded_by,
        }


@dataclass(frozen=True)
class MemoryHygieneOccurrence:
    section: str
    index: int
    item: dict[str, Any]
    payload: NormalizedMemoryHygienePayload
    evidence_role: str | None
    source_availability: str | None
    pre_cr_decision: dict[str, Any] | None = None


@dataclass(frozen=True)
class CorrectedRelationshipState:
    valid_keys: frozenset[tuple[str, int]]
    invalid_keys: frozenset[tuple[str, int]]
    predecessor_keys: frozenset[tuple[str, int]]


@dataclass(frozen=True)
class MemoryHygieneApplicationResult:
    retrieval_bundle: dict[str, Any]
    trace: dict[str, Any]


def _domain_debug_summary(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    debug = retrieval_bundle.get("bundle", {}).get("retrieval_debug")
    if not isinstance(debug, dict):
        return {
            "domain_filters_requested": False,
            "allowed_filter_count": 0,
            "blocked_filter_count": 0,
            "tagged_records_evaluated": 0,
            "tagged_records_filtered": 0,
            "untagged_records_not_domain_enforced": 0,
            "domain_debug_status": "malformed_or_absent",
            "tagged_domain_enforcement_applied": False,
            "domain_enforcement_mode": None,
        }

    domain_filters_requested = bool(debug.get("domain_filters_requested", False))
    allowed_domains = debug.get("allowed_memory_domains")
    blocked_domains = debug.get("blocked_memory_domains")
    has_expected_fields = all(
        key in debug
        for key in (
            "tagged_records_evaluated",
            "tagged_records_filtered",
            "untagged_records_not_domain_enforced",
        )
    )
    if not domain_filters_requested:
        status = "not_requested"
    elif has_expected_fields:
        status = "available"
    else:
        status = "malformed_or_absent"

    return {
        "domain_filters_requested": domain_filters_requested,
        "allowed_filter_count": len(allowed_domains) if isinstance(allowed_domains, list) else 0,
        "blocked_filter_count": len(blocked_domains) if isinstance(blocked_domains, list) else 0,
        "tagged_records_evaluated": _safe_int(debug.get("tagged_records_evaluated")),
        "tagged_records_filtered": _safe_int(debug.get("tagged_records_filtered")),
        "untagged_records_not_domain_enforced": _safe_int(
            debug.get("untagged_records_not_domain_enforced")
        ),
        "domain_debug_status": status,
        "tagged_domain_enforcement_applied": bool(
            debug.get("tagged_domain_enforcement_applied", False)
        ),
        "domain_enforcement_mode": debug.get("domain_enforcement_mode"),
    }


def disabled_memory_hygiene_trace(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempted": False,
        "status": "disabled",
        "included": False,
        "runtime_call_status": "disabled",
        **_domain_debug_summary(retrieval_bundle),
    }


def _safe_int(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _normalize_freshness_state(value: Any) -> str:
    if not isinstance(value, str):
        return "unknown_freshness"
    normalized = value.strip().lower()
    if normalized in _VALID_FRESHNESS_STATES:
        return normalized
    return "unknown_freshness"


def _bounded_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped or len(stripped) > _MAX_STRUCTURAL_VALUE_LENGTH:
        return None
    return stripped


def _normalize_source_key(item: dict[str, Any]) -> RetrievalKey | None:
    source_ref = item.get("source_ref")
    if not isinstance(source_ref, dict):
        return None
    ref_type = _bounded_string(source_ref.get("ref_type"))
    ref_id = _bounded_string(source_ref.get("ref_id"))
    if ref_type is None or ref_id is None:
        return None
    return (ref_type, ref_id)


def _normalize_evidence_role(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if normalized in _VALID_EVIDENCE_ROLES else None


def _normalize_source_availability(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if normalized in _VALID_SOURCE_AVAILABILITY else None


def _normalize_durable_status(value: Any) -> str | None:
    normalized = _bounded_string(value)
    if normalized is None:
        return None
    normalized = normalized.lower()
    return normalized if normalized in _VALID_DURABLE_STATUSES else None


def _owner_matches(item: dict[str, Any], owner_id: str) -> bool:
    item_owner = item.get("owner_id")
    return isinstance(item_owner, str) and item_owner == owner_id


def _valid_item_identity(item: dict[str, Any], *, section: str) -> bool:
    if section in {"recent", "semantic"}:
        return _bounded_string(item.get("message_id")) is not None
    if section == "artifact_refs":
        return _bounded_string(item.get("artifact_id")) is not None
    return False


def _valid_source_check(check: Any) -> bool:
    if not isinstance(check, dict):
        return False
    return all(
        _bounded_string(check.get(key)) is not None
        for key in ("ref_type", "ref_id", "support_kind", "availability")
    ) and check.get("availability") == "available"


def _source_checks_available(item: dict[str, Any]) -> bool:
    checks = item.get("source_checks")
    if checks is None:
        return False
    if not isinstance(checks, list) or not checks:
        return False
    return all(_valid_source_check(check) for check in checks)


def _valid_source_refs(source_refs: Any) -> bool:
    if not isinstance(source_refs, list) or not source_refs:
        return False
    for ref in source_refs:
        if not isinstance(ref, dict):
            return False
        if not all(
            _bounded_string(ref.get(key)) is not None
            for key in ("ref_type", "ref_id", "support_kind")
        ):
            return False
    return True


def _valid_provenance(item: dict[str, Any], owner_id: str) -> bool:
    provenance = item.get("provenance")
    if not isinstance(provenance, dict):
        return False
    if _bounded_string(provenance.get("derived_id")) is None:
        return False
    if _bounded_string(provenance.get("owner_id")) != owner_id:
        return False
    if _bounded_string(provenance.get("derivation_type")) is None:
        return False
    return _valid_source_refs(provenance.get("source_refs"))


def _item_identity_values(occurrence: MemoryHygieneOccurrence) -> set[str]:
    values: set[str] = set()
    memory_id = _bounded_string(occurrence.payload.memory_id)
    if memory_id is not None:
        values.add(memory_id)
    source_key = _normalize_source_key(occurrence.item)
    if source_key is not None:
        values.add(source_key[1])
    if occurrence.section in {"recent", "semantic"}:
        message_id = _bounded_string(occurrence.item.get("message_id"))
        if message_id is not None:
            values.add(message_id)
    elif occurrence.section == "artifact_refs":
        artifact_id = _bounded_string(occurrence.item.get("artifact_id"))
        if artifact_id is not None:
            values.add(artifact_id)
    return values


def _stable_item_identity(occurrence: MemoryHygieneOccurrence) -> str | None:
    memory_id = _bounded_string(occurrence.payload.memory_id)
    if memory_id is not None:
        return memory_id
    source_key = _normalize_source_key(occurrence.item)
    if source_key is not None:
        return source_key[1]
    return None


def _corrected_relationship_state(
    occurrences: list[MemoryHygieneOccurrence],
) -> CorrectedRelationshipState:
    relationship_eligible_occurrences = [
        occurrence for occurrence in occurrences if occurrence.pre_cr_decision is None
    ]
    identity_index: dict[str, list[MemoryHygieneOccurrence]] = {}
    for occurrence in relationship_eligible_occurrences:
        for identity in _item_identity_values(occurrence):
            identity_index.setdefault(identity, []).append(occurrence)

    valid_keys: set[tuple[str, int]] = set()
    invalid_keys: set[tuple[str, int]] = set()
    predecessor_keys: set[tuple[str, int]] = set()
    for occurrence in relationship_eligible_occurrences:
        durable_status = occurrence.payload.durable_status
        if occurrence.payload.freshness_state != "corrected" and durable_status != "corrected":
            continue
        occurrence_key = (occurrence.section, occurrence.index)
        identity = _stable_item_identity(occurrence)
        supersedes = _bounded_string(occurrence.payload.supersedes)
        superseded_by = _bounded_string(occurrence.payload.superseded_by)
        if identity is None or supersedes is None or supersedes == identity or superseded_by:
            invalid_keys.add(occurrence_key)
            continue
        predecessors = [
            candidate
            for candidate in identity_index.get(supersedes, [])
            if (candidate.section, candidate.index) != occurrence_key
        ]
        if not predecessors:
            invalid_keys.add(occurrence_key)
            continue
        if any(
            candidate.item.get("owner_id") != occurrence.item.get("owner_id")
            for candidate in predecessors
        ):
            invalid_keys.add(occurrence_key)
            continue
        if any(
            _bounded_string(candidate.payload.superseded_by) not in {None, identity}
            for candidate in predecessors
        ):
            invalid_keys.add(occurrence_key)
            continue
        valid_keys.add(occurrence_key)
        for predecessor in predecessors:
            predecessor_keys.add((predecessor.section, predecessor.index))

    return CorrectedRelationshipState(
        valid_keys=frozenset(valid_keys),
        invalid_keys=frozenset(invalid_keys),
        predecessor_keys=frozenset(predecessor_keys),
    )


def _canonical_unknown(reason: str, freshness_state: str) -> dict[str, Any]:
    return _decision_dict(
        freshness_state=freshness_state,
        use_allowed=True,
        mention_as_current_allowed=False,
        framing="unknown_or_unverified",
    ) | {"reason": reason}


def _omit(reason: str, freshness_state: str = "unknown_freshness") -> dict[str, Any]:
    return _decision_dict(
        freshness_state=freshness_state,
        use_allowed=False,
        mention_as_current_allowed=False,
        framing="omit",
    ) | {"reason": reason}


def _pre_cr_decision(
    *,
    item: dict[str, Any],
    owner_id: str,
    section: str,
) -> tuple[str | None, str | None, dict[str, Any] | None]:
    evidence_role = _normalize_evidence_role(item.get("evidence_role"))
    source_availability = _normalize_source_availability(item.get("source_availability"))
    freshness_state = _normalize_freshness_state(item.get("freshness_state"))
    durable_status = _normalize_durable_status(item.get("durable_status"))

    if not _owner_matches(item, owner_id):
        return evidence_role, source_availability, _omit("owner_mismatch")

    expected_role = "derived" if section == "artifact_refs" else "canonical"
    if evidence_role != expected_role:
        return evidence_role, source_availability, _omit("invalid_evidence_role")

    if _normalize_source_key(item) is None:
        if evidence_role == "canonical":
            return (
                evidence_role,
                source_availability,
                _canonical_unknown("canonical_source_ref_invalid", freshness_state),
            )
        return (
            evidence_role,
            source_availability,
            _omit("derived_source_ref_invalid", freshness_state),
        )

    if not _valid_item_identity(item, section=section):
        if evidence_role == "canonical":
            return (
                evidence_role,
                source_availability,
                _canonical_unknown("canonical_identity_invalid", freshness_state),
            )
        return (
            evidence_role,
            source_availability,
            _omit("derived_identity_invalid", freshness_state),
        )

    if durable_status is None:
        if evidence_role == "canonical":
            return (
                evidence_role,
                source_availability,
                _canonical_unknown("canonical_durable_status_invalid", freshness_state),
            )
        return (
            evidence_role,
            source_availability,
            _omit("derived_durable_status_invalid", freshness_state),
        )

    if evidence_role == "canonical":
        if source_availability != "not_applicable":
            return (
                evidence_role,
                source_availability,
                _canonical_unknown("canonical_source_availability_malformed", freshness_state),
            )
        return evidence_role, source_availability, None

    if source_availability != "available":
        return (
            evidence_role,
            source_availability,
            _omit(f"derived_source_{source_availability or 'malformed'}", freshness_state),
        )
    if not _source_checks_available(item):
        return (
            evidence_role,
            source_availability,
            _omit("derived_source_checks_invalid", freshness_state),
        )
    if not _valid_provenance(item, owner_id):
        return (
            evidence_role,
            source_availability,
            _omit("derived_provenance_invalid", freshness_state),
        )
    if (
        freshness_state in _DERIVED_OMIT_FRESHNESS_STATES
        or durable_status in _DERIVED_OMIT_DURABLE_STATUSES
    ):
        return (
            evidence_role,
            source_availability,
            _omit("derived_lifecycle_omitted", freshness_state),
        )
    return evidence_role, source_availability, None


def _normalize_payload(item: dict[str, Any]) -> NormalizedMemoryHygienePayload:
    confidence = item.get("confidence")
    if not isinstance(confidence, (int, float)) or isinstance(confidence, bool):
        confidence = None
    else:
        confidence = float(confidence)
    return NormalizedMemoryHygienePayload(
        memory_id=item.get("memory_id") if isinstance(item.get("memory_id"), str) else None,
        freshness_state=_normalize_freshness_state(item.get("freshness_state")),
        durable_status=_normalize_durable_status(item.get("durable_status")),
        last_verified_at=(
            item.get("last_verified_at")
            if isinstance(item.get("last_verified_at"), str)
            else None
        ),
        source_kind=item.get("source_kind") if isinstance(item.get("source_kind"), str) else None,
        confidence=confidence,
        supersedes=item.get("supersedes") if isinstance(item.get("supersedes"), str) else None,
        superseded_by=(
            item.get("superseded_by") if isinstance(item.get("superseded_by"), str) else None
        ),
    )


def _iter_occurrences(
    retrieval_bundle: dict[str, Any],
    *,
    owner_id: str,
) -> list[MemoryHygieneOccurrence]:
    bundle = retrieval_bundle.get("bundle", {})
    occurrences: list[MemoryHygieneOccurrence] = []
    for section in ("recent", "semantic", "artifact_refs"):
        items = bundle.get(section)
        if not isinstance(items, list):
            continue
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            evidence_role, source_availability, pre_cr_decision = _pre_cr_decision(
                item=item,
                owner_id=owner_id,
                section=section,
            )
            occurrences.append(
                MemoryHygieneOccurrence(
                    section=section,
                    index=index,
                    item=item,
                    payload=_normalize_payload(item),
                    evidence_role=evidence_role,
                    source_availability=source_availability,
                    pre_cr_decision=pre_cr_decision,
                )
            )
    return occurrences


def _decision_dict(
    *,
    freshness_state: str,
    use_allowed: bool,
    mention_as_current_allowed: bool,
    framing: str,
) -> dict[str, Any]:
    return {
        "freshness_state": freshness_state,
        "use_allowed": use_allowed,
        "mention_as_current_allowed": mention_as_current_allowed,
        "framing": framing,
    }


def _fallback_for_payload(payload: NormalizedMemoryHygienePayload) -> dict[str, Any]:
    use_allowed, mention_as_current_allowed, framing, _ = _FRESHNESS_FALLBACKS[
        payload.freshness_state
    ]
    return _decision_dict(
        freshness_state=payload.freshness_state,
        use_allowed=use_allowed,
        mention_as_current_allowed=mention_as_current_allowed,
        framing=framing,
    )


def _local_permission_ceiling(
    occurrence: MemoryHygieneOccurrence,
    *,
    corrected_relationships: CorrectedRelationshipState,
) -> dict[str, Any]:
    occurrence_key = (occurrence.section, occurrence.index)
    freshness_state = occurrence.payload.freshness_state
    durable_status = occurrence.payload.durable_status

    if occurrence_key in corrected_relationships.predecessor_keys:
        return _omit("superseded_predecessor_omitted", freshness_state)

    if occurrence_key in corrected_relationships.invalid_keys:
        if occurrence.evidence_role == "canonical":
            return _canonical_unknown("invalid_corrected_relationship", freshness_state)
        return _omit("invalid_corrected_relationship", freshness_state)

    if occurrence_key in corrected_relationships.valid_keys:
        return _decision_dict(
            freshness_state="corrected",
            use_allowed=True,
            mention_as_current_allowed=True,
            framing="corrected_replacement",
        ) | {"reason": "valid_corrected_relationship"}

    if durable_status in {
        "contradicted",
        "superseded",
        "invalidated",
        "retracted",
        "forgotten_or_demoted",
        "rebuilding",
    } or freshness_state in {
        "contradicted",
        "superseded",
        "invalidated",
        "retracted",
        "forgotten_or_demoted",
        "rebuilding",
    }:
        return _omit("lifecycle_omitted", freshness_state)

    if durable_status == "expired" or freshness_state == "expired":
        return _decision_dict(
            freshness_state="expired",
            use_allowed=True,
            mention_as_current_allowed=False,
            framing="stale_or_unverified",
        ) | {"reason": "expired_non_current"}

    if durable_status == "parked" or freshness_state == "parked":
        return _decision_dict(
            freshness_state="parked",
            use_allowed=True,
            mention_as_current_allowed=False,
            framing="parked_or_historical",
        ) | {"reason": "parked_non_current"}

    if durable_status == "stale" or freshness_state == "stale":
        return _decision_dict(
            freshness_state="stale",
            use_allowed=True,
            mention_as_current_allowed=False,
            framing="stale_or_unverified",
        ) | {"reason": "stale_non_current"}

    if durable_status == "corrected" or freshness_state == "corrected":
        if occurrence.evidence_role == "canonical":
            return _canonical_unknown("corrected_relationship_unverified", freshness_state)
        return _omit("corrected_relationship_unverified", freshness_state)

    if freshness_state == "unknown_freshness":
        return _decision_dict(
            freshness_state="unknown_freshness",
            use_allowed=True,
            mention_as_current_allowed=False,
            framing="unknown_or_unverified",
        ) | {"reason": "unknown_non_current"}

    return _decision_dict(
        freshness_state="active",
        use_allowed=True,
        mention_as_current_allowed=True,
        framing="current",
    ) | {"reason": "active_current"}


def _logical_source_fallback(
    occurrences: list[MemoryHygieneOccurrence],
) -> dict[str, Any]:
    if any(
        occurrence.payload.freshness_state in {"superseded", "forgotten_or_demoted"}
        for occurrence in occurrences
    ):
        return _decision_dict(
            freshness_state="unknown_freshness",
            use_allowed=False,
            mention_as_current_allowed=False,
            framing="omit",
        )
    return _decision_dict(
        freshness_state="unknown_freshness",
        use_allowed=True,
        mention_as_current_allowed=False,
        framing="unknown_or_unverified",
    )


def _decision_key(decision: dict[str, Any]) -> tuple[Any, ...]:
    return (
        decision.get("freshness_state"),
        decision.get("use_allowed"),
        decision.get("mention_as_current_allowed"),
        decision.get("framing"),
    )


def _strict_runtime_decision(decision: dict[str, Any]) -> dict[str, Any] | None:
    freshness_state = decision.get("freshness_state")
    use_allowed = decision.get("use_allowed")
    mention_as_current_allowed = decision.get("mention_as_current_allowed")
    framing = decision.get("framing")

    if not isinstance(freshness_state, str):
        return None
    normalized_freshness_state = freshness_state.strip().lower()
    if normalized_freshness_state not in _VALID_FRESHNESS_STATES:
        return None
    if not isinstance(use_allowed, bool):
        return None
    if not isinstance(mention_as_current_allowed, bool):
        return None
    if not isinstance(framing, str) or framing not in _VALID_RUNTIME_FRAMINGS:
        return None
    if framing == "omit" and (use_allowed or mention_as_current_allowed):
        return None
    if not use_allowed and framing != "omit":
        return None
    if mention_as_current_allowed and framing not in _CURRENT_FRAMINGS:
        return None
    if framing in _CURRENT_FRAMINGS and not (use_allowed and mention_as_current_allowed):
        return None
    if framing in _NON_CURRENT_FRAMINGS and (not use_allowed or mention_as_current_allowed):
        return None
    if framing == "current" and (
        normalized_freshness_state != "active"
        or not use_allowed
        or not mention_as_current_allowed
    ):
        return None
    if framing == "corrected_replacement" and (
        normalized_freshness_state != "corrected"
        or not use_allowed
        or not mention_as_current_allowed
    ):
        return None
    if framing == "parked_or_historical" and (
        normalized_freshness_state != "parked"
        or not use_allowed
        or mention_as_current_allowed
    ):
        return None
    if framing == "stale_or_unverified" and (
        normalized_freshness_state not in {"stale", "expired"}
        or not use_allowed
        or mention_as_current_allowed
    ):
        return None
    if framing == "unknown_or_unverified" and (
        normalized_freshness_state != "unknown_freshness"
        or not use_allowed
        or mention_as_current_allowed
    ):
        return None
    if framing == "omit" and normalized_freshness_state == "active":
        return None

    return _decision_dict(
        freshness_state=normalized_freshness_state,
        use_allowed=use_allowed,
        mention_as_current_allowed=mention_as_current_allowed,
        framing=framing,
    )


def _decision_rank(decision: dict[str, Any]) -> int:
    if not decision["use_allowed"] or decision["framing"] == "omit":
        return 0
    if not decision["mention_as_current_allowed"] or decision["framing"] in _NON_CURRENT_FRAMINGS:
        return 1
    return 2


def _intersect_decisions(
    *,
    local_decision: dict[str, Any],
    runtime_decision: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    local_framing = local_decision["framing"]
    runtime_framing = runtime_decision["framing"]

    if not local_decision["use_allowed"]:
        if runtime_decision["use_allowed"] or runtime_framing != "omit":
            reasons.append("runtime_exceeded_local_omit")
        return {
            key: local_decision[key]
            for key in ("freshness_state", "use_allowed", "mention_as_current_allowed", "framing")
        }, reasons

    if not runtime_decision["use_allowed"]:
        return {
            key: runtime_decision[key]
            for key in ("freshness_state", "use_allowed", "mention_as_current_allowed", "framing")
        }, reasons

    if local_framing == "corrected_replacement":
        if runtime_framing == "omit":
            return _decision_dict(
                freshness_state="corrected",
                use_allowed=False,
                mention_as_current_allowed=False,
                framing="omit",
            ), reasons
        if runtime_framing in _NON_CURRENT_FRAMINGS:
            return _decision_dict(
                freshness_state=runtime_decision["freshness_state"],
                use_allowed=True,
                mention_as_current_allowed=False,
                framing=runtime_framing,
            ), reasons
        if runtime_framing == "current":
            reasons.append("runtime_current_for_corrected_replacement")
        return _decision_dict(
            freshness_state="corrected",
            use_allowed=True,
            mention_as_current_allowed=True,
            framing="corrected_replacement",
        ), reasons

    if runtime_framing == "corrected_replacement":
        reasons.append("runtime_corrected_without_local_relationship")
        runtime_framing = "current"
        runtime_decision = _decision_dict(
            freshness_state="active",
            use_allowed=True,
            mention_as_current_allowed=True,
            framing="current",
        )

    local_rank = _decision_rank(local_decision)
    runtime_rank = _decision_rank(runtime_decision)
    if runtime_rank > local_rank:
        reasons.append("runtime_exceeded_local_currentness")
        selected = local_decision
    else:
        selected = runtime_decision

    return {
        key: selected[key]
        for key in ("freshness_state", "use_allowed", "mention_as_current_allowed", "framing")
    }, reasons


def _annotate_item(item: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    framed_item = dict(item)
    framing = decision["framing"]
    if framing in _NON_CURRENT_FRAMINGS:
        framed_item["memory_hygiene"] = {
            "freshness_state": decision["freshness_state"],
            "mention_as_current_allowed": False,
            "framing": framing,
        }
    else:
        framed_item.pop("memory_hygiene", None)
    framed_item["_truth_framing"] = framing
    return framed_item


def _reason_counts(occurrences: list[MemoryHygieneOccurrence]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for occurrence in occurrences:
        reason = (occurrence.pre_cr_decision or {}).get("reason")
        if isinstance(reason, str):
            counts[reason] = counts.get(reason, 0) + 1
    return counts


async def apply_memory_hygiene(
    *,
    runtime: Any | None,
    enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    retrieval_bundle: dict[str, Any],
) -> MemoryHygieneApplicationResult:
    if not enabled:
        return MemoryHygieneApplicationResult(
            retrieval_bundle=retrieval_bundle,
            trace=disabled_memory_hygiene_trace(retrieval_bundle),
        )

    occurrences = _iter_occurrences(retrieval_bundle, owner_id=owner_id)
    corrected_relationships = _corrected_relationship_state(occurrences)
    grouped: dict[RetrievalKey, list[MemoryHygieneOccurrence]] = {}
    invalid_occurrences: list[MemoryHygieneOccurrence] = []
    for occurrence in occurrences:
        if occurrence.pre_cr_decision is not None:
            continue
        key = _normalize_source_key(occurrence.item)
        if key is None:
            invalid_occurrences.append(occurrence)
            continue
        grouped.setdefault(key, []).append(occurrence)

    ambiguous_keys: set[RetrievalKey] = set()
    runtime_items: list[dict[str, Any]] = []
    local_permission_ceilings: dict[RetrievalKey, dict[str, Any]] = {}
    for key, key_occurrences in grouped.items():
        first_payload = key_occurrences[0].payload
        if any(occurrence.payload != first_payload for occurrence in key_occurrences[1:]):
            ambiguous_keys.add(key)
            continue
        local_permission_ceilings[key] = _local_permission_ceiling(
            key_occurrences[0],
            corrected_relationships=corrected_relationships,
        )
        runtime_items.append(first_payload.to_runtime_item(key=key))

    runtime_call_status = "skipped_no_submittable_items"
    valid_runtime_decisions: dict[RetrievalKey, dict[str, Any]] = {}
    duplicate_decision_count = 0
    conflicting_decision_count = 0
    invalid_decision_count = 0
    fallback_reason: str | None = None
    conflicting_keys: set[RetrievalKey] = set()
    invalid_decision_keys: set[RetrievalKey] = set()

    if runtime_items:
        if runtime is None:
            runtime_call_status = "failed"
            fallback_reason = "runtime_client_not_configured"
        else:
            try:
                response = await runtime.evaluate_memory_hygiene(
                    request_id=request_id,
                    owner_id=owner_id,
                    conversation_id=conversation_id,
                    surface=surface,
                    runtime_session_id=runtime_session_id,
                    runtime_turn_id=runtime_turn_id,
                    items=runtime_items,
                )
                runtime_call_status = "included"
            except Exception:
                response = None
                runtime_call_status = "failed"
                fallback_reason = "runtime_unavailable"

            if runtime_call_status == "included":
                result = response.get("result") if isinstance(response, dict) else None
                decisions = result.get("decisions") if isinstance(result, dict) else None
                if not isinstance(decisions, list):
                    runtime_call_status = "malformed"
                    fallback_reason = "malformed_memory_hygiene_response"
                else:
                    for decision in decisions:
                        if not isinstance(decision, dict):
                            continue
                        item_ref = decision.get("item_ref")
                        if not isinstance(item_ref, dict):
                            continue
                        ref_type = item_ref.get("ref_type")
                        ref_id = item_ref.get("ref_id")
                        if not isinstance(ref_type, str) or not isinstance(ref_id, str):
                            continue
                        key = (ref_type, ref_id)
                        if key not in grouped or key in ambiguous_keys:
                            continue
                        normalized = _strict_runtime_decision(decision)
                        if normalized is None:
                            invalid_decision_count += 1
                            invalid_decision_keys.add(key)
                            valid_runtime_decisions.pop(key, None)
                            continue
                        if key in invalid_decision_keys:
                            continue
                        existing = valid_runtime_decisions.get(key)
                        if existing is None and key not in conflicting_keys:
                            valid_runtime_decisions[key] = normalized
                            continue
                        if existing is not None and _decision_key(existing) == _decision_key(
                            normalized
                        ):
                            duplicate_decision_count += 1
                            continue
                        conflicting_decision_count += 1
                        conflicting_keys.add(key)
                        valid_runtime_decisions.pop(key, None)

                    if conflicting_keys:
                        fallback_reason = "conflicting_runtime_decisions"
                    elif invalid_decision_keys:
                        fallback_reason = "invalid_runtime_decisions"

    grouped_decisions: dict[RetrievalKey, dict[str, Any]] = {}
    fallback_applied = False
    missing_decision_keys: set[RetrievalKey] = set()
    runtime_decision_narrowed_count = 0
    runtime_decision_narrowing_reasons: dict[str, int] = {}

    def intersect_for_key(
        key: RetrievalKey,
        runtime_decision: dict[str, Any],
    ) -> dict[str, Any]:
        nonlocal runtime_decision_narrowed_count
        local_ceiling = local_permission_ceilings.get(key)
        if local_ceiling is None:
            local_ceiling = _local_permission_ceiling(
                grouped[key][0],
                corrected_relationships=corrected_relationships,
            )
        intersected, reasons = _intersect_decisions(
            local_decision=local_ceiling,
            runtime_decision=runtime_decision,
        )
        if reasons or _decision_key(intersected) != _decision_key(runtime_decision):
            runtime_decision_narrowed_count += 1
        for reason in reasons:
            runtime_decision_narrowing_reasons[reason] = (
                runtime_decision_narrowing_reasons.get(reason, 0) + 1
            )
        return intersected

    for key, key_occurrences in grouped.items():
        if key in ambiguous_keys:
            grouped_decisions[key] = intersect_for_key(
                key,
                _logical_source_fallback(key_occurrences),
            )
            fallback_applied = True
            continue
        if key in conflicting_keys:
            grouped_decisions[key] = intersect_for_key(
                key,
                _logical_source_fallback(key_occurrences),
            )
            fallback_applied = True
            missing_decision_keys.add(key)
            continue
        if key in invalid_decision_keys:
            grouped_decisions[key] = intersect_for_key(
                key,
                _fallback_for_payload(key_occurrences[0].payload),
            )
            fallback_applied = True
            missing_decision_keys.add(key)
            continue
        decision = valid_runtime_decisions.get(key)
        if decision is not None:
            grouped_decisions[key] = intersect_for_key(key, decision)
            continue
        if runtime_items:
            grouped_decisions[key] = intersect_for_key(
                key,
                _fallback_for_payload(key_occurrences[0].payload),
            )
            fallback_applied = True
            missing_decision_keys.add(key)

    sanitized_bundle = dict(retrieval_bundle.get("bundle", {}))
    counts_by_framing: dict[str, int] = {}
    omitted_occurrence_count = 0
    retained_non_current_occurrence_count = 0
    missing_decision_count = len(missing_decision_keys)

    for section in ("recent", "semantic", "artifact_refs"):
        original_items = sanitized_bundle.get(section)
        if not isinstance(original_items, list):
            continue
        updated_items: list[dict[str, Any]] = []
        for index, item in enumerate(original_items):
            if not isinstance(item, dict):
                updated_items.append(item)
                continue
            matching_occurrence = next(
                (
                    occurrence
                    for occurrence in occurrences
                    if occurrence.section == section and occurrence.index == index
                ),
                None,
            )
            if matching_occurrence is None:
                updated_items.append(item)
                continue

            key = _normalize_source_key(item)
            if matching_occurrence.pre_cr_decision is not None:
                allowed_decision_fields = {
                    "freshness_state",
                    "use_allowed",
                    "mention_as_current_allowed",
                    "framing",
                }
                decision = {
                    key: value
                    for key, value in matching_occurrence.pre_cr_decision.items()
                    if key in allowed_decision_fields
                }
                fallback_applied = True
            elif key is None:
                decision = _fallback_for_payload(matching_occurrence.payload)
                fallback_applied = True
            else:
                decision = grouped_decisions.get(key)
                if decision is None:
                    decision = intersect_for_key(key, _logical_source_fallback(grouped[key]))
                    fallback_applied = True

            framing = decision["framing"]
            counts_by_framing[framing] = counts_by_framing.get(framing, 0) + 1
            if not decision["use_allowed"]:
                omitted_occurrence_count += 1
                continue
            if framing in _NON_CURRENT_FRAMINGS:
                retained_non_current_occurrence_count += 1
            updated_items.append(_annotate_item(item, decision))
        sanitized_bundle[section] = updated_items

    current_count = 0
    historical_count = 0
    current_canonical_count = 0
    current_supported_derived_count = 0
    corrected_replacement_count = 0
    stale_or_unverified_count = 0
    for section in ("recent", "semantic", "artifact_refs"):
        items = sanitized_bundle.get(section)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            framing = item.get("_truth_framing", "current")
            role = item.get("evidence_role")
            if framing in _CURRENT_FRAMINGS:
                current_count += 1
                if role == "canonical":
                    current_canonical_count += 1
                elif role == "derived":
                    current_supported_derived_count += 1
                if framing == "corrected_replacement":
                    corrected_replacement_count += 1
            else:
                historical_count += 1
                if framing in {"stale_or_unverified", "unknown_or_unverified"}:
                    stale_or_unverified_count += 1
    no_safe_current_evidence = current_count == 0

    if runtime_items:
        evaluated_decision_count = len(valid_runtime_decisions)
    else:
        evaluated_decision_count = 0
    local_permission_ceiling_applied_count = sum(
        1
        for decision in grouped_decisions.values()
        if decision.get("framing") != "current" or decision.get("freshness_state") != "active"
    )

    if runtime_call_status == "included":
        if fallback_applied:
            status = "fallback_partial"
        else:
            status = "included"
    elif runtime_call_status in {"failed", "malformed"}:
        status = "fallback_all"
    else:
        status = "included"

    trace = {
        "attempted": True,
        "status": status,
        "included": bool(
            evaluated_decision_count
            or fallback_applied
            or ambiguous_keys
            or invalid_occurrences
        ),
        "runtime_call_status": runtime_call_status,
        "submitted_unique_item_count": len(runtime_items),
        "evaluated_decision_count": evaluated_decision_count,
        "omitted_occurrence_count": omitted_occurrence_count,
        "retained_non_current_occurrence_count": retained_non_current_occurrence_count,
        "counts_by_framing": counts_by_framing,
        "fallback_applied": fallback_applied,
        "fallback_reason": fallback_reason,
        "duplicate_metadata_conflict_count": len(ambiguous_keys),
        "invalid_source_ref_occurrence_count": len(invalid_occurrences),
        "duplicate_decision_count": duplicate_decision_count,
        "conflicting_decision_count": conflicting_decision_count,
        "invalid_decision_count": invalid_decision_count,
        "missing_decision_count": missing_decision_count,
        "local_permission_ceiling_applied_count": local_permission_ceiling_applied_count,
        "runtime_decision_narrowed_count": runtime_decision_narrowed_count,
        "runtime_decision_narrowing_reasons": runtime_decision_narrowing_reasons,
        "truth_selection": {
            "current_canonical_evidence_count": current_canonical_count,
            "current_supported_derivative_count": current_supported_derived_count,
            "historical_or_parked_context_count": historical_count,
            "stale_or_unverified_context_count": stale_or_unverified_count,
            "omitted_context_count": omitted_occurrence_count,
            "corrected_replacement_count": corrected_replacement_count,
            "valid_corrected_relationship_count": len(corrected_relationships.valid_keys),
            "invalid_corrected_relationship_count": len(corrected_relationships.invalid_keys),
            "superseded_predecessor_omission_count": len(
                corrected_relationships.predecessor_keys
            ),
            "no_safe_current_evidence": no_safe_current_evidence,
            "pre_cr_rejection_reasons": _reason_counts(occurrences),
            "provider_visible_current_count": current_count,
            "provider_visible_historical_count": historical_count,
        },
        **_domain_debug_summary(retrieval_bundle),
    }

    return MemoryHygieneApplicationResult(
        retrieval_bundle={**retrieval_bundle, "bundle": sanitized_bundle},
        trace=trace,
    )
