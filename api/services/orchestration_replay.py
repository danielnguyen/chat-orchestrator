from __future__ import annotations

import hashlib
import json
import tempfile
from copy import deepcopy
from difflib import unified_diff
from pathlib import Path
from typing import Any

import httpx
from services.orchestrate import orchestrate_chat

DEFAULT_CORPUS_PATH = (
    Path(__file__).resolve().parents[1] / "replay" / "orchestration_scenarios.v1.json"
)
RULES_PATH = Path(__file__).resolve().parents[1] / "router" / "rules.yaml"
NO_FALLBACK_RULES_PATH = Path(__file__).resolve().parents[1] / "replay" / "rules_no_fallback.yaml"
REGISTRY_PATH = Path(__file__).resolve().parents[1] / "router" / "model_registry.yaml"

_BANNED_SNAPSHOT_KEYS = {
    "content",
    "messages",
    "query",
    "answer",
    "authorization",
    "api_key",
    "prompt_text",
    "raw_response",
    "exception",
    "stack_trace",
    "snippet",
}


class BoundaryFailure(RuntimeError):
    pass


class ReplayMemoryStore:
    def __init__(self, scenario: dict[str, Any], calls: list[dict[str, Any]]) -> None:
        self.scenario = scenario
        self.calls = calls
        self.trace: dict[str, Any] | None = None
        self.message_ordinal = 0

    def _record(self, name: str, request_id: str | None = None, **details: Any) -> None:
        self.calls.append({"name": name, "request_id": request_id, **details})

    async def resolve_conversation(self, **kwargs: Any) -> dict[str, Any]:
        self._record("conversation_resolution")
        return {"conversation_id": "00000000-0000-0000-0000-000000000001", "reused": False}

    async def add_message(self, **kwargs: Any) -> dict[str, Any]:
        self.message_ordinal += 1
        role = kwargs["role"]
        request_id = (kwargs.get("metadata") or {}).get("request_id")
        self._record(
            f"{role}_message_persistence",
            request_id,
            policy_metadata_present=kwargs.get("policy_metadata") is not None,
        )
        return {"message_id": f"message-{self.message_ordinal}"}

    async def resolve_profile(self, **kwargs: Any) -> dict[str, Any]:
        self._record("profile_resolution")
        profile = self.scenario.get("profile")
        profile = profile if isinstance(profile, dict) else {}
        return {
            "profile_name": "neutral",
            "source": "global_default",
            "profile_version": 1,
            "effective_profile_ref": "owner:neutral:1",
            "prompt_overlay": profile.get("prompt_overlay", ""),
            "retrieval_policy": {},
            "routing_policy": profile.get("routing_policy", {}),
            "response_style": {},
            "safety_policy": {},
            "tool_policy": {},
            "prompt_budget": profile.get("prompt_budget"),
        }

    async def retrieve_bundle(self, **kwargs: Any) -> dict[str, Any]:
        request_id = kwargs["request_id"]
        self._record(
            "bms_retrieval",
            request_id,
            containment_policy_present=kwargs.get("containment_policy") is not None,
        )
        mode = self.scenario.get("retrieval", "normal")
        if mode == "unavailable":
            raise BoundaryFailure("bms_unavailable")
        if mode == "request_id_mismatch":
            raise RuntimeError("retrieval_request_id_mismatch")
        debug: dict[str, Any] = {"vector_status": "ok"}
        semantic: list[dict[str, Any]] = [
            {
                "owner_id": "owner-replay",
                "conversation_id": kwargs["conversation_id"],
                "evidence_role": "canonical",
                "message_id": "memory-1",
                "created_at": "2026-01-01T00:00:00+00:00",
                "role": "assistant",
                "content": "neutral memory fixture",
                "source_ref": {"ref_type": "message", "ref_id": "memory-1"},
                "source_availability": "not_applicable",
                "freshness_state": "active",
                "durable_status": "active",
                "policy_metadata": {
                    "memory_domains": ["technical"],
                    "sensitivity": "medium",
                    "entity_ids": ["entity_repo"],
                    "relationship_ids": ["rel_project"],
                    "relationship_scopes": ["project_context"],
                },
            }
        ]
        artifacts: list[dict[str, Any]] = [
            {
                "owner_id": "owner-replay",
                "evidence_role": "derived",
                "artifact_id": "artifact-1",
                "file_path": "fixture.txt",
                "snippet": "neutral artifact fixture",
                "relevance_score": 0.9,
                "source_ref": {"ref_type": "derived_text", "ref_id": "derived-1"},
                "source_availability": "available",
                "source_checks": [
                    {
                        "ref_type": "message",
                        "ref_id": "memory-1",
                        "support_kind": "direct",
                        "availability": "available",
                    }
                ],
                "provenance": {
                    "derived_id": "derived-1",
                    "owner_id": "owner-replay",
                    "derivation_type": "derived_text",
                    "source_refs": [
                        {
                            "ref_type": "message",
                            "ref_id": "memory-1",
                            "support_kind": "direct",
                        }
                    ],
                    "derivation_version": "v1",
                    "created_at": "2026-01-01T00:00:00Z",
                    "status": "active",
                    "effective_status": "active",
                    "confidence": 0.9,
                    "explanation": "bounded provenance",
                    "generation_trace_id": "trace-1",
                    "compatibility_defaults": [],
                    "provenance_status": "complete",
                    "retrieval_reason": "semantic_match",
                },
                "freshness_state": "active",
                "durable_status": "active",
                "policy_metadata": {
                    "memory_domains": ["technical"],
                    "sensitivity": "medium",
                    "content_class": "document",
                    "entity_ids": ["entity_repo"],
                    "relationship_ids": ["rel_project"],
                    "relationship_scopes": ["project_context"],
                },
            }
        ]
        if mode == "missing_derivative":
            semantic = []
            debug.update({"degraded": True, "fallback": "missing_derivative_source"})
        elif mode == "stale_derivative":
            semantic[0]["freshness_state"] = "contradicted"
            semantic[0]["memory_hygiene"] = {
                "freshness_state": "contradicted",
                "framing": "stale_or_unverified",
            }
            debug.update({"degraded": True, "fallback": "contradicted_derivative"})
        elif mode == "malformed_metadata":
            semantic[0]["source_ref"] = "invalid"
            debug.update({"degraded": True, "fallback": "malformed_retrieval_metadata"})
        elif mode == "vector_unavailable":
            debug.update(
                {
                    "degraded": True,
                    "fallback": "vector_unavailable",
                    "vector_status": "unavailable",
                }
            )
        elif mode == "artifact_unavailable":
            artifacts = []
            debug.update({"degraded": True, "fallback": "artifact_unavailable"})
        elif mode == "wave3b_mixed_result_boundary":
            semantic.append(
                {
                    **semantic[0],
                    "message_id": "memory-restricted",
                    "content": "restricted replay memory",
                    "source_ref": {"ref_type": "message", "ref_id": "memory-restricted"},
                    "policy_metadata": {
                        **semantic[0]["policy_metadata"],
                        "sensitivity": "restricted",
                    },
                }
            )
            artifacts.append(
                {
                    **artifacts[0],
                    "artifact_id": "artifact-blocked",
                    "snippet": "blocked replay artifact",
                    "source_ref": {
                        "ref_type": "derived_text",
                        "ref_id": "derived-blocked",
                    },
                    "policy_metadata": {
                        **artifacts[0]["policy_metadata"],
                        "memory_domains": ["finance"],
                    },
                }
            )
        elif mode == "wave3b_unauthorized_artifact_returned":
            artifacts[0].update(
                {
                    "artifact_id": "artifact-unauthorized",
                    "snippet": "unauthorized replay artifact",
                    "source_ref": {
                        "ref_type": "derived_text",
                        "ref_id": "derived-unauthorized",
                    },
                    "policy_metadata": {
                        **artifacts[0]["policy_metadata"],
                        "memory_domains": ["finance"],
                    },
                }
            )
        elif mode == "wave3b_relationship_projection":
            semantic[0]["content"] = "selected relationship scoped replay memory"
            semantic.append(
                {
                    **semantic[0],
                    "message_id": "memory-revoked-relationship",
                    "content": "excluded relationship replay memory",
                    "source_ref": {
                        "ref_type": "message",
                        "ref_id": "memory-revoked-relationship",
                    },
                    "policy_metadata": {
                        **semantic[0]["policy_metadata"],
                        "entity_ids": ["entity_revoked"],
                        "relationship_ids": ["rel_revoked"],
                    },
                }
            )
            artifacts = []
        elif mode == "wave3b_privacy_side_channels":
            semantic[0]["content"] = "PRIVATE_WAVE3B_REPLAY_CONTENT_SENTINEL"
            semantic[0]["policy_metadata"] = {
                **semantic[0]["policy_metadata"],
                "sensitivity": "high",
            }
            artifacts[0].update(
                {
                    "snippet": "PRIVATE_WAVE3B_REPLAY_ARTIFACT_SENTINEL",
                    "download_url": "https://signed.example/PRIVATE_WAVE3B_SIGNED_URL",
                    "object_uri": "memory://PRIVATE_WAVE3B_OBJECT_URI",
                    "credentials": "PRIVATE_WAVE3B_CREDENTIAL",
                    "provenance": {
                        **artifacts[0]["provenance"],
                        "explanation": "PRIVATE_WAVE3B_PROVENANCE_SENTINEL",
                    },
                }
            )
            debug.update({"reason_codes": ["source_unavailable"]})
        elif mode == "wave3b_malformed_response":
            return {
                "request_id": "wrong-replay-request",
                "conversation_id": kwargs["conversation_id"],
                "bundle": {
                    "recent": [],
                    "semantic": semantic,
                    "artifact_refs": artifacts,
                    "observed_metadata": {"has_code_like_content": True},
                    "retrieval_debug": debug,
                },
            }
        elif mode == "truth_active_parked":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Old plan was Beta."
            artifacts[0]["freshness_state"] = "parked"
            artifacts[0]["durable_status"] = "parked"
        elif mode == "truth_active_stale":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Old plan was Beta."
            artifacts[0]["freshness_state"] = "stale"
            artifacts[0]["durable_status"] = "stale"
        elif mode == "truth_stale_only":
            semantic[0]["content"] = "Old plan was Beta."
            semantic[0]["freshness_state"] = "stale"
            semantic[0]["durable_status"] = "stale"
            artifacts = []
        elif mode == "truth_missing_source":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Missing-source derivative says Beta."
            artifacts[0]["source_availability"] = "missing"
        elif mode == "truth_cross_owner":
            semantic = []
            artifacts[0]["owner_id"] = "other-owner"
            artifacts[0]["snippet"] = "Cross-owner derivative says Beta."
        elif mode == "truth_malformed_source_ref":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Malformed derivative says Beta."
            artifacts[0]["source_ref"] = {"ref_type": "", "ref_id": "derived-1"}
        elif mode == "truth_incomplete_source_check":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Incomplete-check derivative says Beta."
            artifacts[0]["source_checks"] = [{"availability": "available"}]
        elif mode == "truth_missing_provenance_identity":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Missing-provenance-id derivative says Beta."
            artifacts[0]["provenance"].pop("derived_id", None)
        elif mode == "truth_missing_provenance_type":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Missing-provenance-type derivative says Beta."
            artifacts[0]["provenance"].pop("derivation_type", None)
        elif mode == "truth_unknown_durable_status":
            semantic[0]["content"] = "Current plan is Alpha."
            artifacts[0]["snippet"] = "Unknown-durable derivative says Beta."
            artifacts[0]["durable_status"] = "mysterious"
        elif mode == "truth_stale_overpermissive":
            semantic[0]["content"] = "Old plan was Beta."
            semantic[0]["freshness_state"] = "stale"
            semantic[0]["durable_status"] = "stale"
            artifacts = []
        elif mode == "truth_parked_overpermissive":
            semantic[0]["content"] = "Old plan was Beta."
            semantic[0]["freshness_state"] = "parked"
            semantic[0]["durable_status"] = "parked"
            artifacts = []
        elif mode == "truth_corrected_valid":
            semantic[0].update(
                {
                    "message_id": "plan-beta-message",
                    "content": "Old plan was Beta.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-beta"},
                    "freshness_state": "superseded",
                    "durable_status": "superseded",
                    "memory_id": "memory-beta",
                    "superseded_by": "memory-alpha",
                }
            )
            semantic.append(
                {
                    "owner_id": "owner-replay",
                    "evidence_role": "canonical",
                    "message_id": "plan-alpha-message",
                    "created_at": "2026-01-02T00:00:00+00:00",
                    "role": "assistant",
                    "content": "Current plan is Alpha.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-alpha"},
                    "source_availability": "not_applicable",
                    "freshness_state": "corrected",
                    "durable_status": "corrected",
                    "memory_id": "memory-alpha",
                    "supersedes": "memory-beta",
                }
            )
            artifacts = []
        elif mode == "truth_corrected_missing_relationship":
            semantic[0].update(
                {
                    "content": "Current plan is Alpha.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-alpha"},
                    "freshness_state": "corrected",
                    "durable_status": "corrected",
                    "memory_id": "memory-alpha",
                }
            )
            artifacts = []
        elif mode == "truth_corrected_self_supersession":
            semantic[0].update(
                {
                    "content": "Current plan is Alpha.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-alpha"},
                    "freshness_state": "corrected",
                    "durable_status": "corrected",
                    "memory_id": "memory-alpha",
                    "supersedes": "memory-alpha",
                }
            )
            artifacts = []
        elif mode == "truth_corrected_dangling":
            semantic[0].update(
                {
                    "content": "Current plan is Alpha.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-alpha"},
                    "freshness_state": "corrected",
                    "durable_status": "corrected",
                    "memory_id": "memory-alpha",
                    "supersedes": "memory-beta",
                }
            )
            artifacts = []
        elif mode == "truth_corrected_rejected_replacement":
            semantic[0].update(
                {
                    "content": "Current fallback plan is Beta.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-beta"},
                    "freshness_state": "active",
                    "durable_status": "active",
                    "memory_id": "memory-beta",
                }
            )
            artifacts[0].update(
                {
                    "snippet": "Replacement plan is Alpha.",
                    "source_ref": {"ref_type": "derived_text", "ref_id": "plan-alpha"},
                    "freshness_state": "corrected",
                    "durable_status": "corrected",
                    "memory_id": "memory-alpha",
                    "supersedes": "memory-beta",
                    "source_availability": "missing",
                }
            )
        elif mode == "truth_corrected_rejected_predecessor":
            semantic[0].update(
                {
                    "message_id": "plan-beta-message",
                    "content": "Malformed predecessor Beta.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-beta"},
                    "freshness_state": "active",
                    "durable_status": "active",
                    "memory_id": "memory-beta",
                }
            )
            semantic[0].pop("message_id", None)
            semantic.append(
                {
                    "owner_id": "owner-replay",
                    "evidence_role": "canonical",
                    "message_id": "plan-alpha-message",
                    "created_at": "2026-01-02T00:00:00+00:00",
                    "role": "assistant",
                    "content": "Replacement plan is Alpha.",
                    "source_ref": {"ref_type": "message", "ref_id": "plan-alpha"},
                    "source_availability": "not_applicable",
                    "freshness_state": "corrected",
                    "durable_status": "corrected",
                    "memory_id": "memory-alpha",
                    "supersedes": "memory-beta",
                }
            )
            artifacts = []
        elif mode == "wave2d_under_budget":
            semantic[0]["content"] = "Compact current evidence."
            artifacts = []
        elif mode == "wave2d_empty":
            semantic = []
            artifacts = []
            recent = []
        elif mode == "wave2d_recent_overflow":
            semantic = []
            artifacts = []
            recent = [
                {"role": "assistant", "content": "RECENT_OLDEST_MARKER " * 80},
                {"role": "assistant", "content": "RECENT_NEWEST_MARKER " * 10},
            ]
        elif mode == "wave2d_historical_current":
            semantic[0]["content"] = "HISTORICAL_REPLAY_MARKER " * 70
            semantic[0]["freshness_state"] = "stale"
            semantic[0]["durable_status"] = "stale"
            semantic.append(
                {
                    "owner_id": "owner-replay",
                    "evidence_role": "canonical",
                    "message_id": "current-memory-1",
                    "created_at": "2026-01-02T00:00:00+00:00",
                    "role": "assistant",
                    "content": "CURRENT_REPLAY_MARKER compact.",
                    "source_ref": {"ref_type": "message", "ref_id": "current-memory-1"},
                    "source_availability": "not_applicable",
                    "freshness_state": "active",
                    "durable_status": "active",
                }
            )
            artifacts = []
        elif mode == "wave2d_current_tie":
            semantic = [
                {
                    "owner_id": "owner-replay",
                    "evidence_role": "canonical",
                    "message_id": "current-low",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "role": "assistant",
                    "content": "TIE_FIRST_MARKER " * 50,
                    "score": 0.5,
                    "source_ref": {"ref_type": "message", "ref_id": "current-low"},
                    "source_availability": "not_applicable",
                    "freshness_state": "active",
                    "durable_status": "active",
                },
                {
                    "owner_id": "owner-replay",
                    "evidence_role": "canonical",
                    "message_id": "current-high",
                    "created_at": "2026-01-02T00:00:00+00:00",
                    "role": "assistant",
                    "content": "TIE_SECOND_MARKER compact.",
                    "score": 0.5,
                    "source_ref": {"ref_type": "message", "ref_id": "current-high"},
                    "source_availability": "not_applicable",
                    "freshness_state": "active",
                    "durable_status": "active",
                },
            ]
            artifacts = []
        elif mode == "wave2d_artifact_overflow":
            semantic = []
            artifacts[0]["artifact_id"] = "artifact-wave2d-private"
            artifacts[0]["snippet"] = "Private replay artifact. " * 80
        else:
            recent = []
        if "recent" not in locals():
            recent = []
        if kwargs.get("containment_policy") is not None:
            for artifact in artifacts:
                provenance = artifact.get("provenance")
                if not isinstance(provenance, dict):
                    continue
                source_refs = provenance.get("source_refs")
                if not isinstance(source_refs, list):
                    continue
                allowed_ref_fields = {
                    "ref_type",
                    "ref_id",
                    "support_kind",
                    "span",
                    "field_path",
                    "note",
                    "metadata",
                }
                provenance["source_refs"] = [
                    {
                        key: value
                        for key, value in ref.items()
                        if key in allowed_ref_fields
                    }
                    for ref in source_refs
                    if isinstance(ref, dict)
                ]
        return {
            "request_id": request_id,
            "conversation_id": kwargs["conversation_id"],
            "bundle": {
                "recent": recent,
                "semantic": semantic,
                "artifact_refs": artifacts,
                "observed_metadata": {"has_code_like_content": False},
                "retrieval_debug": debug,
            },
        }

    async def create_trace(self, *, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._record("trace_persistence", request_id)
        if self.scenario.get("trace_persistence") == "failure":
            raise BoundaryFailure("trace_persistence_failed")
        self.trace = deepcopy(payload)
        return {"trace_id": "trace-1", "request_id": request_id}


class ReplayRuntime:
    def __init__(self, scenario: dict[str, Any], calls: list[dict[str, Any]]) -> None:
        self.scenario = scenario
        self.calls = calls
        self.terminal_status: str | None = None

    def _record(self, name: str, request_id: str, **details: Any) -> None:
        self.calls.append({"name": name, "request_id": request_id, **details})

    def _maybe_fail(self) -> None:
        if self.scenario.get("runtime") == "unavailable":
            raise BoundaryFailure("runtime_unavailable")

    async def resolve_session(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_session", kwargs["request_id"])
        self._maybe_fail()
        return {
            "runtime_session": {
                "runtime_session_id": "runtime-session-1",
                "status": "active",
                "surface": kwargs["surface"],
            }
        }

    async def start_turn(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_turn_start", kwargs["request_id"])
        self._maybe_fail()
        return {
            "runtime_session": {
                "runtime_session_id": "runtime-session-1",
                "status": "active",
                "surface": kwargs["surface"],
            },
            "runtime_turn": {
                "runtime_turn_id": "runtime-turn-1",
                "turn_status": "received",
            },
        }

    async def update_turn(self, **kwargs: Any) -> dict[str, Any]:
        self._record(
            "cr_turn_update",
            kwargs["request_id"],
            turn_status=kwargs["turn_status"],
        )
        self._maybe_fail()
        return {"runtime_turn": {"turn_status": kwargs["turn_status"]}}

    async def complete_turn(self, **kwargs: Any) -> dict[str, Any]:
        self.terminal_status = kwargs["turn_status"]
        self._record(
            "cr_turn_complete",
            kwargs["request_id"],
            turn_status=kwargs["turn_status"],
        )
        self._maybe_fail()
        return {"runtime_turn": {"turn_status": kwargs["turn_status"]}}

    async def resolve_identity(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_identity", kwargs["request_id"])
        self._maybe_fail()
        return {
            "runtime_identity": {"content": "Neutral runtime identity."},
            "trace": {
                "runtime_session_id": "runtime-session-1",
                "active_persona_id": "neutral",
                "surface_id": kwargs["surface"],
            },
        }

    async def world_state_resolve(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_world_state", kwargs["request_id"])
        self._maybe_fail()
        return {
            "included_claims": [],
            "prompt_content": None,
            "trace": {
                "included_claim_count": 0,
                "excluded_claim_count": 0,
            },
        }

    async def relationship_select(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_relationships", kwargs["request_id"])
        self._maybe_fail()
        projection = self.scenario.get("relationship_projection")
        if not isinstance(projection, dict):
            projection = {
                "applied": False,
                "relationship_ids": [],
                "entity_ids": [],
                "relationship_scopes": [],
                "reason_codes": ["no_eligible_relationship_scope"],
            }
        return {
            "selected_relationships": [],
            "prompt_content": None,
            "retrieval_scope_projection": projection,
            "trace": {
                "selected_relationship_count": 0,
                "excluded_relationship_count": 0,
                "relationship_edges_used": [],
                "relationship_edges_excluded": [],
                "relationship_exclusion_reasons": {},
                "relationship_context_overlay_applied": False,
                "relationship_conflicts": [],
                "relationship_confirmation_required": False,
                "active_persona_id": kwargs.get("active_persona_id"),
                "allowed_relationship_scopes": projection.get("relationship_scopes", []),
            },
        }

    async def evaluate_interaction_governance(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_interaction_governance", kwargs["request_id"])
        self._maybe_fail()
        return {
            "result": {
                "interaction_kind": "question",
                "response_posture": "direct",
                "persona_scope_hint": None,
                "privacy_sensitivity_hint": "normal",
                "commentary_allowed": False,
                "humor_allowed": False,
                "action_allowed": False,
                "requires_confirmation": False,
                "confidence": 0.9,
                "reason_summary": ["replay_default"],
            }
        }

    async def evaluate_persona_containment(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_persona_containment", kwargs["request_id"])
        self._maybe_fail()
        return {
            "result": {
                "active_persona_id": "technical_architect",
                "capability_domain": "technical",
                "allowed_memory_domains": ["technical"],
                "blocked_memory_domains": ["finance"],
                "allowed_world_state_domains": ["technical"],
                "allowed_relationship_domains": ["project"],
                "allowed_tool_domains": ["technical"],
                "cross_scope_access_allowed": False,
                "cross_scope_reason": "not_requested",
                "confidence": 0.9,
                "reason_summary": ["replay_default"],
                "artifact_access_policy": {
                    "enforcement_mode": "mandatory",
                    "allowed_content_classes": ["document"],
                    "allowed_domains": ["technical"],
                    "maximum_sensitivity": "medium",
                    "surface_content_capabilities": ["document"],
                    "reason_codes": ["replay_default"],
                },
            }
        }

    async def evaluate_restraint(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_restraint", kwargs["request_id"])
        self._maybe_fail()
        policy = self.scenario.get("restraint_policy", "answer_normally")
        suppressed = bool(self.scenario.get("retrieval_suppressed", False))
        return {
            "result": {
                "restraint_policy": policy,
                "domains": ["retrieval"],
                "reason": "replay_default",
                "prompt_overlay": None,
                "confidence": 0.9,
                "reason_summary": ["replay_default"],
                "retrieval_suppressed": suppressed,
                "personalization_suppressed": False,
                "proactive_output_suppressed": False,
                "brevity_preferred": False,
                "clarification_preferred": False,
            }
        }

    async def evaluate_privacy_context(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_privacy_context", kwargs["request_id"])
        self._maybe_fail()
        enforce = self.scenario.get("privacy_context") == "replace_answer"
        return {
            "request_id": kwargs["request_id"],
            "owner_id": kwargs["owner_id"],
            "conversation_id": kwargs["conversation_id"],
            "surface": kwargs["surface"],
            "runtime_session_id": kwargs.get("runtime_session_id"),
            "runtime_turn_id": kwargs.get("runtime_turn_id"),
            "result": {
                "surface_type": "public_projector" if enforce else "desktop_private",
                "redaction_required": enforce,
                "safe_summary_required": enforce,
                "sensitive_detail_allowed": not enforce,
                "screen_detail_allowed": not enforce,
                "template_id": "privacy_safe_summary" if enforce else None,
                "confidence": 0.9,
                "reason_codes": ["replay_privacy_boundary"],
            },
        }

    async def overlay(self, **kwargs: Any) -> Any:
        self._record("cr_overlay", kwargs["request_id"])
        self._maybe_fail()
        mode = self.scenario.get("runtime", "omitted")
        if mode == "malformed":
            return ["invalid-overlay-response"]
        if mode == "included":
            return {
                "runtime_state": {
                    "runtime_state_id": "runtime-state-1",
                    "reset_after_turn": False,
                },
                "overlay": {
                    "overlay_id": "runtime-overlay-1",
                    "overlay_type": "runtime_state",
                    "role": "system",
                    "content": "Neutral runtime overlay.",
                    "source_fields": ["interaction_mode"],
                },
                "omitted": False,
            }
        if mode == "wave2d_long_overlay":
            return {
                "runtime_state": {
                    "runtime_state_id": "runtime-state-1",
                    "reset_after_turn": False,
                },
                "overlay": {
                    "overlay_id": "runtime-overlay-long",
                    "overlay_type": "runtime_state",
                    "role": "system",
                    "content": "RUNTIME_OVERLAY_MARKER " * 18,
                    "source_fields": ["fixture"],
                },
                "omitted": False,
            }
        return {
            "runtime_state": {
                "runtime_state_id": "runtime-state-1",
                "reset_after_turn": False,
            },
            "overlay": None,
            "omitted": True,
            "omission_reason": "empty_runtime_state",
        }

    async def evaluate_memory_hygiene(self, **kwargs: Any) -> dict[str, Any]:
        self._record("cr_memory_hygiene", kwargs["request_id"])
        if self.scenario.get("memory_hygiene") == "unavailable":
            raise BoundaryFailure("memory_hygiene_unavailable")
        if self.scenario.get("memory_hygiene") == "malformed":
            return {"result": {"decisions": "invalid"}}
        decisions: list[dict[str, Any]] = []
        for item in kwargs.get("items", []):
            freshness = item.get("freshness_state", "unknown_freshness")
            item_ref = item.get("item_ref")
            if self.scenario.get("memory_hygiene") == "overpermissive_current":
                decision = (True, True, "current")
                freshness = "active"
            elif self.scenario.get("memory_hygiene") == "stale_current_conflict":
                decision = (True, True, "current")
                freshness = "stale"
            elif self.scenario.get("memory_hygiene") == "active_stale_framing_conflict":
                decision = (True, False, "stale_or_unverified")
                freshness = "active"
            elif freshness == "active":
                decision = (True, True, "current")
            elif freshness == "corrected":
                decision = (True, True, "corrected_replacement")
            elif freshness == "parked":
                decision = (True, False, "parked_or_historical")
            elif freshness == "stale":
                decision = (True, False, "stale_or_unverified")
            elif freshness == "unknown_freshness":
                decision = (True, False, "unknown_or_unverified")
            else:
                decision = (False, False, "omit")
            decisions.append(
                {
                    "item_ref": item_ref,
                    "freshness_state": freshness,
                    "use_allowed": decision[0],
                    "mention_as_current_allowed": decision[1],
                    "framing": decision[2],
                }
            )
            if self.scenario.get("memory_hygiene") == "conflicting":
                decisions.append(
                    {
                        "item_ref": item_ref,
                        "freshness_state": freshness,
                        "use_allowed": not decision[0],
                        "mention_as_current_allowed": False,
                        "framing": "omit",
                    }
                )
                break
        return {"result": {"decisions": decisions, "aggregate": {}}}


class ReplayDSA:
    def __init__(self, scenario: dict[str, Any], calls: list[dict[str, Any]]) -> None:
        self.scenario = scenario
        self.calls = calls

    async def context_pack(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append({"name": "dsa_context_pack", "request_id": None})
        mode = self.scenario.get("dsa")
        if mode != "wave2d_external":
            return {"sources_used": [], "items": []}
        return {
            "sources_used": ["wave2d_external_source"],
            "items": [
                {
                    "source_name": "Replay DSA",
                    "title": "Wave 2D external context",
                    "source_ref": "external-wave2d-1",
                    "text": "EXT_CONTEXT_MARKER " * 120,
                }
            ],
        }


class ReplayProvider:
    def __init__(self, scenario: dict[str, Any], calls: list[dict[str, Any]]) -> None:
        self.scenario = scenario
        self.calls = calls
        self.attempt = 0

    async def chat(self, **kwargs: Any) -> dict[str, Any]:
        messages = kwargs.get("messages") or []
        if self.attempt == 0:
            self.calls.append({"name": "prompt_assembly", "request_id": kwargs["request_id"]})
        self.attempt += 1
        self.calls.append(
            {
                "name": "provider_attempt",
                "request_id": kwargs["request_id"],
                "attempt": self.attempt,
                "model": kwargs["model"],
                "prompt_fingerprint": _message_fingerprint(messages),
                "message_count": len(messages),
                "role_sequence": [
                    str(message.get("role", ""))
                    for message in messages
                    if isinstance(message, dict)
                ],
                "prompt_evidence": _provider_prompt_evidence(messages),
                "has_beta": "Beta"
                in "\n".join(
                    message.get("content", "") for message in messages if isinstance(message, dict)
                ),
            }
        )
        provider_mode = self.scenario.get("provider", "success")
        should_fail = (
            provider_mode == "exhausted"
            or (provider_mode == "fallback_success" and self.attempt == 1)
            or provider_mode == "no_fallback"
        )
        if should_fail:
            request = httpx.Request("POST", "http://provider.local/v1/chat/completions")
            response = httpx.Response(503, request=request)
            raise httpx.HTTPStatusError(
                "provider failure fixture",
                request=request,
                response=response,
            )
        joined = "\n".join(message.get("content", "") for message in kwargs["messages"])
        if "Current memory evidence:" in joined and "Current plan is Alpha." in joined:
            content = "Current plan is Alpha."
        elif "Current memory evidence:" in joined and "Current fallback plan is Beta." in joined:
            content = "Current fallback plan is Beta."
        elif "Historical or unverified memory context:" in joined:
            content = "I only have historical or unverified memory context."
        else:
            content = "neutral response"
        return {"choices": [{"message": {"content": content}}]}


def _provider_prompt_evidence(messages: Any) -> dict[str, bool]:
    joined = "\n".join(
        message.get("content", "") for message in messages if isinstance(message, dict)
    )
    return {
        "old_request_present": "older request context" in joined,
        "final_current_turn_present": "neutral request" in joined,
        "recent_oldest_present": "RECENT_OLDEST_MARKER" in joined,
        "recent_newest_present": "RECENT_NEWEST_MARKER" in joined,
        "historical_retrieval_present": "HISTORICAL_REPLAY_MARKER" in joined,
        "current_retrieval_present": "CURRENT_REPLAY_MARKER" in joined,
        "tie_first_present": "TIE_FIRST_MARKER" in joined,
        "tie_second_present": "TIE_SECOND_MARKER" in joined,
        "external_context_present": "EXT_CONTEXT_MARKER" in joined,
        "runtime_overlay_present": "RUNTIME_OVERLAY_MARKER" in joined,
        "artifact_context_present": "Private replay artifact" in joined,
        "selected_relationship_memory_present": (
            "selected relationship scoped replay memory" in joined
        ),
        "excluded_relationship_memory_present": (
            "excluded relationship replay memory" in joined
        ),
        "unauthorized_artifact_present": "unauthorized replay artifact" in joined,
        "privacy_replay_sentinel_present": "PRIVATE_WAVE3B_REPLAY" in joined,
    }


def _message_fingerprint(messages: Any) -> str:
    normalized = [
        {
            "role": str(message.get("role", "")),
            "content": str(message.get("content", "")),
        }
        for message in messages
        if isinstance(message, dict)
    ]
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _answer_category(result: dict[str, Any] | None) -> str | None:
    if not result:
        return None
    answer = result.get("answer")
    if answer == "Current plan is Alpha.":
        return "current_alpha"
    if answer == "Current fallback plan is Beta.":
        return "current_beta"
    if answer == "I only have historical or unverified memory context.":
        return "historical_or_unverified"
    if answer == "neutral response":
        return "neutral"
    return "other"


def load_corpus(path: Path = DEFAULT_CORPUS_PATH) -> list[dict[str, Any]]:
    return json.loads(path.read_text())["scenarios"]


def _payload(scenario: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "owner_id": "owner-replay",
        "client_id": "client-replay",
        "surface": scenario.get("surface", "chat"),
        "messages": scenario.get("messages") or [{"role": "user", "content": "neutral request"}],
        "sensitivity": "private",
        "retrieval": None,
        "response_mode": "normal",
        "brief_type": "general",
        "interrupt_policy_mode": "off",
    }
    if scenario.get("external_context_enabled") is not None:
        payload["external_context_enabled"] = bool(scenario.get("external_context_enabled"))
    if isinstance(scenario.get("external_context"), dict):
        payload["external_context"] = scenario["external_context"]
    return payload


def _layer_by_name(raw_prompt: dict[str, Any]) -> dict[str, dict[str, Any]]:
    layers = raw_prompt.get("layers")
    if not isinstance(layers, list):
        return {}
    return {
        layer["name"]: layer
        for layer in layers
        if isinstance(layer, dict) and isinstance(layer.get("name"), str)
    }


def _bounded_layer_state(layer: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(layer, dict):
        return {"included": False, "message_count": 0}
    return {
        "included": bool(layer.get("included")),
        "message_count": layer.get("message_count", 0),
    }


def _retrieval_snippet_projection(layer: dict[str, Any] | None) -> dict[str, Any]:
    metadata = layer.get("metadata", {}) if isinstance(layer, dict) else {}
    snippets = metadata.get("snippets", {}) if isinstance(metadata, dict) else {}
    semantic = snippets.get("semantic", []) if isinstance(snippets, dict) else []
    artifact_refs = snippets.get("artifact_refs", []) if isinstance(snippets, dict) else []
    semantic_ids = [
        item.get("message_id")
        for item in semantic
        if isinstance(item, dict) and isinstance(item.get("message_id"), str)
    ]
    artifact_ids = [
        item.get("artifact_id")
        for item in artifact_refs
        if isinstance(item, dict) and isinstance(item.get("artifact_id"), str)
    ]
    return {
        "semantic_message_ids": semantic_ids,
        "artifact_ids": artifact_ids,
        "current_count": snippets.get("current_count") if isinstance(snippets, dict) else None,
        "historical_or_unverified_count": (
            snippets.get("historical_or_unverified_count") if isinstance(snippets, dict) else None
        ),
    }


def _normalize(
    *,
    scenario: dict[str, Any],
    request_id: str,
    calls: list[dict[str, Any]],
    result: dict[str, Any] | None,
    error: BaseException | None,
    memory: ReplayMemoryStore,
    runtime: ReplayRuntime,
) -> dict[str, Any]:
    trace = memory.trace or {}
    prompt = trace.get("prompt") if isinstance(trace.get("prompt"), dict) else {}
    raw_prompt = (
        (trace.get("retrieval") or {}).get("prompt_assembly", {})
        if isinstance(trace.get("retrieval"), dict)
        else {}
    )
    raw_prompt = raw_prompt if isinstance(raw_prompt, dict) else {}
    artifacts = trace.get("artifacts") if isinstance(trace.get("artifacts"), dict) else {}
    truncation = raw_prompt.get("truncation") or prompt.get("truncation")
    truncation = truncation if isinstance(truncation, dict) else {"applied": False}
    prompt_budget = raw_prompt.get("prompt_budget") or prompt.get("prompt_budget")
    prompt_budget = prompt_budget if isinstance(prompt_budget, dict) else {}
    dropped_context = prompt_budget.get("dropped_context")
    dropped_context = dropped_context if isinstance(dropped_context, dict) else {}
    raw_layers = _layer_by_name(raw_prompt)
    provider_attempts = [call for call in calls if call.get("name") == "provider_attempt"]
    retrieval_dispatch = raw_prompt.get("retrieval_dispatch")
    retrieval_dispatch = retrieval_dispatch if isinstance(retrieval_dispatch, dict) else {}
    result_boundary = raw_prompt.get("result_boundary")
    result_boundary = result_boundary if isinstance(result_boundary, dict) else {}
    persona_containment = raw_prompt.get("persona_containment")
    persona_containment = persona_containment if isinstance(persona_containment, dict) else {}
    return {
        "schema_version": "orchestration-replay-v1",
        "scenario": scenario["scenario"],
        "category": scenario["category"],
        "request_id": request_id,
        "outcome": {
            "status": result.get("status") if result else "failed",
            "error_type": type(error).__name__ if error else None,
            "error_code": str(error) if isinstance(error, RuntimeError) else None,
            "selected_model": result.get("selected_model") if result else None,
            "answer_category": _answer_category(result),
        },
        "call_order": [call["name"] for call in calls],
        "request_ids": [call["request_id"] for call in calls if call.get("request_id") is not None],
        "trace": {
            "persisted": memory.trace is not None,
            "status": trace.get("status"),
            "model_calls": trace.get("model_calls", []),
            "model_call": trace.get("model_call", {}),
            "fallback": trace.get("fallback", {}),
            "prompt_layers": prompt.get("ordered_layer_names", []),
            "prompt_included": prompt.get("included_layers", []),
            "runtime_overlay": prompt.get("runtime_overlay", {}),
            "budget_enforcement": (prompt.get("token_accounting", {}).get("budget_enforcement")),
            "artifacts": artifacts,
            "references": trace.get("references", []),
            "retrieval": (trace.get("retrieval") or {}).get("bundle", {}),
            "memory_hygiene": (
                (trace.get("retrieval") or {}).get("prompt_assembly", {}).get("memory_hygiene", {})
            ),
            "provider_prompt": prompt.get("provider_prompt", {}),
            "provider_fallback_context": prompt.get("provider_fallback_context", {}),
            "prompt_budget": {
                "status": prompt_budget.get("status"),
                "failure_reason": prompt_budget.get("failure_reason"),
                "final_within_budget": prompt_budget.get("final_within_budget"),
                "omission_or_truncation_occurred": prompt_budget.get(
                    "omission_or_truncation_occurred"
                ),
                "effective_hard_input_budget": prompt_budget.get("effective_hard_input_budget"),
                "estimated_tokens_before_budgeting": prompt_budget.get(
                    "estimated_tokens_before_budgeting"
                ),
                "estimated_tokens_after_budgeting": prompt_budget.get(
                    "estimated_tokens_after_budgeting"
                ),
                "effective_min_context_limit": prompt_budget.get("effective_min_context_limit"),
                "dropped_total": (
                    dropped_context.get("total_count")
                    if isinstance(dropped_context, dict)
                    else None
                ),
                "dropped_by_reason": dropped_context.get("by_reason", {}),
                "dropped_by_layer": dropped_context.get("by_layer", {}),
                "profile_clamp": prompt_budget.get("profile_clamp"),
                "retained_source_ids": prompt_budget.get("retained_source_ids")
                or raw_prompt.get("retained_source_ids"),
            },
            "truncation": truncation,
            "wave2d_layers": {
                name: _bounded_layer_state(raw_layers.get(name))
                for name in (
                    "external_source_context",
                    "runtime_overlay",
                    "retrieval_augmentation",
                    "recent_history",
                    "current_messages",
                )
            },
            "wave2d_retrieval_projection": _retrieval_snippet_projection(
                raw_layers.get("retrieval_augmentation")
            ),
            "dsa": raw_prompt.get("dsa", {}),
            "retrieval_dispatch": {
                "mandatory_containment_requested": retrieval_dispatch.get(
                    "mandatory_containment_requested"
                ),
                "policy_validation_status": retrieval_dispatch.get(
                    "policy_validation_status"
                ),
                "bms_retrieval_call_issued": retrieval_dispatch.get(
                    "bms_retrieval_call_issued"
                ),
                "bms_retrieval_call_suppressed": retrieval_dispatch.get(
                    "bms_retrieval_call_suppressed"
                ),
                "suppression_or_dependency_reason": retrieval_dispatch.get(
                    "suppression_or_dependency_reason"
                ),
                "relationship_projection_applied": retrieval_dispatch.get(
                    "relationship_projection_applied"
                ),
                "relationship_id_count": retrieval_dispatch.get("relationship_id_count"),
                "entity_id_count": retrieval_dispatch.get("entity_id_count"),
                "relationship_scope_count": retrieval_dispatch.get(
                    "relationship_scope_count"
                ),
                "neutral_persistence_classification": retrieval_dispatch.get(
                    "neutral_persistence_classification"
                ),
            },
            "persona_containment": {
                "active_persona_id": persona_containment.get("active_persona_id"),
                "retrieval_scope_status": persona_containment.get(
                    "retrieval_scope_status"
                ),
                "retrieval_scope_reason": persona_containment.get(
                    "retrieval_scope_reason"
                ),
                "artifact_request_status": persona_containment.get(
                    "artifact_request_status"
                ),
                "artifact_request_reason": persona_containment.get(
                    "artifact_request_reason"
                ),
                "artifact_result_status": persona_containment.get(
                    "artifact_result_status"
                ),
                "artifact_result_reason": persona_containment.get(
                    "artifact_result_reason"
                ),
            },
            "result_boundary": {
                "enforcement_mode": result_boundary.get("enforcement_mode"),
                "validation_status": result_boundary.get("validation_status"),
                "envelope_validation_failed": result_boundary.get(
                    "envelope_validation_failed"
                ),
                "input_counts": result_boundary.get("input_counts"),
                "retained_counts": result_boundary.get("retained_counts"),
                "omission_counts_by_reason": result_boundary.get(
                    "omission_counts_by_reason"
                ),
                "relationship_policy_applied": result_boundary.get(
                    "relationship_policy_applied"
                ),
                "artifact_policy_applied": result_boundary.get("artifact_policy_applied"),
                "post_budget_survivor_filter_removed_sources": result_boundary.get(
                    "post_budget_survivor_filter_removed_sources"
                ),
            },
        },
        "provider_attempt_count": len(provider_attempts),
        "provider_fingerprints": [
            attempt.get("prompt_fingerprint") for attempt in provider_attempts
        ],
        "provider_message_counts": [attempt.get("message_count") for attempt in provider_attempts],
        "provider_role_sequences": [attempt.get("role_sequence") for attempt in provider_attempts],
        "provider_prompt_evidence": [
            attempt.get("prompt_evidence") for attempt in provider_attempts
        ],
        "sources_count": len(result.get("sources", [])) if result else 0,
        "runtime_terminal_status": runtime.terminal_status,
    }


def _router_files_for_scenario(scenario: dict[str, Any], directory: Path) -> tuple[Path, Path]:
    rules = directory / "rules.yaml"
    models = directory / "models.yaml"
    primary = scenario.get("primary_model", "gpt-4o-mini")
    fallback = scenario.get("fallback_model")
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        f"      selected_model: {primary}\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        + (
            "      fallbacks:\n"
            f"        - selected_model: {fallback}\n"
            "          provider: cloud\n"
            if fallback
            else "      fallbacks: []\n"
        ),
        encoding="utf-8",
    )
    model_limits = scenario.get("model_limits")
    model_limits = model_limits if isinstance(model_limits, dict) else {}
    models_text = "models:\n"
    for model in [primary, fallback]:
        if not model:
            continue
        limit = model_limits.get(model, 128000)
        models_text += f"  {model}:\n    provider: cloud\n"
        if limit != "missing":
            models_text += f"    max_context_tokens: {limit}\n"
    models.write_text(models_text, encoding="utf-8")
    return rules, models


async def run_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    calls: list[dict[str, Any]] = []
    memory = ReplayMemoryStore(scenario, calls)
    runtime = ReplayRuntime(scenario, calls)
    provider = ReplayProvider(scenario, calls)
    dsa = ReplayDSA(scenario, calls) if scenario.get("dsa") else None
    request_id = f"request-{scenario['scenario']}"
    result = None
    error = None
    try:
        if scenario.get("model_limits") is not None:
            with tempfile.TemporaryDirectory() as tmp:
                rules_path, registry_path = _router_files_for_scenario(
                    scenario,
                    Path(tmp),
                )
                result = await orchestrate_chat(
                    payload=_payload(scenario),
                    memory_store=memory,
                    litellm=provider,
                    runtime=runtime,
                    dsa=dsa,
                    dsa_enabled=bool(scenario.get("dsa")),
                    rules_path=str(rules_path),
                    model_registry_path=str(registry_path),
                    allow_manual_override=False,
                    enable_runtime_overlays=True,
                    interaction_governance_enabled=bool(
                        scenario.get("interaction_governance_enabled")
                    ),
                    persona_containment_enabled=bool(
                        scenario.get("persona_containment_enabled")
                    ),
                    restraint_enabled=bool(scenario.get("restraint_enabled")),
                    memory_hygiene_enabled=scenario.get("memory_hygiene_enabled", True),
                    privacy_context_enabled=bool(scenario.get("privacy_context_enabled")),
                    request_id=request_id,
                    prompt_output_token_reserve=scenario.get(
                        "prompt_output_token_reserve",
                        0,
                    ),
                    prompt_context_safety_margin=scenario.get(
                        "prompt_context_safety_margin",
                        0,
                    ),
                )
        else:
            result = await orchestrate_chat(
                payload=_payload(scenario),
                memory_store=memory,
                litellm=provider,
                runtime=runtime,
                dsa=dsa,
                dsa_enabled=bool(scenario.get("dsa")),
                rules_path=str(
                    NO_FALLBACK_RULES_PATH
                    if scenario.get("provider") == "no_fallback"
                    else RULES_PATH
                ),
                model_registry_path=str(REGISTRY_PATH),
                allow_manual_override=False,
                enable_runtime_overlays=True,
                interaction_governance_enabled=bool(
                    scenario.get("interaction_governance_enabled")
                ),
                persona_containment_enabled=bool(scenario.get("persona_containment_enabled")),
                restraint_enabled=bool(scenario.get("restraint_enabled")),
                memory_hygiene_enabled=scenario.get("memory_hygiene_enabled", True),
                privacy_context_enabled=bool(scenario.get("privacy_context_enabled")),
                request_id=request_id,
            )
    except Exception as exc:  # replay snapshots intentionally cover failures
        error = exc
    return _normalize(
        scenario=scenario,
        request_id=request_id,
        calls=calls,
        result=result,
        error=error,
        memory=memory,
        runtime=runtime,
    )


def compare_snapshot(expected: dict[str, Any], actual: dict[str, Any], scenario: str) -> None:
    if expected == actual:
        return
    expected_text = json.dumps(expected, indent=2, sort_keys=True).splitlines()
    actual_text = json.dumps(actual, indent=2, sort_keys=True).splitlines()
    diff = "\n".join(
        unified_diff(
            expected_text,
            actual_text,
            fromfile=f"{scenario}:expected",
            tofile=f"{scenario}:actual",
            lineterm="",
        )
    )
    raise AssertionError(f"replay snapshot mismatch for {scenario}\n{diff}")


def project_snapshot(actual: Any, expected_shape: Any) -> Any:
    if isinstance(expected_shape, dict):
        if not isinstance(actual, dict):
            return actual
        return {
            key: project_snapshot(actual.get(key), nested) for key, nested in expected_shape.items()
        }
    if isinstance(expected_shape, list):
        if (
            isinstance(actual, list)
            and len(actual) == len(expected_shape)
            and all(isinstance(item, dict) for item in expected_shape)
        ):
            return [
                project_snapshot(actual_item, expected_item)
                for actual_item, expected_item in zip(actual, expected_shape, strict=True)
            ]
        return actual
    return actual


def assert_snapshot_privacy_safe(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if key.lower() in _BANNED_SNAPSHOT_KEYS:
                raise AssertionError(f"privacy-unsafe replay key at {path}.{key}")
            assert_snapshot_privacy_safe(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            assert_snapshot_privacy_safe(nested, f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.lower()
        for banned in ("bearer ", "api-key", "traceback", "provider failure fixture"):
            if banned in lowered:
                raise AssertionError(f"privacy-unsafe replay value at {path}")
