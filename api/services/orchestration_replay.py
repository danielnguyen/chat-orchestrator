from __future__ import annotations

import hashlib
import json
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
NO_FALLBACK_RULES_PATH = (
    Path(__file__).resolve().parents[1] / "replay" / "rules_no_fallback.yaml"
)
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
        self._record(f"{role}_message_persistence", request_id)
        return {"message_id": f"message-{self.message_ordinal}"}

    async def resolve_profile(self, **kwargs: Any) -> dict[str, Any]:
        self._record("profile_resolution")
        return {
            "profile_name": "neutral",
            "source": "global_default",
            "profile_version": 1,
            "effective_profile_ref": "owner:neutral:1",
            "prompt_overlay": "",
            "retrieval_policy": {},
            "routing_policy": {},
            "response_style": {},
            "safety_policy": {},
            "tool_policy": {},
        }

    async def retrieve_bundle(self, **kwargs: Any) -> dict[str, Any]:
        request_id = kwargs["request_id"]
        self._record("bms_retrieval", request_id)
        mode = self.scenario.get("retrieval", "normal")
        if mode == "unavailable":
            raise BoundaryFailure("bms_unavailable")
        if mode == "request_id_mismatch":
            raise RuntimeError("retrieval_request_id_mismatch")
        debug: dict[str, Any] = {"vector_status": "ok"}
        semantic: list[dict[str, Any]] = [
            {
                "owner_id": "owner-replay",
                "evidence_role": "canonical",
                "message_id": "memory-1",
                "created_at": "2026-01-01T00:00:00+00:00",
                "role": "assistant",
                "content": "neutral memory fixture",
                "source_ref": {"ref_type": "message", "ref_id": "memory-1"},
                "source_availability": "not_applicable",
                "freshness_state": "active",
                "durable_status": "active",
            }
        ]
        artifacts: list[dict[str, Any]] = [
            {
                "owner_id": "owner-replay",
                "evidence_role": "derived",
                "artifact_id": "artifact-1",
                "file_path": "fixture.txt",
                "snippet": "neutral artifact fixture",
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
                },
                "freshness_state": "active",
                "durable_status": "active",
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
        return {
            "request_id": request_id,
            "conversation_id": kwargs["conversation_id"],
            "bundle": {
                "recent": [],
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
        return {
            "selected_relationships": [],
            "prompt_content": None,
            "trace": {
                "selected_relationship_count": 0,
                "excluded_relationship_count": 0,
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
            if freshness == "active":
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


class ReplayProvider:
    def __init__(self, scenario: dict[str, Any], calls: list[dict[str, Any]]) -> None:
        self.scenario = scenario
        self.calls = calls
        self.attempt = 0

    async def chat(self, **kwargs: Any) -> dict[str, Any]:
        if self.attempt == 0:
            self.calls.append(
                {"name": "prompt_assembly", "request_id": kwargs["request_id"]}
            )
        self.attempt += 1
        self.calls.append(
            {
                "name": "provider_attempt",
                "request_id": kwargs["request_id"],
                "attempt": self.attempt,
                "model": kwargs["model"],
                "prompt_fingerprint": _message_fingerprint(kwargs.get("messages")),
                "has_beta": "Beta" in "\n".join(
                    message.get("content", "")
                    for message in kwargs.get("messages", [])
                    if isinstance(message, dict)
                ),
            }
        )
        provider_mode = self.scenario.get("provider", "success")
        should_fail = provider_mode == "exhausted" or (
            provider_mode == "fallback_success" and self.attempt == 1
        ) or provider_mode == "no_fallback"
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
        elif "Historical or unverified memory context:" in joined:
            content = "I only have historical or unverified memory context."
        else:
            content = "neutral response"
        return {"choices": [{"message": {"content": content}}]}


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
    if answer == "I only have historical or unverified memory context.":
        return "historical_or_unverified"
    if answer == "neutral response":
        return "neutral"
    return "other"


def load_corpus(path: Path = DEFAULT_CORPUS_PATH) -> list[dict[str, Any]]:
    return json.loads(path.read_text())["scenarios"]


def _payload(scenario: dict[str, Any]) -> dict[str, Any]:
    return {
        "owner_id": "owner-replay",
        "client_id": "client-replay",
        "surface": scenario.get("surface", "chat"),
        "messages": [{"role": "user", "content": "neutral request"}],
        "sensitivity": "private",
        "retrieval": None,
        "response_mode": "normal",
        "brief_type": "general",
        "interrupt_policy_mode": "off",
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
    artifacts = trace.get("artifacts") if isinstance(trace.get("artifacts"), dict) else {}
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
        "request_ids": [
            call["request_id"] for call in calls if call.get("request_id") is not None
        ],
        "trace": {
            "persisted": memory.trace is not None,
            "status": trace.get("status"),
            "model_calls": trace.get("model_calls", []),
            "model_call": trace.get("model_call", {}),
            "fallback": trace.get("fallback", {}),
            "prompt_layers": prompt.get("ordered_layer_names", []),
            "prompt_included": prompt.get("included_layers", []),
            "runtime_overlay": prompt.get("runtime_overlay", {}),
            "budget_enforcement": (
                prompt.get("token_accounting", {}).get("budget_enforcement")
            ),
            "artifacts": artifacts,
            "references": trace.get("references", []),
            "retrieval": (trace.get("retrieval") or {}).get("bundle", {}),
            "memory_hygiene": (
                (trace.get("retrieval") or {})
                .get("prompt_assembly", {})
                .get("memory_hygiene", {})
            ),
            "provider_prompt": prompt.get("provider_prompt", {}),
            "provider_fallback_context": prompt.get("provider_fallback_context", {}),
        },
        "runtime_terminal_status": runtime.terminal_status,
    }


async def run_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    calls: list[dict[str, Any]] = []
    memory = ReplayMemoryStore(scenario, calls)
    runtime = ReplayRuntime(scenario, calls)
    provider = ReplayProvider(scenario, calls)
    request_id = f"request-{scenario['scenario']}"
    result = None
    error = None
    try:
        result = await orchestrate_chat(
            payload=_payload(scenario),
            memory_store=memory,
            litellm=provider,
            runtime=runtime,
            rules_path=str(
                NO_FALLBACK_RULES_PATH
                if scenario.get("provider") == "no_fallback"
                else RULES_PATH
            ),
            model_registry_path=str(REGISTRY_PATH),
            allow_manual_override=False,
            enable_runtime_overlays=True,
            memory_hygiene_enabled=True,
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
        return {
            key: project_snapshot(actual.get(key), nested)
            for key, nested in expected_shape.items()
        }
    if isinstance(expected_shape, list):
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
