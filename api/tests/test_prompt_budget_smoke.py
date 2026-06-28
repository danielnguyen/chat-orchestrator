import json

import pytest
from services.orchestrate import orchestrate_chat


class BudgetMemoryStore:
    def __init__(
        self,
        *,
        profile_overlay: str = "Required profile anchor.",
        profile_budget: dict | None = None,
        retrieval_bundle: dict | None = None,
        routing_policy: dict | None = None,
    ):
        self.profile_overlay = profile_overlay
        self.profile_budget = profile_budget
        self.routing_policy = routing_policy or {}
        self.retrieval_bundle = retrieval_bundle or {
            "recent": [],
            "semantic": [],
            "artifact_refs": [],
            "observed_metadata": {"has_code_like_content": False},
        }
        self.added_messages = []
        self.trace_calls = []

    async def resolve_conversation(self, **kwargs):
        return {"conversation_id": "conv-budget", "reused": False}

    async def add_message(self, **kwargs):
        self.added_messages.append(kwargs)
        return {"message_id": f"message-{len(self.added_messages)}"}

    async def resolve_profile(self, **kwargs):
        return {
            "profile_name": "budget",
            "source": "global_default",
            "profile_version": 1,
            "effective_profile_ref": "owner:budget:1",
            "prompt_overlay": self.profile_overlay,
            "prompt_budget": self.profile_budget,
            "retrieval_policy": {},
            "routing_policy": self.routing_policy,
            "response_style": {},
            "safety_policy": {},
            "tool_policy": {},
        }

    async def retrieve_bundle(self, **kwargs):
        return {
            "request_id": kwargs["request_id"],
            "conversation_id": kwargs["conversation_id"],
            "bundle": self.retrieval_bundle,
        }

    async def create_trace(self, **kwargs):
        self.trace_calls.append(kwargs)
        return {"trace_id": "trace-budget", "request_id": kwargs["request_id"]}


class BudgetRuntime:
    def __init__(self, *, overlay_content: str | None = None):
        self.overlay_content = overlay_content
        self.terminal_status = None

    async def start_turn(self, **kwargs):
        return {
            "runtime_session": {
                "runtime_session_id": "session-budget",
                "status": "active",
                "surface": kwargs["surface"],
            },
            "runtime_turn": {
                "runtime_turn_id": "turn-budget",
                "turn_status": "received",
            },
        }

    async def update_turn(self, **kwargs):
        return {"runtime_turn": {"turn_status": kwargs["turn_status"]}}

    async def complete_turn(self, **kwargs):
        self.terminal_status = kwargs["turn_status"]
        return {"runtime_turn": {"turn_status": kwargs["turn_status"]}}

    async def resolve_identity(self, **kwargs):
        return {
            "runtime_identity": {"content": "Runtime identity required anchor."},
            "trace": {"status": "ok"},
        }

    async def world_state_resolve(self, **kwargs):
        return {"included_claims": [], "prompt_content": None, "trace": {}}

    async def relationship_select(self, **kwargs):
        return {"selected_relationships": [], "prompt_content": None, "trace": {}}

    async def overlay(self, **kwargs):
        if self.overlay_content is None:
            return {
                "runtime_state": {"runtime_state_id": "runtime-state-budget"},
                "overlay": None,
                "omitted": True,
                "omission_reason": "empty_runtime_state",
            }
        return {
            "runtime_state": {"runtime_state_id": "runtime-state-budget"},
            "overlay": {
                "overlay_id": "runtime-overlay-budget",
                "overlay_type": "runtime_state",
                "role": "system",
                "content": self.overlay_content,
                "source_fields": ["fixture"],
            },
            "omitted": False,
        }

    async def evaluate_memory_hygiene(self, **kwargs):
        return {"result": {"decisions": [], "aggregate": {}}}


class BudgetProvider:
    def __init__(self, *, fail_first: bool = False, fail_all: bool = False):
        self.calls = []
        self.fail_first = fail_first
        self.fail_all = fail_all

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail_all or (self.fail_first and len(self.calls) == 1):
            raise RuntimeError("provider failed")
        return {"choices": [{"message": {"content": "budget answer"}}]}


def _payload(messages):
    return {
        "owner_id": "owner",
        "client_id": "vscode",
        "surface": "vscode",
        "messages": messages,
        "sensitivity": "private",
        "model_override": None,
    }


def _write_router(tmp_path, *, fallback: bool = False, primary_limit=100, fallback_limit=80):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    fallback_block = (
        "      fallbacks:\n"
        "        - selected_model: fallback-small\n"
        "          provider: cloud\n"
        if fallback
        else "      fallbacks: []\n"
    )
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: primary-large\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        f"{fallback_block}",
        encoding="utf-8",
    )
    models_text = (
        "models:\n"
        "  primary-large:\n"
        "    provider: cloud\n"
        f"    max_context_tokens: {primary_limit}\n"
    )
    if fallback:
        if fallback_limit is None:
            models_text += "  fallback-small:\n    provider: cloud\n"
        else:
            models_text += (
                "  fallback-small:\n"
                "    provider: cloud\n"
                f"    max_context_tokens: {fallback_limit}\n"
            )
    models.write_text(models_text, encoding="utf-8")
    return rules, models


def _trace(memory_store):
    return memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]


def _private_trace_text(trace):
    private_trace = json.loads(json.dumps(trace))
    private_trace.get("prompt_budget", {}).pop("retained_source_ids", None)
    return json.dumps(private_trace, sort_keys=True)


def _layer(trace, name):
    return next(layer for layer in trace["layers"] if layer["name"] == name)


@pytest.mark.asyncio
async def test_smoke_budget_pressure_reduces_optional_context_and_preserves_required_turn(
    tmp_path,
):
    rules, models = _write_router(tmp_path, primary_limit=90)
    memory = BudgetMemoryStore(
        profile_overlay="REQUIRED_PROFILE_ANCHOR",
        retrieval_bundle={
            "recent": [{"role": "assistant", "content": "OLD_RECENT_SENTINEL " * 40}],
            "semantic": [],
            "artifact_refs": [],
            "observed_metadata": {"has_code_like_content": False},
        },
    )
    provider = BudgetProvider()

    out = await orchestrate_chat(
        payload=_payload(
            [
                {"role": "user", "content": "OLD_REQUEST_SENTINEL " * 40},
                {"role": "user", "content": "FINAL_TURN_SENTINEL"},
            ]
        ),
        memory_store=memory,
        litellm=provider,
        rules_path=str(rules),
        model_registry_path=str(models),
        request_id="rid-budget-pressure",
        allow_manual_override=True,
        prompt_output_token_reserve=0,
        prompt_context_safety_margin=0,
    )

    prompt_trace = _trace(memory)
    budget = prompt_trace["prompt_budget"]
    sent_messages = provider.calls[0]["messages"]
    sent_text = json.dumps(sent_messages)
    assert out["status"] == "ok"
    assert budget["estimated_tokens_before_budgeting"] > budget["effective_hard_input_budget"]
    assert budget["estimated_tokens_after_budgeting"] <= budget["effective_hard_input_budget"]
    assert budget["omission_or_truncation_occurred"] is True
    assert prompt_trace["truncation"]["applied"] is True
    assert "REQUIRED_PROFILE_ANCHOR" in sent_text
    assert sent_messages[-1] == {"role": "user", "content": "FINAL_TURN_SENTINEL"}
    assert "OLD_REQUEST_SENTINEL" not in sent_text
    assert "OLD_RECENT_SENTINEL" not in sent_text
    assert "FINAL_TURN_SENTINEL" not in _private_trace_text(prompt_trace)


@pytest.mark.asyncio
async def test_smoke_smaller_fallback_context_reuses_primary_prompt(tmp_path):
    rules, models = _write_router(tmp_path, fallback=True, primary_limit=500, fallback_limit=90)
    memory = BudgetMemoryStore(
        retrieval_bundle={
            "recent": [{"role": "assistant", "content": "OLD_RECENT_SENTINEL " * 30}],
            "semantic": [],
            "artifact_refs": [],
            "observed_metadata": {"has_code_like_content": False},
        },
    )
    provider = BudgetProvider(fail_first=True)

    out = await orchestrate_chat(
        payload=_payload(
            [
                {"role": "user", "content": "OLD_REQUEST_SENTINEL " * 30},
                {"role": "user", "content": "FINAL_TURN_SENTINEL"},
            ]
        ),
        memory_store=memory,
        litellm=provider,
        rules_path=str(rules),
        model_registry_path=str(models),
        request_id="rid-budget-fallback",
        allow_manual_override=True,
        prompt_output_token_reserve=0,
        prompt_context_safety_margin=0,
    )

    prompt_trace = _trace(memory)
    assert out["status"] == "degraded"
    assert len(provider.calls) == 2
    assert provider.calls[0]["messages"] == provider.calls[1]["messages"]
    assert prompt_trace["prompt_budget"]["effective_min_context_limit"] == 90
    assert prompt_trace["provider_fallback_context"]["same_sanitized_messages_reused"] is True


@pytest.mark.asyncio
async def test_smoke_dropped_artifact_is_not_provider_visible_or_public_source(tmp_path):
    rules, models = _write_router(tmp_path, primary_limit=55)
    memory = BudgetMemoryStore(
        retrieval_bundle={
            "recent": [],
            "semantic": [],
            "artifact_refs": [
                {
                    "owner_id": "owner",
                    "evidence_role": "derived",
                    "artifact_id": "artifact-private",
                    "file_path": "private.md",
                    "snippet": "PRIVATE_ARTIFACT_SENTINEL " * 30,
                    "source_ref": {"ref_type": "derived_text", "ref_id": "derived-private"},
                    "source_availability": "available",
                    "source_checks": [],
                    "provenance": {"derived_id": "derived-private"},
                    "freshness_state": "active",
                    "durable_status": "active",
                }
            ],
            "observed_metadata": {"has_code_like_content": False},
        },
    )
    provider = BudgetProvider()

    out = await orchestrate_chat(
        payload=_payload([{"role": "user", "content": "FINAL_TURN_SENTINEL"}]),
        memory_store=memory,
        litellm=provider,
        rules_path=str(rules),
        model_registry_path=str(models),
        request_id="rid-budget-source",
        allow_manual_override=True,
        prompt_output_token_reserve=0,
        prompt_context_safety_margin=0,
    )

    prompt_trace = _trace(memory)
    assert out["sources"] == []
    assert "PRIVATE_ARTIFACT_SENTINEL" not in json.dumps(provider.calls[0]["messages"])
    assert prompt_trace["retained_source_ids"]["artifact_ids"] == []
    retrieval_layer = _layer(prompt_trace, "retrieval_augmentation")
    assert retrieval_layer["metadata"]["snippets"]["artifact_refs"] == []


@pytest.mark.asyncio
async def test_smoke_required_content_overflow_fails_without_false_truncation(tmp_path):
    rules, models = _write_router(tmp_path, primary_limit=30)
    memory = BudgetMemoryStore(profile_overlay="REQUIRED_PROFILE_SENTINEL " * 80)
    runtime = BudgetRuntime()
    provider = BudgetProvider()

    with pytest.raises(RuntimeError, match="required_prompt_content_exceeds_budget"):
        await orchestrate_chat(
            payload=_payload([{"role": "user", "content": "FINAL_REQUIRED_SENTINEL"}]),
            memory_store=memory,
            litellm=provider,
            runtime=runtime,
            rules_path=str(rules),
            model_registry_path=str(models),
            request_id="rid-budget-required-overflow",
            allow_manual_override=True,
            prompt_output_token_reserve=0,
            prompt_context_safety_margin=0,
        )

    prompt_trace = _trace(memory)
    assert provider.calls == []
    assert (
        prompt_trace["prompt_budget"]["failure_reason"] == "required_prompt_content_exceeds_budget"
    )
    assert prompt_trace["truncation"]["applied"] is False
    assert "REQUIRED_PROFILE_SENTINEL" not in _private_trace_text(prompt_trace)
    assert runtime.terminal_status == "abandoned"


@pytest.mark.asyncio
async def test_smoke_required_overflow_after_optional_removal_reports_truncation(tmp_path):
    rules, models = _write_router(tmp_path, primary_limit=50)
    memory = BudgetMemoryStore(
        profile_overlay="REQUIRED_PROFILE_SENTINEL " * 45,
        retrieval_bundle={
            "recent": [{"role": "assistant", "content": "OPTIONAL_RECENT_SENTINEL " * 40}],
            "semantic": [],
            "artifact_refs": [],
            "observed_metadata": {"has_code_like_content": False},
        },
    )
    runtime = BudgetRuntime()
    provider = BudgetProvider()

    with pytest.raises(RuntimeError, match="required_prompt_content_exceeds_budget"):
        await orchestrate_chat(
            payload=_payload([{"role": "user", "content": "FINAL_REQUIRED_SENTINEL"}]),
            memory_store=memory,
            litellm=provider,
            runtime=runtime,
            rules_path=str(rules),
            model_registry_path=str(models),
            request_id="rid-budget-required-after-drop",
            allow_manual_override=True,
            prompt_output_token_reserve=0,
            prompt_context_safety_margin=0,
        )

    prompt_trace = _trace(memory)
    assert provider.calls == []
    assert (
        prompt_trace["prompt_budget"]["failure_reason"] == "required_prompt_content_exceeds_budget"
    )
    assert prompt_trace["truncation"]["applied"] is True
    assert prompt_trace["prompt_budget"]["dropped_context"]["total_count"] > 0
    assert runtime.terminal_status == "abandoned"


@pytest.mark.asyncio
async def test_smoke_metadata_failure_is_bounded_and_skips_provider(tmp_path):
    rules, models = _write_router(tmp_path, fallback=True, primary_limit=500, fallback_limit=None)
    memory = BudgetMemoryStore()
    runtime = BudgetRuntime()
    provider = BudgetProvider()

    with pytest.raises(RuntimeError, match="model_context_limit_unavailable"):
        await orchestrate_chat(
            payload=_payload([{"role": "user", "content": "FINAL_TURN_SENTINEL"}]),
            memory_store=memory,
            litellm=provider,
            runtime=runtime,
            rules_path=str(rules),
            model_registry_path=str(models),
            request_id="rid-budget-metadata-failure",
            allow_manual_override=True,
            prompt_output_token_reserve=0,
            prompt_context_safety_margin=0,
        )

    prompt_trace = _trace(memory)
    assert provider.calls == []
    assert prompt_trace["prompt_budget"]["failure_reason"] == "model_context_limit_unavailable"
    assert prompt_trace["truncation"]["applied"] is False
    assert runtime.terminal_status == "abandoned"


@pytest.mark.asyncio
async def test_smoke_unexpected_budget_exception_is_bounded_and_skips_provider(
    tmp_path,
    monkeypatch,
):
    rules, models = _write_router(tmp_path, primary_limit=100)
    memory = BudgetMemoryStore()
    runtime = BudgetRuntime()
    provider = BudgetProvider()

    def explode(messages):
        raise RuntimeError("PRIVATE_ESTIMATOR_SENTINEL")

    monkeypatch.setattr("services.prompt_assembly.estimate_prompt_tokens", explode)

    with pytest.raises(RuntimeError, match="prompt_budget_evaluation_failed"):
        await orchestrate_chat(
            payload=_payload([{"role": "user", "content": "FINAL_TURN_SENTINEL"}]),
            memory_store=memory,
            litellm=provider,
            runtime=runtime,
            rules_path=str(rules),
            model_registry_path=str(models),
            request_id="rid-budget-estimator-failure",
            allow_manual_override=True,
            prompt_output_token_reserve=0,
            prompt_context_safety_margin=0,
        )

    prompt_trace = _trace(memory)
    assert provider.calls == []
    assert prompt_trace["prompt_budget"]["failure_reason"] == "prompt_budget_evaluation_failed"
    assert "PRIVATE_ESTIMATOR_SENTINEL" not in _private_trace_text(prompt_trace)
    assert runtime.terminal_status == "abandoned"


@pytest.mark.asyncio
async def test_smoke_provider_exhaustion_preserves_budget_fingerprint(tmp_path):
    rules, models = _write_router(tmp_path, fallback=True, primary_limit=160, fallback_limit=120)
    memory = BudgetMemoryStore()
    runtime = BudgetRuntime()
    provider = BudgetProvider(fail_all=True)

    with pytest.raises(RuntimeError, match="provider failed"):
        await orchestrate_chat(
            payload=_payload([{"role": "user", "content": "FINAL_TURN_SENTINEL"}]),
            memory_store=memory,
            litellm=provider,
            runtime=runtime,
            rules_path=str(rules),
            model_registry_path=str(models),
            request_id="rid-budget-provider-exhaustion",
            allow_manual_override=True,
            prompt_output_token_reserve=0,
            prompt_context_safety_margin=0,
        )

    prompt_trace = _trace(memory)
    assert len(provider.calls) == 2
    assert prompt_trace["prompt_budget"]["final_within_budget"] is True
    assert (
        prompt_trace["provider_prompt"]["fingerprint"]
        == prompt_trace["provider_fallback_context"]["prompt_fingerprint"]
    )
    assert runtime.terminal_status == "abandoned"
