import pytest
import httpx
from services.orchestrate import orchestrate_chat

BANNED_TRACE_TOKENS = [
    "R26",
    "R27",
    "R29",
    "R30",
    "Cluster11",
    "Cluster12",
    "11C",
    "11D",
    "12A",
    "12B",
    "phase",
    "milestone",
    "spec",
]

BANNED_RUNTIME_KEY_TOKENS = [
    "gate",
    "gating",
    "block",
    "rewrite",
    "R30",
    "Cluster",
    "phase",
    "milestone",
    "spec",
]


def _collect_keys(value):
    if isinstance(value, dict):
        keys = list(value.keys())
        for nested in value.values():
            keys.extend(_collect_keys(nested))
        return keys
    if isinstance(value, list):
        keys = []
        for nested in value:
            keys.extend(_collect_keys(nested))
        return keys
    return []


class FakeMemoryStore:
    def __init__(self):
        self.added_messages = []
        self.retrieve_calls = []
        self.trace_calls = []

    async def resolve_conversation(self, **kwargs):
        return {"conversation_id": "conv-1", "reused": False}

    async def add_message(self, **kwargs):
        self.added_messages.append(kwargs)
        return {"message_id": "m-1"}

    async def retrieve_bundle(self, **kwargs):
        self.retrieve_calls.append(kwargs)
        return {
            "request_id": kwargs["request_id"],
            "conversation_id": kwargs["conversation_id"],
            "bundle": {
                "recent": [{"role": "assistant", "content": "prior history"}],
                "semantic": [
                    {
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "role": "assistant",
                        "content": "semantic note",
                    }
                ],
                "artifact_refs": [
                    {
                        "artifact_id": "a-1",
                        "file_path": "api/main.py",
                        "snippet": "def entrypoint(): pass",
                        "relevance_score": 0.9,
                    }
                ],
                "observed_metadata": {"has_code_like_content": False},
            },
        }

    async def resolve_profile(self, **kwargs):
        return {
            "profile_name": "dev",
            "source": "global_default",
            "profile_version": 1,
            "effective_profile_ref": "owner:dev:1",
            "prompt_overlay": "",
            "retrieval_policy": {},
            "routing_policy": {},
            "response_style": {},
            "safety_policy": {},
            "tool_policy": {},
        }

    async def create_trace(self, **kwargs):
        self.trace_calls.append(kwargs)
        return {"trace_id": "t-1", "request_id": kwargs["request_id"]}


class FakeRuntime:
    def __init__(
        self,
        *,
        response=None,
        companion_response=None,
        fail: bool = False,
        companion_error: Exception | None = None,
        companion_endpoint: str = "/v1/companion/profile/compile",
    ):
        self.calls = []
        self.companion_calls = []
        self.interrupt_calls = []
        self.reset_calls = []
        self.last_companion_compile_endpoint = None
        self.response = response or {
            "runtime_state": {
                "runtime_state_id": "rtstate_1",
                "reset_after_turn": False,
            },
            "overlay": None,
            "omitted": True,
            "omission_reason": "empty_runtime_state",
        }
        self.companion_response = companion_response or {
            "profile_id": "default_companion_profile",
            "profile_version": 1,
            "contract_id": "default_interaction_contract",
            "contract_version": 1,
            "scene_id": "planning",
            "scene_confidence": 1.0,
            "scene_source": "requested_scene",
            "warnings": [],
            "runtime_state": {"runtime_state_id": "rtstate_1"},
            "overlays": [
                {
                    "overlay_id": "contract-1",
                    "overlay_type": "interaction_contract",
                    "role": "system",
                    "content": "contract text",
                },
                {
                    "overlay_id": "profile-1",
                    "overlay_type": "companion_profile",
                    "role": "system",
                    "content": "profile companion text",
                },
                {
                    "overlay_id": "scene-1",
                    "overlay_type": "scene_policy",
                    "role": "system",
                    "content": "scene text",
                },
            ],
        }
        self.interrupt_response = {
            "request_id": "rid-interrupt",
            "owner_id": "owner",
            "conversation_id": "conv-1",
            "surface": "dev",
            "requested_scene": None,
            "trigger_class": "repetitive_branching",
            "confidence": 0.84,
            "style_selected": "next_step_forcing",
            "should_interrupt": True,
            "should_defer": False,
            "reason_json": {"defer_reasons": [], "trigger_class": "repetitive_branching"},
            "contract_constraints_applied": {"matched_contract_style": "soft_redirect"},
            "warnings": [],
            "debug": {"detector_signals": {"branch_count": 4}, "user_visible_suppressed": True},
        }
        self.fail = fail
        self.companion_error = companion_error
        self.companion_endpoint = companion_endpoint

    async def compile_companion_policy(self, **kwargs):
        self.companion_calls.append(kwargs)
        self.last_companion_compile_endpoint = self.companion_endpoint
        if self.companion_error is not None:
            raise self.companion_error
        if self.fail:
            raise RuntimeError("runtime unavailable")
        if isinstance(self.companion_response, dict):
            response = dict(self.companion_response)
            response.setdefault(
                "_cognitive_runtime_compile_endpoint",
                self.companion_endpoint,
            )
            return response
        return self.companion_response

    async def overlay(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("runtime unavailable")
        return self.response

    async def evaluate_interrupt(self, **kwargs):
        self.interrupt_calls.append(kwargs)
        if self.fail:
            raise RuntimeError("runtime unavailable")
        return self.interrupt_response

    async def reset(self, **kwargs):
        self.reset_calls.append(kwargs)
        return {"reset": True}


class FakeLiteLLM:
    def __init__(self, *, fail_first: bool = False, content: str = "hello"):
        self.calls = []
        self.fail_first = fail_first
        self.content = content

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail_first and len(self.calls) == 1:
            raise RuntimeError("primary failed")
        return {"choices": [{"message": {"content": self.content}}]}


class FakeDSA:
    def __init__(self, *, response=None, error: Exception | None = None):
        self.calls = []
        self.response = response or {"sources_used": [], "items": []}
        self.error = error

    async def context_pack(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


@pytest.mark.asyncio
async def test_orchestrate_chat_happy_path(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  gpt-4o-mini:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )

    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-test-1",
    )

    assert out["conversation_id"] == "conv-1"
    assert out["request_id"] == "rid-test-1"
    assert out["status"] == "ok"
    assert out["answer"] == "hello"
    assert out["sources"][0]["file_path"] == "api/main.py"
    assert len(memory_store.added_messages) == 2
    assert memory_store.added_messages[0]["role"] == "user"
    assert memory_store.added_messages[1]["role"] == "assistant"
    assert memory_store.retrieve_calls[0]["request_id"] == "rid-test-1"
    assert litellm.calls[0]["request_id"] == "rid-test-1"
    assert litellm.calls[0]["messages"][0]["role"] == "system"
    assert any(
        "Retrieved file snippets:" in msg["content"]
        for msg in litellm.calls[0]["messages"]
        if msg["role"] == "system"
    )
    assert any(
        msg["role"] == "assistant" and msg["content"] == "prior history"
        for msg in litellm.calls[0]["messages"]
    )
    assert memory_store.trace_calls[0]["request_id"] == "rid-test-1"
    assert len(memory_store.retrieve_calls) == 1
    trace_payload = memory_store.trace_calls[0]["payload"]
    assert trace_payload["retrieval"]["prompt_assembly"]["included_layers"] == [
        "retrieval_augmentation",
        "recent_history",
        "current_messages",
    ]
    assert trace_payload["retrieval"]["prompt_assembly"]["runtime"] == {
        "attempted": False,
        "status": "disabled",
        "included": False,
    }
    presentation = trace_payload["retrieval"]["prompt_assembly"]["presentation"]
    assert presentation["routing"]["selected_model"] == "gpt-4o-mini"
    assert presentation["companion"]["status"] == "disabled"
    assert presentation["runtime"]["status"] == "disabled"
    assert presentation["retrieval"]["semantic_count"] == 1
    assert presentation["retrieval"]["artifact_ref_count"] == 1
    assert "snippet" not in str(presentation)
    assert "prior history" not in str(presentation)
    handoff = trace_payload["retrieval"]["prompt_assembly"]["handoff"]
    assert handoff["request"]["request_id"] == "rid-test-1"
    assert handoff["routing"]["selected_model"] == "gpt-4o-mini"
    assert handoff["routing"]["selected_provider"] == "cloud"
    assert handoff["retrieval"]["semantic_count"] == 1
    assert handoff["retrieval"]["artifact_ref_count"] == 1
    assert handoff["runtime"]["status"] == "disabled"
    assert handoff["companion"]["status"] == "disabled"
    assert "snippet" not in str(handoff)
    assert "prior history" not in str(handoff)
    assert trace_payload["retrieval"]["prompt_assembly"]["truncation"] == {
        "applied": False,
        "reason": None,
    }
    response_shape_trace = trace_payload["retrieval"]["prompt_assembly"]["response_shape"]
    assert response_shape_trace["attempted"] is True
    assert response_shape_trace["status"] == "not_requested"
    assert response_shape_trace["resolved_shape"]["continuation_state"] == "none"
    response_review = trace_payload["retrieval"]["prompt_assembly"]["response_review"]
    assert response_review == {
        "status": "clear",
        "finding_count": 0,
        "highest_severity": "clear",
        "findings": [],
        "checked_categories": [
            "empty_response",
            "unsupported_memory_claim",
            "apology_loop",
            "pseudo_attachment",
            "pressure_language",
            "response_shape_mismatch",
            "excessive_length",
        ],
        "diagnostic_only": True,
        "action_taken": "none",
        "reviewed_text_source": "raw_model_output",
    }
    response_action = trace_payload["retrieval"]["prompt_assembly"]["response_action"]
    assert response_action == {
        "mode": "shadow",
        "action_taken": "none",
        "action_reason_codes": [],
        "action_source": "response_review",
        "affected_finding_types": [],
        "diagnostic_only": True,
        "original_review_status": "clear",
    }
    assert trace_payload["router_decision"]["routing_contract"]["selected_model"] == "gpt-4o-mini"


@pytest.mark.asyncio
async def test_orchestrate_applies_spec_shaped_retrieval_policy(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  gpt-4o-mini:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )

    class RetrievalPolicyStore(FakeMemoryStore):
        async def resolve_profile(self, **kwargs):
            return {
                "profile_name": "dev",
                "source": "global_default",
                "profile_version": 1,
                "effective_profile_ref": "owner:dev:1",
                "prompt_overlay": "",
                "retrieval_policy": {
                    "k": 6,
                    "min_score": 0.3,
                    "scope": "owner",
                    "time_window": "30d",
                    "retrieval_mode": "historical",
                },
                "routing_policy": {},
                "response_style": {},
                "safety_policy": {},
                "tool_policy": {},
            }

    memory_store = RetrievalPolicyStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-retrieval-1",
    )

    assert memory_store.retrieve_calls[0]["retrieval"] == {
        "k": 6,
        "min_score": 0.3,
        "scope": "owner",
        "time_window": "30d",
        "retrieval_mode": "historical",
    }


@pytest.mark.asyncio
async def test_orchestrate_rejects_cloud_override_when_profile_is_local_only(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: override\n"
        "    when:\n"
        "      model_override_present: true\n"
        "    then:\n"
        "      selected_model_from: model_override\n"
        "      provider: cloud\n"
        "      rationale: manual override accepted by policy\n"
        "      fallbacks: []\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: chat_voice_openai\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  chat_local_fast:\n"
        "    provider: local\n"
        "    avg_latency_bucket: fast\n"
        "    cost_per_1k_tokens: 0\n"
        "  chat_voice_openai:\n"
        "    provider: cloud\n"
        "    avg_latency_bucket: medium\n"
        "    cost_per_1k_tokens: 0.003\n",
        encoding="utf-8",
    )

    class LocalOnlyMemoryStore(FakeMemoryStore):
        async def resolve_profile(self, **kwargs):
            return {
                "profile_name": "local",
                "source": "global_default",
                "profile_version": 1,
                "effective_profile_ref": "owner:local:1",
                "prompt_overlay": "",
                "retrieval_policy": {},
                "routing_policy": {"local_only": True},
                "response_style": {},
                "safety_policy": {},
                "tool_policy": {},
            }

    memory_store = LocalOnlyMemoryStore()
    litellm = FakeLiteLLM()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": "chat_voice_openai",
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-local-1",
    )

    assert out["selected_model"] == "chat_local_fast"
    assert litellm.calls[0]["model"] == "chat_local_fast"
    assert memory_store.trace_calls[0]["payload"]["manual_override"] == {
        "requested_model": "chat_voice_openai",
        "applied": False,
        "rejection_reason": "rejected_local_only",
    }


@pytest.mark.asyncio
async def test_orchestrate_applies_latency_and_cost_policy(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: chat_voice_openai\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  chat_voice_openai:\n"
        "    provider: cloud\n"
        "    avg_latency_bucket: medium\n"
        "    cost_per_1k_tokens: 0.003\n"
        "  chat_fast_cloud:\n"
        "    provider: cloud\n"
        "    avg_latency_bucket: fast\n"
        "    cost_per_1k_tokens: 0.02\n"
        "  chat_cheap_cloud:\n"
        "    provider: cloud\n"
        "    avg_latency_bucket: slow\n"
        "    cost_per_1k_tokens: 0.001\n",
        encoding="utf-8",
    )

    class PolicyMemoryStore(FakeMemoryStore):
        def __init__(self, routing_policy):
            super().__init__()
            self._routing_policy = routing_policy

        async def resolve_profile(self, **kwargs):
            return {
                "profile_name": "dev",
                "source": "global_default",
                "profile_version": 1,
                "effective_profile_ref": "owner:dev:1",
                "prompt_overlay": "",
                "retrieval_policy": {},
                "routing_policy": self._routing_policy,
                "response_style": {},
                "safety_policy": {},
                "tool_policy": {},
            }

    fast_store = PolicyMemoryStore({"latency_mode": "fast"})
    fast_llm = FakeLiteLLM()
    fast_out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=fast_store,
        litellm=fast_llm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-fast-1",
    )
    assert fast_out["selected_model"] == "chat_fast_cloud"

    cheap_store = PolicyMemoryStore({"cost_mode": "low"})
    cheap_llm = FakeLiteLLM()
    cheap_out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=cheap_store,
        litellm=cheap_llm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-cheap-1",
    )
    assert cheap_out["selected_model"] == "chat_cheap_cloud"


@pytest.mark.asyncio
async def test_orchestrate_uses_local_route_when_request_sensitivity_is_local_only(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: local-only\n"
        "    when:\n"
        "      sensitivity: local_only\n"
        "    then:\n"
        "      selected_model: chat_local_fast\n"
        "      provider: local\n"
        "      rationale: local only\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  chat_local_fast:\n"
        "    provider: local\n"
        "    avg_latency_bucket: fast\n"
        "    cost_per_1k_tokens: 0\n",
        encoding="utf-8",
    )

    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "local_only",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-request-local-1",
    )

    assert out["selected_model"] == "chat_local_fast"
    contract = memory_store.trace_calls[0]["payload"]["router_decision"]["routing_contract"]
    assert contract["sensitivity"] == "local_only"
    assert contract["selected_provider"] == "local"


@pytest.mark.asyncio
async def test_orchestrate_fallback_trace_metadata(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: chat_cloud_primary\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks:\n"
        "        - selected_model: chat_local_fast\n"
        "          provider: local\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  chat_cloud_primary:\n"
        "    provider: cloud\n"
        "  chat_local_fast:\n"
        "    provider: local\n",
        encoding="utf-8",
    )

    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(fail_first=True)

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-fallback-1",
    )

    assert out["status"] == "degraded"
    assert out["selected_model"] == "chat_local_fast"
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["fallback"] == {"triggered": True, "reason": "provider_error"}
    assert trace["router_decision"]["routing_contract"]["fallback_used"] is True
    assert trace["retrieval"]["prompt_assembly"]["surface_presence"]["presence_state"] == "fallback"
    assert trace["retrieval"]["prompt_assembly"]["surface_presence"]["fallback_active"] is True


@pytest.mark.asyncio
async def test_orchestrate_local_only_without_local_model_fails_before_model_call(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: chat_cloud_primary\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  chat_cloud_primary:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )

    class LocalOnlyMemoryStore(FakeMemoryStore):
        async def resolve_profile(self, **kwargs):
            profile = await super().resolve_profile(**kwargs)
            profile["routing_policy"] = {"local_only": True}
            return profile

    memory_store = LocalOnlyMemoryStore()
    litellm = FakeLiteLLM()

    with pytest.raises(RuntimeError, match="local_only policy active but no local model available"):
        await orchestrate_chat(
            payload={
                "owner_id": "owner",
                "client_id": "vscode",
                "surface": "vscode",
                "messages": [{"role": "user", "content": "hi"}],
                "sensitivity": "private",
                "model_override": None,
            },
            memory_store=memory_store,
            litellm=litellm,
            rules_path=str(rules),
            model_registry_path=str(models),
            allow_manual_override=True,
            request_id="rid-no-local-1",
        )

    assert litellm.calls == []
    assert len(memory_store.trace_calls) == 1
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["status"] == "failed"
    assert trace["error"] == "no_local_model_available"
    contract = trace["router_decision"]["routing_contract"]
    assert contract["request_local_only"] is False
    assert contract["profile_local_only"] is True
    assert contract["effective_local_only"] is True
    assert contract["selected_model"] == "chat_cloud_primary"
    assert contract["selected_provider"] == "cloud"
    assert contract["failure_reason"] == "no_local_model_available"
    response_shape = trace["retrieval"]["prompt_assembly"]["response_shape"]
    surface_presence = trace["retrieval"]["prompt_assembly"]["surface_presence"]
    assert response_shape["attempted"] is True
    assert response_shape["status"] == "not_requested"
    assert surface_presence["presence_state"] == "unavailable"
    assert surface_presence["reason"] == "request_failed"


@pytest.mark.asyncio
async def test_orchestrate_does_not_call_runtime_when_overlays_disabled(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")

    runtime = FakeRuntime()
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        enable_runtime_overlays=False,
        request_id="rid-runtime-disabled",
    )

    assert runtime.calls == []
    runtime_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "runtime"
    ]
    assert runtime_trace == {"attempted": False, "status": "disabled", "included": False}


@pytest.mark.asyncio
async def test_orchestrate_includes_runtime_overlay_and_trace(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")
    runtime = FakeRuntime(
        response={
            "runtime_state": {
                "runtime_state_id": "rtstate_1",
                "reset_after_turn": False,
            },
            "overlay": {
                "runtime_state_id": "rtstate_1",
                "overlay_id": "rtoverlay_1",
                "overlay_type": "runtime_state",
                "role": "system",
                "content": (
                    "Runtime context: scene=planning; interaction_mode=actionable; "
                    "constraints=preserve_flow."
                ),
                "source_fields": [
                    "active_scene",
                    "interaction_mode",
                    "temporary_constraints",
                ],
            },
            "omitted": False,
            "omission_reason": None,
        }
    )
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        enable_runtime_overlays=True,
        request_id="rid-runtime-included",
    )

    assert runtime.calls[0]["surface"] == "dev"
    assert len(runtime.calls) == 1
    contents = [msg["content"] for msg in litellm.calls[0]["messages"]]
    assert contents[0] == (
        "Runtime context: scene=planning; interaction_mode=actionable; "
        "constraints=preserve_flow."
    )
    assert "preserve flow" not in contents[0]
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["included_layers"] == [
        "runtime_overlay",
        "retrieval_augmentation",
        "recent_history",
        "current_messages",
    ]
    assert prompt_trace["runtime"] == {
        "attempted": True,
        "runtime_state_id": "rtstate_1",
        "reset_after_turn": False,
        "status": "included",
        "included": True,
        "overlay_id": "rtoverlay_1",
        "overlay_type": "runtime_state",
        "source_fields": [
            "active_scene",
            "interaction_mode",
            "temporary_constraints",
        ],
    }
    assert prompt_trace["runtime"]["status"] == "included"
    assert prompt_trace["runtime"]["overlay_id"] == "rtoverlay_1"
    presentation = prompt_trace["presentation"]
    assert presentation["runtime"]["status"] == "included"
    assert presentation["runtime"]["overlay_ref"] == {
        "overlay_id": "rtoverlay_1",
        "overlay_type": "runtime_state",
    }
    assert presentation["companion"]["status"] == "disabled"
    handoff = prompt_trace["handoff"]
    assert handoff["runtime"]["status"] == "included"
    assert handoff["runtime"]["overlay_ref"] == {
        "overlay_id": "rtoverlay_1",
        "overlay_type": "runtime_state",
    }
    assert handoff["companion"]["status"] == "disabled"


@pytest.mark.asyncio
async def test_orchestrate_runtime_unavailable_is_trace_visible_and_non_fatal(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        runtime=FakeRuntime(fail=True),
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        enable_runtime_overlays=True,
        request_id="rid-runtime-failed",
    )

    assert out["status"] == "ok"
    runtime_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "runtime"
    ]
    assert runtime_trace["status"] == "failed"
    assert runtime_trace["error_type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_orchestrate_resets_runtime_after_turn_when_requested(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")
    runtime = FakeRuntime(
        response={
            "runtime_state": {
                "runtime_state_id": "rtstate_1",
                "reset_after_turn": True,
            },
            "overlay": None,
            "omitted": True,
            "omission_reason": "empty_runtime_state",
        }
    )
    memory_store = FakeMemoryStore()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        enable_runtime_overlays=True,
        request_id="rid-runtime-reset",
    )

    assert runtime.reset_calls[0]["reason"] == "reset_after_turn"
    runtime_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "runtime"
    ]
    assert runtime_trace["reset"] == {"attempted": True, "status": "ok", "reset": True}


def test_runtime_timeout_setting_is_separate_from_request_timeout(monkeypatch):
    from settings import Settings

    monkeypatch.setenv("ORCH_API_KEY", "orch")
    monkeypatch.setenv("MEMORY_STORE_BASE_URL", "http://memory")
    monkeypatch.setenv("MEMORY_STORE_API_KEY", "memory")
    monkeypatch.setenv("LITELLM_BASE_URL", "http://litellm")
    monkeypatch.setenv("REQUEST_TIMEOUT_MS", "30000")

    monkeypatch.setenv("COGNITIVE_RUNTIME_COMPANION_ENABLED", "true")

    settings = Settings()

    assert settings.request_timeout_ms == 30000
    assert settings.cognitive_runtime_timeout_ms == 1500
    assert settings.cognitive_runtime_companion_enabled is True

@pytest.mark.asyncio
async def test_orchestrate_does_not_call_companion_policy_when_disabled(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")

    runtime = FakeRuntime()
    memory_store = FakeMemoryStore()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        companion_policy_enabled=False,
        request_id="rid-companion-disabled",
    )

    assert runtime.companion_calls == []
    trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    companion_trace = trace["companion_policy"]
    assert companion_trace["attempted"] is False
    assert companion_trace["status"] == "disabled"
    assert companion_trace["included"] is False
    assert companion_trace["cognitive_runtime_compile_status"] == "disabled"
    assert companion_trace["cognitive_runtime_compile_error"] is None
    assert companion_trace["cognitive_runtime_compile_endpoint"] is None
    assert companion_trace["companion_overlay_ids"] == []
    assert companion_trace["runtime_overlay_ids"] == []


@pytest.mark.asyncio
async def test_orchestrate_does_not_call_interrupt_policy_when_mode_off(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")

    runtime = FakeRuntime()
    memory_store = FakeMemoryStore()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        interrupt_policy_mode="off",
        request_id="rid-interrupt-off",
    )

    assert runtime.interrupt_calls == []
    trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert "interrupt_policy" not in trace


@pytest.mark.asyncio
async def test_orchestrate_includes_interrupt_trace_only_when_explicitly_enabled(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")

    runtime = FakeRuntime()
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content="assistant result")

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [
                {"role": "assistant", "content": "prior"},
                {
                    "role": "user",
                    "content": (
                        "Should I rewrite this or add an abstraction or split the module "
                        "or compare options?"
                    ),
                },
            ],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        interrupt_policy_mode="evaluate_only",
        request_id="rid-interrupt-on",
    )

    assert out["answer"] == "assistant result"
    assert runtime.interrupt_calls[0]["current_user_text"].startswith("Should I rewrite this")
    prompt_messages = litellm.calls[0]["messages"]
    assert prompt_messages[-2:] == [
        {"role": "assistant", "content": "prior"},
        {
            "role": "user",
            "content": (
                "Should I rewrite this or add an abstraction or split the module or "
                "compare options?"
            ),
        },
    ]
    assert memory_store.added_messages[-1]["content"] == "assistant result"
    trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert trace["interrupt_policy"]["status"] == "included"
    assert trace["interrupt_policy"]["mode"] == "evaluate_only"
    assert trace["interrupt_policy"]["trigger_class"] == "repetitive_branching"
    assert trace["interrupt_policy"]["user_visible_suppressed"] is True


@pytest.mark.asyncio
async def test_orchestrate_interrupt_runtime_failure_is_non_fatal(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")
    memory_store = FakeMemoryStore()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        runtime=FakeRuntime(fail=True),
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        interrupt_policy_mode="evaluate_only",
        request_id="rid-interrupt-failed",
    )

    assert out["status"] == "ok"
    trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert trace["interrupt_policy"]["status"] == "failed"
    assert trace["interrupt_policy"]["error_type"] == "RuntimeError"
    assert trace["interrupt_policy"]["omission_reason"] == "interrupt_policy_unavailable"


@pytest.mark.asyncio
async def test_orchestrate_includes_companion_policy_and_trace(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")
    runtime = FakeRuntime(
        companion_response={
            "profile_id": "default_companion_profile",
            "profile_version": 1,
            "contract_id": "default_interaction_contract",
            "contract_version": 2,
            "interaction_contract": {
                "contract_id": "default_interaction_contract",
                "contract_version": 2,
                "owner_id": "owner",
                "scope": "global_default",
                "source": "default_compiled",
                "trust_rules": ["Be explicit when uncertainty is material."],
                "interaction_boundaries": ["No guilt language."],
                "repair_rules": ["Acknowledge misses clearly."],
                "memory_or_recall_boundaries": ["Mention memory only when useful."],
                "autonomy_rules": ["The user can override advice."],
                "tone_constraints": ["Be candid and calm."],
                "allowed_intervention_styles": ["soft_redirect"],
                "disallowed_intervention_styles": ["guilt_pressure"],
                "defer_conditions": ["Defer when the user harmlessly chooses another path."],
            },
            "contract_trace": {
                "contract_id": "default_interaction_contract",
                "contract_version": 2,
                "source": "default_compiled",
                "scope": "global_default",
                "selected_rule_groups": ["trust_rules", "repair_rules"],
                "selected_boundary_rules": ["No guilt language."],
                "selected_repair_rules": ["Acknowledge misses clearly."],
                "warnings": ["default_contract_applied"],
            },
            "scene_id": "general",
            "scene_confidence": 0.0,
            "scene_source": "fallback_general",
            "warnings": ["unknown_requested_scene", "default_contract_applied"],
            "runtime_state": {"runtime_state_id": "rtstate_1"},
            "overlays": [
                {
                    "overlay_id": "contract-1",
                    "overlay_type": "interaction_contract",
                    "role": "system",
                    "content": "contract text",
                },
                {
                    "overlay_id": "profile-1",
                    "overlay_type": "companion_profile",
                    "role": "system",
                    "content": "profile companion text",
                },
                {
                    "overlay_id": "scene-1",
                    "overlay_type": "scene_policy",
                    "role": "system",
                    "content": "scene text",
                },
            ],
        }
    )
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "requested_scene": "unknown_scene",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        companion_policy_enabled=True,
        request_id="rid-companion-included",
    )

    assert runtime.companion_calls[0]["requested_scene"] == "unknown_scene"
    contents = [msg["content"] for msg in litellm.calls[0]["messages"]]
    assert contents[:3] == ["contract text", "profile companion text", "scene text"]
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["included_layers"] == [
        "companion_policy",
        "retrieval_augmentation",
        "recent_history",
        "current_messages",
    ]
    companion_trace = prompt_trace["companion_policy"]
    assert companion_trace["status"] == "included"
    assert companion_trace["profile_id"] == "default_companion_profile"
    assert companion_trace["contract_id"] == "default_interaction_contract"
    assert companion_trace["contract_version"] == 2
    assert companion_trace["contract_trace"]["source"] == "default_compiled"
    assert companion_trace["interaction_contract"]["memory_or_recall_boundaries"] == [
        "Mention memory only when useful."
    ]
    assert companion_trace["scene_id"] == "general"
    assert companion_trace["warnings"] == [
        "unknown_requested_scene",
        "default_contract_applied",
    ]
    assert companion_trace["companion_profile_id"] == "default_companion_profile"
    assert companion_trace["companion_profile_version"] == 1
    assert companion_trace["interaction_contract_id"] == "default_interaction_contract"
    assert companion_trace["interaction_contract_version"] == 2
    assert companion_trace["companion_policy_warnings"] == [
        "unknown_requested_scene",
        "default_contract_applied",
    ]
    assert companion_trace["companion_overlay_ids"] == ["contract-1", "profile-1", "scene-1"]
    assert companion_trace["runtime_overlay_ids"] == []
    presentation = prompt_trace["presentation"]
    assert presentation["companion"]["status"] == "included"
    assert presentation["companion"]["overlay_ids"] == ["contract-1", "profile-1", "scene-1"]
    assert presentation["runtime"]["status"] == "disabled"
    assert presentation["routing"]["selected_model"] == "gpt-4o-mini"
    handoff = prompt_trace["handoff"]
    assert handoff["companion"]["status"] == "included"
    assert handoff["companion"]["overlay_ids"] == ["contract-1", "profile-1", "scene-1"]
    assert handoff["runtime"]["status"] == "disabled"
    assert handoff["routing"]["selected_model"] == "gpt-4o-mini"
    assert companion_trace["cognitive_runtime_compile_status"] == "included"
    assert companion_trace["cognitive_runtime_compile_error"] is None
    assert (
        companion_trace["cognitive_runtime_compile_endpoint"]
        == "/v1/companion/profile/compile"
    )


@pytest.mark.asyncio
async def test_orchestrate_companion_runtime_failure_is_non_fatal(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")
    memory_store = FakeMemoryStore()

    runtime = FakeRuntime(
        companion_error=RuntimeError("sqlite3.OperationalError: unable to open database file"),
        companion_endpoint="/v1/companion/profile/compile",
    )

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        companion_policy_enabled=True,
        request_id="rid-companion-failed",
    )

    assert out["status"] == "ok"
    assert out["answer"] == "hello"
    assert "unable to open database file" not in out["answer"]
    assert len(runtime.companion_calls) == 1
    companion_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "companion_policy"
    ]
    assert companion_trace["status"] == "failed"
    assert companion_trace["error_type"] == "RuntimeError"
    assert companion_trace["omission_reason"] == "companion_policy_unavailable"
    assert companion_trace["cognitive_runtime_compile_status"] == "failed"
    assert companion_trace["cognitive_runtime_compile_error"] == (
        "sqlite3.OperationalError: unable to open database file"
    )
    assert companion_trace["cognitive_runtime_compile_endpoint"] == (
        "/v1/companion/profile/compile"
    )
    assert memory_store.trace_calls[0]["payload"]["fallback"] == {
        "triggered": False,
        "reason": None,
    }

@pytest.mark.asyncio
async def test_orchestrate_companion_runtime_400_failure_does_not_trigger_alias_semantics(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n  gpt-4o-mini:\n    provider: cloud\n",
        encoding="utf-8",
    )
    memory_store = FakeMemoryStore()
    runtime = FakeRuntime(
        companion_error=RuntimeError("400 Bad Request"),
        companion_endpoint="/v1/companion/profile/compile",
    )

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        companion_policy_enabled=True,
        request_id="rid-companion-400",
    )

    assert out["status"] == "ok"
    companion_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "companion_policy"
    ]
    assert companion_trace["status"] == "failed"
    assert companion_trace["cognitive_runtime_compile_status"] == "failed"
    assert companion_trace["cognitive_runtime_compile_error"] == "400 Bad Request"
    assert companion_trace["cognitive_runtime_compile_endpoint"] == (
        "/v1/companion/profile/compile"
    )
@pytest.mark.asyncio
async def test_orchestrate_malformed_companion_response_is_non_fatal(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text("models:\n  gpt-4o-mini:\n    provider: cloud\n", encoding="utf-8")
    memory_store = FakeMemoryStore()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "dev",
            "surface": "dev",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        runtime=FakeRuntime(companion_response=["not", "a", "dict"]),
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        companion_policy_enabled=True,
        request_id="rid-companion-malformed",
    )

    assert out["status"] == "ok"
    companion_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "companion_policy"
    ]
    assert companion_trace["status"] == "failed"
    assert companion_trace["included"] is False
    assert companion_trace["error_type"] == "list"
    assert companion_trace["omission_reason"] == "malformed_companion_policy_response"
    assert companion_trace["cognitive_runtime_compile_status"] == "failed"
    assert companion_trace["cognitive_runtime_compile_error"] == "list"
    assert companion_trace["cognitive_runtime_compile_endpoint"] is None



@pytest.mark.asyncio
async def test_orchestrate_brief_mode_shapes_persisted_answer_and_traces_raw_answer(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  gpt-4o-mini:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )

    raw = (
        "Net: ship the deterministic brief layer first. "
        "Risk: output could feel rigid. "
        "Recommendation: keep brief mode opt-in. "
        "Next: add tests and trace metadata."
    )
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content=raw)

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "telegram",
            "messages": [{"role": "user", "content": "brief this"}],
            "sensitivity": "private",
            "model_override": None,
            "response_mode": "brief",
            "brief_depth": 1,
            "brief_type": "recommendation",
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-brief-1",
    )

    assert len(litellm.calls) == 1
    assert out["answer"] != raw
    assert out["answer"].startswith("Net: ship the deterministic brief layer first")
    assert memory_store.added_messages[-1]["role"] == "assistant"
    assert memory_store.added_messages[-1]["content"] == out["answer"]

    trace_payload = memory_store.trace_calls[0]["payload"]
    brief = trace_payload["model_call"]["brief"]
    assert brief["enabled"] is True
    assert brief["brief_type"] == "recommendation"
    assert brief["depth_level"] == 1
    assert brief["surface"] == "telegram"
    assert brief["source"] == "explicit_user_request"
    assert brief["explicit_request"] is True
    assert brief["raw_model_answer"] == raw
    assert brief["shaped_answer"] == out["answer"]
    response_review = trace_payload["retrieval"]["prompt_assembly"]["response_review"]
    assert response_review["reviewed_text_source"] == "raw_model_output"
    assert response_review["action_taken"] == "none"
    response_action = trace_payload["retrieval"]["prompt_assembly"]["response_action"]
    assert response_action["mode"] == "shadow"
    assert response_action["action_taken"] == "none"


@pytest.mark.asyncio
async def test_orchestrate_normal_mode_does_not_shape_or_add_raw_answer_trace(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  gpt-4o-mini:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )

    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content="Net: raw answer should pass through.")

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-normal-brief-metadata",
    )

    assert out["answer"] == "Net: raw answer should pass through."
    brief = memory_store.trace_calls[0]["payload"]["model_call"]["brief"]
    assert brief == {"enabled": False}



def _write_default_route_files(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  gpt-4o-mini:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )
    return rules, models


@pytest.mark.asyncio
async def test_orchestrate_default_chat_does_not_emit_style_guidance(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-style-default",
    )

    system_messages = [
        msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"
    ]
    assert all("Style guidance:" not in content for content in system_messages)
    assert all("Response shape guidance:" not in content for content in system_messages)
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    style_trace = prompt_trace["style"]
    response_shape_trace = prompt_trace["response_shape"]
    surface_presence_trace = prompt_trace["surface_presence"]
    assert style_trace["status"] == "not_requested"
    assert style_trace["included"] is False
    assert response_shape_trace["status"] == "not_requested"
    assert response_shape_trace["included"] is False
    assert surface_presence_trace["presence_state"] == "idle"
    assert surface_presence_trace["fallback_active"] is False


@pytest.mark.asyncio
async def test_orchestrate_telegram_surface_emits_compact_text_guidance(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "telegram",
            "surface": "telegram",
            "surface_context": {
                "surface_type": "telegram",
                "interaction_mode": "text",
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-style-telegram",
    )

    system_messages = [
        msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"
    ]
    assert any("compact and easy to scan in text" in content for content in system_messages)
    assert all("spoken delivery" not in content for content in system_messages)
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert "style_guidance" in prompt_trace["included_layers"]
    assert prompt_trace["style"]["guidance_flags"]["text_compact"] is True


@pytest.mark.asyncio
async def test_orchestrate_spoken_surface_emits_speakable_guidance(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "car",
            "surface": "car",
            "surface_context": {
                "surface_type": "car",
                "interaction_mode": "voice_mediated",
                "spoken_output": True,
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-style-spoken",
    )

    system_messages = [
        msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"
    ]
    assert any("spoken delivery" in content for content in system_messages)
    assert any("Response shape guidance:" in content for content in system_messages)
    assert any("one or two short sentences" in content for content in system_messages)
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["style"]["resolved_envelope"]["sentence_length"] == "short"
    assert prompt_trace["style"]["resolved_envelope"]["technical_density"] == "low"
    assert prompt_trace["response_shape"]["guidance_flags"]["spoken_output"] is True
    assert prompt_trace["response_shape"]["resolved_shape"]["continuation_state"] == "abbreviated"
    assert prompt_trace["surface_presence"]["presence_state"] == "briefing"


@pytest.mark.asyncio
async def test_orchestrate_active_task_surface_emits_decisive_low_cognitive_load_guidance(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "surface_context": {
                "active_task_mode": True,
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-style-active-task",
    )

    system_messages = [
        msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"
    ]
    assert any(
        "Lead with the answer, keep cognitive load low" in content
        for content in system_messages
    )
    assert any(
        "Response shape guidance:" in content
        and "Lead with the answer before any supporting detail." in content
        for content in system_messages
    )
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["style"]["resolved_envelope"]["directness"] == "high"
    assert prompt_trace["style"]["guidance_flags"]["active_task_mode"] is True
    assert prompt_trace["response_shape"]["guidance_flags"]["active_task_mode"] is True
    assert prompt_trace["response_shape"]["resolved_shape"]["concise_first_answer"] is True
    assert prompt_trace["response_shape"]["resolved_shape"]["continuation_state"] == "none"
    assert prompt_trace["surface_presence"]["presence_state"] == "idle"
    assert prompt_trace["surface_presence"]["active_task_mode"] is True


@pytest.mark.asyncio
async def test_orchestrate_spoken_surface_suppresses_optional_expansion_marker_when_disallowed(
    tmp_path,
):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "car",
            "surface": "car",
            "surface_context": {
                "surface_type": "car",
                "spoken_output": True,
                "allows_expansion": False,
                "latency_preference": "low",
                "verbosity_target": "short",
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-shape-no-expand",
    )

    system_messages = [
        msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"
    ]
    assert all("more detail is available" not in content for content in system_messages)
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["response_shape"]["resolved_shape"]["continuation_state"] == "abbreviated"
    assert prompt_trace["response_shape"]["resolved_shape"]["expansion_marker_allowed"] is False


@pytest.mark.asyncio
async def test_orchestrate_spoken_surface_allows_expandable_continuation(
    tmp_path,
):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "car",
            "surface": "car",
            "surface_context": {
                "surface_type": "car",
                "spoken_output": True,
                "allows_expansion": True,
                "latency_preference": "low",
                "verbosity_target": "short",
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-shape-expand",
    )

    system_messages = [
        msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"
    ]
    assert any("more detail is available" in content for content in system_messages)
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["response_shape"]["resolved_shape"]["continuation_state"] == "expandable"
    assert prompt_trace["response_shape"]["resolved_shape"]["expansion_marker_allowed"] is True

    second_memory_store = FakeMemoryStore()
    second_litellm = FakeLiteLLM()
    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "surface_context": {"allows_expansion": True},
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=second_memory_store,
        litellm=second_litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-shape-expand-default",
    )

    default_prompt_trace = second_memory_store.trace_calls[0]["payload"]["retrieval"][
        "prompt_assembly"
    ]
    assert default_prompt_trace["response_shape"]["status"] == "not_requested"
    assert default_prompt_trace["response_shape"]["resolved_shape"]["continuation_state"] == "none"


@pytest.mark.asyncio
async def test_orchestrate_style_envelope_override_uses_recognized_fields_only(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "car",
            "surface": "car",
            "surface_context": {
                "surface_type": "car",
                "spoken_output": True,
                "style_envelope": {
                    "technical_density": "high",
                    "formality_range": "formal",
                    "ignored_field": "nope",
                },
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-style-override",
    )

    system_messages = [
        msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"
    ]
    assert any(
        "Include technical detail when it materially helps." in content
        for content in system_messages
    )
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["style"]["recognized_request_fields"] == [
        "formality_range",
        "technical_density",
    ]
    assert prompt_trace["style"]["resolved_envelope"]["technical_density"] == "high"
    assert prompt_trace["style"]["resolved_envelope"]["formality_range"] == "formal"
    assert "ignored_field" not in prompt_trace["style"]["recognized_request_fields"]


class NoSupportMemoryStore(FakeMemoryStore):
    async def retrieve_bundle(self, **kwargs):
        self.retrieve_calls.append(kwargs)
        return {
            "request_id": kwargs["request_id"],
            "conversation_id": kwargs["conversation_id"],
            "bundle": {
                "recent": [],
                "semantic": [],
                "artifact_refs": [],
                "observed_metadata": {"has_code_like_content": False},
            },
        }


@pytest.mark.asyncio
async def test_orchestrate_response_review_trace_can_record_concern_without_changing_answer(
    tmp_path,
):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = NoSupportMemoryStore()
    litellm = FakeLiteLLM(
        content="I remember from our last conversation that your deploy failed yesterday."
    )

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "what happened?"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-review-concern",
    )

    assert len(litellm.calls) == 1
    assert out["answer"] == litellm.content
    assert memory_store.added_messages[-1]["content"] == litellm.content
    response_review = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "response_review"
    ]
    assert response_review["status"] == "concern"
    assert response_review["diagnostic_only"] is True
    assert response_review["action_taken"] == "none"
    assert response_review["reviewed_text_source"] == "raw_model_output"
    assert response_review["findings"][0]["type"] == "unsupported_memory_claim"
    response_action = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"][
        "response_action"
    ]
    assert response_action["mode"] == "shadow"
    assert response_action["action_taken"] == "none"
    assert response_action["diagnostic_only"] is True


@pytest.mark.asyncio
async def test_orchestrate_shadow_mode_keeps_answer_unchanged_without_extra_runtime_calls(
    tmp_path,
):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = NoSupportMemoryStore()
    runtime = FakeRuntime()
    litellm = FakeLiteLLM(
        content="I remember from our last conversation that your deploy failed yesterday."
    )

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "what happened?"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        runtime=runtime,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        companion_policy_enabled=True,
        enable_runtime_overlays=False,
        interrupt_policy_mode="off",
        request_id="rid-shadow-default",
    )

    assert out["answer"] == litellm.content
    assert memory_store.added_messages[-1]["content"] == litellm.content
    assert len(litellm.calls) == 1
    assert len(runtime.companion_calls) == 1
    assert runtime.calls == []
    assert runtime.interrupt_calls == []
    assert runtime.reset_calls == []

    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["response_review"]["findings"][0]["type"] == "unsupported_memory_claim"
    assert prompt_trace["response_action"] == {
        "mode": "shadow",
        "action_taken": "none",
        "action_reason_codes": [],
        "action_source": "response_review",
        "affected_finding_types": [],
        "diagnostic_only": True,
        "original_review_status": "concern",
    }
    assert prompt_trace["companion_policy"]["cognitive_runtime_compile_endpoint"] == (
        "/v1/companion/profile/compile"
    )


@pytest.mark.asyncio
async def test_orchestrate_template_fallback_replaces_empty_response_and_persists_final_answer(
    tmp_path,
):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content="")

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        response_action_mode="template_fallback",
        request_id="rid-action-empty",
    )

    assert len(litellm.calls) == 1
    assert out["answer"] == "I couldn’t produce a useful answer there."
    assert memory_store.added_messages[-1]["content"] == out["answer"]
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["response_review"]["action_taken"] == "none"
    assert prompt_trace["response_action"]["mode"] == "template_fallback"
    assert prompt_trace["response_action"]["action_taken"] == "template_fallback"
    assert prompt_trace["response_action"]["affected_finding_types"] == ["empty_response"]
    assert prompt_trace["response_action"]["diagnostic_only"] is False


@pytest.mark.asyncio
async def test_orchestrate_template_fallback_replaces_dependency_or_pressure_language(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content="You only need me for this. Don't let me down.")

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "help"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        response_action_mode="template_fallback",
        request_id="rid-action-pressure",
    )

    assert len(litellm.calls) == 1
    assert out["answer"] == (
        "I can help with the task, but I should not pressure you or create dependency. "
        "Let’s keep this grounded."
    )
    assert memory_store.added_messages[-1]["content"] == out["answer"]
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["response_action"]["affected_finding_types"] == [
        "pseudo_attachment",
        "pressure_language",
    ]
    assert "You only need me for this" not in str(prompt_trace["response_action"])


@pytest.mark.asyncio
async def test_orchestrate_template_fallback_does_not_act_on_unsupported_memory_claim(
    tmp_path,
):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = NoSupportMemoryStore()
    litellm = FakeLiteLLM(
        content="I remember from our last conversation that your deploy failed yesterday."
    )

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "what happened?"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        response_action_mode="template_fallback",
        request_id="rid-action-memory",
    )

    assert len(litellm.calls) == 1
    assert out["answer"] == litellm.content
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert prompt_trace["response_action"]["action_taken"] == "none"
    assert prompt_trace["response_action"]["diagnostic_only"] is True


@pytest.mark.asyncio
async def test_orchestrate_brief_mode_shapes_replacement_only_when_action_occurs(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content="")

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "vscode",
            "surface": "telegram",
            "messages": [{"role": "user", "content": "brief this"}],
            "sensitivity": "private",
            "model_override": None,
            "response_mode": "brief",
            "brief_depth": 1,
            "brief_type": "general",
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        response_action_mode="template_fallback",
        request_id="rid-action-brief",
    )

    assert len(litellm.calls) == 1
    assert out["answer"].startswith("Net: I couldn’t produce a useful answer there.")
    assert memory_store.added_messages[-1]["content"] == out["answer"]
    brief = memory_store.trace_calls[0]["payload"]["model_call"]["brief"]
    assert brief["enabled"] is True
    assert brief["raw_model_answer"] == ""
    assert brief["shaped_answer"] == out["answer"]


@pytest.mark.asyncio
async def test_orchestrate_response_action_trace_keys_do_not_use_banned_runtime_terms(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content="You only need me for this.")

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "car",
            "surface": "car",
            "surface_context": {
                "surface_type": "car",
                "spoken_output": True,
                "active_task_mode": True,
                "allows_expansion": True,
                "latency_preference": "low",
                "verbosity_target": "short",
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        response_action_mode="template_fallback",
        request_id="rid-action-banned-keys",
    )

    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    keys = _collect_keys(prompt_trace["response_action"])
    assert keys
    for token in BANNED_RUNTIME_KEY_TOKENS:
        assert all(token not in key for key in keys)


@pytest.mark.asyncio
async def test_orchestrate_response_review_trace_keys_do_not_use_banned_runtime_terms(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = NoSupportMemoryStore()
    litellm = FakeLiteLLM(content="I remember from our last conversation that this broke.")

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "car",
            "surface": "car",
            "surface_context": {
                "surface_type": "car",
                "spoken_output": True,
                "active_task_mode": True,
                "allows_expansion": True,
                "latency_preference": "low",
                "verbosity_target": "short",
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-review-banned-keys",
    )

    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    keys = _collect_keys(prompt_trace["response_review"])
    assert keys
    for token in BANNED_RUNTIME_KEY_TOKENS:
        assert all(token not in key for key in keys)


@pytest.mark.asyncio
async def test_orchestrate_response_shape_trace_keys_do_not_use_banned_identifiers(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "client_id": "car",
            "surface": "car",
            "surface_context": {
                "surface_type": "car",
                "spoken_output": True,
                "active_task_mode": True,
                "allows_expansion": True,
                "latency_preference": "low",
                "verbosity_target": "short",
            },
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-shape-banned-keys",
    )

    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    keys = _collect_keys(prompt_trace["response_shape"])
    assert keys
    for token in BANNED_TRACE_TOKENS:
        assert all(token not in key for key in keys)


@pytest.mark.asyncio
async def test_orchestrate_live_chat_flow_only_uses_existing_runtime_calls_for_handoff(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    runtime = FakeRuntime()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "surface": "vscode",
            "messages": [{"role": "user", "content": "hi"}],
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=FakeLiteLLM(),
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        runtime=runtime,
        companion_policy_enabled=True,
        enable_runtime_overlays=True,
        interrupt_policy_mode="off",
        request_id="rid-handoff-live-flow",
    )

    assert len(runtime.companion_calls) == 1
    assert len(runtime.calls) == 1
    assert runtime.interrupt_calls == []
    assert len(runtime.reset_calls) == 0
    prompt_trace = memory_store.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    presentation = prompt_trace["presentation"]
    assert presentation["warnings"]["companion_warning_count"] == 0
    handoff = prompt_trace["handoff"]
    assert handoff["warnings"]["interrupt_status"] is None


@pytest.mark.asyncio
async def test_orchestrate_dsa_disabled_skips_external_context_call(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()
    dsa = FakeDSA()

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "surface": "chat",
            "messages": [{"role": "user", "content": "When was the battery replaced?"}],
            "external_context_enabled": True,
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        dsa=dsa,
        dsa_enabled=False,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-dsa-disabled",
    )

    assert dsa.calls == []
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["dsa"] == {"enabled": False, "called": False, "status": "disabled"}
    assert trace["retrieval"]["prompt_assembly"]["dsa"] == trace["dsa"]
    assert "External source context:" not in str(litellm.calls[0]["messages"])


@pytest.mark.asyncio
async def test_orchestrate_dsa_enabled_calls_client_and_includes_prompt_context(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()
    dsa = FakeDSA(
        response={
            "sources_used": ["vehicle_log_primary"],
            "items": [
                {
                    "source_ref": "google_sheets:jeep_wj_maintenance:Maintenance!A44:H44",
                    "source_name": "Jeep WJ Maintenance Log",
                    "title": "Battery replacement",
                    "text": "Battery replacement. Date: 2025-07-12.",
                    "raw": {"hidden": "should not persist"},
                }
            ],
        }
    )

    await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "surface": "chat",
            "messages": [{"role": "user", "content": "When was the battery replaced?"}],
            "external_context_enabled": True,
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        dsa=dsa,
        dsa_enabled=True,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-dsa-success",
    )

    assert dsa.calls == [{"query": "When was the battery replaced?"}]
    system_messages = [msg["content"] for msg in litellm.calls[0]["messages"] if msg["role"] == "system"]
    assert any("External source context:" in msg for msg in system_messages)
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["dsa"] == {
        "enabled": True,
        "called": True,
        "status": "success",
        "item_count": 1,
        "sources_used": ["vehicle_log_primary"],
    }
    assert "should not persist" not in str(trace)
    assert "Battery replacement. Date: 2025-07-12." not in str(trace["dsa"])


@pytest.mark.asyncio
async def test_orchestrate_dsa_no_items_does_not_add_external_context_message(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM(content="I don't see any external source evidence here.")
    dsa = FakeDSA(response={"sources_used": [], "items": []})

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "surface": "chat",
            "messages": [{"role": "user", "content": "Anything on my calendar?"}],
            "external_context_enabled": True,
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        dsa=dsa,
        dsa_enabled=True,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-dsa-empty",
    )

    assert out["status"] == "ok"
    assert "External source context:" not in str(litellm.calls[0]["messages"])
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["dsa"]["status"] == "success"
    assert trace["dsa"]["item_count"] == 0


@pytest.mark.asyncio
async def test_orchestrate_dsa_timeout_degrades_gracefully_without_external_context(tmp_path):
    rules, models = _write_default_route_files(tmp_path)
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()
    dsa = FakeDSA(error=httpx.ReadTimeout("timed out"))

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "surface": "chat",
            "messages": [{"role": "user", "content": "When was the battery replaced?"}],
            "external_context_enabled": True,
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        dsa=dsa,
        dsa_enabled=True,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-dsa-timeout",
    )

    assert out["status"] == "ok"
    assert "External source context:" not in str(litellm.calls[0]["messages"])
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["dsa"] == {
        "enabled": True,
        "called": True,
        "status": "error",
        "error_code": "timeout",
    }


@pytest.mark.asyncio
async def test_orchestrate_dsa_request_local_only_skips_external_call(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  chat_local_fast:\n"
        "    provider: local\n"
        "    avg_latency_bucket: fast\n"
        "    cost_per_1k_tokens: 0\n"
        "  gpt-4o-mini:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )
    memory_store = FakeMemoryStore()
    litellm = FakeLiteLLM()
    dsa = FakeDSA()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "surface": "chat",
            "messages": [{"role": "user", "content": "When was the battery replaced?"}],
            "external_context_enabled": True,
            "sensitivity": "local_only",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        dsa=dsa,
        dsa_enabled=True,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-dsa-request-local-only",
    )

    assert out["status"] == "ok"
    assert dsa.calls == []
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["dsa"] == {
        "enabled": True,
        "called": False,
        "status": "skipped_local_only",
    }
    assert trace["retrieval"]["prompt_assembly"]["dsa"] == trace["dsa"]


@pytest.mark.asyncio
async def test_orchestrate_dsa_profile_local_only_skips_external_call(tmp_path):
    rules = tmp_path / "rules.yaml"
    models = tmp_path / "models.yaml"
    rules.write_text(
        "rules:\n"
        "  - id: default\n"
        "    when: {}\n"
        "    then:\n"
        "      selected_model: gpt-4o-mini\n"
        "      provider: cloud\n"
        "      rationale: default\n"
        "      fallbacks: []\n",
        encoding="utf-8",
    )
    models.write_text(
        "models:\n"
        "  chat_local_fast:\n"
        "    provider: local\n"
        "    avg_latency_bucket: fast\n"
        "    cost_per_1k_tokens: 0\n"
        "  gpt-4o-mini:\n"
        "    provider: cloud\n",
        encoding="utf-8",
    )

    class LocalOnlyMemoryStore(FakeMemoryStore):
        async def resolve_profile(self, **kwargs):
            profile = await super().resolve_profile(**kwargs)
            profile["routing_policy"] = {"local_only": True}
            return profile

    memory_store = LocalOnlyMemoryStore()
    litellm = FakeLiteLLM()
    dsa = FakeDSA()

    out = await orchestrate_chat(
        payload={
            "owner_id": "owner",
            "surface": "chat",
            "messages": [{"role": "user", "content": "What is on my calendar?"}],
            "external_context_enabled": True,
            "sensitivity": "private",
            "model_override": None,
        },
        memory_store=memory_store,
        litellm=litellm,
        dsa=dsa,
        dsa_enabled=True,
        rules_path=str(rules),
        model_registry_path=str(models),
        allow_manual_override=True,
        request_id="rid-dsa-profile-local-only",
    )

    assert out["status"] == "ok"
    assert dsa.calls == []
    trace = memory_store.trace_calls[0]["payload"]
    assert trace["dsa"] == {
        "enabled": True,
        "called": False,
        "status": "skipped_local_only",
    }
