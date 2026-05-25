import pytest
from services.orchestrate import orchestrate_chat


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
    def __init__(self, *, response=None, fail: bool = False):
        self.calls = []
        self.reset_calls = []
        self.response = response or {
            "runtime_state": {
                "runtime_state_id": "rtstate_1",
                "reset_after_turn": False,
            },
            "overlay": None,
            "omitted": True,
            "omission_reason": "empty_runtime_state",
        }
        self.fail = fail

    async def overlay(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("runtime unavailable")
        return self.response

    async def reset(self, **kwargs):
        self.reset_calls.append(kwargs)
        return {"reset": True}


class FakeLiteLLM:
    def __init__(self, *, fail_first: bool = False):
        self.calls = []
        self.fail_first = fail_first

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail_first and len(self.calls) == 1:
            raise RuntimeError("primary failed")
        return {"choices": [{"message": {"content": "hello"}}]}


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
    assert trace_payload["retrieval"]["prompt_assembly"]["truncation"] == {
        "applied": False,
        "reason": None,
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
    assert prompt_trace["runtime"]["status"] == "included"
    assert prompt_trace["runtime"]["overlay_id"] == "rtoverlay_1"


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
