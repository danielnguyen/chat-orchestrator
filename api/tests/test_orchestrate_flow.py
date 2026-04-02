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
                "recent": [],
                "semantic": [],
                "artifact_refs": [],
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


class FakeLiteLLM:
    def __init__(self):
        self.calls = []

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
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
    assert len(memory_store.added_messages) == 2
    assert memory_store.added_messages[0]["role"] == "user"
    assert memory_store.added_messages[1]["role"] == "assistant"
    assert memory_store.retrieve_calls[0]["request_id"] == "rid-test-1"
    assert litellm.calls[0]["request_id"] == "rid-test-1"
    assert memory_store.trace_calls[0]["request_id"] == "rid-test-1"


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
