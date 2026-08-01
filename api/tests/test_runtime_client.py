from __future__ import annotations

import asyncio
import importlib
from typing import Any

import httpx
import pytest
from clients.runtime import RuntimeClient


class _FakeResponse:
    def __init__(
        self,
        path: str,
        payload: Any,
        *,
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.request = httpx.Request("POST", f"http://runtime.local{path}")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = httpx.Response(self.status_code, request=self.request)
            raise httpx.HTTPStatusError(
                f"status {self.status_code}",
                request=self.request,
                response=response,
            )

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _FakeAsyncClient:
    def __init__(self, responses: list[Any] | None = None) -> None:
        self.responses = list(responses or [])
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.close_calls = 0

    async def post(self, path: str, *, json: dict[str, Any]) -> _FakeResponse:
        self.posts.append((path, json))
        item = self.responses.pop(0) if self.responses else {"ok": True}
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, _FakeResponse):
            return item
        if isinstance(item, tuple):
            status_code, payload = item
            return _FakeResponse(path, payload, status_code=status_code)
        return _FakeResponse(path, item)

    async def aclose(self) -> None:
        self.close_calls += 1


class _ConcurrentAsyncClient(_FakeAsyncClient):
    def __init__(self) -> None:
        super().__init__()
        self.active_requests = 0
        self.maximum_active_requests = 0

    async def post(self, path: str, *, json: dict[str, Any]) -> _FakeResponse:
        self.active_requests += 1
        self.maximum_active_requests = max(
            self.maximum_active_requests,
            self.active_requests,
        )
        try:
            await asyncio.sleep(0)
            return await super().post(path, json=json)
        finally:
            self.active_requests -= 1


class _ClientFactory:
    def __init__(self, clients: list[_FakeAsyncClient] | None = None) -> None:
        self.pending_clients = list(clients or [])
        self.clients: list[_FakeAsyncClient] = []
        self.kwargs: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> _FakeAsyncClient:
        self.kwargs.append(kwargs)
        client = (
            self.pending_clients.pop(0)
            if self.pending_clients
            else _FakeAsyncClient()
        )
        self.clients.append(client)
        return client


def _load_main(monkeypatch):
    monkeypatch.setenv("ORCH_API_KEY", "orch-test")
    monkeypatch.setenv("MEMORY_STORE_BASE_URL", "http://memory")
    monkeypatch.setenv("MEMORY_STORE_API_KEY", "memory")
    monkeypatch.setenv("LITELLM_BASE_URL", "http://litellm")
    monkeypatch.setenv("COGNITIVE_RUNTIME_BASE_URL", "http://runtime.local")

    import settings

    settings.get_settings.cache_clear()
    import main

    return importlib.reload(main)


@pytest.mark.asyncio
async def test_runtime_client_lifecycle_is_explicit_idempotent_and_final():
    factory = _ClientFactory()
    client = RuntimeClient(
        "http://runtime.local/",
        "runtime-key",
        client_factory=factory,
    )

    with pytest.raises(RuntimeError, match="^runtime_client_not_started$"):
        await client.overlay(
            request_id="before-open",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )

    await asyncio.gather(client.open(), client.open(), client.open())
    assert len(factory.clients) == 1

    await client.close()
    await client.close()
    assert factory.clients[0].close_calls == 1

    with pytest.raises(RuntimeError, match="^runtime_client_closed$"):
        await client.overlay(
            request_id="after-close",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )
    with pytest.raises(RuntimeError, match="^runtime_client_closed$"):
        await client.open()
    assert len(factory.clients) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("api_key", "expected_headers"),
    [
        ("runtime-key", {"X-API-Key": "runtime-key"}),
        (None, None),
    ],
)
async def test_runtime_client_builds_bounded_transport(api_key, expected_headers):
    factory = _ClientFactory()
    client = RuntimeClient(
        "http://runtime.local/",
        api_key,
        timeout_ms=1500,
        client_factory=factory,
    )

    await client.open()

    assert len(factory.kwargs) == 1
    created = factory.kwargs[0]
    assert created["base_url"] == "http://runtime.local"
    assert created["headers"] == expected_headers
    assert created["timeout"] == 1.5
    assert vars(created["limits"]) == {
        "max_connections": 20,
        "max_keepalive_connections": 10,
        "keepalive_expiry": 5.0,
    }
    await client.close()


@pytest.mark.asyncio
async def test_runtime_client_accepts_valid_pool_overrides():
    factory = _ClientFactory()
    client = RuntimeClient(
        "http://runtime.local",
        None,
        max_connections=8,
        max_keepalive_connections=3,
        keepalive_expiry=2.5,
        client_factory=factory,
    )

    await client.open()

    assert vars(factory.kwargs[0]["limits"]) == {
        "max_connections": 8,
        "max_keepalive_connections": 3,
        "keepalive_expiry": 2.5,
    }
    await client.close()


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"max_connections": 0}, "runtime_client_max_connections_invalid"),
        ({"max_connections": True}, "runtime_client_max_connections_invalid"),
        (
            {"max_keepalive_connections": -1},
            "runtime_client_max_keepalive_connections_invalid",
        ),
        (
            {"max_connections": 4, "max_keepalive_connections": 5},
            "runtime_client_max_keepalive_connections_invalid",
        ),
        ({"keepalive_expiry": 0}, "runtime_client_keepalive_expiry_invalid"),
        ({"keepalive_expiry": True}, "runtime_client_keepalive_expiry_invalid"),
    ],
)
def test_runtime_client_rejects_invalid_pool_bounds(overrides, error):
    with pytest.raises(ValueError, match=f"^{error}$"):
        RuntimeClient("http://runtime.local", None, **overrides)


@pytest.mark.asyncio
async def test_runtime_client_reuses_one_transport_for_sequential_operations():
    factory = _ClientFactory()
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    for ordinal in range(3):
        await client.overlay(
            request_id=f"sequential-{ordinal}",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )

    assert len(factory.clients) == 1
    assert len(factory.clients[0].posts) == 3
    await client.close()


@pytest.mark.asyncio
async def test_runtime_client_reuses_one_transport_without_serializing_requests():
    shared_client = _ConcurrentAsyncClient()
    factory = _ClientFactory([shared_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await asyncio.gather(client.open(), client.open())

    await asyncio.gather(
        *(
            client.overlay(
                request_id=f"concurrent-{ordinal}",
                owner_id="owner",
                conversation_id="conversation",
                surface="web",
            )
            for ordinal in range(8)
        )
    )

    assert len(factory.clients) == 1
    assert len(shared_client.posts) == 8
    assert shared_client.maximum_active_requests > 1
    await client.close()


@pytest.mark.asyncio
async def test_transport_failure_is_not_replayed_and_later_call_replaces_client():
    request = httpx.Request("POST", "http://runtime.local/v1/runtime/overlay")
    failure = httpx.ConnectError("connection failed", request=request)
    failed_client = _FakeAsyncClient([failure])
    replacement_client = _FakeAsyncClient([{"ok": True}])
    factory = _ClientFactory([failed_client, replacement_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    with pytest.raises(httpx.ConnectError) as exc:
        await client.overlay(
            request_id="failed-call",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )
    assert exc.value is failure
    assert len(failed_client.posts) == 1
    assert failed_client.close_calls == 1
    assert len(factory.clients) == 1
    await client.open()
    assert len(factory.clients) == 1

    result = await client.overlay(
        request_id="later-call",
        owner_id="owner",
        conversation_id="conversation",
        surface="web",
    )
    assert result == {"ok": True}
    assert len(factory.clients) == 2
    assert len(replacement_client.posts) == 1
    await client.close()


@pytest.mark.asyncio
async def test_concurrent_later_calls_share_one_replacement_transport():
    request = httpx.Request("POST", "http://runtime.local/v1/runtime/overlay")
    failed_client = _FakeAsyncClient(
        [httpx.ConnectError("connection failed", request=request)]
    )
    replacement_client = _ConcurrentAsyncClient()
    factory = _ClientFactory([failed_client, replacement_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    with pytest.raises(httpx.ConnectError):
        await client.overlay(
            request_id="failed-call",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )

    await asyncio.gather(
        *(
            client.overlay(
                request_id=f"replacement-{ordinal}",
                owner_id="owner",
                conversation_id="conversation",
                surface="web",
            )
            for ordinal in range(6)
        )
    )
    assert len(factory.clients) == 2
    assert len(replacement_client.posts) == 6
    assert replacement_client.maximum_active_requests > 1
    await client.close()


@pytest.mark.asyncio
async def test_transport_cleanup_does_not_mask_original_failure():
    class CloseFailingClient(_FakeAsyncClient):
        async def aclose(self) -> None:
            self.close_calls += 1
            raise RuntimeError("close_failed")

    request = httpx.Request("POST", "http://runtime.local/v1/runtime/overlay")
    failure = httpx.ReadTimeout("read timed out", request=request)
    failed_client = CloseFailingClient([failure])
    factory = _ClientFactory([failed_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    with pytest.raises(httpx.ReadTimeout) as exc:
        await client.overlay(
            request_id="failed-call",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )
    assert exc.value is failure
    assert failed_client.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [409, 503])
async def test_http_failure_does_not_retry_or_invalidate_transport(status_code):
    shared_client = _FakeAsyncClient(
        [
            (status_code, {"detail": "bounded"}),
            {"ok": True},
        ]
    )
    factory = _ClientFactory([shared_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    with pytest.raises(httpx.HTTPStatusError) as exc:
        await client.overlay(
            request_id="http-failure",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )
    assert exc.value.response.status_code == status_code
    assert len(shared_client.posts) == 1
    assert shared_client.close_calls == 0

    assert await client.overlay(
        request_id="later-call",
        owner_id="owner",
        conversation_id="conversation",
        surface="web",
    ) == {"ok": True}
    assert len(factory.clients) == 1
    await client.close()


@pytest.mark.asyncio
async def test_malformed_json_does_not_retry_or_invalidate_transport():
    shared_client = _FakeAsyncClient(
        [
            _FakeResponse(
                "/v1/runtime/overlay",
                ValueError("invalid_json"),
            ),
            {"ok": True},
        ]
    )
    factory = _ClientFactory([shared_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    with pytest.raises(ValueError, match="^invalid_json$"):
        await client.overlay(
            request_id="malformed-json",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )
    assert shared_client.close_calls == 0
    assert await client.overlay(
        request_id="later-call",
        owner_id="owner",
        conversation_id="conversation",
        surface="web",
    ) == {"ok": True}
    assert len(factory.clients) == 1
    await client.close()


@pytest.mark.asyncio
async def test_response_validator_failure_does_not_invalidate_transport():
    shared_client = _FakeAsyncClient([{"ok": True}, {"ok": True}])
    factory = _ClientFactory([shared_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    with pytest.raises(RuntimeError, match="^runtime_turn_response_invalid$"):
        await client.start_turn(
            request_id="invalid-start",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
            input_message_id=None,
        )
    assert shared_client.close_calls == 0
    assert await client.overlay(
        request_id="later-call",
        owner_id="owner",
        conversation_id="conversation",
        surface="web",
    ) == {"ok": True}
    assert len(factory.clients) == 1
    await client.close()


@pytest.mark.asyncio
async def test_cancellation_is_not_retried_or_converted():
    shared_client = _FakeAsyncClient([asyncio.CancelledError(), {"ok": True}])
    factory = _ClientFactory([shared_client])
    client = RuntimeClient(
        "http://runtime.local",
        None,
        client_factory=factory,
    )
    await client.open()

    with pytest.raises(asyncio.CancelledError):
        await client.overlay(
            request_id="cancelled",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )
    assert shared_client.close_calls == 0
    assert await client.overlay(
        request_id="later-call",
        owner_id="owner",
        conversation_id="conversation",
        surface="web",
    ) == {"ok": True}
    assert len(factory.clients) == 1
    await client.close()


@pytest.mark.asyncio
async def test_fastapi_lifespan_opens_and_closes_same_runtime_client(monkeypatch):
    main = _load_main(monkeypatch)

    class ManagedRuntime:
        def __init__(self) -> None:
            self.open_calls = 0
            self.close_calls = 0

        async def open(self) -> None:
            self.open_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

    configured = ManagedRuntime()
    replacement = ManagedRuntime()
    monkeypatch.setattr(main, "runtime", configured)

    async with main.app.router.lifespan_context(main.app):
        assert configured.open_calls == 1
        assert configured.close_calls == 0
        monkeypatch.setattr(main, "runtime", replacement)

    assert configured.close_calls == 1
    assert replacement.open_calls == 0
    assert replacement.close_calls == 0


@pytest.mark.asyncio
async def test_runtime_disabled_lifespan_does_not_manage_other_clients(monkeypatch):
    main = _load_main(monkeypatch)

    class UnexpectedLifecycle:
        async def open(self) -> None:
            raise AssertionError("unexpected open")

        async def close(self) -> None:
            raise AssertionError("unexpected close")

    monkeypatch.setattr(main, "runtime", None)
    monkeypatch.setattr(main, "memory_store", UnexpectedLifecycle())
    monkeypatch.setattr(main, "litellm", UnexpectedLifecycle())
    monkeypatch.setattr(main, "dsa", UnexpectedLifecycle())

    async with main.app.router.lifespan_context(main.app):
        pass


def _history_policy(**overrides):
    policy = {
        "status": "accepted",
        "intent": "support_explanation",
        "candidate_source": "deterministic",
        "target_mode": "immediate_previous",
        "explanation_kind": "support",
        "acquisition_question": None,
        "history_lookup_allowed": True,
        "new_verification_requested": False,
        "new_verification_allowed_after_history_resolution": False,
        "clarification_required": False,
        "confidence_band": "high",
        "reason_codes": ["deterministic_candidate_accepted"],
    }
    policy.update(overrides)
    return policy


def _status_error(path: str, status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", f"http://runtime.local{path}")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        f"status {status_code}",
        request=request,
        response=response,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "path", "specific"),
    [
        (
            "derive_evidence_shape",
            "/v1/runtime/evidence-shapes/derive",
            {
                "task_text": "Verify the record.",
                "interaction_kind": "question",
                "task_context": {
                    "evidence_input_kinds": [],
                    "external_verification_required": False,
                    "freshness_sensitive": False,
                    "high_stakes_accuracy_required": False,
                    "continuation_of_prior_evidence_task": False,
                    "prior_task_shape": None,
                },
            },
        ),
        (
            "compile_evidence_plan",
            "/v1/runtime/evidence-plans/compile",
            {
                "question_anchor": "Verify the record.",
                "task_shape": "targeted_lookup",
                "declared_scope": {
                    "source_ids": [],
                    "source_categories": [],
                    "inventory_status": "complete_for_declared_scope",
                },
                "source_inventory": [],
            },
        ),
        (
            "evaluate_evidence_sufficiency",
            "/v1/runtime/evidence-sufficiency/evaluate",
            {
                "evidence_plan_id": "evidence_plan_1",
                "acquisition_manifest_id": "evidence_manifest_1",
                "task_shape": "targeted_lookup",
                "declared_requirements": [
                    {
                        "requirement_id": "targeted-evidence",
                        "requirement_kind": "targeted_evidence",
                        "criticality": "material",
                    }
                ],
                "acquisition_facts": [
                    {
                        "requirement_id": "targeted-evidence",
                        "outcome": "satisfied",
                    }
                ],
            },
        ),
        (
            "select_evidence_next_step",
            "/v1/runtime/evidence-next-steps/select",
            {
                "evaluation_id": "evidence_eval_1",
                "evidence_plan_id": "evidence_plan_1",
                "acquisition_manifest_id": "evidence_manifest_1",
                "evaluated_requirements": [
                    {
                        "requirement_id": "targeted-evidence",
                        "requirement_kind": "targeted_evidence",
                        "criticality": "material",
                        "effective_outcome": "satisfied",
                    }
                ],
                "current_premise": {
                    "question_anchor_digest": f"sha256:{'a' * 64}",
                    "task_shape": "targeted_lookup",
                    "declared_scope": {
                        "source_ids": ["source_a"],
                        "source_categories": [],
                        "exact_source_refs": [],
                        "inventory_status": "complete_for_declared_scope",
                        "time_scope_ref": None,
                        "version_scope_ref": None,
                        "domain_scope_ref": None,
                        "project_scope_ref": None,
                    },
                    "source_inventory": [],
                    "selected_strategies": ["targeted_retrieval"],
                },
            },
        ),
    ],
)
async def test_evidence_runtime_methods_send_exact_scope_and_endpoint(
    method_name,
    path,
    specific,
):
    client = RuntimeClient("http://runtime.local", None)
    calls = []
    scope = {
        "request_id": "rid",
        "owner_id": "owner",
        "conversation_id": "conv",
        "surface": "dev",
        "runtime_session_id": "rtsession_1",
        "runtime_turn_id": "rtturn_1",
    }

    async def fake_post(called_path, *, json):
        calls.append((called_path, json))
        response = {**scope, "result": {}}
        if method_name == "evaluate_evidence_sufficiency":
            response.update(
                {
                    "evidence_plan_id": specific["evidence_plan_id"],
                    "acquisition_manifest_id": specific["acquisition_manifest_id"],
                }
            )
        if method_name == "select_evidence_next_step":
            response["result"] = {
                "evaluation_id": specific["evaluation_id"],
                "evidence_plan_id": specific["evidence_plan_id"],
                "acquisition_manifest_id": specific[
                    "acquisition_manifest_id"
                ],
            }
        return response

    client._post = fake_post  # type: ignore[method-assign]
    response = await getattr(client, method_name)(**scope, **specific)

    assert response["request_id"] == "rid"
    assert calls == [(path, {**scope, **specific})]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_error"),
    [
        ([], "evidence_shape_response_invalid"),
        (
            {
                "request_id": "rid",
                "owner_id": "other-owner",
                "conversation_id": "conv",
                "surface": "dev",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
            },
            "evidence_shape_response_invalid",
        ),
    ],
)
async def test_derive_evidence_shape_rejects_malformed_or_mismatched_scope(
    response,
    expected_error,
):
    client = RuntimeClient("http://runtime.local", None)

    async def fake_post(path, *, json):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match=expected_error):
        await client.derive_evidence_shape(
            request_id="rid",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
            runtime_session_id="rtsession_1",
            runtime_turn_id="rtturn_1",
            task_text="Verify the record.",
            interaction_kind="question",
            task_context={
                "evidence_input_kinds": [],
                "external_verification_required": False,
                "freshness_sensitive": False,
                "high_stakes_accuracy_required": False,
                "continuation_of_prior_evidence_task": False,
                "prior_task_shape": None,
            },
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "specific", "expected_error"),
    [
        (
            "compile_evidence_plan",
            {
                "question_anchor": "Verify the record.",
                "task_shape": "targeted_lookup",
                "declared_scope": {
                    "source_ids": [],
                    "source_categories": [],
                    "inventory_status": "complete_for_declared_scope",
                },
                "source_inventory": [],
            },
            "evidence_plan_response_invalid",
        ),
        (
            "evaluate_evidence_sufficiency",
            {
                "evidence_plan_id": "evidence_plan_1",
                "acquisition_manifest_id": "evidence_manifest_1",
                "task_shape": "targeted_lookup",
                "declared_requirements": [
                    {
                        "requirement_id": "targeted-evidence",
                        "requirement_kind": "targeted_evidence",
                        "criticality": "material",
                    }
                ],
                "acquisition_facts": [
                    {
                        "requirement_id": "targeted-evidence",
                        "outcome": "satisfied",
                    }
                ],
            },
            "evidence_sufficiency_response_invalid",
        ),
        (
            "select_evidence_next_step",
            {
                "evaluation_id": "evidence_eval_1",
                "evidence_plan_id": "evidence_plan_1",
                "acquisition_manifest_id": "evidence_manifest_1",
                "evaluated_requirements": [
                    {
                        "requirement_id": "targeted-evidence",
                        "requirement_kind": "targeted_evidence",
                        "criticality": "material",
                        "effective_outcome": "satisfied",
                    }
                ],
                "current_premise": {
                    "question_anchor_digest": f"sha256:{'a' * 64}",
                    "task_shape": "targeted_lookup",
                    "declared_scope": {
                        "source_ids": [],
                        "source_categories": [],
                        "exact_source_refs": [],
                        "inventory_status": "unknown",
                        "time_scope_ref": None,
                        "version_scope_ref": None,
                        "domain_scope_ref": None,
                        "project_scope_ref": None,
                    },
                    "source_inventory": [],
                    "selected_strategies": ["targeted_retrieval"],
                },
            },
            "evidence_next_step_response_invalid",
        ),
    ],
)
async def test_evidence_runtime_methods_reject_scope_mismatch(
    method_name,
    specific,
    expected_error,
):
    client = RuntimeClient("http://runtime.local", None)
    scope = {
        "request_id": "rid",
        "owner_id": "owner",
        "conversation_id": "conv",
        "surface": "dev",
        "runtime_session_id": "rtsession_1",
        "runtime_turn_id": "rtturn_1",
    }

    async def fake_post(path, *, json):
        response = {**scope, "owner_id": "other-owner", "result": {}}
        response["evidence_plan_id"] = specific.get("evidence_plan_id")
        response["acquisition_manifest_id"] = specific.get(
            "acquisition_manifest_id"
        )
        if method_name == "select_evidence_next_step":
            response["result"] = {
                "evaluation_id": specific["evaluation_id"],
                "evidence_plan_id": specific["evidence_plan_id"],
                "acquisition_manifest_id": specific[
                    "acquisition_manifest_id"
                ],
            }
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match=expected_error):
        await getattr(client, method_name)(**scope, **specific)


@pytest.mark.asyncio
async def test_select_evidence_next_step_sends_one_bounded_follow_up_input():
    client = RuntimeClient("http://runtime.local", None)
    calls = []
    scope = {
        "request_id": "rid",
        "owner_id": "owner",
        "conversation_id": "conv",
        "surface": "dev",
        "runtime_session_id": "rtsession_1",
        "runtime_turn_id": "rtturn_1",
    }
    premise = {
        "question_anchor_digest": f"sha256:{'a' * 64}",
        "task_shape": "targeted_lookup",
        "declared_scope": {
            "source_ids": ["source_a"],
            "source_categories": [],
            "exact_source_refs": [],
            "inventory_status": "complete_for_declared_scope",
            "time_scope_ref": None,
            "version_scope_ref": None,
            "domain_scope_ref": None,
            "project_scope_ref": None,
        },
        "source_inventory": [],
        "selected_strategies": ["targeted_retrieval"],
    }

    async def fake_post(path, *, json):
        calls.append((path, json))
        return {
            **scope,
            "result": {
                "evaluation_id": "evidence_eval_1",
                "evidence_plan_id": "evidence_plan_1",
                "acquisition_manifest_id": "evidence_manifest_1",
            },
        }

    client._post = fake_post  # type: ignore[method-assign]
    await client.select_evidence_next_step(
        **scope,
        evaluation_id="evidence_eval_1",
        evidence_plan_id="evidence_plan_1",
        acquisition_manifest_id="evidence_manifest_1",
        evaluated_requirements=[],
        current_premise=premise,
        clarification_target="source_scope",
    )

    assert calls == [
        (
            "/v1/runtime/evidence-next-steps/select",
            {
                **scope,
                "evaluation_id": "evidence_eval_1",
                "evidence_plan_id": "evidence_plan_1",
                "acquisition_manifest_id": "evidence_manifest_1",
                "evaluated_requirements": [],
                "current_premise": premise,
                "clarification_target": "source_scope",
            },
        )
    ]
    assert "proposed_acquisition_premise" not in calls[0][1]


@pytest.mark.asyncio
async def test_interaction_governance_sends_and_validates_history_candidate():
    client = RuntimeClient("http://runtime.local", None)
    calls = []
    candidate = {
        "source": "deterministic",
        "intent": "support_explanation",
        "confidence": 1.0,
        "target_mode": "immediate_previous",
        "new_verification_requested": False,
    }

    async def fake_post(path, *, json):
        calls.append((path, json))
        return {
            "request_id": "rid-history",
            "owner_id": "owner",
            "conversation_id": "conv",
            "surface": "dev",
            "runtime_session_id": "rtsession_1",
            "runtime_turn_id": "rtturn_1",
            "result": {"history_followup_policy": _history_policy()},
        }

    client._post = fake_post  # type: ignore[method-assign]
    await client.evaluate_interaction_governance(
        request_id="rid-history",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        current_user_text="How are you sure?",
        history_followup_candidate=candidate,
    )

    assert calls == [
        (
            "/v1/runtime/interaction-governance/evaluate",
            {
                "request_id": "rid-history",
                "owner_id": "owner",
                "conversation_id": "conv",
                "surface": "dev",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "current_user_text": "How are you sure?",
                "history_followup_candidate": candidate,
            },
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {
            "request_id": "rid-history",
            "owner_id": "wrong-owner",
            "conversation_id": "conv",
            "surface": "dev",
            "runtime_session_id": "rtsession_1",
            "runtime_turn_id": "rtturn_1",
            "result": {"history_followup_policy": _history_policy()},
        },
        {
            "request_id": "rid-history",
            "owner_id": "owner",
            "conversation_id": "conv",
            "surface": "dev",
            "runtime_session_id": "rtsession_1",
            "runtime_turn_id": "rtturn_1",
            "result": {
                "history_followup_policy": _history_policy(
                    record_id="forbidden-record"
                )
            },
        },
        {
            "request_id": "rid-history",
            "owner_id": "owner",
            "conversation_id": "conv",
            "surface": "dev",
            "runtime_session_id": "rtsession_1",
            "runtime_turn_id": "rtturn_1",
            "result": {
                "history_followup_policy": _history_policy(
                    history_lookup_allowed=False
                )
            },
        },
    ],
)
async def test_interaction_governance_rejects_mismatched_or_malformed_history_policy(
    response,
):
    client = RuntimeClient("http://runtime.local", None)

    async def fake_post(path, *, json):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="history_followup_policy_response"):
        await client.evaluate_interaction_governance(
            request_id="rid-history",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
            runtime_session_id="rtsession_1",
            runtime_turn_id="rtturn_1",
            current_user_text="How are you sure?",
            history_followup_candidate={
                "source": "deterministic",
                "intent": "support_explanation",
                "confidence": 1.0,
                "target_mode": "immediate_previous",
                "new_verification_requested": False,
            },
        )
@pytest.mark.asyncio
async def test_compile_companion_policy_prefers_profile_endpoint_then_falls_back_on_404():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[str] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append(path)
        if path == "/v1/companion/profile/compile":
            raise _status_error(path, 404)
        return {"overlays": []}

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.compile_companion_policy(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
    )

    assert calls == [
        "/v1/companion/profile/compile",
        "/v1/companion/policy/compile",
    ]
    assert client.last_companion_compile_endpoint == "/v1/companion/policy/compile"
    assert response["_cognitive_runtime_compile_endpoint"] == "/v1/companion/policy/compile"


@pytest.mark.asyncio
async def test_compile_companion_policy_falls_back_on_405():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[str] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append(path)
        if path == "/v1/companion/profile/compile":
            raise _status_error(path, 405)
        return {"overlays": []}

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.compile_companion_policy(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
    )

    assert calls == [
        "/v1/companion/profile/compile",
        "/v1/companion/policy/compile",
    ]
    assert client.last_companion_compile_endpoint == "/v1/companion/policy/compile"
    assert response["_cognitive_runtime_compile_endpoint"] == "/v1/companion/policy/compile"


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 422, 500])
async def test_compile_companion_policy_does_not_fall_back_on_other_statuses(status_code: int):
    client = RuntimeClient("http://runtime.local", None)
    calls: list[str] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append(path)
        raise _status_error(path, status_code)

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(httpx.HTTPStatusError):
        await client.compile_companion_policy(
            request_id="rid",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
        )

    assert calls == ["/v1/companion/profile/compile"]
    assert client.last_companion_compile_endpoint == "/v1/companion/profile/compile"


@pytest.mark.asyncio
async def test_compile_companion_policy_does_not_fall_back_on_timeout():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[str] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append(path)
        raise httpx.ReadTimeout("timed out")

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(httpx.ReadTimeout):
        await client.compile_companion_policy(
            request_id="rid",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
        )

    assert calls == ["/v1/companion/profile/compile"]
    assert client.last_companion_compile_endpoint == "/v1/companion/profile/compile"


@pytest.mark.asyncio
async def test_compile_companion_policy_does_not_fall_back_on_connection_failure():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[str] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append(path)
        raise httpx.ConnectError("offline")

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(httpx.ConnectError):
        await client.compile_companion_policy(
            request_id="rid",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
        )

    assert calls == ["/v1/companion/profile/compile"]
    assert client.last_companion_compile_endpoint == "/v1/companion/profile/compile"


@pytest.mark.asyncio
async def test_runtime_identity_and_turn_methods_use_expected_endpoints():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        if path == "/v1/runtime/turns/start":
            return {
                "runtime_session": {
                    "runtime_session_id": "rtsession_1",
                    "owner_id": json["owner_id"],
                    "conversation_id": json["conversation_id"],
                    "surface": json["surface"],
                },
                "runtime_turn": {
                    "runtime_turn_id": "rtturn_1",
                    "runtime_session_id": "rtsession_1",
                    "input_message_id": json.get("input_message_id"),
                    "turn_status": "received",
                },
                "event": {
                    "runtime_session_id": "rtsession_1",
                    "runtime_turn_id": "rtturn_1",
                    "event_type": "turn_started",
                },
            }
        if path == "/v1/runtime/privacy-context/evaluate":
            return {
                "result": {
                    "privacy_zone": "private",
                    "surface_type": "desktop_private",
                    "sensitivity_level": "sensitive",
                    "sensitive_detail_allowed": True,
                    "notification_detail_allowed": False,
                    "voice_detail_allowed": False,
                    "screen_detail_allowed": True,
                    "redaction_required": False,
                    "safe_summary_required": False,
                    "reason_codes": ["private_surface"],
                }
            }
        return {"ok": True}

    client._post = fake_post  # type: ignore[method-assign]

    await client.resolve_session(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
    )
    await client.start_turn(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        input_message_id="m-1",
    )
    await client.update_turn(
        request_id="rid",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        turn_status="retrieving",
    )
    await client.complete_turn(
        request_id="rid",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        turn_status="completed",
    )
    await client.resolve_identity(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
    )
    await client.world_state_resolve(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        active_persona_id="technical_architect",
    )
    await client.relationship_select(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        active_persona_id="technical_architect",
    )
    await client.evaluate_interaction_governance(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        surface_session_id="surface-session-1",
        active_mode="focused",
        current_user_text="rename this variable to count",
        recent_messages=[
            {"role": "assistant", "content": "prior"},
            {"role": "user", "content": "rename this variable to count"},
        ],
        surface_metadata_json={"surface_type": "developer_surface"},
    )
    await client.evaluate_persona_containment(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        persona_scope_hint="technical_architect",
        interaction_kind="question",
        current_user_text="review this module",
        recent_messages=[
            {"role": "assistant", "content": "prior"},
            {"role": "user", "content": "review this module"},
        ],
        surface_metadata_json={"surface_type": "developer_surface"},
    )
    await client.evaluate_restraint(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        interaction_kind="question",
        response_posture="direct",
        active_persona_id="technical_architect",
        capability_domain="technical",
        current_user_text="give me the prompt",
        recent_messages=[
            {"role": "assistant", "content": "prior"},
            {"role": "user", "content": "give me the prompt"},
        ],
        surface_metadata_json={"surface_type": "developer_surface"},
    )
    await client.evaluate_memory_hygiene(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        items=[
            {
                "item_ref": {"ref_type": "message", "ref_id": "msg-1"},
                "memory_id": "memory-1",
                "freshness_state": "parked",
                "last_verified_at": "2026-01-01T00:00:00Z",
                "source_kind": "message",
                "confidence": 0.8,
                "supersedes": "memory-0",
                "superseded_by": None,
            }
        ],
    )
    await client.evaluate_privacy_context(
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        surface_category="desktop_private",
        sensitivity_level="sensitive",
        sensitivity_domains=["personal", "financial"],
    )

    assert [path for path, _ in calls] == [
        "/v1/runtime/sessions/resolve",
        "/v1/runtime/turns/start",
        "/v1/runtime/turns/update",
        "/v1/runtime/turns/complete",
        "/v1/runtime/identity/resolve",
        "/v1/world-state/resolve",
        "/v1/relationships/select",
        "/v1/runtime/interaction-governance/evaluate",
        "/v1/runtime/persona-containment/evaluate",
        "/v1/runtime/restraint/evaluate",
        "/v1/runtime/memory-hygiene/evaluate",
        "/v1/runtime/privacy-context/evaluate",
    ]
    assert calls[5][1]["active_persona_id"] == "technical_architect"
    assert calls[-5][1]["runtime_session_id"] == "rtsession_1"
    assert calls[-5][1]["runtime_turn_id"] == "rtturn_1"
    assert calls[-5][1]["surface_session_id"] == "surface-session-1"
    assert calls[-5][1]["active_mode"] == "focused"
    assert calls[-5][1]["recent_messages"][1]["content"] == "rename this variable to count"
    assert calls[-5][1]["surface_metadata_json"] == {"surface_type": "developer_surface"}
    assert calls[-4][1]["persona_scope_hint"] == "technical_architect"
    assert calls[-4][1]["interaction_kind"] == "question"
    assert calls[-4][1]["runtime_turn_id"] == "rtturn_1"
    assert calls[-3][1]["response_posture"] == "direct"
    assert calls[-3][1]["active_persona_id"] == "technical_architect"
    assert calls[-3][1]["capability_domain"] == "technical"
    assert calls[-2][1]["runtime_turn_id"] == "rtturn_1"
    assert calls[-2][1]["items"][0]["item_ref"] == {"ref_type": "message", "ref_id": "msg-1"}
    assert "content" not in calls[-2][1]["items"][0]
    assert calls[-1][1]["surface_category"] == "desktop_private"
    assert calls[-1][1]["sensitivity_level"] == "sensitive"
    assert calls[-1][1]["sensitivity_domains"] == ["personal", "financial"]
    assert "current_user_text" not in calls[-1][1]


@pytest.mark.asyncio
async def test_evaluate_privacy_context_rejects_malformed_boolean_fields():
    client = RuntimeClient("http://runtime.local", None)

    async def fake_post(path: str, *, json: dict[str, object]):
        return {
            "result": {
                "privacy_zone": "private",
                "surface_type": "desktop_private",
                "sensitivity_level": "normal",
                "sensitive_detail_allowed": "true",
                "notification_detail_allowed": False,
                "voice_detail_allowed": False,
                "screen_detail_allowed": True,
                "redaction_required": False,
                "safe_summary_required": False,
                "reason_codes": ["private_surface"],
            }
        }

    client._post = fake_post  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        await client.evaluate_privacy_context(
            request_id="rid",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
            sensitivity_level="normal",
            sensitivity_domains=[],
        )


@pytest.mark.asyncio
async def test_evaluate_privacy_context_rejects_invalid_enums():
    client = RuntimeClient("http://runtime.local", None)

    async def fake_post(path: str, *, json: dict[str, object]):
        return {
            "result": {
                "privacy_zone": "private",
                "surface_type": "developer_surface",
                "sensitivity_level": "normal",
                "sensitive_detail_allowed": True,
                "notification_detail_allowed": False,
                "voice_detail_allowed": False,
                "screen_detail_allowed": True,
                "redaction_required": False,
                "safe_summary_required": False,
                "reason_codes": ["private_surface"],
            }
        }

    client._post = fake_post  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        await client.evaluate_privacy_context(
            request_id="rid",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
            sensitivity_level="normal",
            sensitivity_domains=[],
        )


@pytest.mark.asyncio
async def test_authorize_capability_posts_expected_exposure_payload():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"result": {"allowed": True}}

    client._post = fake_post  # type: ignore[method-assign]

    await client.authorize_capability(
        request_id="rid:cap:exposure",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
        authorization_phase="exposure",
        capability_id="runtime.world_state.read",
        capability_domain="software_architecture",
        operation_class="read",
        supported_surfaces=["dev", "vscode"],
    )

    assert calls == [
        (
            "/v1/capabilities/authorize",
            {
                "request_id": "rid:cap:exposure",
                "owner_id": "owner",
                "conversation_id": "conv",
                "surface": "dev",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "active_persona_id": "technical_architect",
                "authorization_phase": "exposure",
                "capability_id": "runtime.world_state.read",
                "capability_domain": "software_architecture",
                "operation_class": "read",
                "argument_digest": None,
                "supported_surfaces": ["dev", "vscode"],
                "relationship_requirements": [],
                "selected_relationship_ids": [],
                "world_state_requirements": [],
                "selected_world_state_claim_ids": [],
                "confirmation_challenge_ref": None,
            },
        )
    ]


@pytest.mark.asyncio
async def test_action_authority_posts_expected_bounded_payload():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"result": {"authority_level": "execute_low_risk", "action_taken": False}}

    client._post = fake_post  # type: ignore[method-assign]

    await client.action_authority(
        request_id="rid:cap:authority",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
        capability_id="office_lights_on",
        target_resolution_state="resolved",
        world_state_freshness="unknown",
        consequence_flags={"external_consequence": False},
        interaction_governance_kind="command",
        interaction_governance_tension="low",
        user_authorization_signal="explicit",
    )

    assert calls == [
        (
            "/v1/capabilities/authority",
            {
                "request_id": "rid:cap:authority",
                "owner_id": "owner",
                "conversation_id": "conv",
                "surface": "dev",
                "active_persona_id": "technical_architect",
                "capability_id": "office_lights_on",
                "target_resolution_state": "resolved",
                "world_state_freshness": "unknown",
                "consequence_flags": {"external_consequence": False},
                "user_authorization_signal": "explicit",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "interaction_governance_kind": "command",
                "interaction_governance_tension": "low",
            },
        )
    ]


@pytest.mark.asyncio
async def test_action_flow_posts_expected_bounded_payload():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"result": {"execution_allowed": False, "action_taken": False}}

    client._post = fake_post  # type: ignore[method-assign]

    await client.action_flow(
        request_id="rid:cap:flow",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
        capability_id="office_lights_on",
        flow_intent="preview_requested",
        target_resolution_state="resolved",
        target_label="office lights",
        world_state_freshness="unknown",
        affects_multiple_systems=False,
        consequence_flags={"external_consequence": False},
        interaction_governance_kind="command",
        interaction_governance_tension="low",
        user_authorization_signal="explicit",
    )

    assert calls == [
        (
            "/v1/capabilities/flow",
            {
                "request_id": "rid:cap:flow",
                "owner_id": "owner",
                "conversation_id": "conv",
                "surface": "dev",
                "active_persona_id": "technical_architect",
                "capability_id": "office_lights_on",
                "flow_intent": "preview_requested",
                "target_resolution_state": "resolved",
                "world_state_freshness": "unknown",
                "affects_multiple_systems": False,
                "consequence_flags": {"external_consequence": False},
                "user_authorization_signal": "explicit",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "target_label": "office lights",
                "interaction_governance_kind": "command",
                "interaction_governance_tension": "low",
            },
        )
    ]


@pytest.mark.asyncio
async def test_action_summary_posts_exact_bounded_payload():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"result": {"action_id": "act_123"}}

    client._post = fake_post  # type: ignore[method-assign]

    await client.action_summary(
        request_id="rid:cap:summary",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        capability_id="runtime.world_state.read",
        active_persona_id="technical_architect",
        risk_level="read_only",
        authority_level="answer_only",
        confirmation_status="not_required",
        policy_reason_codes=["registered_capability", "execution_allowed_by_policy"],
        execution_status="executed",
        execution_reason_code="adapter_completed",
        verification_status="failed",
        verification_reason_code="result_check_failed",
        degradation_reason="result_check_failed",
    )

    assert calls == [
        (
            "/v1/capabilities/action-summary",
            {
                "request_id": "rid:cap:summary",
                "owner_id": "owner",
                "conversation_id": "conv",
                "surface": "dev",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "capability_id": "runtime.world_state.read",
                "active_persona_id": "technical_architect",
                "risk_level": "read_only",
                "authority_level": "answer_only",
                "confirmation_status": "not_required",
                "policy_reason_codes": [
                    "registered_capability",
                    "execution_allowed_by_policy",
                ],
                "execution_status": "executed",
                "execution_reason_code": "adapter_completed",
                "verification_status": "failed",
                "verification_reason_code": "result_check_failed",
                "degradation_reason": "result_check_failed",
            },
        )
    ]


@pytest.mark.asyncio
async def test_world_state_claim_verify_posts_expected_structural_payload():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"claim": {"world_state_claim_id": json["world_state_claim_id"]}}

    client._post = fake_post  # type: ignore[method-assign]

    await client.world_state_claim_verify(
        request_id="rid:verify",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        world_state_claim_id="claim-1",
        expected_value_digest="wsvalue_claim-1",
        verifier_id="cr-verifier-local",
        verification_source_type="tool_output",
        verification_source_ref="local-deterministic-revalidator",
        observed_at="2026-07-06T00:00:00+00:00",
        verified_at="2026-07-06T00:00:01+00:00",
        resulting_authority="verified_tool_output",
        resulting_confidence=0.9,
        resulting_freshness_state="fresh",
        resulting_ttl_seconds=300,
        resulting_revalidation_interval_seconds=120,
    )

    assert calls == [
        (
            "/v1/world-state/claims/verify",
            {
                "request_id": "rid:verify",
                "owner_id": "owner",
                "conversation_id": "conv",
                "surface": "dev",
                "world_state_claim_id": "claim-1",
                "expected_value_digest": "wsvalue_claim-1",
                "verification_source_type": "tool_output",
                "verification_source_ref": "local-deterministic-revalidator",
                "observed_at": "2026-07-06T00:00:00+00:00",
                "verified_at": "2026-07-06T00:00:01+00:00",
                "resulting_authority": "verified_tool_output",
                "resulting_confidence": 0.9,
                "resulting_freshness_state": "fresh",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "verifier_id": "cr-verifier-local",
                "resulting_ttl_seconds": 300,
                "resulting_revalidation_interval_seconds": 120,
            },
        )
    ]


@pytest.mark.asyncio
async def test_confirm_capability_posts_expected_structural_payload():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {
            "confirmation_challenge_ref": json["confirmation_challenge_ref"],
            "confirmation_state": "accepted",
        }

    client._post = fake_post  # type: ignore[method-assign]

    await client.confirm_capability(
        request_id="rid:confirm",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        confirmation_challenge_ref="challenge-1",
        capability_id="draft.local_message",
        operation_class="draft",
        argument_digest="capargs_123",
        confirmed=True,
    )

    assert calls == [
        (
            "/v1/capabilities/confirm",
            {
                "request_id": "rid:confirm",
                "owner_id": "owner",
                "conversation_id": "conv",
                "surface": "dev",
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "confirmation_challenge_ref": "challenge-1",
                "capability_id": "draft.local_message",
                "operation_class": "draft",
                "argument_digest": "capargs_123",
                "confirmed": True,
            },
        )
    ]
