import asyncio
import importlib
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest


@pytest.fixture
def work_reads(monkeypatch):
    from test_memory_store_client import result_projection

    main = _load_main(monkeypatch)
    value = result_projection()

    async def exact(**identity):
        if any(value["work"][key] != item for key, item in identity.items()):
            return None
        return deepcopy(value)

    async def current(**identity):
        if any(value["work"][key] != item for key, item in identity.items()):
            return {"status": "none", "work": None}
        return {"status": "resolved", "work": deepcopy(value["work"])}

    forbidden = [AsyncMock(side_effect=AssertionError("polling side effect")) for _ in range(5)]
    memory = SimpleNamespace(get_work_result=AsyncMock(side_effect=exact),
                             get_current_work=AsyncMock(side_effect=current))
    for method in ("create_work", "transition_work", "set_current_work", "add_message",
                   "create_trace", "retrieve"):
        setattr(memory, method, forbidden[4])
    monkeypatch.setattr(main, "memory_store", memory)
    monkeypatch.setattr(main, "orchestrate_chat", forbidden[0])
    monkeypatch.setattr(main, "litellm", SimpleNamespace(chat=forbidden[1]))
    monkeypatch.setattr(main, "dsa", SimpleNamespace(context_pack=forbidden[2]))
    monkeypatch.setattr(main, "runtime", SimpleNamespace(start_turn=forbidden[3]))
    return main, value, memory, forbidden


async def _poll_work(main, work, *, current=False, headers=None, **overrides):
    params = {"owner_id": work["owner_id"]}
    params.update({"client_id": work["client_id"]} if current else {
        "conversation_id": work["conversation_id"],
    })
    work_id = overrides.pop("work_id", work["work_id"])
    params.update(overrides)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                 base_url="http://test") as client:
        return await client.get(
            "/v1/current-work" if current else f"/v1/work-items/{work_id}", params=params,
            headers={"X-API-Key": "orch-test"} if headers is None else headers,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["pending", "running", "completed", "failed"])
async def test_work_polling_exact_current_private_and_zero_cognition(work_reads, state):
    from test_memory_store_client import result_projection

    main, value, memory, forbidden = work_reads
    value.update(result_projection(state))
    expected = {key: value["work"][key] for key in (
        "work_id", "conversation_id", "request_id", "state", "failure_code",
    )}
    expected["result"] = None if value["result"] is None else {
        "assistant_message_id": value["result"]["assistant_message_id"],
        "answer": value["result"]["content"],
    }
    before = deepcopy(value)
    for _ in range(3):
        for current in (False, True):
            response = await _poll_work(main, value["work"], current=current)
            assert response.status_code == 200
            assert response.json() == ({"status": "resolved", "work": expected}
                                       if current else expected)
    assert value == before
    assert memory.get_work_result.await_count == 6
    assert memory.get_current_work.await_count == 3
    for call in memory.get_work_result.call_args_list:
        assert call.kwargs == {key: value["work"][key] for key in (
            "work_id", "owner_id", "conversation_id",
        )}
    for counter in forbidden:
        counter.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["owner_id", "conversation_id", "work_id"])
async def test_exact_work_wrong_identity_is_indistinguishable(work_reads, field):
    main, value, memory, _ = work_reads
    wrong = "other" if field == "owner_id" else "00000000-0000-4000-8000-000000000099"
    response = await _poll_work(main, value["work"], **{field: wrong})
    assert response.status_code == 404
    assert response.json() == {"detail": "work_not_found"}
    memory.get_current_work.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["owner_id", "client_id", "missing"])
async def test_current_work_has_no_recency_fallback(work_reads, field):
    main, value, memory, _ = work_reads
    overrides = {} if field == "missing" else {field: "other"}
    if field == "missing":
        memory.get_current_work.side_effect = None
        memory.get_current_work.return_value = {"status": "none", "work": None}
    response = await _poll_work(main, value["work"], current=True, **overrides)
    assert response.status_code == 200
    assert response.json() == {"status": "none", "work": None}
    memory.get_work_result.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("current", [False, True])
@pytest.mark.parametrize("failure", ["dependency", "extra", "provisional", "reference"])
async def test_work_polling_failures_are_bounded(work_reads, current, failure):
    main, value, memory, _ = work_reads
    if failure == "dependency":
        memory.get_work_result.side_effect = RuntimeError("private URL SQL sentinel")
    elif failure == "extra":
        value["result"]["metadata"] = "private sentinel"
    elif failure == "provisional":
        from test_memory_store_client import projection
        value["work"] = projection("running")
    else:
        value["result"]["assistant_message_id"] = value["work"]["work_id"]
    response = await _poll_work(main, value["work"], current=current)
    assert response.status_code == 503
    assert response.json() == {"detail": "work_unavailable"}


@pytest.mark.asyncio
@pytest.mark.parametrize("field", [
    "work_id", "owner_id", "conversation_id", "request_id", "client_id", "surface",
    "created_at", "disappeared", "locator_malformed", "locator_unavailable",
])
async def test_current_result_immutable_association_is_revalidated(work_reads, field):
    main, value, memory, _ = work_reads
    locator = {"status": "resolved", "work": deepcopy(value["work"])}
    memory.get_current_work.side_effect = None
    memory.get_current_work.return_value = locator
    memory.get_work_result.side_effect = None
    memory.get_work_result.return_value = value
    if field == "disappeared":
        memory.get_work_result.return_value = None
    elif field == "locator_malformed":
        locator["private"] = "sentinel"
    elif field == "locator_unavailable":
        memory.get_current_work.side_effect = RuntimeError("private sentinel")
    else:
        value["work"][field] = (
            "00000000-0000-4000-8000-000000000099" if field.endswith("_id")
            else "2026-09-20T00:00:00Z" if field == "created_at" else "other"
        )
    response = await _poll_work(main, locator["work"], current=True)
    assert response.status_code == 503
    assert response.json() == {"detail": "work_unavailable"}


@pytest.mark.asyncio
async def test_current_result_allows_lifecycle_advance(work_reads):
    from test_memory_store_client import projection
    main, value, memory, _ = work_reads
    memory.get_current_work.side_effect = None
    memory.get_current_work.return_value = {"status": "resolved", "work": projection("running")}
    response = await _poll_work(main, value["work"], current=True)
    assert response.status_code == 200
    assert response.json()["work"]["state"] == "completed"


@pytest.mark.asyncio
async def test_exact_work_can_be_read_from_another_surface_without_rewriting_origin(work_reads):
    main, value, memory, _ = work_reads
    before = deepcopy(value)
    response = await _poll_work(main, value["work"], headers={
        "X-API-Key": "orch-test", "X-Client-ID": "another-client", "X-Surface": "another-surface",
    })
    assert response.status_code == 200
    assert response.json()["result"]["answer"] == value["result"]["content"]
    assert value == before
    memory.get_current_work.assert_not_called()


@pytest.mark.parametrize("change", ["extra", "completed_null", "pending_result", "failure",
                                        "bad_uuid", "request_id", "result_extra"])
def test_public_work_models_enforce_strict_shape_and_lifecycle(change):
    from models import CurrentWorkStatus, WorkStatus
    from pydantic import ValidationError
    from test_memory_store_client import result_projection

    source = result_projection()
    value = {key: source["work"][key] for key in (
        "work_id", "conversation_id", "request_id", "state", "failure_code",
    )}
    value["result"] = {"assistant_message_id": source["result"]["assistant_message_id"],
                       "answer": source["result"]["content"]}
    if change == "extra":
        value["owner_id"] = "private-owner"
    elif change == "completed_null":
        value["result"] = None
    elif change == "pending_result":
        value["state"] = "pending"
    elif change == "failure":
        value["failure_code"] = "interrupted"
    elif change == "bad_uuid":
        value["result"]["assistant_message_id"] = "not-a-uuid"
    elif change == "request_id":
        value["request_id"] = "x" * 121
    else:
        value["result"]["sources"] = []
    with pytest.raises(ValidationError):
        WorkStatus.model_validate(value)
    for current in ({"status": "resolved", "work": None},
                    {"status": "none", "work": None, "private": "sentinel"}):
        with pytest.raises(ValidationError):
            CurrentWorkStatus.model_validate(current)


@pytest.mark.asyncio
@pytest.mark.parametrize("current", [False, True])
async def test_work_polling_requires_service_auth_and_valid_scope(work_reads, current):
    main, value, memory, _ = work_reads
    response = await _poll_work(main, value["work"], current=current, headers={})
    assert response.status_code == 401
    for overrides in ({"owner_id": ""}, {"client_id" if current else "conversation_id": ""}):
        response = await _poll_work(main, value["work"], current=current, **overrides)
        assert response.status_code == 422
    memory.get_work_result.assert_not_called()
    memory.get_current_work.assert_not_called()


@pytest.mark.asyncio
async def test_same_deferred_task_result_is_retrieved_without_recomputation(
    delivery_flow, monkeypatch,
):
    main, memory, provider, _, release, _, _ = delivery_flow
    invocation = AsyncMock(wraps=main.orchestrate_chat)
    monkeypatch.setattr(main, "orchestrate_chat", invocation)

    async def exact(**identity):
        assert identity == {key: memory.work[key] for key in (
            "work_id", "owner_id", "conversation_id",
        )}
        assistant = next(m for m in memory.added_messages if m["role"] == "assistant")
        return {"work": dict(memory.work), "result": {
            "assistant_message_id": memory.work["assistant_message_id"],
            "content": assistant["content"],
        }}

    monkeypatch.setattr(memory, "get_work_result", exact, raising=False)
    response = await _delivery_post(main)
    try:
        assert response.status_code == 202
        task, = main._owned_chat_tasks
    finally:
        release.set()
        completed = await asyncio.gather(*main._owned_chat_tasks)
    canonical = completed[0]
    assert task.done() and not main._owned_chat_tasks
    for _ in range(3):
        result = await _poll_work(main, memory.work)
        assert result.status_code == 200
        result = result.json()
        for key in ("work_id", "conversation_id", "request_id"):
            assert result[key] == response.json()[key]
        assert result["result"]["answer"] == canonical["answer"]
    assert invocation.await_count == 1 and len(provider.calls) == 1
    assert [c[0] for c in memory.work_calls] == ["create", "running", "completed"]
    assert [m["role"] for m in memory.added_messages] == ["user", "assistant"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "cr", "bms"])
async def test_startup_reconciliation_order_and_fail_closed(monkeypatch, failure):
    main = _load_main(monkeypatch)
    events = []

    class Runtime:
        async def open(self):
            events.append("open")

        async def reconcile_interrupted_turns(self, request_id):
            assert str(UUID(request_id)) == request_id
            events.append("cr")
            if failure == "cr":
                raise RuntimeError("runtime_reconciliation_response_invalid")
            return {"interrupted_count": 1}

        async def close(self):
            events.append("close")

    class Memory:
        async def reconcile_interrupted_work(self):
            events.append("bms")
            if failure == "bms":
                raise RuntimeError("work_reconciliation_response_invalid")
            return {"interrupted_count": 1}

    monkeypatch.setattr(main, "runtime", Runtime())
    monkeypatch.setattr(main, "memory_store", Memory())
    if failure:
        with pytest.raises(RuntimeError, match="reconciliation_response_invalid"):
            async with main.lifespan(main.app):
                pytest.fail("startup admitted traffic")
        assert events == (["open", "cr", "close"] if failure == "cr"
                          else ["open", "cr", "bms", "close"])
    else:
        started = asyncio.Event()

        async def owned():
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                events.append("cleanup")

        async with main.lifespan(main.app):
            events.append("yield")
            task = asyncio.create_task(owned())
            main._owned_chat_tasks.add(task)
            task.add_done_callback(main._chat_task_done)
            await started.wait()
        assert events == ["open", "cr", "bms", "yield", "cleanup", "close"]
        assert not main._owned_chat_tasks


@pytest.mark.asyncio
async def test_startup_without_runtime_still_reconciles_work(monkeypatch):
    main = _load_main(monkeypatch)
    events = []

    class Memory:
        async def reconcile_interrupted_work(self):
            events.append("bms")
            return {"interrupted_count": 0}

    monkeypatch.setattr(main, "runtime", None)
    monkeypatch.setattr(main, "memory_store", Memory())
    async with main.lifespan(main.app):
        assert events == ["bms"]


@pytest.mark.parametrize("fields", [
    {"allow_deferred": True}, {"delivery_wait_ms": 100},
    {"allow_deferred": True, "delivery_wait_ms": 99},
    {"allow_deferred": True, "delivery_wait_ms": 30001},
    {"allow_deferred": True, "delivery_wait_ms": True},
    {"allow_deferred": True, "delivery_wait_ms": 100.0},
])
def test_delivery_request_rejects_invalid_controls(fields):
    from models import ChatRequest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ChatRequest(**_full_chat_payload(**fields))


@pytest.fixture
def delivery_flow(monkeypatch, tmp_path):
    from test_orchestrate_flow import (
        DurableWorkMemoryStore,
        FakeLiteLLM,
        FakeRuntime,
        _write_router_files,
    )

    main = _load_main(monkeypatch)
    rules, models = _write_router_files(tmp_path)
    memory = DurableWorkMemoryStore()
    entered, release = asyncio.Event(), asyncio.Event()
    locator_calls = []
    locator_failed = asyncio.Event()

    class Provider(FakeLiteLLM):
        cancelled = False

        async def chat(self, **kwargs):
            assert memory.work["state"] == "running"
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            return await super().chat(**kwargs)

    async def set_locator(**association):
        locator_calls.append(association)
        if locator_failed.is_set():
            raise RuntimeError("locator unavailable")
        return {"status": "resolved", "work": dict(memory.work)}

    provider = Provider(content="Canonical delivery answer.")
    monkeypatch.setattr(memory, "set_current_work", set_locator)
    monkeypatch.setattr(main, "memory_store", memory)
    monkeypatch.setattr(main, "litellm", provider)
    monkeypatch.setattr(main, "runtime", FakeRuntime())
    monkeypatch.setattr(main.settings, "cognitive_runtime_capability_registry_enabled", False)
    monkeypatch.setattr(main.settings, "router_rules_path", str(rules))
    monkeypatch.setattr(main.settings, "model_registry_path", str(models))
    return main, memory, provider, entered, release, locator_calls, locator_failed


async def _delivery_post(main, **overrides):
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.post(
            "/v1/chat", headers={"X-API-Key": "orch-test"},
            json=_full_chat_payload(**{
                "messages": [{"role": "user", "content": "Hello."}],
                "allow_deferred": True, "delivery_wait_ms": 100, **overrides,
            }),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("client_id", ["node-red", None])
async def test_slow_delivery_preserves_one_canonical_execution(delivery_flow, client_id):
    main, memory, provider, entered, release, locators, _ = delivery_flow
    response = await _delivery_post(main, client_id=client_id)
    try:
        assert response.status_code == 202
        result = response.json()
        assert result == {
            "request_id": memory.work["request_id"],
            "conversation_id": memory.work["conversation_id"],
            "work_id": memory.work["work_id"], "delivery_status": "pending",
        }
        assert entered.is_set() and not provider.cancelled
        assert memory.work["state"] == "running"
        assert [m["role"] for m in memory.added_messages] == ["user"]
        assert len(main._owned_chat_tasks) == 1
        assert locators == ([] if client_id is None else [{
            "owner_id": "owner", "client_id": client_id, "work_id": result["work_id"],
        }])
    finally:
        release.set()
        await asyncio.gather(*main._owned_chat_tasks)
    assert not main._owned_chat_tasks
    assert memory.work["state"] == "completed"
    assert memory.work["assistant_message_id"] == "00000000-0000-4000-8000-000000000002"
    assert [c[0] for c in memory.work_calls] == ["create", "running", "completed"]
    assert [m["role"] for m in memory.added_messages] == ["user", "assistant"]
    assert len(provider.calls) == 1


@pytest.mark.asyncio
async def test_fast_delivery_is_unchanged_and_controls_never_enter_cognition(
    delivery_flow, monkeypatch,
):
    main, memory, provider, _, release, locators, _ = delivery_flow
    original = main.orchestrate_chat
    payloads = []

    async def observed(**kwargs):
        payloads.append(kwargs["payload"])
        return await original(**kwargs)

    monkeypatch.setattr(main, "orchestrate_chat", observed)
    release.set()
    response = await _delivery_post(main, delivery_wait_ms=30000)
    assert response.status_code == 200
    assert set(response.json()) == {
        "request_id", "conversation_id", "profile_name", "selected_model",
        "answer", "status", "sources",
    }
    assert response.json()["answer"] == "Canonical delivery answer."
    assert len(payloads) == len(provider.calls) == 1
    assert not {"allow_deferred", "delivery_wait_ms"} & payloads[0].keys()
    assert memory.work["state"] == "completed" and not locators


@pytest.mark.asyncio
async def test_delivery_waiter_cancellation_does_not_cancel_cognition(delivery_flow):
    main, memory, provider, entered, release, locators, _ = delivery_flow
    waiter = asyncio.create_task(_delivery_post(main, delivery_wait_ms=30000))
    await entered.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert len(main._owned_chat_tasks) == 1
    assert not provider.cancelled and not locators
    release.set()
    await asyncio.gather(*main._owned_chat_tasks)
    assert not main._owned_chat_tasks
    assert memory.work["state"] == "completed" and len(provider.calls) == 1


@pytest.mark.asyncio
async def test_owned_shutdown_cancellation_runs_existing_work_cleanup(delivery_flow):
    main, memory, provider, _, _, _, _ = delivery_flow
    response = await _delivery_post(main)
    assert response.status_code == 202
    task, = main._owned_chat_tasks
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert provider.cancelled and not main._owned_chat_tasks
    assert memory.work["state"] == "failed"
    assert memory.work["failure_code"] == "execution_failed"
    assert [m["role"] for m in memory.added_messages] == ["user"]


@pytest.mark.asyncio
async def test_detached_exception_is_consumed_without_private_logging(delivery_flow, caplog):
    main, memory, provider, _, release, _, _ = delivery_flow
    memory.fail_at = "trace"
    response = await _delivery_post(main)
    assert response.status_code == 202
    tasks = tuple(main._owned_chat_tasks)
    release.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert not main._owned_chat_tasks and len(provider.calls) == 1
    assert memory.work["state"] == "failed"
    assert "owned_chat_execution_failed" in caplog.text
    assert "Canonical delivery answer" not in caplog.text
    assert "failed:trace" not in caplog.text


@pytest.mark.asyncio
async def test_locator_failure_continues_same_execution_synchronously(delivery_flow):
    main, memory, provider, entered, release, locators, fail_locator = delivery_flow
    fail_locator.set()
    waiter = asyncio.create_task(_delivery_post(main))
    await entered.wait()
    try:
        # Wait beyond the delivery deadline, without releasing the provider.
        await asyncio.sleep(0.15)
        assert len(locators) == 1 and not waiter.done()
        assert len(main._owned_chat_tasks) == 1 and not provider.cancelled
    finally:
        release.set()
    response = await waiter
    assert response.status_code == 200 and "work_id" not in response.json()
    assert memory.work["state"] == "completed" and len(provider.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["runtime_absent", "capabilities", "confirmation", "default"])
async def test_ineligible_delivery_remains_synchronous(monkeypatch, gate):
    main = _load_main(monkeypatch)
    monkeypatch.setattr(main, "runtime", None if gate == "runtime_absent" else object())
    monkeypatch.setattr(main.settings, "cognitive_runtime_capability_registry_enabled",
                        gate == "capabilities")
    entered, release = asyncio.Event(), asyncio.Event()

    async def cognition(**kwargs):
        assert kwargs["on_work_admitted"] is None
        assert "delivery_wait_ms" not in kwargs["payload"]
        entered.set()
        await release.wait()
        return dict(request_id=kwargs["request_id"], profile_name="default",
                    selected_model="test", answer="Unchanged.", status="ok", sources=[])

    monkeypatch.setattr(main, "orchestrate_chat", cognition)
    options = {"capability_confirmation": {"confirmed": True}} if gate == "confirmation" else {}
    if gate == "default":
        options.update(allow_deferred=False, delivery_wait_ms=None)
    waiter = asyncio.create_task(_delivery_post(main, **options))
    await entered.wait()
    assert not main._owned_chat_tasks
    release.set()
    response = await waiter
    assert response.status_code == 200 and "work_id" not in response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize("before_admission", ["delayed", "result", "error"])
async def test_delivery_wait_begins_only_after_admission(monkeypatch, before_admission):
    from test_memory_store_client import projection

    main = _load_main(monkeypatch)
    monkeypatch.setattr(main, "runtime", object())
    monkeypatch.setattr(main.settings, "cognitive_runtime_capability_registry_enabled", False)
    entered, admit, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def cognition(**kwargs):
        entered.set()
        await admit.wait()
        if before_admission == "error":
            raise RuntimeError("private sentinel")
        if before_admission == "delayed":
            kwargs["on_work_admitted"](projection(request_id=kwargs["request_id"]))
            await release.wait()
        return dict(request_id=kwargs["request_id"], profile_name="default",
                    selected_model="test", answer="Unchanged.", status="ok", sources=[])

    monkeypatch.setattr(main, "orchestrate_chat", cognition)
    waiter = asyncio.create_task(_delivery_post(main, client_id=None))
    await entered.wait()
    await asyncio.sleep(0.15)
    assert not waiter.done()
    admit.set()
    try:
        response = await waiter
        assert response.status_code == {"delayed": 202, "result": 200, "error": 500}[
            before_admission
        ]
        assert ("work_id" in response.json()) == (before_admission == "delayed")
        assert "private sentinel" not in response.text
    finally:
        release.set()
        await asyncio.gather(*main._owned_chat_tasks, return_exceptions=True)


def _load_main(monkeypatch):
    monkeypatch.setenv("ORCH_API_KEY", "orch-test")
    monkeypatch.setenv("MEMORY_STORE_BASE_URL", "http://memory")
    monkeypatch.setenv("MEMORY_STORE_API_KEY", "memory")
    monkeypatch.setenv("LITELLM_BASE_URL", "http://litellm")
    monkeypatch.setenv("DSA_ENABLED", "true")
    monkeypatch.setenv("DSA_BASE_URL", "http://dsa")

    import settings

    settings.get_settings.cache_clear()
    import main

    return importlib.reload(main)


def _full_chat_payload(**overrides):
    payload = {
        "owner_id": "owner",
        "client_id": "node-red",
        "surface": "node_red",
        "surface_context": {
            "surface_type": "node_red",
            "interaction_mode": "text",
            "spoken_output": False,
            "active_task_mode": False,
            "output_format": "markdown",
        },
        "messages": [
            {
                "role": "user",
                "content": "Do I have any vehicle maintenance records for the battery?",
            }
        ],
        "requested_profile": "default",
        "sensitivity": "private",
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
@pytest.mark.parametrize("completion_failure", [False, True])
async def test_chat_api_durable_work_keeps_synchronous_contract(
    monkeypatch, tmp_path, completion_failure,
):
    from test_orchestrate_flow import (
        DurableWorkMemoryStore,
        FakeLiteLLM,
        FakeRuntime,
        _write_router_files,
    )

    main = _load_main(monkeypatch)
    rules, models = _write_router_files(tmp_path)
    memory = DurableWorkMemoryStore(fail_at="completed" if completion_failure else None)
    provider = FakeLiteLLM(content="Canonical synchronous answer.")
    monkeypatch.setattr(main, "memory_store", memory)
    monkeypatch.setattr(main, "litellm", provider)
    monkeypatch.setattr(main, "runtime", FakeRuntime())
    monkeypatch.setattr(main.settings, "router_rules_path", str(rules))
    monkeypatch.setattr(main.settings, "model_registry_path", str(models))
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/v1/chat", headers={"X-API-Key": "orch-test"},
            json=_full_chat_payload(messages=[{"role": "user", "content": "Hello."}]),
        )
    result = response.json()
    assert memory.work["request_id"] == result["request_id"]
    assert len(provider.calls) == 1
    assert [m["role"] for m in memory.added_messages] == ["user", "assistant"]
    assert "work_id" not in result
    if completion_failure:
        assert response.status_code == 500
        assert result["error"]["code"] == "orchestration_error"
        assert memory.work["state"] == "running"
        assert "answer" not in result
    else:
        assert response.status_code == 200
        assert result["answer"] == "Canonical synchronous answer."
        assert set(result) == {
            "request_id", "conversation_id", "profile_name", "selected_model",
            "answer", "status", "sources",
        }
        assert memory.work["state"] == "completed"
        assert memory.work["conversation_id"] == result["conversation_id"]
        assert memory.work["assistant_message_id"] == "00000000-0000-4000-8000-000000000002"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_enabled", "expected_external_context"),
    [
        (_full_chat_payload(), False, None),
        (_full_chat_payload(external_context_enabled=True), True, None),
        (
            _full_chat_payload(
                external_context={
                    "enabled": True,
                    "source_ids": ["example_source"],
                    "max_results": 5,
                }
            ),
            False,
            {
                "allowed_sensitivity": None,
                "domain_tags": None,
                "enabled": True,
                "source_ids": ["example_source"],
                "max_results": 5,
            },
        ),
    ],
)
async def test_chat_endpoint_preserves_request_level_external_context_contract(
    monkeypatch,
    payload,
    expected_enabled,
    expected_external_context,
):
    main = _load_main(monkeypatch)
    captured_payloads = []

    async def fake_orchestrate_chat(**kwargs):
        captured_payloads.append(kwargs["payload"])
        return {
            "request_id": "rid-chat-api",
            "conversation_id": "conv-1",
            "profile_name": "default",
            "selected_model": "gpt-4o-mini",
            "answer": "ok",
            "status": "ok",
            "sources": [],
        }

    monkeypatch.setattr(main, "orchestrate_chat", fake_orchestrate_chat)

    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/v1/chat",
            headers={"X-API-Key": "orch-test"},
            json=payload,
        )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert set(response.json()) == {
        "request_id", "conversation_id", "profile_name", "selected_model",
        "answer", "status", "sources",
    }
    assert len(captured_payloads) == 1
    assert captured_payloads[0]["external_context_enabled"] is expected_enabled
    assert captured_payloads[0]["external_context"] == expected_external_context


@pytest.mark.asyncio
async def test_chat_endpoint_passes_independent_classifier_timeouts(monkeypatch):
    monkeypatch.setenv("INTENT_CLASSIFIER_TIMEOUT_MS", "1111")
    monkeypatch.setenv("EVIDENCE_INTERPRETER_TIMEOUT_MS", "4321")
    main = _load_main(monkeypatch)
    captured_kwargs = []

    async def fake_orchestrate_chat(**kwargs):
        captured_kwargs.append(kwargs)
        return {
            "request_id": "rid-chat-api",
            "conversation_id": "conv-1",
            "profile_name": "default",
            "selected_model": "gpt-4o-mini",
            "answer": "ok",
            "status": "ok",
            "sources": [],
        }

    monkeypatch.setattr(main, "orchestrate_chat", fake_orchestrate_chat)

    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/v1/chat",
            headers={"X-API-Key": "orch-test"},
            json=_full_chat_payload(),
        )

    assert response.status_code == 200
    assert main.settings.intent_classifier_timeout_ms == 1111
    assert main.settings.evidence_interpreter_timeout_ms == 4321
    assert captured_kwargs[0]["intent_classifier_timeout_ms"] == 1111
    assert captured_kwargs[0]["evidence_interpreter_timeout_ms"] == 4321


def test_classifier_timeout_defaults_are_independent(monkeypatch):
    main = _load_main(monkeypatch)

    assert main.settings.intent_classifier_timeout_ms == 3000
    assert main.settings.evidence_interpreter_timeout_ms == 5000


@pytest.mark.asyncio
async def test_chat_endpoint_does_not_expose_orchestration_exception_text(monkeypatch):
    main = _load_main(monkeypatch)

    async def fake_orchestrate_chat(**kwargs):
        raise RuntimeError("PRIVATE-DIAGNOSTIC-SENTINEL-BMS-EXCEPTION")

    monkeypatch.setattr(main, "orchestrate_chat", fake_orchestrate_chat)

    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/v1/chat",
            headers={"X-API-Key": "orch-test"},
            json=_full_chat_payload(),
        )

    assert response.status_code == 500
    body = response.json()
    assert body["status"] == "failed"
    assert body["error"] == {
        "code": "orchestration_error",
        "message": "The chat request could not be completed.",
    }
    assert "PRIVATE-DIAGNOSTIC-SENTINEL" not in str(body)
