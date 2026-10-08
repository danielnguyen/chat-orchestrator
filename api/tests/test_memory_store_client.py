import asyncio
import json
from copy import deepcopy
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from clients.memory_store import MemoryStoreClient, _validate_work_projection

WORK_ID = "00000000-0000-4000-8000-000000000010"
CONVERSATION_ID = "00000000-0000-4000-8000-000000000020"
MESSAGE_ID = "00000000-0000-4000-8000-000000000030"
ASSOCIATION = {
    "owner_id": "owner",
    "conversation_id": CONVERSATION_ID,
    "request_id": "request-1",
    "client_id": "web:one",
    "surface": "web",
}


def projection(state="pending", **overrides):
    value = {
        **ASSOCIATION,
        "work_id": WORK_ID,
        "state": state,
        "created_at": "2026-09-21T00:00:00+00:00",
        "started_at": None,
        "completed_at": None,
        "assistant_message_id": None,
        "failure_code": None,
    }
    if state in {"running", "completed"}:
        value["started_at"] = "2026-09-21T00:00:01+00:00"
    if state in {"completed", "failed"}:
        value["completed_at"] = "2026-09-21T00:00:02+00:00"
    if state == "completed":
        value["assistant_message_id"] = MESSAGE_ID
    if state == "failed":
        value["failure_code"] = "execution_failed"
    return {**value, **overrides}


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 3])
async def test_reconcile_work_is_bodyless_count_only(service, count):
    client, responses, calls = service
    responses.append({"interrupted_count": count})
    assert await client.reconcile_interrupted_work() == {"interrupted_count": count}
    assert len(calls) == 1
    assert calls[0].method == "POST"
    assert calls[0].url.path == "/v1/internal/work-items/reconcile-interrupted"
    assert calls[0].content == b""
    assert calls[0].headers["X-API-Key"] == "test-key"


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [
    None, [], {}, {"interrupted_count": True}, {"interrupted_count": -1},
    {"interrupted_count": 1.0}, {"interrupted_count": "1"},
    {"interrupted_count": 0, "work": []},
])
async def test_reconcile_work_rejects_malformed_without_retry(service, response):
    client, responses, calls = service
    responses.append(response)
    with pytest.raises(RuntimeError, match="work_reconciliation_response_invalid"):
        await client.reconcile_interrupted_work()
    assert len(calls) == 1


@pytest_asyncio.fixture
async def service(monkeypatch):
    calls = []
    responses = []
    async_client = httpx.AsyncClient

    def handler(request):
        calls.append(request)
        response = responses.pop(0)
        return response if isinstance(response, httpx.Response) else httpx.Response(
            200, content=json.dumps(response),
        )

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: async_client(
            **kwargs,
            transport=httpx.MockTransport(handler),
        ),
    )
    client = MemoryStoreClient("http://memory", "test-key")
    await client.open()
    try:
        yield client, responses, calls
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_work_client_all_operations_use_exact_bms_contract(service):
    client, responses, calls = service
    pending, running, completed = (projection(s) for s in ("pending", "running", "completed"))
    responses.extend(
        [
            pending,
            pending,
            running,
            completed,
            {"status": "resolved", "work": completed},
            {"status": "resolved", "work": completed},
            {"status": "none", "work": None},
        ]
    )
    assert await client.create_work(**ASSOCIATION) == pending
    assert (
        await client.get_work(work_id=WORK_ID, owner_id="owner", conversation_id=CONVERSATION_ID)
        == pending
    )
    assert await client.transition_work(work=pending, state="running") == running
    assert (
        await client.transition_work(
            work=running, state="completed", assistant_message_id=MESSAGE_ID
        )
        == completed
    )
    assert (await client.set_current_work(owner_id="owner", client_id="web:one", work_id=WORK_ID))[
        "work"
    ] == completed
    assert (await client.get_current_work(owner_id="owner", client_id="web:one"))[
        "work"
    ] == completed
    assert await client.get_current_work(owner_id="owner", client_id="web:one") == {
        "status": "none",
        "work": None,
    }
    assert [r.method for r in calls] == ["POST", "GET", "PATCH", "PATCH", "PUT", "GET", "GET"]
    assert calls[0].url.path == "/v1/internal/work-items"
    assert dict(calls[1].url.params) == {"owner_id": "owner", "conversation_id": CONVERSATION_ID}
    assert calls[2].url.path == f"/v1/internal/work-items/{WORK_ID}"
    assert all(r.headers["X-API-Key"] == "test-key" for r in calls)
    assert calls[0].headers["X-Request-ID"] == "request-1"
    assert json.loads(calls[0].content) == ASSOCIATION
    assert json.loads(calls[2].content) == {
        "owner_id": "owner", "conversation_id": CONVERSATION_ID,
        "state": "running", "assistant_message_id": None, "failure_code": None,
    }
    assert json.loads(calls[3].content) == {
        "owner_id": "owner", "conversation_id": CONVERSATION_ID,
        "state": "completed", "assistant_message_id": MESSAGE_ID, "failure_code": None,
    }
    assert json.loads(calls[4].content) == {
        "owner_id": "owner", "client_id": "web:one", "work_id": WORK_ID,
    }
    assert all("X-Request-ID" not in r.headers for r in calls[1:])
    assert b'"content"' not in b"".join(r.content for r in calls)


@pytest.mark.parametrize("state", ["pending", "running", "completed", "failed"])
def test_work_projection_valid_states_and_nullable_client(state):
    value = projection(state, client_id=None)
    assert _validate_work_projection(value) == value


@pytest.mark.parametrize(
    "field,value",
    [
        ("work_id", "not-a-uuid"),
        ("conversation_id", CONVERSATION_ID.replace("-", "")),
        ("owner_id", ""),
        ("owner_id", "private owner"),
        ("request_id", "x" * 121),
        ("client_id", ""),
        ("surface", "x" * 65),
        ("state", "queued"),
        ("state", []),
        ("assistant_message_id", MESSAGE_ID),
        ("failure_code", "execution_failed"),
        ("created_at", None),
        ("created_at", "not-time"),
        ("created_at", "2026-09-21T00:00:00"),
        ("started_at", "2026-09-21T00:00:01Z"),
        ("completed_at", "2026-09-21T00:00:02Z"),
    ],
)
def test_work_projection_rejects_malformed_pending(field, value):
    with pytest.raises(RuntimeError, match="work_projection_invalid"):
        _validate_work_projection({**projection(), field: value})


@pytest.mark.parametrize(
    "state,changes",
    [
        ("running", {"started_at": None}),
        ("running", {"started_at": "2026-09-20T00:00:00Z"}),
        ("completed", {"assistant_message_id": None}),
        ("completed", {"assistant_message_id": "message-1"}),
        ("completed", {"completed_at": None}),
        ("completed", {"completed_at": "2026-09-20T00:00:00Z"}),
        ("completed", {"failure_code": "execution_failed"}),
        ("failed", {"failure_code": None}),
        ("failed", {"failure_code": "arbitrary-sentinel"}),
        ("failed", {"assistant_message_id": MESSAGE_ID}),
    ],
)
def test_work_projection_rejects_incoherent_terminal_and_timestamps(state, changes):
    with pytest.raises(RuntimeError, match="work_projection_invalid"):
        _validate_work_projection(projection(state, **changes))


@pytest.mark.parametrize("field", ["answer", "prompt", "source_payload", "credentials", "metadata"])
def test_work_projection_rejects_extra_private_content(field):
    with pytest.raises(RuntimeError, match="work_projection_invalid"):
        _validate_work_projection(projection(**{field: "PRIVATE-SENTINEL"}))


@pytest.mark.parametrize("field", list(projection()))
def test_work_projection_requires_every_field(field):
    value = projection()
    del value[field]
    with pytest.raises(RuntimeError, match="work_projection_invalid"):
        _validate_work_projection(value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", "other"),
        ("conversation_id", WORK_ID),
        ("request_id", "other"),
        ("client_id", None),
        ("surface", "other"),
    ],
)
async def test_create_rejects_immutable_association_mismatch(service, field, value):
    client, responses, _ = service
    responses.append(projection(**{field: value}))
    with pytest.raises(RuntimeError, match="context_mismatch"):
        await client.create_work(**ASSOCIATION)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("work_id", MESSAGE_ID),
        ("owner_id", "other"),
        ("request_id", "other"),
        ("client_id", None),
        ("surface", "other"),
        ("conversation_id", MESSAGE_ID),
        ("created_at", "2026-09-20T00:00:00Z"),
    ],
)
async def test_transition_rejects_changed_identity(service, field, value):
    client, responses, _ = service
    responses.append(projection("running", **{field: value}))
    with pytest.raises(RuntimeError, match="context_mismatch"):
        await client.transition_work(work=projection(), state="running")


@pytest.mark.asyncio
async def test_transition_rejects_different_completion_reference(service):
    client, responses, _ = service
    responses.append(projection("completed", assistant_message_id=WORK_ID))
    with pytest.raises(RuntimeError, match="context_mismatch"):
        await client.transition_work(
            work=projection("running"), state="completed", assistant_message_id=MESSAGE_ID
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {},
        {"status": "none", "work": projection()},
        {"status": "resolved", "work": None},
        {"status": "unknown", "work": None},
        {"status": "none", "work": None, "answer": "PRIVATE-SENTINEL"},
        {"status": "resolved", "work": projection(owner_id="other")},
        {"status": "resolved", "work": projection(client_id="other")},
    ],
)
async def test_current_work_rejects_malformed_or_mismatched_resolution(service, response):
    client, responses, _ = service
    responses.append(deepcopy(response))
    with pytest.raises(RuntimeError):
        await client.get_current_work(owner_id="owner", client_id="web:one")


@pytest.mark.asyncio
async def test_current_work_set_cannot_return_none_or_different_work(service):
    client, responses, _ = service
    responses.extend(
        [
            {"status": "none", "work": None},
            {"status": "resolved", "work": projection(work_id=MESSAGE_ID)},
        ]
    )
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await client.set_current_work(owner_id="owner", client_id="web:one", work_id=WORK_ID)


@pytest.mark.asyncio
async def test_exact_work_lookup_is_context_bound_and_does_not_retry(service):
    client, responses, calls = service
    responses.append(projection(owner_id="other"))
    with pytest.raises(RuntimeError, match="context_mismatch"):
        await client.get_work(work_id=WORK_ID, owner_id="owner", conversation_id=CONVERSATION_ID)
    assert len(calls) == 1


def result_projection(state="completed", **work_overrides):
    work = projection(state, **work_overrides)
    return {"work": work, "result": {
        "assistant_message_id": MESSAGE_ID, "content": "Exact\ncanonical α. ",
    } if state == "completed" else None}


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["pending", "running", "completed", "failed"])
async def test_work_result_exact_contract(service, state):
    client, responses, calls = service
    value = result_projection(state)
    responses.append(value)
    assert await client.get_work_result(
        work_id=WORK_ID, owner_id="owner", conversation_id=CONVERSATION_ID,
    ) == value
    assert len(calls) == 1
    assert calls[0].method == "GET"
    assert calls[0].url.path == f"/v1/internal/work-items/{WORK_ID}/result"
    assert dict(calls[0].url.params) == {
        "owner_id": "owner", "conversation_id": CONVERSATION_ID,
    }
    assert calls[0].headers["X-API-Key"] == "test-key"
    assert calls[0].content == b""


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 401, 500, 503])
async def test_work_result_http_failure_no_retry(service, status):
    client, responses, calls = service
    responses.append(httpx.Response(status, json={"detail": "private-sentinel"}))
    kwargs = dict(work_id=WORK_ID, owner_id="owner", conversation_id=CONVERSATION_ID)
    if status == 404:
        assert await client.get_work_result(**kwargs) is None
    else:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_work_result(**kwargs)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [
    ("work_id", "bad"), ("work_id", WORK_ID.replace("-", "")),
    ("conversation_id", "bad"), ("owner_id", ""), ("owner_id", "x" * 121),
    ("owner_id", "private owner"),
])
async def test_work_result_validates_input_before_request(service, field, value):
    client, _, calls = service
    kwargs = dict(work_id=WORK_ID, owner_id="owner", conversation_id=CONVERSATION_ID)
    kwargs[field] = value
    with pytest.raises(RuntimeError):
        await client.get_work_result(**kwargs)
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("malformation", [
    "outer_extra", "outer_missing", "work_extra", "work_missing", "work_malformed",
    "work_id", "owner_id", "conversation_id", "result_extra", "result_missing",
    "result_null", "result_type", "message_id", "message_uuid", "content_type",
    "pending_result", "running_result", "failed_result",
])
async def test_work_result_rejects_malformed_or_mismatched_response(service, malformation):
    client, responses, calls = service
    value = result_projection()
    if malformation == "outer_extra":
        value["trace"] = "private-sentinel"
    elif malformation == "outer_missing":
        del value["result"]
    elif malformation == "work_extra":
        value["work"]["prompt"] = "private-sentinel"
    elif malformation == "work_missing":
        del value["work"]["surface"]
    elif malformation == "work_malformed":
        value["work"]["state"] = "unknown"
    elif malformation in {"work_id", "owner_id", "conversation_id"}:
        value["work"][malformation] = "other" if malformation == "owner_id" else MESSAGE_ID
    elif malformation == "result_extra":
        value["result"]["metadata"] = "private-sentinel"
    elif malformation == "result_missing":
        del value["result"]["content"]
    elif malformation in {"result_null", "result_type"}:
        value["result"] = None if malformation == "result_null" else []
    elif malformation in {"message_id", "message_uuid"}:
        value["result"]["assistant_message_id"] = (
            WORK_ID if malformation == "message_id" else MESSAGE_ID.replace("-", "")
        )
    elif malformation == "content_type":
        value["result"]["content"] = 42
    else:
        value["work"] = projection(malformation.split("_")[0])
    responses.append(value)
    with pytest.raises(RuntimeError):
        await client.get_work_result(
            work_id=WORK_ID, owner_id="owner", conversation_id=CONVERSATION_ID,
        )
    assert len(calls) == 1


def _proactive_preference(*, persisted=False, enabled=False):
    return {
        "owner_id": "owner", "enabled": enabled,
        "allowed_surfaces_json": ["telegram"] if persisted else [],
        "rule_prefs_json": {"private": "sentinel"} if persisted else {},
        "created_at": "2026-09-28T00:00:00Z" if persisted else None,
        "updated_at": "2026-09-28 01:00:00+00:00" if persisted else None,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted,enabled", [(False, False), (True, False), (True, True)])
async def test_proactive_preferences_exact_get_and_persistence_forms(service, persisted, enabled):
    client, responses, calls = service
    value = _proactive_preference(persisted=persisted, enabled=enabled)
    responses.append(value)
    assert await client.get_proactive_preferences(owner_id="owner") == value
    assert len(calls) == 1
    assert calls[0].method == "GET"
    assert calls[0].url.path == "/v1/proactive/preferences"
    assert dict(calls[0].url.params) == {"owner_id": "owner"}
    assert calls[0].headers["X-API-Key"] == "test-key"


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"owner_id": "other"}, {"owner_id": 1}, {"extra": "private"},
    {"enabled": 0}, {"enabled": "false"}, {"enabled": None},
    {"allowed_surfaces_json": "telegram"}, {"allowed_surfaces_json": [1]},
    {"rule_prefs_json": []}, {"rule_prefs_json": None},
    {"created_at": None}, {"updated_at": None},
    {"created_at": "invalid"}, {"updated_at": "2026-09-28T01:00:00"},
    {"created_at": "2026-09-28"}, {"updated_at": 1},
])
async def test_proactive_preferences_rejects_malformed_or_mismatched_record(service, changes):
    client, responses, calls = service
    responses.append({**_proactive_preference(persisted=True), **changes})
    with pytest.raises(RuntimeError, match="proactive_preferences_"):
        await client.get_proactive_preferences(owner_id="owner")
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field", list(_proactive_preference()))
async def test_proactive_preferences_rejects_missing_fields(service, field):
    client, responses, _ = service
    value = _proactive_preference()
    del value[field]
    responses.append(value)
    with pytest.raises(RuntimeError, match="proactive_preferences_response_invalid"):
        await client.get_proactive_preferences(owner_id="owner")


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [
    None, [], {},
    {**_proactive_preference(), "enabled": True},
    {**_proactive_preference(), "allowed_surfaces_json": ["telegram"]},
    {**_proactive_preference(), "rule_prefs_json": {"private": True}},
])
async def test_proactive_preferences_rejects_inconsistent_synthetic_form(service, value):
    client, responses, _ = service
    responses.append(value)
    with pytest.raises(RuntimeError, match="proactive_preferences_response_invalid"):
        await client.get_proactive_preferences(owner_id="owner")

def _permission_record(**changes):
    return {
        "owner_id": "owner", "surface": "alexa", "configured": True,
        "conversation_context_allowed": True, "proactive_presence_allowed": False,
        "ambient_listening_allowed": False, "created_at": "2026-10-05T00:00:00+00:00",
        "updated_at": "2026-10-05T00:00:00+00:00", **changes,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True])
async def test_presence_permission_exact_lookup_and_shape(service, configured):
    client, responses, calls = service
    row = _permission_record() if configured else _permission_record(
        configured=False, conversation_context_allowed=False, created_at=None, updated_at=None,
    )
    responses.append(row)
    assert await client.get_presence_surface_permission(owner_id="owner", surface="alexa") == row
    assert len(calls) == 1
    assert calls[0].url.path == "/v1/presence/surface-permissions"
    assert dict(calls[0].url.params) == {"owner_id": "owner", "surface": "alexa"}


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"owner_id": "other"}, {"surface": "telegram"}, {"configured": 1},
    {"conversation_context_allowed": "true"}, {"proactive_presence_allowed": None},
    {"ambient_listening_allowed": 0}, {"unexpected": True},
    {"configured": False}, {"created_at": None}, {"updated_at": "invalid"},
    {"created_at": "2026-10-05"}, {"updated_at": "x" * 65},
])
async def test_presence_permission_malformed_fails_without_retry(service, changes):
    client, responses, calls = service
    responses.append(_permission_record(**changes))
    with pytest.raises(RuntimeError, match="surface_permission_"):
        await client.get_presence_surface_permission(owner_id="owner", surface="alexa")
    assert len(calls) == 1


@pytest_asyncio.fixture
async def managed_transport(monkeypatch):
    constructions, calls, responses = [], [], []
    async_client = httpx.AsyncClient

    def handler(request):
        calls.append(request)
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, httpx.Response):
            return response
        return httpx.Response(200, content=json.dumps(response))

    def factory(**kwargs):
        client = async_client(**kwargs, transport=httpx.MockTransport(handler))
        client.aclose = AsyncMock(wraps=client.aclose)
        constructions.append((kwargs, client))
        return client

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    client = MemoryStoreClient("http://memory/", "test-key", timeout_ms=12345)
    try:
        yield client, constructions, calls, responses
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_memory_transport_explicit_lifecycle_and_sequential_reuse(managed_transport):
    client, constructions, calls, responses = managed_transport
    assert constructions == []
    with pytest.raises(RuntimeError, match="^memory_store_client_not_started$"):
        await client._get("/v1/conversations")
    assert constructions == [] and calls == []
    await asyncio.gather(client.open(), client.open())
    await client.open()
    assert len(constructions) == 1
    kwargs, transport = constructions[0]
    assert set(kwargs) == {"timeout", "limits"}
    assert kwargs["timeout"] == 12.345
    assert kwargs["limits"].max_connections == 100
    assert kwargs["limits"].max_keepalive_connections == 20
    assert kwargs["limits"].keepalive_expiry == 5.0
    assert client._client is transport
    responses.extend([{"ok": True}] * 4 + [{"interrupted_count": 0}])
    assert await client._post("/v1/traces", request_id="request-one", json={"ok": True})
    assert await client._get("/v1/conversations", params={"owner_id": "owner"})
    assert await client._work_write("PATCH", "/v1/internal/work-items/one", {"state": "running"})
    assert await client._work_write("PUT", "/v1/internal/current-work", {"work_id": "one"})
    assert await client.reconcile_interrupted_work() == {"interrupted_count": 0}
    assert len(constructions) == 1 and len(calls) == 5
    assert [call.method for call in calls] == ["POST", "GET", "PATCH", "PUT", "POST"]
    assert all(call.headers["X-API-Key"] == "test-key" for call in calls)
    assert calls[0].headers["X-Request-ID"] == "request-one"
    assert "X-Request-ID" not in calls[1].headers
    assert dict(calls[1].url.params) == {"owner_id": "owner"}
    assert calls[-1].content == b""
    transport.aclose.assert_not_awaited()
    await asyncio.gather(client.close(), client.close())
    await client.close()
    transport.aclose.assert_awaited_once()
    with pytest.raises(RuntimeError, match="^memory_store_client_closed$"):
        await client._post("/v1/traces", json={})
    with pytest.raises(RuntimeError, match="^memory_store_client_closed$"):
        await client.open()
    assert len(constructions) == 1 and len(calls) == 5


@pytest.mark.asyncio
async def test_memory_transport_close_before_open_is_final(managed_transport):
    client, constructions, calls, _ = managed_transport
    await client.close()
    await client.close()
    with pytest.raises(RuntimeError, match="^memory_store_client_closed$"):
        await client.open()
    assert constructions == [] and calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["POST", "GET", "PATCH", "PUT", "RECONCILE"])
@pytest.mark.parametrize("failure", ["http", "timeout", "transport", "decode", "pool_timeout"])
async def test_memory_transport_failures_never_replay(
    managed_transport, method, failure,
):
    client, constructions, calls, responses = managed_transport
    await client.open()
    if failure == "http":
        responses.append(httpx.Response(503))
        expected = httpx.HTTPStatusError
    elif failure == "timeout":
        responses.append(httpx.ReadTimeout("private-timeout"))
        expected = httpx.ReadTimeout
    elif failure == "pool_timeout":
        responses.append(httpx.PoolTimeout("private-pool"))
        expected = httpx.PoolTimeout
    elif failure == "transport":
        responses.append(httpx.ConnectError("private-transport"))
        expected = httpx.ConnectError
    else:
        responses.append(httpx.Response(200, content=b"not-json"))
        expected = ValueError
    with pytest.raises(expected):
        if method == "POST":
            await client._post("/v1/traces", request_id="failed-request", json={"mutation": True})
        elif method == "GET":
            await client._get("/v1/conversations", params={"owner_id": "owner"})
        elif method == "RECONCILE":
            await client.reconcile_interrupted_work()
        else:
            await client._work_write(method, "/v1/internal/work-items/one", {"mutation": True})
    assert len(calls) == 1
    assert calls[0].method == ("POST" if method == "RECONCILE" else method)
    if method == "RECONCILE":
        assert calls[0].content == b""
    assert len(constructions) == 1
    failed_client = constructions[0][1]
    replace = failure in {"timeout", "transport"}
    if replace:
        assert client._client is None
        failed_client.aclose.assert_awaited_once()
    else:
        assert client._client is failed_client
        failed_client.aclose.assert_not_awaited()
    responses.append({"ok": True})
    assert await client._post("/v1/traces", request_id="independent-request", json={"new": True})
    assert len(calls) == 2 and calls[1].headers["X-Request-ID"] == "independent-request"
    assert json.loads(calls[1].content) == {"new": True}
    assert len(constructions) == (2 if replace else 1)
    assert client._client is constructions[-1][1]
    if replace:
        assert client._client is not failed_client
        failed_client.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_old_failure_cannot_invalidate_later_replacement(managed_transport):
    client, constructions, calls, responses = managed_transport
    await client.open()
    failed_client = client._client
    responses.append(httpx.ConnectError("transport-unavailable"))
    with pytest.raises(httpx.ConnectError):
        await client._post("/v1/traces", json={"first": True})
    assert client._client is None and len(calls) == 1
    responses.append({"ok": True})
    await client._post("/v1/traces", json={"independent": True})
    replacement = client._client
    await asyncio.gather(client._invalidate_client(failed_client), client.open())
    assert client._client is replacement and len(constructions) == 2
    failed_client.aclose.assert_awaited_once()
    replacement.aclose.assert_not_awaited()
    await client.close()
    replacement.aclose.assert_awaited_once()
    with pytest.raises(RuntimeError, match="^memory_store_client_closed$"):
        await client._get("/v1/conversations")
    assert len(calls) == 2 and len(constructions) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", ["invalidate", "close"])
async def test_transport_cleanup_failure_preserves_original_error(
    managed_transport, monkeypatch, cleanup,
):
    client, constructions, calls, responses = managed_transport
    await client.open()
    failed_client = client._client
    original_close = failed_client.aclose
    failure = httpx.ReadTimeout("transport-unconfirmed")
    responses.append(failure)
    cleanup_failure = AsyncMock(side_effect=RuntimeError("cleanup-unavailable"))
    if cleanup == "invalidate":
        monkeypatch.setattr(client, "_invalidate_client", cleanup_failure)
    else:
        monkeypatch.setattr(failed_client, "aclose", cleanup_failure)
    try:
        with pytest.raises(httpx.ReadTimeout) as caught:
            await client._post("/v1/traces", json={"mutation": True})
        assert caught.value is failure
        assert len(calls) == 1 and len(constructions) == 1
        cleanup_failure.assert_awaited_once()
    finally:
        if cleanup == "close":
            await original_close()


@pytest.mark.asyncio
async def test_memory_transport_cancellation_propagates_without_replay(managed_transport):
    client, constructions, calls, responses = managed_transport
    await client.open()
    responses.append(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await client._post("/v1/traces", json={"mutation": True})
    assert len(calls) == 1 and len(constructions) == 1
    assert client._client is constructions[0][1]
    constructions[0][1].aclose.assert_not_awaited()
    responses.append({"ok": True})
    await client._get("/v1/conversations")
    assert len(calls) == 2 and len(constructions) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [None, {}, {"interrupted_count": True}])
async def test_reconciliation_validation_preserves_shared_client(managed_transport, malformed):
    client, constructions, calls, responses = managed_transport
    await client.open()
    responses.append(malformed)
    with pytest.raises(RuntimeError, match="^work_reconciliation_response_invalid$"):
        await client.reconcile_interrupted_work()
    assert len(calls) == 1 and len(constructions) == 1
    assert client._client is constructions[0][1]
    constructions[0][1].aclose.assert_not_awaited()
    responses.append({"interrupted_count": 0})
    assert await client.reconcile_interrupted_work() == {"interrupted_count": 0}
    assert len(calls) == 2 and len(constructions) == 1
