import json
from copy import deepcopy

import httpx
import pytest
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


@pytest.fixture
def service(monkeypatch):
    calls = []
    responses = []
    async_client = httpx.AsyncClient

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=json.dumps(responses.pop(0)))

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: async_client(
            **kwargs,
            transport=httpx.MockTransport(handler),
        ),
    )
    return MemoryStoreClient("http://memory", "test-key"), responses, calls


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
