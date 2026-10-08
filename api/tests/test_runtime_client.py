from __future__ import annotations

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from typing import Any

import httpx
import pytest
from clients.runtime import RuntimeClient


@asynccontextmanager
async def _held_http_connection(*, release: asyncio.Event | None = None):
    """Hold the first HTTP response so the real httpx pool stays occupied."""
    received = []
    connections = []
    entered = asyncio.Event()
    disconnected = asyncio.Event()
    handlers = set()

    async def handle(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        connections.append(writer.get_extra_info("peername"))
        try:
            headers = (await reader.readuntil(b"\r\n\r\n")).decode("ascii")
            length = next(int(line.split(":", 1)[1]) for line in headers.splitlines()
                          if line.lower().startswith("content-length:"))
            payload = json.loads(await reader.readexactly(length))
            received.append((headers.splitlines()[0], payload))
            if len(received) == 1:
                entered.set()
                if release is None:
                    # Explicit shutdown must close this occupied socket.
                    assert await reader.read() == b""
                    disconnected.set()
                    return
                await release.wait()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n"
                b"Content-Type: application/json\r\nConnection: close\r\n\r\n"
                b'{"ok":true}'
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.remove(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}", received, connections, entered, disconnected
    finally:
        server.close()
        await server.wait_closed()
        pending = tuple(handlers)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


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


def _runtime_thread_projection() -> dict[str, Any]:
    return {
        "owner_id": "owner",
        "conversation_id": "conversation",
        "state": "idle",
        "revision": 7,
        "active_runtime_session_id": None,
        "active_runtime_turn_id": None,
        "active_surface": None,
        "participating_surfaces": ["voice", "web"],
        "participating_session_count": 2,
        "last_activity_at": "2026-08-01T07:00:00-05:00",
        "created_at": "2026-07-01T12:00:00+00:00",
        "updated_at": "2026-08-01T12:00:00+00:00",
    }


def _retirement_reservation_response(outcome: str) -> dict[str, Any]:
    reason = {
        "reserved": "safe_idle_retirement_reserved",
        "wait": "runtime_thread_active",
        "decline": "runtime_state_missing",
    }[outcome]
    result: dict[str, Any] = {
        "outcome": outcome,
        "reason_codes": [reason],
        "policy_version": "conversation-retirement-safety.v1",
    }
    if outcome == "reserved":
        result.update(
            reservation_id="retirement-reservation",
            reserved_thread_revision=7,
            reserved_durable_updated_at="2026-08-01T07:00:00-05:00",
        )
    return {
        "schema_version": "runtime-retirement-reservation.v1",
        "request_id": "retirement-request",
        "owner_id": "owner",
        "conversation_id": "conversation",
        "result": result,
    }


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


def _continuation_response(outcome: str = "resume") -> dict[str, Any]:
    timing = {
        "resume": "resume_previous_thread",
        "create_new": "answer_now",
        "clarify": "ask_clarifying_question",
        "wait": "pause_or_wait",
        "decline": "close_turn",
    }[outcome]
    reasons = {
        "resume": ["one_eligible_candidate"],
        "create_new": ["no_eligible_candidates"],
        "clarify": ["multiple_eligible_candidates"],
        "wait": ["active_thread_present"],
        "decline": ["unavailable_thread_present"],
    }[outcome]
    return {
        "schema_version": "runtime-continuation-selection.v1",
        "request_id": "selection-request",
        "owner_id": "owner",
        "surface": "voice",
        "result": {
            "outcome": outcome,
            "timing_policy": timing,
            "selected_conversation_id": (
                "00000000-0000-4000-8000-000000000001"
                if outcome == "resume"
                else None
            ),
            "selected_thread_revision": 7 if outcome == "resume" else None,
            "candidate_count": 1,
            "eligible_candidate_count": 1 if outcome in {"resume", "clarify"} else 0,
            "reason_codes": reasons,
            "policy_version": "continuation-selection.v1",
        },
    }


def _continuation_candidates(count: int) -> list[dict[str, str]]:
    return [
        {
            "conversation_id": f"00000000-0000-4000-8000-{index:012d}",
            "lifecycle_state": "open",
            "durable_updated_at": "2026-08-01T00:00:00+00:00",
        }
        for index in range(1, count + 1)
    ]


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
async def test_claim_support_client_posts_split_authority_and_validates_response():
    authority = {
        "owner_id": "owner",
        "conversation_id": "conversation",
        "surface": "web",
        "runtime_session_id": "session-1",
        "runtime_turn_id": "turn-1",
        "evidence_references": [],
        "complete_declared_scope_required": False,
        "complete_declared_scope_established": None,
        "material_acquisition_limited": False,
        "privacy_policy_allows_claim": True,
        "consequence_policy_allows_claim": True,
        "executed_derivations": [],
    }
    proposal = {
        "proposed_claim": "A bounded claim.",
        "supporting_evidence_ref_ids": [],
        "counterevidence_ref_ids": [],
        "material_exclusions": [],
        "executed_derivation_ref_ids": [],
    }
    response = {
        "request_id": "request-1",
        **{key: authority[key] for key in (
            "owner_id",
            "conversation_id",
            "surface",
            "runtime_session_id",
            "runtime_turn_id",
        )},
        "result": {
            "claim_id": "claim-1",
            "claim_digest": "sha256:" + "1" * 64,
            "calibration_status": "unsupported",
            "conclusion_disposition": "withheld",
            "qualification_required": True,
            "limitation_codes": ["no_supporting_evidence"],
            "validated_supporting_evidence_ref_ids": [],
            "validated_counterevidence_ref_ids": [],
            "validated_material_exclusions": [],
            "validated_executed_derivation_ref_ids": [],
            "user_safe_summary": "The claim was withheld.",
        },
    }
    fake = _FakeAsyncClient([response])
    client = RuntimeClient(
        "http://runtime.local",
        "runtime-key",
        client_factory=_ClientFactory([fake]),
    )
    await client.open()

    validated = await client.evaluate_claim_support(
        request_id="request-1",
        authority_context=authority,
        proposal=proposal,
    )

    assert validated == response
    assert fake.posts == [
        (
            "/v1/runtime/claim-support/evaluate",
            {
                "request_id": "request-1",
                "authority_context": authority,
                "proposal": proposal,
            },
        )
    ]


@pytest.mark.asyncio
async def test_claim_support_client_rejects_extra_or_mismatched_response_fields():
    authority = {
        "owner_id": "owner",
        "conversation_id": "conversation",
        "surface": "web",
        "runtime_session_id": "session-1",
        "runtime_turn_id": "turn-1",
    }
    invalid = {
        "request_id": "request-1",
        **authority,
        "result": {
            "claim_id": "claim-1",
            "claim_digest": "sha256:" + "1" * 64,
            "calibration_status": "supported",
            "conclusion_disposition": "allowed",
            "qualification_required": False,
            "limitation_codes": [],
            "validated_supporting_evidence_ref_ids": [],
            "validated_counterevidence_ref_ids": [],
            "validated_material_exclusions": [],
            "validated_executed_derivation_ref_ids": [],
            "user_safe_summary": "Bounded.",
            "provider_confidence": "high",
        },
    }
    client = RuntimeClient(
        "http://runtime.local",
        "runtime-key",
        client_factory=_ClientFactory([_FakeAsyncClient([invalid])]),
    )
    await client.open()

    with pytest.raises(RuntimeError, match="claim_support_response_invalid"):
        await client.evaluate_claim_support(
            request_id="request-1",
            authority_context=authority,
            proposal={"proposed_claim": "Bounded."},
        )

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
async def test_pool_exhaustion_bounds_connections_and_does_not_replay_start_turn():
    release = asyncio.Event()
    async with _held_http_connection(release=release) as (
        url, received, connections, entered, disconnected,
    ):
        transports = []
        attempts = []

        async def record_attempt(request):
            attempts.append((request.url.path, json.loads(request.content)))

        def factory(**kwargs):
            # Keep the normal httpx/httpcore transport and its real pool semantics.
            transport = httpx.AsyncClient(
                **kwargs, trust_env=False, event_hooks={"request": [record_attempt]},
            )
            transports.append(transport)
            return transport

        client = RuntimeClient(url, None, timeout_ms=10000, max_connections=1,
                               max_keepalive_connections=1, client_factory=factory)
        await client.open()
        held = asyncio.create_task(client.overlay(
            request_id="held-call", owner_id="owner", conversation_id="conversation",
            surface="web",
        ))
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            # The occupied request retains its long read timeout. Only the queued
            # request gets a short pool timeout, without changing production defaults.
            transports[0].timeout = httpx.Timeout(10.0, pool=0.05)
            transition = dict(request_id="exhausted-start", owner_id="owner",
                              conversation_id="conversation", surface="web")
            with pytest.raises(httpx.PoolTimeout) as exc:
                await asyncio.wait_for(client.start_turn(**transition), timeout=2)
            assert exc.value.request.url.path == "/v1/runtime/turns/start"
            assert attempts == [
                ("/v1/runtime/overlay", dict(request_id="held-call", owner_id="owner",
                                             conversation_id="conversation", surface="web")),
                ("/v1/runtime/turns/start", transition),
            ]
            # The exhausted transition never obtained a second socket or reached CR.
            assert len(connections) == 1
            assert len(received) == 1
            assert not transports[0].is_closed
            assert client._client is transports[0]
            assert len(transports) == 1
            assert not held.done()
            assert not disconnected.is_set()
            # Pool saturation belongs to the waiting request, not the occupied pool.
            release.set()
            assert await asyncio.wait_for(held, timeout=2) == {"ok": True}
            assert await client.overlay(
                request_id="independent-call", owner_id="owner",
                conversation_id="conversation", surface="web",
            ) == {"ok": True}
            assert len(transports) == 1
            assert client._client is transports[0]
            assert not transports[0].is_closed
            assert len(connections) == 2
            assert [payload["request_id"] for _, payload in received] == [
                "held-call", "independent-call",
            ]
            assert [path for path, _ in attempts].count("/v1/runtime/turns/start") == 1
        finally:
            release.set()
            await client.close()
            if not held.done():
                held.cancel()
            await asyncio.gather(held, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", [httpx.ReadTimeout, httpx.RemoteProtocolError])
async def test_start_turn_transport_failure_is_never_replayed(failure_type):
    path = "/v1/runtime/turns/start"
    failure = failure_type("response lost", request=httpx.Request("POST", f"http://runtime{path}"))
    failed = _FakeAsyncClient([failure])
    replacement = _FakeAsyncClient([{"ok": True}])
    factory = _ClientFactory([failed, replacement])
    client = RuntimeClient("http://runtime", None, client_factory=factory)
    transition = dict(request_id="failed-transition", owner_id="owner",
                      conversation_id="conversation", surface="web", expected_thread_revision=7)
    await client.open()
    try:
        with pytest.raises(failure_type) as exc:
            await client.start_turn(**transition)
        assert exc.value is failure
        assert failed.posts == [(path, transition)]
        assert failed.close_calls == 1
        assert client._client is None
        assert len(factory.clients) == 1
        assert await client.overlay(
            request_id="independent-call", owner_id="owner",
            conversation_id="conversation", surface="web",
        ) == {"ok": True}
        assert len(factory.clients) == 2
        assert [posted_path for posted_path, _ in replacement.posts] == ["/v1/runtime/overlay"]
        assert failed.posts == [(path, transition)]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_shutdown_closes_inflight_http_request_without_replacement_or_replay():
    async with _held_http_connection() as (url, received, connections, entered, disconnected):
        transports = []

        def factory(**kwargs):
            transport = httpx.AsyncClient(**kwargs, trust_env=False)
            transports.append(transport)
            return transport

        client = RuntimeClient(url, None, timeout_ms=10000, client_factory=factory)
        await client.open()
        held = asyncio.create_task(client.overlay(
            request_id="shutdown-call", owner_id="owner", conversation_id="conversation",
            surface="web",
        ))
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            await asyncio.wait_for(client.close(), timeout=2)
            with pytest.raises(httpx.TransportError):
                await asyncio.wait_for(held, timeout=2)
            await asyncio.wait_for(disconnected.wait(), timeout=2)
            assert transports[0].is_closed
            assert client._client is None
            assert len(transports) == len(connections) == len(received) == 1
            with pytest.raises(RuntimeError, match="^runtime_client_closed$"):
                await client.overlay(request_id="after-shutdown", owner_id="owner",
                                     conversation_id="conversation", surface="web")
            assert len(transports) == 1
        finally:
            await client.close()
            if not held.done():
                held.cancel()
            await asyncio.gather(held, return_exceptions=True)


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
@pytest.mark.parametrize("outcome", ["resume", "create_new", "clarify", "wait", "decline"])
async def test_runtime_client_accepts_coherent_continuation_outcomes(outcome):
    client = RuntimeClient("http://runtime.local", None)
    calls = []
    candidates = [
        {
            "conversation_id": "00000000-0000-4000-8000-000000000001",
            "lifecycle_state": "open",
            "durable_updated_at": "2026-08-01T00:00:00+00:00",
        }
    ]
    response = _continuation_response(outcome)
    if outcome == "clarify":
        candidates.append(
            {
                "conversation_id": "00000000-0000-4000-8000-000000000002",
                "lifecycle_state": "open",
                "durable_updated_at": "2026-08-01T00:00:00+00:00",
            }
        )
        response["result"]["candidate_count"] = 2
        response["result"]["eligible_candidate_count"] = 2

    async def fake_post(path, *, json):
        calls.append((path, json))
        return response

    client._post = fake_post  # type: ignore[method-assign]
    actual = await client.select_continuation(
        request_id="selection-request",
        owner_id="owner",
        surface="voice",
        candidate_set_complete=True,
        stale_after_seconds=1800,
        candidates=candidates,
    )

    assert actual == response
    assert calls == [
        (
            "/v1/runtime/continuations/select",
            {
                "request_id": "selection-request",
                "owner_id": "owner",
                "surface": "voice",
                "surface_permission_status": "unconfigured",
                "conversation_context_allowed": False,
                "candidate_set_complete": True,
                "stale_after_seconds": 1800,
                "candidates": candidates,
            },
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "candidate_count", "eligible_count", "reason_codes"),
    [
        ("create_new", 0, 0, ["no_candidates"]),
        (
            "create_new",
            1,
            0,
            ["no_eligible_candidates", "candidate_not_open"],
        ),
        (
            "create_new",
            4,
            0,
            [
                "no_eligible_candidates",
                "candidate_not_open",
                "runtime_state_missing",
                "runtime_session_missing",
                "candidate_stale",
            ],
        ),
        ("clarify", 1, 0, ["candidate_set_incomplete"]),
        ("clarify", 2, 2, ["multiple_eligible_candidates"]),
        ("wait", 1, 0, ["active_thread_present"]),
        ("wait", 2, 1, ["active_thread_present"]),
        ("decline", 1, 0, ["contended_thread_present"]),
        ("decline", 1, 0, ["unavailable_thread_present"]),
        ("decline", 1, 0, ["runtime_state_inconsistent"]),
        (
            "decline",
            1,
            0,
            [
                "contended_thread_present",
                "unavailable_thread_present",
                "runtime_state_inconsistent",
            ],
        ),
    ],
)
async def test_runtime_client_accepts_coherent_continuation_reason_shapes(
    outcome,
    candidate_count,
    eligible_count,
    reason_codes,
):
    client = RuntimeClient("http://runtime.local", None)
    candidates = _continuation_candidates(candidate_count)
    response = _continuation_response(outcome)
    response["result"].update(
        candidate_count=candidate_count,
        eligible_candidate_count=eligible_count,
        reason_codes=reason_codes,
    )
    calls = []

    async def fake_post(path, *, json):
        calls.append((path, json))
        return response

    client._post = fake_post  # type: ignore[method-assign]
    actual = await client.select_continuation(
        request_id="selection-request",
        owner_id="owner",
        surface="voice",
        candidate_set_complete=reason_codes != ["candidate_set_incomplete"],
        stale_after_seconds=1800,
        candidates=candidates,
    )

    assert actual == response
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "candidate_count", "eligible_count", "reason_codes"),
    [
        (
            "resume",
            1,
            1,
            ["one_eligible_candidate", "candidate_stale"],
        ),
        ("resume", 1, 0, ["one_eligible_candidate"]),
        (
            "create_new",
            1,
            0,
            ["no_eligible_candidates", "active_thread_present"],
        ),
        (
            "create_new",
            1,
            0,
            ["no_eligible_candidates", "contended_thread_present"],
        ),
        (
            "create_new",
            1,
            0,
            ["no_eligible_candidates", "unavailable_thread_present"],
        ),
        (
            "create_new",
            1,
            0,
            ["no_eligible_candidates", "runtime_state_inconsistent"],
        ),
        ("create_new", 1, 0, ["no_candidates"]),
        ("create_new", 0, 0, ["no_eligible_candidates"]),
        (
            "create_new",
            1,
            0,
            ["no_candidates", "no_eligible_candidates"],
        ),
        ("create_new", 1, 1, ["no_eligible_candidates"]),
        (
            "create_new",
            2,
            0,
            [
                "no_eligible_candidates",
                "runtime_state_missing",
                "candidate_not_open",
            ],
        ),
        (
            "clarify",
            2,
            2,
            ["multiple_eligible_candidates", "unavailable_thread_present"],
        ),
        (
            "clarify",
            1,
            0,
            ["candidate_set_incomplete", "active_thread_present"],
        ),
        ("clarify", 1, 1, ["candidate_set_incomplete"]),
        ("clarify", 2, 0, ["multiple_eligible_candidates"]),
        ("clarify", 2, 1, ["multiple_eligible_candidates"]),
        (
            "clarify",
            2,
            2,
            ["candidate_set_incomplete", "multiple_eligible_candidates"],
        ),
        (
            "wait",
            1,
            0,
            ["active_thread_present", "runtime_state_inconsistent"],
        ),
        (
            "wait",
            1,
            0,
            ["active_thread_present", "unavailable_thread_present"],
        ),
        (
            "wait",
            1,
            0,
            ["active_thread_present", "multiple_eligible_candidates"],
        ),
        ("wait", 1, 0, ["candidate_stale"]),
        (
            "decline",
            1,
            0,
            ["contended_thread_present", "active_thread_present"],
        ),
        (
            "decline",
            1,
            0,
            ["contended_thread_present", "candidate_stale"],
        ),
        (
            "decline",
            1,
            0,
            ["contended_thread_present", "multiple_eligible_candidates"],
        ),
        ("decline", 1, 0, ["active_thread_present"]),
        (
            "decline",
            1,
            0,
            ["runtime_state_inconsistent", "contended_thread_present"],
        ),
    ],
)
async def test_runtime_client_rejects_contradictory_continuation_reason_shapes(
    outcome,
    candidate_count,
    eligible_count,
    reason_codes,
):
    client = RuntimeClient("http://runtime.local", None)
    candidates = _continuation_candidates(candidate_count)
    response = _continuation_response(outcome)
    response["result"].update(
        candidate_count=candidate_count,
        eligible_candidate_count=eligible_count,
        reason_codes=reason_codes,
    )
    original_response = deepcopy(response)
    calls = []

    async def fake_post(path, *, json):
        calls.append((path, json))
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(
        RuntimeError,
        match="^continuation_selection_response_invalid$",
    ):
        await client.select_continuation(
            request_id="selection-request",
            owner_id="owner",
            surface="voice",
            candidate_set_complete=reason_codes != ["candidate_set_incomplete"],
            stale_after_seconds=1800,
            candidates=candidates,
        )

    assert len(calls) == 1
    assert response == original_response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate,expected_error",
    [
        (lambda response: response.update(schema_version="wrong"), "context_mismatch"),
        (lambda response: response.update(request_id="other"), "context_mismatch"),
        (
            lambda response: response["result"].update(timing_policy="answer_now"),
            "invalid",
        ),
        (lambda response: response["result"].update(candidate_count=2), "invalid"),
        (lambda response: response["result"].update(eligible_candidate_count=True), "invalid"),
        (lambda response: response["result"].update(reason_codes=["unknown"]), "invalid"),
        (lambda response: response["result"].update(policy_version="wrong"), "invalid"),
        (
            lambda response: response["result"].update(
                selected_conversation_id="00000000-0000-4000-8000-000000000099"
            ),
            "context_mismatch",
        ),
        (
            lambda response: response["result"].update(selected_thread_revision=-1),
            "context_mismatch",
        ),
        (lambda response: response.update(extra=True), "invalid"),
        (lambda response: response["result"].update(extra=True), "invalid"),
    ],
)
async def test_runtime_client_rejects_invalid_continuation_responses(
    mutate,
    expected_error,
):
    client = RuntimeClient("http://runtime.local", None)
    response = _continuation_response()
    mutate(response)

    async def fake_post(path, *, json):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(
        RuntimeError,
        match=f"^continuation_selection_response_{expected_error}$",
    ):
        await client.select_continuation(
            request_id="selection-request",
            owner_id="owner",
            surface="voice",
            candidate_set_complete=True,
            stale_after_seconds=1800,
            candidates=[
                {
                    "conversation_id": "00000000-0000-4000-8000-000000000001",
                    "lifecycle_state": "open",
                    "durable_updated_at": "2026-08-01T00:00:00+00:00",
                }
            ],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"candidate_set_complete": 1},
        {"stale_after_seconds": True},
        {"stale_after_seconds": 59},
        {
            "candidates": [
                {
                    "conversation_id": "not-a-uuid",
                    "lifecycle_state": "open",
                    "durable_updated_at": "2026-08-01T00:00:00+00:00",
                }
            ]
        },
        {
            "candidates": [
                {
                    "conversation_id": "00000000-0000-4000-8000-000000000001",
                    "lifecycle_state": "open",
                    "durable_updated_at": "2026-08-01T00:00:00",
                }
            ]
        },
        {
            "candidates": [
                {
                    "conversation_id": "00000000-0000-4000-8000-000000000001",
                    "lifecycle_state": "open",
                    "durable_updated_at": "2026-08-01T00:00:00+00:00",
                    "title": "forbidden",
                }
            ]
        },
    ],
)
async def test_runtime_client_rejects_invalid_selection_request_before_transport(
    overrides,
):
    client = RuntimeClient("http://runtime.local", None)
    called = False

    async def fake_post(path, *, json):
        nonlocal called
        called = True
        return _continuation_response()

    client._post = fake_post  # type: ignore[method-assign]
    arguments = {
        "request_id": "selection-request",
        "owner_id": "owner",
        "surface": "voice",
        "candidate_set_complete": True,
        "stale_after_seconds": 1800,
        "candidates": [],
    }
    arguments.update(overrides)
    with pytest.raises(ValueError, match="^continuation_selection_request_invalid$"):
        await client.select_continuation(**arguments)
    assert called is False


@pytest.mark.asyncio
async def test_runtime_client_sends_expected_revision_only_when_supplied():
    client = RuntimeClient("http://runtime.local", None)
    calls = []

    async def fake_post(path, *, json):
        calls.append(json)
        return _with_fresh_return_event({
            "runtime_session": {
                "runtime_session_id": "session",
                "owner_id": json["owner_id"],
                "conversation_id": json["conversation_id"],
                "surface": json["surface"],
            },
            "runtime_turn": {
                "runtime_turn_id": "turn",
                "runtime_session_id": "session",
                "input_message_id": json.get("input_message_id"),
                "turn_status": "received",
            },
        }, json["request_id"])

    client._post = fake_post  # type: ignore[method-assign]
    common = {
        "request_id": "request",
        "owner_id": "owner",
        "conversation_id": "conversation",
        "surface": "web",
    }
    await client.start_turn(**common)
    await client.start_turn(**common, expected_thread_revision=7)

    assert "expected_thread_revision" not in calls[0]
    assert calls[1]["expected_thread_revision"] == 7
    with pytest.raises(ValueError, match="^expected_thread_revision_invalid$"):
        await client.start_turn(**common, expected_thread_revision=True)
    assert len(calls) == 2


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

        async def reconcile_interrupted_turns(self, request_id):
            return {"interrupted_count": 0}

    class Memory:
        async def reconcile_interrupted_work(self):
            return {"interrupted_count": 0}

    configured = ManagedRuntime()
    replacement = ManagedRuntime()
    monkeypatch.setattr(main, "runtime", configured)
    monkeypatch.setattr(main, "memory_store", Memory())

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
        async def reconcile_interrupted_work(self):
            return {"interrupted_count": 0}

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


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [
    {"interrupted_count": 0}, {"interrupted_count": 4},
    None, [], {}, {"interrupted_count": True}, {"interrupted_count": -1},
    {"interrupted_count": 1.0}, {"interrupted_count": "1"},
    {"interrupted_count": 0, "turns": []},
])
async def test_reconcile_turns_uses_open_pool_strict_count_and_no_retry(monkeypatch, response):
    http = _FakeAsyncClient([response])
    factory = _ClientFactory([http])
    monkeypatch.setattr(httpx, "AsyncClient", factory)
    client = RuntimeClient("http://runtime", "key")
    await client.open()
    try:
        if response in ({"interrupted_count": 0}, {"interrupted_count": 4}):
            assert await client.reconcile_interrupted_turns("startup-request") == response
        else:
            with pytest.raises(RuntimeError, match="runtime_reconciliation_response_invalid"):
                await client.reconcile_interrupted_turns("startup-request")
        assert http.posts == [(
            "/v1/runtime/turns/reconcile-interrupted", {"request_id": "startup-request"},
        )]
        assert len(factory.clients) == 1
    finally:
        await client.close()


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
async def test_compile_evidence_plan_forwards_optional_aggregate_spec_only_when_given():
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
    common = {
        **scope,
        "question_anchor": "Verify the record.",
        "task_shape": "targeted_lookup",
        "declared_scope": {
            "source_ids": ["source_a"],
            "source_categories": [],
            "exact_source_refs": [],
            "inventory_status": "complete_for_declared_scope",
        },
        "source_inventory": [
            {
                "source_id": "source_a",
                "source_categories": ["records"],
                "capabilities": ["targeted_retrieval"],
                "availability": "available",
                "authority_role": "authoritative",
            }
        ],
    }

    async def fake_post(path, *, json):
        calls.append((path, json))
        return {**scope, "result": {}}

    client._post = fake_post  # type: ignore[method-assign]
    await client.compile_evidence_plan(**common)

    aggregate = deepcopy(common)
    aggregate["task_shape"] = "aggregate"
    aggregate["source_inventory"][0]["content_fields"] = [
        "Date",
        "Fuel (L)",
        "Odometer",
    ]
    aggregate_spec = {"function": "median", "field_name": "Fuel (L)"}
    await client.compile_evidence_plan(
        **aggregate,
        aggregate_spec=aggregate_spec,
    )

    assert calls[0] == (
        "/v1/runtime/evidence-plans/compile",
        common,
    )
    assert "aggregate_spec" not in calls[0][1]
    assert calls[1] == (
        "/v1/runtime/evidence-plans/compile",
        {**aggregate, "aggregate_spec": aggregate_spec},
    )


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
            return _with_fresh_return_event({
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
            }, json["request_id"])
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


def _situated_request() -> dict[str, Any]:
    return {
        "request_id": "rid:situated",
        "owner_id": "owner",
        "conversation_id": "conv",
        "surface": "telegram",
        "runtime_session_id": "rtsession_1",
        "runtime_turn_id": "rtturn_1",
        "surface_context": {"visibility": "private", "constraint": "normal"},
        "interaction_governance": {
            "interaction_kind": "joke_or_playful",
            "tension_level": "low",
            "commentary_allowed": True,
            "humor_allowed": True,
            "action_allowed": False,
            "requires_confirmation": False,
            "privacy_sensitivity_hint": "normal",
            "response_posture": "playful",
            "confidence": 0.9,
        },
        "restraint": {
            "restraint_policy": "answer_normally",
            "proactive_output_suppressed": True,
            "personalization_suppressed": True,
            "brevity_preferred": False,
            "clarification_preferred": False,
            "confidence": 0.9,
        },
    }


def _situated_response(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "situated-presence.v1",
        **{
            field: request[field]
            for field in (
                "request_id",
                "owner_id",
                "conversation_id",
                "surface",
                "runtime_session_id",
                "runtime_turn_id",
            )
        },
        "result": {
            "commentary_allowed": True,
            "humor_allowed": True,
            "emotional_attunement_allowed": "none",
            "challenge_allowed": "low",
            "silence_preferred": False,
            "surface_allows_commentary": True,
            "response_posture": "playful",
            "action_implication_allowed": False,
            "reason_summary": [
                "light_commentary_allowed",
                "proactive_output_suppressed",
                "personalization_suppressed",
            ],
            "policy_version": "situated-presence.v1",
        },
    }


def _valid_situated_case(case: str) -> tuple[dict[str, Any], dict[str, Any]]:
    request = _situated_request()
    result = deepcopy(_situated_response(request)["result"])
    governance = request["interaction_governance"]
    restraint = request["restraint"]

    if case == "low_confidence":
        governance["confidence"] = 0.59
        result.update(
            commentary_allowed=False,
            humor_allowed=False,
            emotional_attunement_allowed="none",
            challenge_allowed="none",
            silence_preferred=True,
            response_posture="silent_or_minimal",
            reason_summary=["upstream_confidence_insufficient"],
        )
    elif case == "tense":
        governance.update(
            interaction_kind="tense_debugging",
            tension_level="high",
            response_posture="tactical",
        )
        restraint["personalization_suppressed"] = False
        result.update(
            commentary_allowed=False,
            humor_allowed=False,
            emotional_attunement_allowed="minimal",
            challenge_allowed="medium",
            silence_preferred=False,
            response_posture="tactical",
            reason_summary=[
                "tense_context",
                "tactical_response_required",
                "proactive_output_suppressed",
            ],
        )
    elif case == "high_impact":
        governance.update(
            interaction_kind="high_impact_decision",
            response_posture="direct",
        )
        restraint["personalization_suppressed"] = False
        result.update(
            commentary_allowed=False,
            humor_allowed=False,
            emotional_attunement_allowed="minimal",
            challenge_allowed="low",
            silence_preferred=False,
            response_posture="direct",
            reason_summary=[
                "high_impact_context",
                "proactive_output_suppressed",
            ],
        )
    elif case == "vent":
        governance.update(
            interaction_kind="vent_or_expression",
            commentary_allowed=False,
            humor_allowed=False,
            response_posture="supportive",
        )
        result.update(
            commentary_allowed=False,
            humor_allowed=False,
            emotional_attunement_allowed="brief",
            challenge_allowed="none",
            silence_preferred=False,
            response_posture="brief",
            reason_summary=[
                "brief_steadying_allowed",
                "proactive_output_suppressed",
                "personalization_suppressed",
                "upstream_commentary_suppressed",
                "upstream_humor_suppressed",
            ],
        )
    elif case == "mistake":
        governance.update(
            interaction_kind="mistake_or_failure_report",
            commentary_allowed=False,
            humor_allowed=False,
            requires_confirmation=True,
            privacy_sensitivity_hint="private",
            response_posture="supportive",
        )
        result.update(
            commentary_allowed=False,
            humor_allowed=False,
            emotional_attunement_allowed="brief",
            challenge_allowed="low",
            silence_preferred=False,
            response_posture="brief",
            reason_summary=[
                "brief_steadying_allowed",
                "privacy_sensitive",
                "proactive_output_suppressed",
                "personalization_suppressed",
                "confirmation_required",
                "upstream_commentary_suppressed",
                "upstream_humor_suppressed",
            ],
        )
    elif case == "ambiguous":
        governance.update(
            interaction_kind="ambiguous",
            commentary_allowed=False,
            humor_allowed=False,
            response_posture="silent_or_minimal",
        )
        result.update(
            commentary_allowed=False,
            humor_allowed=False,
            emotional_attunement_allowed="none",
            challenge_allowed="none",
            silence_preferred=True,
            response_posture="silent_or_minimal",
            reason_summary=[
                "ambiguous_context",
                "proactive_output_suppressed",
                "personalization_suppressed",
                "upstream_commentary_suppressed",
                "upstream_humor_suppressed",
            ],
        )
    elif case in {"command", "question", "brainstorm"}:
        governance.update(
            interaction_kind=case,
            commentary_allowed=False,
            humor_allowed=False,
            response_posture="reflective" if case == "brainstorm" else "direct",
        )
        restraint.update(
            proactive_output_suppressed=False,
            personalization_suppressed=False,
        )
        result.update(
            commentary_allowed=False,
            humor_allowed=False,
            emotional_attunement_allowed="none",
            challenge_allowed="low" if case == "brainstorm" else "none",
            silence_preferred=False,
            response_posture="reflective" if case == "brainstorm" else "direct",
            reason_summary=[
                "upstream_commentary_suppressed",
                "upstream_humor_suppressed",
            ],
        )
    elif case != "playful":
        raise AssertionError(f"unknown situated test case: {case}")

    response = _situated_response(request)
    response["result"] = result
    return request, response


@pytest.mark.asyncio
async def test_situated_presence_posts_compact_projection_and_accepts_valid_result():
    client = RuntimeClient("http://runtime.local", None)
    request = _situated_request()
    calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_post(path: str, *, json: dict[str, Any]):
        calls.append((path, json))
        return _situated_response(request)

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.evaluate_situated_presence(**request)

    assert response["result"]["humor_allowed"] is True
    assert calls == [("/v1/runtime/situated-presence/evaluate", request)]
    assert "current_user_text" not in str(calls)
    assert "recent_messages" not in str(calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        lambda response: response.update(schema_version="wrong"),
        lambda response: response.update(owner_id="other"),
        lambda response: response["result"].update(extra=True),
        lambda response: response["result"].update(action_implication_allowed=True),
        lambda response: response["result"].update(commentary_allowed=False),
        lambda response: response["result"].update(silence_preferred=True),
        lambda response: response["result"].update(
            reason_summary=[
                "personalization_suppressed",
                "light_commentary_allowed",
            ]
        ),
    ],
)
async def test_situated_presence_rejects_malformed_or_loosening_results(mutate):
    client = RuntimeClient("http://runtime.local", None)
    request = _situated_request()
    response = _situated_response(request)
    mutate(response)
    calls = 0

    async def fake_post(path: str, *, json: dict[str, Any]):
        nonlocal calls
        calls += 1
        return response

    client._post = fake_post  # type: ignore[method-assign]
    expected = (
        "situated_presence_response_context_mismatch"
        if response.get("owner_id") == "other"
        else "situated_presence_response_invalid"
    )
    with pytest.raises(RuntimeError, match=expected):
        await client.evaluate_situated_presence(**request)
    assert calls == 1


@pytest.mark.asyncio
async def test_situated_presence_rejects_non_strict_request_before_transport():
    client = RuntimeClient("http://runtime.local", None)
    request = _situated_request()
    request["interaction_governance"] = {
        **request["interaction_governance"],
        "commentary_allowed": 1,
    }
    calls = 0

    async def fake_post(path: str, *, json: dict[str, Any]):
        nonlocal calls
        calls += 1
        return {}

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="situated_presence_request_invalid"):
        await client.evaluate_situated_presence(**request)
    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "playful",
        "command",
        "question",
        "brainstorm",
        "vent",
        "mistake",
        "tense",
        "high_impact",
        "ambiguous",
        "low_confidence",
    ],
)
async def test_situated_presence_accepts_representative_pinned_results(case):
    client = RuntimeClient("http://runtime.local", None)
    request, response = _valid_situated_case(case)
    calls = 0

    async def fake_post(path: str, *, json: dict[str, Any]):
        nonlocal calls
        calls += 1
        return response

    client._post = fake_post  # type: ignore[method-assign]
    assert await client.evaluate_situated_presence(**request) == response
    assert calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "medium_tension_commentary",
        "confirmation_commentary",
        "private_hint_commentary",
        "non_playful_humor",
        "command_attunement",
        "question_attunement",
        "brainstorm_attunement",
        "playful_attunement",
        "public_brief_attunement",
        "shared_brief_attunement",
        "unknown_brief_attunement",
        "constrained_brief_attunement",
        "personalization_tense_attunement",
        "playful_challenge_above_maximum",
        "command_challenge",
        "question_challenge",
        "vent_challenge",
        "non_silent_ambiguous",
        "tense_silence",
        "high_impact_silence",
        "vent_silence",
        "mistake_silence",
        "contradictory_posture",
        "missing_required_reason",
        "contradictory_extra_reason",
    ],
)
async def test_situated_presence_rejects_pinned_contract_contradictions(case):
    client = RuntimeClient("http://runtime.local", None)
    base_case = "playful"
    if case.startswith(("command_", "question_", "brainstorm_", "vent_")):
        base_case = case.split("_", 1)[0]
    elif case.startswith("mistake_"):
        base_case = "mistake"
    elif case.startswith("tense_") or case in {
        "personalization_tense_attunement",
        "contradictory_posture",
    }:
        base_case = "tense"
    elif case.startswith("high_impact_"):
        base_case = "high_impact"
    elif case == "non_silent_ambiguous":
        base_case = "ambiguous"
    elif case.endswith("brief_attunement"):
        base_case = "vent"

    request, response = _valid_situated_case(base_case)
    governance = request["interaction_governance"]
    restraint = request["restraint"]
    result = response["result"]
    if case == "medium_tension_commentary":
        governance["tension_level"] = "medium"
    elif case == "confirmation_commentary":
        governance["requires_confirmation"] = True
    elif case == "private_hint_commentary":
        governance["privacy_sensitivity_hint"] = "private"
    elif case == "non_playful_humor":
        governance["interaction_kind"] = "question"
    elif case.endswith("_attunement") and not case.startswith(
        ("public_", "shared_", "unknown_", "constrained_", "personalization_")
    ):
        result["emotional_attunement_allowed"] = "brief"
    elif case.endswith("brief_attunement"):
        if case.startswith("constrained_"):
            request["surface_context"]["constraint"] = "constrained"
        else:
            request["surface_context"]["visibility"] = case.split("_", 1)[0]
        result["surface_allows_commentary"] = False
    elif case == "personalization_tense_attunement":
        restraint["personalization_suppressed"] = True
    elif case == "playful_challenge_above_maximum":
        result["challenge_allowed"] = "medium"
    elif case.endswith("_challenge"):
        result["challenge_allowed"] = "low"
    elif case == "non_silent_ambiguous":
        result["silence_preferred"] = False
    elif case.endswith("_silence"):
        result["silence_preferred"] = True
    elif case == "contradictory_posture":
        result["response_posture"] = "direct"
    elif case == "missing_required_reason":
        result["reason_summary"].remove("proactive_output_suppressed")
    else:
        result["reason_summary"] = [
            "high_impact_context",
            *result["reason_summary"],
        ]

    original_request = deepcopy(request)
    original_response = deepcopy(response)

    calls = 0

    async def fake_post(path: str, *, json: dict[str, Any]):
        nonlocal calls
        calls += 1
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="situated_presence_response_invalid"):
        await client.evaluate_situated_presence(**request)
    assert calls == 1
    assert request == original_request
    assert response == original_response


@pytest.mark.asyncio
async def test_runtime_thread_resolution_posts_exact_scope_and_validates_projection():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def fake_post(path: str, *, json: dict[str, Any]):
        calls.append((path, json))
        return _runtime_thread_projection()

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.resolve_thread(
        request_id="thread-request",
        owner_id="owner",
        conversation_id="conversation",
    )

    assert response == _runtime_thread_projection()
    assert calls == [
        (
            "/v1/runtime/threads/resolve",
            {
                "request_id": "thread-request",
                "owner_id": "owner",
                "conversation_id": "conversation",
            },
        )
    ]
    assert datetime.fromisoformat(response["last_activity_at"]).utcoffset() is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda response: response.update(extra=True),
        lambda response: response.update(owner_id="other"),
        lambda response: response.update(state="unknown"),
        lambda response: response.update(revision=True),
        lambda response: response.update(participating_session_count=1),
        lambda response: response.update(last_activity_at="2026-08-01T12:00:00"),
        lambda response: response.update(updated_at="malformed"),
        lambda response: response.update(
            state="active",
            active_runtime_session_id=None,
            active_runtime_turn_id="turn",
            active_surface="voice",
        ),
    ],
)
async def test_runtime_thread_resolution_rejects_malformed_or_mismatched_projection(
    mutation,
):
    client = RuntimeClient("http://runtime.local", None)
    response = _runtime_thread_projection()
    mutation(response)

    async def fake_post(path: str, *, json: dict[str, Any]):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="runtime_thread_response"):
        await client.resolve_thread(
            request_id="thread-request",
            owner_id="owner",
            conversation_id="conversation",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["reserved", "wait", "decline"])
async def test_runtime_retirement_reserve_posts_exact_aware_facts_and_validates_result(
    outcome,
):
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, Any]]] = []
    durable_updated_at = datetime.fromisoformat("2026-08-01T07:00:00-05:00")
    retirement_before = datetime.fromisoformat("2026-08-02T12:00:00+00:00")

    async def fake_post(path: str, *, json: dict[str, Any]):
        calls.append((path, json))
        return _retirement_reservation_response(outcome)

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.reserve_retirement(
        request_id="retirement-request",
        owner_id="owner",
        conversation_id="conversation",
        lifecycle_state="open",
        durable_updated_at=durable_updated_at,
        retirement_before=retirement_before,
    )

    assert response == _retirement_reservation_response(outcome)
    assert calls == [
        (
            "/v1/runtime/retirements/reserve",
            {
                "request_id": "retirement-request",
                "owner_id": "owner",
                "conversation_id": "conversation",
                "lifecycle_state": "open",
                "durable_updated_at": durable_updated_at.isoformat(),
                "retirement_before": retirement_before.isoformat(),
            },
        )
    ]


@pytest.mark.asyncio
async def test_runtime_retirement_reserve_rejects_naive_time_before_transport():
    client = RuntimeClient("http://runtime.local", None)
    calls = 0

    async def fake_post(path: str, *, json: dict[str, Any]):
        nonlocal calls
        calls += 1
        return {}

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="runtime_timestamp_timezone_required"):
        await client.reserve_retirement(
            request_id="retirement-request",
            owner_id="owner",
            conversation_id="conversation",
            lifecycle_state="open",
            durable_updated_at=datetime(2026, 8, 1, 12),
            retirement_before=datetime.fromisoformat("2026-08-02T12:00:00+00:00"),
        )
    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda response: response.update(extra=True),
        lambda response: response.update(owner_id="other"),
        lambda response: response["result"].update(extra=True),
        lambda response: response["result"].update(policy_version="wrong"),
        lambda response: response["result"].update(reserved_thread_revision=True),
        lambda response: response["result"].update(
            reserved_durable_updated_at="2026-08-01T12:00:00"
        ),
        lambda response: response["result"].update(
            outcome="wait", reason_codes=["safe_idle_retirement_reserved"]
        ),
    ],
)
async def test_runtime_retirement_reserve_rejects_malformed_or_loosening_result(
    mutation,
):
    client = RuntimeClient("http://runtime.local", None)
    response = _retirement_reservation_response("reserved")
    mutation(response)

    async def fake_post(path: str, *, json: dict[str, Any]):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="retirement_reservation_response"):
        await client.reserve_retirement(
            request_id="retirement-request",
            owner_id="owner",
            conversation_id="conversation",
            lifecycle_state="open",
            durable_updated_at=datetime.fromisoformat("2026-08-01T12:00:00+00:00"),
            retirement_before=datetime.fromisoformat("2026-08-02T12:00:00+00:00"),
        )


@pytest.mark.asyncio
async def test_runtime_retirement_cancel_and_finalize_validate_identity_and_revision():
    client = RuntimeClient("http://runtime.local", None)
    calls: list[tuple[str, dict[str, Any]]] = []
    common = {
        "request_id": "retirement-request",
        "owner_id": "owner",
        "conversation_id": "conversation",
        "reservation_id": "retirement-reservation",
        "reserved_thread_revision": 7,
    }
    responses = [
        {
            "schema_version": "runtime-retirement-cancellation.v1",
            "request_id": "retirement-request",
            "owner_id": "owner",
            "conversation_id": "conversation",
            "reservation_id": "retirement-reservation",
            "thread_revision": 7,
            "outcome": "cancelled",
        },
        {
            "schema_version": "runtime-retirement-finalization.v1",
            "request_id": "retirement-request",
            "owner_id": "owner",
            "conversation_id": "conversation",
            "reservation_id": "retirement-reservation",
            "previous_thread_revision": 7,
            "fenced_thread_revision": 8,
            "outcome": "finalized",
        },
    ]

    async def fake_post(path: str, *, json: dict[str, Any]):
        calls.append((path, json))
        return responses.pop(0)

    client._post = fake_post  # type: ignore[method-assign]
    cancelled = await client.cancel_retirement(**common)
    finalized = await client.finalize_retirement(**common)

    assert cancelled["thread_revision"] == 7
    assert finalized["previous_thread_revision"] == 7
    assert finalized["fenced_thread_revision"] == 8
    expected_payload = {
        **{key: value for key, value in common.items() if key != "reserved_thread_revision"},
        "reserved_thread_revision": 7,
    }
    assert calls == [
        ("/v1/runtime/retirements/cancel", expected_payload),
        ("/v1/runtime/retirements/finalize", expected_payload),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "response"),
    [
        (
            "cancel_retirement",
            {
                "schema_version": "runtime-retirement-cancellation.v1",
                "request_id": "retirement-request",
                "owner_id": "owner",
                "conversation_id": "conversation",
                "reservation_id": "wrong",
                "thread_revision": 7,
                "outcome": "cancelled",
            },
        ),
        (
            "finalize_retirement",
            {
                "schema_version": "runtime-retirement-finalization.v1",
                "request_id": "retirement-request",
                "owner_id": "owner",
                "conversation_id": "conversation",
                "reservation_id": "retirement-reservation",
                "previous_thread_revision": 7,
                "fenced_thread_revision": 9,
                "outcome": "finalized",
            },
        ),
    ],
)
async def test_runtime_retirement_mutations_reject_mismatched_or_invalid_result(
    operation,
    response,
):
    client = RuntimeClient("http://runtime.local", None)

    async def fake_post(path: str, *, json: dict[str, Any]):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="retirement_"):
        await getattr(client, operation)(
            request_id="retirement-request",
            owner_id="owner",
            conversation_id="conversation",
            reservation_id="retirement-reservation",
            reserved_thread_revision=7,
        )


_PRESENCE_SCOPE = {
    "request_id": "presence-request", "owner_id": "owner",
    "conversation_id": "conversation", "surface": "web",
    "runtime_session_id": "session", "runtime_turn_id": "turn",
}


def _presence_response(**updates):
    result = {
        "presence_state": "active_conversation", "previous_presence_state": None,
        "state_changed": True, "proactive_output_suppressed": False,
        "required_help_allowed": True, "reason_codes": ["thread_active"],
        "policy_version": "runtime-presence.v1",
    }
    result.update(updates)
    return {**_PRESENCE_SCOPE, "result": result}


@pytest.mark.asyncio
async def test_presence_endpoint_payload_and_persistent_transport():
    response = _presence_response()
    transport = _FakeAsyncClient([response, response])
    factory = _ClientFactory([transport])
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    await client.open()
    for _ in range(2):
        assert await client.evaluate_presence(surface_permission_status="configured",
            proactive_presence_allowed=True, **_PRESENCE_SCOPE) == response
    assert transport.posts == [("/v1/runtime/presence/evaluate", {
        **_PRESENCE_SCOPE, "active_task_mode": False, "proactive_output_suppressed": False,
        "explicit_proactive_opt_out": False,
        "surface_permission_status": "configured", "proactive_presence_allowed": True,
        "ambient_listening_allowed": False,
    })] * 2
    assert len(factory.clients) == 1
    await client.close()
    assert transport.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field", list(_PRESENCE_SCOPE))
@pytest.mark.parametrize("value", [None, "", "   ", 1, True, "x" * 121])
async def test_presence_request_scope_rejected_before_transport(field, value):
    factory = _ClientFactory()
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    with pytest.raises(ValueError, match="presence_request_invalid"):
        await client.evaluate_presence(**{**_PRESENCE_SCOPE, field: value})
    assert factory.clients == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [
    ("surface", "x" * 65), ("active_task_mode", 1), ("active_task_mode", "true"),
    ("active_task_mode", None), ("proactive_output_suppressed", 0),
    ("proactive_output_suppressed", "false"), ("proactive_output_suppressed", None),
])
async def test_presence_request_controls_rejected_before_transport(field, value):
    factory = _ClientFactory()
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    with pytest.raises(ValueError, match="presence_request_invalid"):
        await client.evaluate_presence(**{**_PRESENCE_SCOPE, field: value})
    assert factory.clients == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", list(_PRESENCE_SCOPE))
async def test_presence_response_exact_scope_binding(field):
    response = _presence_response()
    response[field] = "other"
    later_scope = {
        **_PRESENCE_SCOPE, "request_id": "later-request", "runtime_turn_id": "later-turn",
    }
    later_response = {**_presence_response(), **later_scope}
    transport = _FakeAsyncClient([response, later_response])
    factory = _ClientFactory([transport])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=factory)
    await client.open()
    try:
        with pytest.raises(RuntimeError, match="presence_response_context_mismatch"):
            await client.evaluate_presence(surface_permission_status="configured",
            proactive_presence_allowed=True, **_PRESENCE_SCOPE)
        assert len(transport.posts) == 1
        assert transport.close_calls == 0
        assert await client.evaluate_presence(surface_permission_status="configured",
            proactive_presence_allowed=True, **later_scope) == later_response
        assert len(factory.clients) == 1
        assert len(transport.posts) == 2
        assert transport.posts[1][1]["runtime_turn_id"] == "later-turn"
        assert transport.close_calls == 0
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [
    ("presence_state", "unknown"), ("presence_state", []),
    ("previous_presence_state", "unknown"), ("previous_presence_state", {}),
    ("state_changed", 1), ("state_changed", False),
    ("previous_presence_state", "active_conversation"),
    ("proactive_output_suppressed", "false"), ("proactive_output_suppressed", True),
    ("required_help_allowed", 1), ("required_help_allowed", False),
    ("reason_codes", []), ("reason_codes", ["unknown"]),
    ("reason_codes", ["thread_active", "thread_active"]),
    ("reason_codes", ["thread_active", "proactive_suppression_requested", "session_idle"]),
    ("reason_codes", ["session_idle"]), ("reason_codes", ["thread_active", {}]),
    ("reason_codes", ["thread_active", "proactive_suppression_requested"]),
    ("policy_version", "runtime-presence.v2"),
])
async def test_presence_response_rejects_invalid_or_incoherent_result(field, value):
    transport = _FakeAsyncClient([_presence_response(**{field: value})])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    with pytest.raises(RuntimeError, match="presence_response_invalid"):
        await client.evaluate_presence(surface_permission_status="configured",
            proactive_presence_allowed=True, **_PRESENCE_SCOPE)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["extra", "missing", "result_extra", "result_missing", "list"])
async def test_presence_response_exact_key_shape(mutation):
    response = _presence_response()
    if mutation == "extra":
        response["private"] = "not accepted"
    elif mutation == "missing":
        response.pop("surface")
    elif mutation == "result_extra":
        response["result"]["private"] = "not accepted"
    elif mutation == "result_missing":
        response["result"].pop("policy_version")
    else:
        response["result"] = []
    transport = _FakeAsyncClient([response])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    with pytest.raises(RuntimeError, match="presence_response_invalid"):
        await client.evaluate_presence(surface_permission_status="configured",
            proactive_presence_allowed=True, **_PRESENCE_SCOPE)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["returning_after_gap"])
async def test_return_presence_without_matching_reason_is_not_consumed(state):
    transport = _FakeAsyncClient([_presence_response(presence_state=state)])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    with pytest.raises(RuntimeError, match="presence_response_invalid"):
        await client.evaluate_presence(surface_permission_status="configured",
            proactive_presence_allowed=True, **_PRESENCE_SCOPE)


@pytest.mark.asyncio
@pytest.mark.parametrize("state,reason,active", [
    ("not_present", "session_not_present", False),
    ("available", "session_available", False),
    ("active_conversation", "thread_active", False),
    ("idle", "session_idle", False),
    ("idle", "attention_idle", False),
    ("returning_after_gap", "return_gap_elapsed", False),
    ("low_attention", "session_paused", False),
    ("low_attention", "attention_paused", False),
    ("driving_or_active_task", "session_active_task_mode", False),
    ("driving_or_active_task", "active_task_mode", True),
])
@pytest.mark.parametrize("suppressed", [False, True])
async def test_presence_accepts_coherent_v1_states_and_transitions(
    state, reason, active, suppressed,
):
    response = _presence_response(
        presence_state=state, previous_presence_state=state, state_changed=False,
        proactive_output_suppressed=(
            suppressed or state not in {"available", "active_conversation", "returning_after_gap"}
        ),
        required_help_allowed=state != "not_present",
        reason_codes=[reason] + (["proactive_suppression_requested"] if suppressed else []),
    )
    transport = _FakeAsyncClient([response])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    assert await client.evaluate_presence(
        surface_permission_status="configured", proactive_presence_allowed=True,
        **_PRESENCE_SCOPE, active_task_mode=active, proactive_output_suppressed=suppressed,
    ) == response


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, 1, 0, "true", "false", {}, []])
async def test_presence_opt_out_request_is_strict_before_transport(value):
    factory = _ClientFactory()
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    with pytest.raises(ValueError, match="presence_request_invalid"):
        await client.evaluate_presence(surface_permission_status="configured",
            proactive_presence_allowed=True, **_PRESENCE_SCOPE, explicit_proactive_opt_out=value)
    assert factory.clients == []


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("suppressed", [False, True])
@pytest.mark.parametrize("state,reason", [
    ("do_not_intrude", "explicit_proactive_opt_out"),
    ("not_present", "session_not_present"),
])
async def test_presence_opt_out_precedence_and_exact_payload(active, suppressed, state, reason):
    response = _presence_response(
        presence_state=state, proactive_output_suppressed=True,
        required_help_allowed=state != "not_present",
        reason_codes=[reason] + (["proactive_suppression_requested"] if suppressed else []),
    )
    transport = _FakeAsyncClient([response])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    assert await client.evaluate_presence(
        surface_permission_status="configured", proactive_presence_allowed=True,
        **_PRESENCE_SCOPE, active_task_mode=active,
        proactive_output_suppressed=suppressed, explicit_proactive_opt_out=True,
    ) == response
    assert transport.posts == [("/v1/runtime/presence/evaluate", {
        **_PRESENCE_SCOPE, "active_task_mode": active,
        "proactive_output_suppressed": suppressed, "explicit_proactive_opt_out": True,
        "surface_permission_status": "configured", "proactive_presence_allowed": True,
        "ambient_listening_allowed": False,
    })]
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("opt_out,changes", [
    (False, {}), (True, {"proactive_output_suppressed": False}),
    (True, {"required_help_allowed": False}),
    (True, {"reason_codes": ["thread_active"]}),
    (True, {"reason_codes": ["explicit_proactive_opt_out", "proactive_suppression_requested"]}),
    (True, {"presence_state": "active_conversation", "reason_codes": ["thread_active"],
            "proactive_output_suppressed": False}),
    (True, {"presence_state": "driving_or_active_task", "reason_codes": ["active_task_mode"]}),
])
async def test_presence_opt_out_rejects_false_authority_and_incoherence(opt_out, changes):
    response = _presence_response(
        presence_state="do_not_intrude", proactive_output_suppressed=True,
        reason_codes=["explicit_proactive_opt_out"],
    )
    response["result"].update(changes)
    transport = _FakeAsyncClient([response])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    with pytest.raises(RuntimeError, match="presence_response_invalid"):
        await client.evaluate_presence(
            **_PRESENCE_SCOPE, explicit_proactive_opt_out=opt_out, active_task_mode=True,
        )
    await client.close()


def _timing_request(**changes):
    return {
        "request_id": "rid-timing", "owner_id": "owner", "conversation_id": "conv-1",
        "surface": "chat", "runtime_session_id": "session-1", "runtime_turn_id": "turn-1",
        "spoken_output": True, "active_task_mode": False, "requested_detail": "unspecified",
        "latency_budget_class": "ordinary_text", "dependency_state": "ready",
        "continuation_timing_policy": None, **changes,
    }


def _timing_response(request, policy="answer_now"):
    from clients.runtime import (
        RUNTIME_TIMING_BUDGET_MS,
        RUNTIME_TIMING_PROJECTIONS,
        RUNTIME_TIMING_REASON_POLICIES,
    )

    state, expansion, overlay = RUNTIME_TIMING_PROJECTIONS[policy]
    reason = next(reason for reason, value in RUNTIME_TIMING_REASON_POLICIES.items()
                  if value == policy and reason != "dependency_blocking")
    return {
        **{key: request[key] for key in (
            "request_id", "owner_id", "conversation_id", "surface",
            "runtime_session_id", "runtime_turn_id",
        )},
        "result": {
            "timing_policy": policy, "reason_codes": [reason],
            "latency_budget_class": request["latency_budget_class"],
            "latency_budget_ms": RUNTIME_TIMING_BUDGET_MS[request["latency_budget_class"]],
            "expansion_allowed": expansion, "continuation_state": state,
            "degradation_mode": "none", "policy_version": "runtime-timing.v1",
            "prompt_overlay": overlay, "trace_ref": "rtrace-timing-fixture",
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [
    "answer_now", "acknowledge_then_answer", "ask_clarifying_question", "pause_or_wait",
    "defer_expansion", "yield_to_user", "resume_previous_thread", "close_turn",
])
@pytest.mark.parametrize("budget,ms", [
    ("ordinary_text", 300), ("evidence_governed", 350), ("history_followup", 160),
    ("safe_action_preview", 300), ("provider_fallback", 350),
    ("voice_acknowledgment", 250), ("voice_provider_dispatch", 300),
])
async def test_timing_exact_contract_all_policies_and_budgets(policy, budget, ms):
    request = _timing_request(latency_budget_class=budget)
    response = _timing_response(request, policy)
    transport = _FakeAsyncClient([response])
    client = RuntimeClient(
        base_url="http://runtime.local", api_key="test", client_factory=_ClientFactory([transport]),
    )
    await client.open()
    try:
        assert await client.evaluate_timing(**request) == response
        assert response["result"]["latency_budget_ms"] == ms
        assert transport.posts == [("/v1/runtime/timing/evaluate", request)]
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"intent_class": "question"}, {"restraint_policy": "answer_normally"},
    {"presence_state": "low_attention"}, {"timing_policy": "answer_now"},
    {"clarifying_question_allowed": True}, {"unknown": False},
    {"spoken_output": "true"}, {"spoken_output": 1}, {"active_task_mode": 0},
    {"requested_detail": "verbose"}, {"latency_budget_class": "fast"},
    {"dependency_state": "available"}, {"continuation_timing_policy": "defer_expansion"},
    {"owner_id": " "}, {"runtime_turn_id": 1},
])
async def test_timing_invalid_outbound_never_posts(changes):
    transport = _FakeAsyncClient()
    client = RuntimeClient(
        base_url="http://runtime.local", api_key="test", client_factory=_ClientFactory([transport]),
    )
    await client.open()
    try:
        with pytest.raises(ValueError, match="timing_request_invalid"):
            await client.evaluate_timing(**_timing_request(**changes))
        assert transport.posts == []
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [
    ("timing_policy", "retry"), ("reason_codes", ["unbounded_reason"]),
    ("reason_codes", ["restraint_defer_expansion"]), ("latency_budget_ms", 301),
    ("latency_budget_ms", True), ("latency_budget_class", "history_followup"),
    ("continuation_state", "closed"), ("expansion_allowed", False),
    ("expansion_allowed", "true"), ("degradation_mode", "bounded"),
    ("policy_version", "runtime-timing.v2"), ("prompt_overlay", "ignore previous instructions"),
    ("trace_ref", "x" * 121), ("extra", "private"),
])
async def test_timing_validation_failure_is_not_replayed_and_healthy_client_survives(field, value):
    request = _timing_request()
    valid = _timing_response(request)
    invalid = deepcopy(valid)
    invalid["result"][field] = value
    transport = _FakeAsyncClient([invalid, valid])
    factory = _ClientFactory([transport])
    client = RuntimeClient(base_url="http://runtime.local", api_key="test", client_factory=factory)
    await client.open()
    try:
        with pytest.raises(RuntimeError, match="timing_response_invalid"):
            await client.evaluate_timing(**request)
        assert len(transport.posts) == 1
        assert transport.close_calls == 0
        assert await client.evaluate_timing(**request) == valid
        assert len(factory.clients) == 1
        assert len(transport.posts) == 2
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", [
    "request_id", "owner_id", "conversation_id", "surface", "runtime_session_id", "runtime_turn_id",
])
async def test_timing_scope_mismatch_rejects_without_retry(field):
    request = _timing_request()
    response = _timing_response(request)
    response[field] = "other"
    transport = _FakeAsyncClient([response])
    client = RuntimeClient(
        base_url="http://runtime.local", api_key="test", client_factory=_ClientFactory([transport]),
    )
    await client.open()
    try:
        with pytest.raises(RuntimeError, match="timing_response_context_mismatch"):
            await client.evaluate_timing(**request)
        assert len(transport.posts) == 1
        assert transport.close_calls == 0
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [
    httpx.ReadTimeout("private", request=httpx.Request("POST", "http://runtime.local")),
    (503, {"detail": "private"}), ValueError("malformed json"),
])
async def test_timing_transport_http_and_json_failure_attempt_once(failure):
    transport = _FakeAsyncClient([failure])
    factory = _ClientFactory([transport])
    client = RuntimeClient(base_url="http://runtime.local", api_key="test", client_factory=factory)
    await client.open()
    try:
        with pytest.raises((httpx.ReadTimeout, httpx.HTTPStatusError, ValueError)):
            await client.evaluate_timing(**_timing_request())
        assert len(transport.posts) == 1
        assert len(factory.clients) == 1
        assert transport.close_calls == int(isinstance(failure, httpx.ReadTimeout))
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", [
    "timing_policy", "reason_codes", "latency_budget_class", "latency_budget_ms",
    "expansion_allowed", "continuation_state", "degradation_mode", "policy_version",
    "prompt_overlay", "trace_ref",
])
async def test_timing_response_requires_every_result_key(field):
    request = _timing_request()
    response = _timing_response(request)
    del response["result"][field]
    transport = _FakeAsyncClient([response])
    client = RuntimeClient(
        base_url="http://runtime.local", api_key="test", client_factory=_ClientFactory([transport]),
    )
    await client.open()
    try:
        with pytest.raises(RuntimeError, match="timing_response_invalid"):
            await client.evaluate_timing(**request)
        assert len(transport.posts) == 1
        assert transport.close_calls == 0
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["extra_scope", "missing_scope", "scope_type", "degraded"])
async def test_timing_response_scope_keys_types_and_dependency_projection_are_strict(mutation):
    request = _timing_request()
    response = _timing_response(request)
    if mutation == "extra_scope":
        response["user_text"] = "PRIVATE-CONTENT"
    elif mutation == "missing_scope":
        del response["runtime_turn_id"]
    elif mutation == "scope_type":
        response["request_id"] = 1
    else:
        response["result"]["degradation_mode"] = "bounded"
        response["result"]["reason_codes"].append("dependency_degraded")
    transport = _FakeAsyncClient([response])
    client = RuntimeClient(
        base_url="http://runtime.local", api_key="test", client_factory=_ClientFactory([transport]),
    )
    await client.open()
    try:
        with pytest.raises(RuntimeError, match="^timing_response_invalid$"):
            await client.evaluate_timing(**request)
        assert len(transport.posts) == 1
        assert transport.close_calls == 0
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status,allowed,reason", [
    ("configured", False, "surface_proactive_denied"),
    ("unconfigured", False, "surface_permission_unconfigured"),
    ("unavailable", False, "surface_permission_unavailable"),
])
async def test_presence_permission_projection_suppresses_without_inventing_opt_out(
    status, allowed, reason,
):
    response = _presence_response(
        proactive_output_suppressed=True, reason_codes=["thread_active", reason],
    )
    transport = _FakeAsyncClient([response])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    assert await client.evaluate_presence(
        **_PRESENCE_SCOPE, surface_permission_status=status, proactive_presence_allowed=allowed,
    ) == response
    assert transport.posts[0][1]["explicit_proactive_opt_out"] is False
    assert response["result"]["presence_state"] == "active_conversation"
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"surface_permission_status": "unknown"},
    {"proactive_presence_allowed": 1},
    {"ambient_listening_allowed": "true"},
    {"surface_permission_status": "unavailable", "ambient_listening_allowed": True},
    {"surface_permission_status": "unconfigured", "proactive_presence_allowed": True},
])
async def test_presence_permission_invalid_projection_never_posts(changes):
    transport = _FakeAsyncClient([])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    with pytest.raises(ValueError, match="presence_request_invalid"):
        await client.evaluate_presence(**_PRESENCE_SCOPE, **changes)
    assert transport.posts == []
    await client.close()


@pytest.mark.asyncio
async def test_presence_consumer_accepts_permitted_ambient_result():
    response = _presence_response(
        presence_state="ambient_listening", reason_codes=["ambient_mode_permitted"],
    )
    transport = _FakeAsyncClient([response])
    client = RuntimeClient("http://runtime.local", None,
                           client_factory=_ClientFactory([transport]))
    await client.open()
    assert await client.evaluate_presence(
        **_PRESENCE_SCOPE, surface_permission_status="configured",
        proactive_presence_allowed=True, ambient_listening_allowed=True,
    ) == response
    await client.close()


def _return_snapshot(**updates):
    return {
        "schema_version": "runtime-return-after-gap.v1",
        "status": "eligible",
        "threshold_seconds": 300,
        "threshold_met": True,
        "prior_thread_state": "idle",
        "prior_thread_revision": 2,
        "prior_last_activity_at": "2026-01-01T00:00:00+00:00",
        "elapsed_seconds": 301,
        "prior_terminal_turn_id": "rtturn_0123456789abcdef",
        "prior_continuation_state": "deferred_expansion",
        "reason_code": "return_gap_elapsed",
        **updates,
    }


@pytest.mark.parametrize(
    "updates",
    [
        {"schema_version": "wrong"},
        {"status": "unknown"},
        {"threshold_met": "true"},
        {"threshold_seconds": 301},
        {"elapsed_seconds": -1},
        {"elapsed_seconds": True},
        {"prior_terminal_turn_id": "bad"},
        {"prior_thread_state": "active"},
        {"prior_last_activity_at": "2026-01-01T00:00:00"},
        {"prior_continuation_state": "unknown"},
        {"threshold_met": False},
        {"status": "below_threshold"},
        {"extra": "PRIVATE"},
        {"prior_thread_revision": True},
    ],
)
def test_return_snapshot_is_strict_and_co_does_not_recalculate(updates):
    from clients.runtime import validate_return_snapshot

    with pytest.raises(RuntimeError, match="^runtime_return_snapshot_invalid$"):
        validate_return_snapshot(_return_snapshot(**updates))


def test_return_snapshot_preserves_cr_fact_without_local_elapsed_calculation():
    from clients.runtime import validate_return_snapshot

    assert validate_return_snapshot(_return_snapshot()) == _return_snapshot()
    assert (
        validate_return_snapshot(
            _return_snapshot(
                status="below_threshold",
                threshold_met=False,
                elapsed_seconds=299,
                reason_code="return_gap_below_threshold",
            )
        )["status"]
        == "below_threshold"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("valid", [True, False])
async def test_start_turn_validates_exact_return_snapshot_once(valid):
    client = RuntimeClient("http://runtime", None)
    calls = []

    async def post(path, *, json):
        calls.append(path)
        return {
            "runtime_session": {
                "runtime_session_id": "rtsession_1",
                "owner_id": "owner",
                "conversation_id": "thread",
                "surface": "web",
            },
            "runtime_turn": {
                "runtime_turn_id": "rtturn_1",
                "runtime_session_id": "rtsession_1",
                "input_message_id": None,
                "turn_status": "received",
            },
            "event": {
                "runtime_session_id": "rtsession_1",
                "runtime_turn_id": "rtturn_1",
                "event_type": "turn_started",
                "event_payload_json": {
                    "return_after_gap": _return_snapshot(threshold_met=True if valid else "true")
                },
            },
        }

    client._post = post
    if valid:
        response = await client.start_turn(
            request_id="current", owner_id="owner", conversation_id="thread", surface="web"
        )
        assert response["event"]["event_payload_json"]["return_after_gap"] == _return_snapshot()
    else:
        with pytest.raises(RuntimeError, match="runtime_return_snapshot_invalid"):
            await client.start_turn(
                request_id="current", owner_id="owner", conversation_id="thread", surface="web"
            )
    assert calls == ["/v1/runtime/turns/start"]


def _mandatory_start_response(snapshot):
    return {
        "runtime_session": {
            "runtime_session_id": "rtsession_1",
            "owner_id": "owner",
            "conversation_id": "thread",
            "surface": "web",
        },
        "runtime_turn": {
            "runtime_turn_id": "rtturn_1",
            "runtime_session_id": "rtsession_1",
            "input_message_id": None,
            "turn_status": "received",
        },
        "event": {
            "runtime_session_id": "rtsession_1",
            "runtime_turn_id": "rtturn_1",
            "event_type": "turn_started",
            "event_payload_json": {"return_after_gap": snapshot},
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["event", "payload", "null", "string", "list", "snapshot", "malformed"]
)
async def test_both_start_validators_reject_missing_mandatory_snapshot(fault):
    from services.orchestrate import _validate_started_runtime_turn

    response = _mandatory_start_response(_return_snapshot())
    if fault == "event":
        response.pop("event")
    elif fault == "payload":
        response["event"].pop("event_payload_json")
    elif fault in {"null", "string", "list"}:
        response["event"]["event_payload_json"] = {"null": None, "string": "PRIVATE", "list": []}[
            fault
        ]
    elif fault == "snapshot":
        response["event"]["event_payload_json"].pop("return_after_gap")
    else:
        response["event"]["event_payload_json"]["return_after_gap"]["threshold_seconds"] = 301
    with pytest.raises(RuntimeError, match="runtime_return_snapshot_invalid"):
        _validate_started_runtime_turn(
            response,
            owner_id="owner",
            conversation_id="thread",
            surface="web",
            input_message_id=None,
        )
    client = RuntimeClient("http://runtime", None)
    calls = []

    async def post(path, *, json):
        calls.append(path)
        return response

    client._post = post
    with pytest.raises(RuntimeError, match="runtime_return_snapshot_invalid"):
        await client.start_turn(
            request_id="current", owner_id="owner", conversation_id="thread", surface="web"
        )
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["eligible", "below_threshold", "not_applicable"])
async def test_both_start_validators_accept_all_valid_mandatory_snapshot_states(status):
    from services.orchestrate import _validate_started_runtime_turn

    snapshot = _return_snapshot()
    if status == "below_threshold":
        snapshot.update(
            status=status,
            threshold_met=False,
            elapsed_seconds=299,
            reason_code="return_gap_below_threshold",
        )
    if status == "not_applicable":
        snapshot.update(
            status=status,
            threshold_met=False,
            elapsed_seconds=0,
            prior_terminal_turn_id=None,
            prior_continuation_state=None,
            reason_code="no_completed_turn",
        )
    response = _mandatory_start_response(snapshot)
    assert (
        _validate_started_runtime_turn(
            response,
            owner_id="owner",
            conversation_id="thread",
            surface="web",
            input_message_id=None,
        )
        == response
    )
    client = RuntimeClient("http://runtime", None)

    async def post(path, *, json):
        return response

    client._post = post
    assert (
        await client.start_turn(
            request_id="current", owner_id="owner", conversation_id="thread", surface="web"
        )
        == response
    )


def _with_fresh_return_event(response, request_id):
    response["event"] = {
        "runtime_session_id": response["runtime_session"]["runtime_session_id"],
        "runtime_turn_id": response["runtime_turn"]["runtime_turn_id"],
        "event_type": "turn_started",
        "event_payload_json": {"request_id": request_id, "turn_status": "received",
            "input_message_id": response["runtime_turn"].get("input_message_id"),
            "return_after_gap": _return_snapshot(status="not_applicable", threshold_met=False,
                prior_thread_revision=0, elapsed_seconds=0, prior_terminal_turn_id=None,
                prior_continuation_state=None, reason_code="no_completed_turn")},
    }
    return response


def _interrupt_lifecycle(state="none", **overrides):
    reasons = {"none": "not_executed", "awaiting_feedback": "execution_recorded",
               "accepted": "trigger_not_recurred", "overridden": "trigger_recurred",
               "repeat_suppressed": "repeat_trigger_suppressed", "recovered": "pattern_broken",
               "history_unavailable": "lifecycle_history_invalid"}
    prior = state not in {"none", "history_unavailable"}
    count = 2 if state in {"overridden", "repeat_suppressed", "recovered"} else 0
    return {"state": state, "prior_executed_request_id": "prior-interrupt" if prior else None,
            "prior_trigger": "repetitive_branching" if prior else None,
            "repeated_trigger_count": count,
            "candidate_suppressed": state in {
                "overridden", "repeat_suppressed", "history_unavailable"},
            "reason_code": reasons[state], **overrides}


def _interrupt_response(**overrides):
    value = {
        "request_id": "interrupt-request", "owner_id": "owner", "conversation_id": "conversation",
        "surface": "web", "requested_scene": None, "confidence": 0.95,
        "trigger_class": "repetitive_branching", "style_selected": "next_step_forcing",
        "should_interrupt": True, "should_defer": False,
        "intervention_text": "Pick the next move and test it.",
        "reason_json": {"defer_reasons": [], "trigger_class": "repetitive_branching",
                        "requested_scene": None},
        "contract_constraints_applied": {"matched_contract_style": "soft_redirect"},
        "lifecycle": _interrupt_lifecycle(),
        "warnings": [], "debug": {"advisory_text": "PRIVATE-DIAGNOSTIC-TEXT"},
    }
    value.update(overrides)
    return value


@pytest.mark.parametrize("overrides", [
    {"request_id": "wrong"}, {"owner_id": "wrong"}, {"conversation_id": "wrong"},
    {"surface": "wrong"}, {"requested_scene": "wrong"},
    {"confidence": True}, {"confidence": "0.95"}, {"confidence": -1}, {"confidence": 2},
    {"confidence": float("nan")}, {"confidence": float("inf")},
    {"should_interrupt": "true"}, {"should_defer": 0}, {"should_defer": True},
    {"trigger_class": None}, {"trigger_class": []}, {"style_selected": "unknown"},
    {"style_selected": []}, {"intervention_text": None}, {"intervention_text": True},
    {"intervention_text": "   "}, {"intervention_text": "x" * 241},
    {"should_interrupt": False, "should_defer": True}, {"reason_json": []},
    {"reason_json": {"defer_reasons": "none"}}, {"warnings": "warning"},
    {"warnings": ["x" * 65]}, {"contract_constraints_applied": []},
    {"contract_constraints_applied": {"allowed_styles": "soft_redirect"}},
    {"contract_constraints_applied": {"blocked_candidates": [{}]}},
])
def test_interrupt_response_rejects_unbound_or_incoherent_authority(overrides):
    from clients.runtime import validate_interrupt_response

    with pytest.raises(RuntimeError, match="^interrupt_response_(invalid|context_mismatch)$"):
        validate_interrupt_response(
            _interrupt_response(**overrides), request_id="interrupt-request", owner_id="owner",
            conversation_id="conversation", surface="web",
        )


def test_interrupt_authority_is_independent_of_debug_and_deferred_text_is_null():
    from clients.runtime import validate_interrupt_response

    scope = dict(request_id="interrupt-request", owner_id="owner",
                 conversation_id="conversation", surface="web")
    value = _interrupt_response()
    del value["debug"]
    assert validate_interrupt_response(value, **scope)["intervention_text"] == (
        "Pick the next move and test it."
    )
    value.update(should_interrupt=False, should_defer=True, intervention_text=None,
                 reason_json={"defer_reasons": ["confidence_below_interrupt_threshold"],
                              "trigger_class": "repetitive_branching"})
    assert validate_interrupt_response(value, **scope)["intervention_text"] is None


@pytest.mark.asyncio
async def test_interrupt_client_posts_once_and_reuses_transport_after_validation_failure():
    from clients.runtime import RuntimeClient

    factory = _ClientFactory()
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    await client.open()
    transport = factory.clients[0]
    transport.responses.extend([_interrupt_response(intervention_text=None), _interrupt_response()])
    scope = dict(request_id="interrupt-request", owner_id="owner",
                 conversation_id="conversation", surface="web")
    with pytest.raises(RuntimeError, match="^interrupt_response_invalid$"):
        await client.evaluate_interrupt(**scope, current_user_text="PRIVATE-USER-TEXT")
    assert len(transport.posts) == 1
    response = await client.evaluate_interrupt(**scope, current_user_text="PRIVATE-USER-TEXT")
    assert response["intervention_text"] == "Pick the next move and test it."
    assert len(factory.clients) == 1 and len(transport.posts) == 2
    assert transport.posts[0] == ("/v1/interrupt/evaluate", {**scope,
                                                        "current_user_text": "PRIVATE-USER-TEXT"})
    await client.close()

@pytest.mark.parametrize(
    "state",
    [
        "none",
        "awaiting_feedback",
        "accepted",
        "overridden",
        "repeat_suppressed",
        "recovered",
        "history_unavailable",
    ],
)
def test_interrupt_lifecycle_projection_accepts_producer_states(state):
    from clients.runtime import validate_interrupt_response

    life = _interrupt_lifecycle(state)
    suppressed = life["candidate_suppressed"]
    body = _interrupt_response(
        lifecycle=life,
        should_interrupt=not suppressed,
        should_defer=suppressed,
        intervention_text=None if suppressed else "Pick the next move.",
    )
    assert (
        validate_interrupt_response(
            body,
            request_id="interrupt-request",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )["lifecycle"]
        == life
    )


@pytest.mark.parametrize(
    "life",
    [
        None,
        [],
        "private-lifecycle",
        True,
        _interrupt_lifecycle(state="none", state_override="private"),
        _interrupt_lifecycle(state="none", reason_code="trigger_recurred"),
        _interrupt_lifecycle(state="none", prior_executed_request_id="forged"),
        _interrupt_lifecycle(state="history_unavailable", prior_trigger="repetitive_branching"),
        _interrupt_lifecycle(state="accepted", prior_executed_request_id=None),
        _interrupt_lifecycle(state="accepted", prior_trigger=None),
        _interrupt_lifecycle(state="overridden", candidate_suppressed=False),
        _interrupt_lifecycle(state="none", candidate_suppressed="false"),
        _interrupt_lifecycle(state="none", repeated_trigger_count=True),
        _interrupt_lifecycle(state="none", repeated_trigger_count=1.0),
        _interrupt_lifecycle(state="none", repeated_trigger_count=-1),
        _interrupt_lifecycle(state="none", repeated_trigger_count=2147483648),
        _interrupt_lifecycle(state="accepted", repeated_trigger_count=1),
        _interrupt_lifecycle(state="repeat_suppressed", repeated_trigger_count=0),
        _interrupt_lifecycle(state="accepted", prior_trigger="private-unknown"),
        _interrupt_lifecycle(state="accepted", prior_executed_request_id=" "),
        _interrupt_lifecycle(state="accepted", prior_executed_request_id="x" * 121),
        _interrupt_lifecycle(state="accepted", prior_trigger=[]),
        {**_interrupt_lifecycle(), "state": "unknown"},
        {key: value for key, value in _interrupt_lifecycle().items() if key != "reason_code"},
    ],
)
def test_interrupt_lifecycle_projection_rejects_malformed_or_incoherent_state(life):
    from clients.runtime import validate_interrupt_lifecycle

    with pytest.raises(RuntimeError, match="^interrupt_lifecycle_invalid$"):
        validate_interrupt_lifecycle(life)


def test_suppression_cannot_coexist_with_an_authorized_intervention():
    from clients.runtime import validate_interrupt_response

    with pytest.raises(RuntimeError, match="^interrupt_response_invalid$"):
        validate_interrupt_response(
            _interrupt_response(lifecycle=_interrupt_lifecycle("overridden")),
            request_id="interrupt-request",
            owner_id="owner",
            conversation_id="conversation",
            surface="web",
        )


def _execution_response(**overrides):
    return {
        "request_id": "interrupt-request",
        "owner_id": "owner",
        "conversation_id": "conversation",
        "surface": "web",
        "execution_recorded": True,
        "idempotent_replay": False,
        "lifecycle": _interrupt_lifecycle(
            "awaiting_feedback", prior_executed_request_id="interrupt-request"
        ),
        **overrides,
    }


def _execution_arguments():
    return dict(
        request_id="interrupt-request",
        owner_id="owner",
        conversation_id="conversation",
        surface="web",
        trigger_class="repetitive_branching",
        style_selected="next_step_forcing",
        intervention_text="Pick the next move and test it.",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state", ["awaiting_feedback", "accepted", "overridden", "repeat_suppressed", "recovered"]
)
async def test_execution_client_sends_exact_payload_once_and_validates_replays(state):
    from clients.runtime import RuntimeClient

    factory = _ClientFactory()
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    await client.open()
    transport = factory.clients[0]
    transport.responses.append(
        _execution_response(
            idempotent_replay=state != "awaiting_feedback",
            lifecycle=_interrupt_lifecycle(state, prior_executed_request_id="interrupt-request"),
        )
    )
    value = await client.execute_interrupt(**_execution_arguments())
    assert value["execution_recorded"] is True
    assert value["lifecycle"]["state"] == state
    assert transport.posts == [("/v1/interrupt/execute", _execution_arguments())]
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"request_id": "wrong"},
        {"owner_id": "wrong"},
        {"conversation_id": "wrong"},
        {"surface": "wrong"},
        {"execution_recorded": False},
        {"execution_recorded": 1},
        {"idempotent_replay": 1},
        {"lifecycle": []},
        {"private": "RAW-RESPONSE"},
        {"lifecycle": _interrupt_lifecycle()},
        {"lifecycle": _interrupt_lifecycle("history_unavailable")},
        {
            "lifecycle": _interrupt_lifecycle(
                "accepted", prior_executed_request_id="interrupt-request"
            )
        },
        {"lifecycle": _interrupt_lifecycle("awaiting_feedback", prior_executed_request_id="other")},
        {
            "lifecycle": _interrupt_lifecycle(
                "awaiting_feedback",
                prior_executed_request_id="interrupt-request",
                prior_trigger="known_recurring_trap_pattern",
            )
        },
    ],
)
async def test_execution_response_invalidates_no_healthy_transport_and_never_replays(overrides):
    from clients.runtime import RuntimeClient

    factory = _ClientFactory()
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    await client.open()
    transport = factory.clients[0]
    transport.responses.extend([_execution_response(**overrides), _execution_response()])
    with pytest.raises(RuntimeError, match="^interrupt_(execution|lifecycle)"):
        await client.execute_interrupt(**_execution_arguments())
    assert len(transport.posts) == 1 and transport.close_calls == 0 and len(factory.clients) == 1
    assert (await client.execute_interrupt(**_execution_arguments()))["execution_recorded"] is True
    assert len(transport.posts) == 2 and len(factory.clients) == 1
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,closed",
    [
        (httpx.ReadTimeout("PRIVATE-TIMEOUT"), 1),
        (httpx.ConnectError("PRIVATE-TRANSPORT"), 1),
        (httpx.PoolTimeout("PRIVATE-POOL-TIMEOUT"), 0),
    ],
)
async def test_execution_transport_loss_is_not_retried(error, closed):
    from clients.runtime import RuntimeClient

    factory = _ClientFactory()
    client = RuntimeClient("http://runtime.local", None, client_factory=factory)
    await client.open()
    transport = factory.clients[0]
    transport.responses.append(error)
    with pytest.raises(type(error)):
        await client.execute_interrupt(**_execution_arguments())
    assert transport.posts == [("/v1/interrupt/execute", _execution_arguments())]
    assert len(factory.clients) == 1 and transport.close_calls == closed
    await client.close()
