from __future__ import annotations

import httpx
import pytest
from clients.runtime import RuntimeClient


def _status_error(path: str, status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", f"http://runtime.local{path}")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        f"status {status_code}",
        request=request,
        response=response,
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

    assert [path for path, _ in calls] == [
        "/v1/runtime/sessions/resolve",
        "/v1/runtime/turns/start",
        "/v1/runtime/turns/update",
        "/v1/runtime/turns/complete",
        "/v1/runtime/identity/resolve",
        "/v1/world-state/resolve",
    ]
    assert calls[-2][1]["runtime_session_id"] == "rtsession_1"
    assert calls[-1][1]["active_persona_id"] == "technical_architect"
