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
