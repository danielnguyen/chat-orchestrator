import json
from copy import deepcopy

import httpx
import pytest
from clients.memory_store import MemoryStoreClient
from clients.runtime import RuntimeClient
from services.orchestration_replay import (
    REGISTRY_PATH,
    RULES_PATH,
    ReplayMemoryStore,
    ReplayProvider,
    ReplayRuntime,
    _payload,
    assert_snapshot_privacy_safe,
    compare_snapshot,
    load_corpus,
    project_snapshot,
    run_scenario,
    run_wave3c_r_smoke_report,
    run_wave3c_smoke_report,
)


async def _snapshot_for_scenario(name: str):
    fixture = next(item for item in load_corpus() if item["scenario"] == name)
    return await run_scenario(fixture)


def _exact_conversation_projection(**overrides):
    projection = {
        "conversation_id": "00000000-0000-4000-8000-000000000001",
        "owner_id": "owner",
        "client_id": "origin-client",
        "title": "Current conversation",
        "lifecycle_state": "open",
        "superseded_by_conversation_id": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-02T00:00:00+00:00",
    }
    projection.update(overrides)
    return projection


def _listed_conversation(ordinal=1, **overrides):
    conversation_id = overrides.pop(
        "conversation_id",
        f"00000000-0000-4000-8000-{ordinal:012d}",
    )
    projection = _exact_conversation_projection(
        conversation_id=conversation_id,
        **overrides,
    )
    return projection


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 8, 9])
async def test_memory_store_client_lists_one_bounded_owner_open_page(count):
    client = MemoryStoreClient("http://memory.local", "key")
    captured = []

    async def fake_get(path, *, params=None):
        captured.append((path, params))
        return {
            "conversations": [_listed_conversation(index + 1) for index in range(count)],
            "next_cursor": "bounded-cursor" if count else None,
        }

    client._get = fake_get  # type: ignore[method-assign]
    response = await client.list_open_conversations(owner_id="owner", limit=9)

    assert len(response["conversations"]) == count
    assert captured == [
        (
            "/v1/conversations",
            {"owner_id": "owner", "lifecycle_state": "open", "limit": 9},
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        [],
        {},
        {"conversations": "invalid", "next_cursor": None},
        {"conversations": [_listed_conversation()] * 2, "next_cursor": None},
        {
            "conversations": [_listed_conversation(owner_id="other-owner")],
            "next_cursor": None,
        },
        {
            "conversations": [_listed_conversation(lifecycle_state="closed")],
            "next_cursor": None,
        },
        {
            "conversations": [_listed_conversation(conversation_id="not-a-uuid")],
            "next_cursor": None,
        },
        {
            "conversations": [_listed_conversation(updated_at="2026-01-02T00:00:00")],
            "next_cursor": None,
        },
        {
            "conversations": [_listed_conversation(updated_at="not-a-time")],
            "next_cursor": None,
        },
        {
            "conversations": [
                _listed_conversation(
                    superseded_by_conversation_id="00000000-0000-4000-8000-000000000099"
                )
            ],
            "next_cursor": None,
        },
        {"conversations": [], "next_cursor": "x" * 2049},
        {
            "conversations": [_listed_conversation(index + 1) for index in range(10)],
            "next_cursor": None,
        },
    ],
)
async def test_memory_store_client_rejects_invalid_open_conversation_pages(response):
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_get(path, *, params=None):
        return response

    client._get = fake_get  # type: ignore[method-assign]
    with pytest.raises(
        RuntimeError,
        match="^conversation_list_response_(?:invalid|context_mismatch)$",
    ):
        await client.list_open_conversations(owner_id="owner", limit=9)


@pytest.mark.asyncio
async def test_memory_store_client_creates_conversation_with_exact_payload():
    client = MemoryStoreClient("http://memory.local", "key")
    captured = []

    async def fake_post(path, *, request_id=None, json):
        captured.append((path, request_id, json))
        return {"conversation_id": "00000000-0000-4000-8000-000000000001"}

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.create_conversation(
        owner_id="owner",
        client_id="current-client",
    )

    assert response == {"conversation_id": "00000000-0000-4000-8000-000000000001"}
    assert captured == [
        (
            "/v1/conversations",
            None,
            {"owner_id": "owner", "client_id": "current-client"},
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        None, [], {"conversation_id": True},
        {"conversation_id": "00000000-0000-4000-8000-00000000000A"},
        {},
        {"conversation_id": "not-a-uuid"},
        {
            "conversation_id": "00000000-0000-4000-8000-000000000001",
            "extra": True,
        },
    ],
)
async def test_memory_store_client_rejects_invalid_create_response(response):
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_post(path, *, request_id=None, json):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="^conversation_create_response_invalid$"):
        await client.create_conversation(owner_id="owner", client_id=None)


@pytest.mark.asyncio
async def test_memory_store_client_gets_exact_owner_scoped_open_conversation():
    client = MemoryStoreClient("http://memory.local", "key")
    captured = {}
    projection = _exact_conversation_projection()

    async def fake_get(path, *, params=None):
        captured.update({"path": path, "params": params})
        return projection

    client._get = fake_get  # type: ignore[method-assign]
    result = await client.get_conversation(
        conversation_id="00000000-0000-4000-8000-000000000001",
        owner_id="owner",
    )

    assert captured == {
        "path": "/v1/conversations/00000000-0000-4000-8000-000000000001",
        "params": {"owner_id": "owner"},
    }
    assert result == projection


@pytest.mark.asyncio
async def test_memory_store_client_accepts_canonical_equivalent_conversation_id():
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_get(path, *, params=None):
        return _exact_conversation_projection(
            conversation_id="00000000-0000-4000-8000-00000000000a"
        )

    client._get = fake_get  # type: ignore[method-assign]
    result = await client.get_conversation(
        conversation_id="{00000000-0000-4000-8000-00000000000A}",
        owner_id="owner",
    )

    assert result["conversation_id"] == "00000000-0000-4000-8000-00000000000a"


@pytest.mark.parametrize(
    "projection",
    [
        _exact_conversation_projection(conversation_id="other-conversation"),
        _exact_conversation_projection(owner_id="other-owner"),
    ],
)
@pytest.mark.asyncio
async def test_memory_store_client_rejects_exact_conversation_context_mismatch(projection):
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_get(path, *, params=None):
        return projection

    client._get = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="^conversation_projection_context_mismatch$"):
        await client.get_conversation(
            conversation_id="00000000-0000-4000-8000-000000000001",
            owner_id="owner",
        )


@pytest.mark.parametrize(
    "projection",
    [
        "PRIVATE-CONVERSATION-PROJECTION-SENTINEL",
        {},
        _exact_conversation_projection(client_id=1),
        _exact_conversation_projection(title=[]),
        _exact_conversation_projection(created_at=None),
        _exact_conversation_projection(updated_at={}),
        _exact_conversation_projection(lifecycle_state="unknown"),
    ],
)
@pytest.mark.asyncio
async def test_memory_store_client_rejects_malformed_conversation_projection(projection):
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_get(path, *, params=None):
        return projection

    client._get = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="^conversation_projection_invalid$"):
        await client.get_conversation(
            conversation_id="00000000-0000-4000-8000-000000000001",
            owner_id="owner",
        )


@pytest.mark.parametrize(
    "projection",
    [
        _exact_conversation_projection(
            lifecycle_state="open",
            superseded_by_conversation_id="replacement-conversation",
        ),
        _exact_conversation_projection(
            lifecycle_state="closed",
            superseded_by_conversation_id="replacement-conversation",
        ),
        _exact_conversation_projection(lifecycle_state="superseded"),
        _exact_conversation_projection(
            lifecycle_state="superseded",
            superseded_by_conversation_id="",
        ),
    ],
)
@pytest.mark.asyncio
async def test_memory_store_client_rejects_incoherent_conversation_lifecycle(projection):
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_get(path, *, params=None):
        return projection

    client._get = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="^conversation_projection_invalid$"):
        await client.get_conversation(
            conversation_id="00000000-0000-4000-8000-000000000001",
            owner_id="owner",
        )


@pytest.mark.asyncio
async def test_memory_store_client_exact_lookup_errors_do_not_copy_response_material():
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_get(path, *, params=None):
        return _exact_conversation_projection(
            client_id={"private": "PRIVATE-CONVERSATION-PROJECTION-SENTINEL"}
        )

    client._get = fake_get  # type: ignore[method-assign]
    with pytest.raises(RuntimeError) as exc:
        await client.get_conversation(
            conversation_id="00000000-0000-4000-8000-000000000001",
            owner_id="owner",
        )

    assert str(exc.value) == "conversation_projection_invalid"
    assert "PRIVATE-CONVERSATION-PROJECTION-SENTINEL" not in str(exc.value)


@pytest.mark.asyncio
async def test_memory_store_client_rejects_retrieval_request_id_mismatch():
    client = MemoryStoreClient("http://memory.local", "key")
    captured = {}

    async def fake_post(path, *, request_id=None, json):
        captured.update({"path": path, "request_id": request_id, "json": json})
        return {"request_id": "different-request", "bundle": {}}

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="retrieval_request_id_mismatch"):
        await client.retrieve_bundle(
            request_id="expected-request",
            conversation_id="conversation-1",
            owner_id="owner",
            query="neutral",
            retrieval=None,
        )
    assert captured["path"] == "/v2/conversations/conversation-1/retrieve"
    assert captured["request_id"] == "expected-request"
    assert captured["json"]["request_id"] == "expected-request"
    assert captured["json"]["owner_id"] == "owner"
    assert captured["json"]["mode"] == "augmented"


@pytest.mark.asyncio
async def test_memory_store_client_serializes_policy_metadata_and_containment_policy():
    client = MemoryStoreClient("http://memory.local", "key")
    captured = []

    async def fake_post(path, *, request_id=None, json):
        captured.append({"path": path, "request_id": request_id, "json": json})
        if path.endswith("/retrieve"):
            return {"request_id": json["request_id"], "bundle": {}}
        return {"message_id": "message-1"}

    client._post = fake_post  # type: ignore[method-assign]
    policy_metadata = {"memory_domains": ["technical"], "sensitivity": "medium"}
    containment_policy = {
        "enforcement_mode": "mandatory",
        "allowed_memory_domains": ["technical"],
        "blocked_memory_domains": [],
        "artifact_access_policy": {
            "enforcement_mode": "mandatory",
            "allowed_content_classes": ["document"],
            "allowed_domains": ["technical"],
            "maximum_sensitivity": "medium",
            "surface_content_capabilities": ["document"],
            "reason_codes": ["test"],
        },
        "relationship_scope_projection": {"applied": False},
    }

    await client.add_message(
        conversation_id="conversation-1",
        owner_id="owner",
        role="user",
        content="hello",
        client_id="client",
        metadata={"surface": "dev"},
        policy_metadata=policy_metadata,
    )
    await client.retrieve_bundle(
        request_id="request-1",
        conversation_id="conversation-1",
        owner_id="owner",
        query="hello",
        retrieval=None,
        allowed_memory_domains=["legacy"],
        blocked_memory_domains=["legacy_blocked"],
        containment_policy=containment_policy,
    )

    assert captured[0]["json"]["policy_metadata"] == policy_metadata
    assert captured[1]["json"]["containment_policy"] == containment_policy
    assert "allowed_memory_domains" not in captured[1]["json"]
    assert "blocked_memory_domains" not in captured[1]["json"]


@pytest.mark.asyncio
async def test_memory_store_client_supplied_message_identity_and_request_header():
    client = MemoryStoreClient("http://memory.local", "key")
    captured = {}
    supplied = "{00000000-0000-4000-8000-00000000000A}"

    async def fake_post(path, *, request_id=None, json):
        captured.update({"path": path, "request_id": request_id, "json": json})
        return {"message_id": "00000000-0000-4000-8000-00000000000a"}

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.add_message(
        conversation_id="conversation-1",
        owner_id="owner",
        role="user",
        content="current input",
        client_id="client",
        message_id=supplied,
        request_id="request-1",
    )

    assert response == {"message_id": "00000000-0000-4000-8000-00000000000a"}
    assert captured["request_id"] == "request-1"
    assert captured["json"]["message_id"] == supplied


@pytest.mark.asyncio
async def test_memory_store_client_omitted_message_identity_preserves_payload_shape():
    client = MemoryStoreClient("http://memory.local", "key")
    captured = {}

    async def fake_post(path, *, request_id=None, json):
        captured.update({"request_id": request_id, "json": json})
        return {"message_id": "server-message"}

    client._post = fake_post  # type: ignore[method-assign]
    await client.add_message(
        conversation_id="conversation-1",
        owner_id="owner",
        role="assistant",
        content="response",
        client_id="client",
    )

    assert "message_id" not in captured["json"]
    assert captured["request_id"] is None


@pytest.mark.parametrize(
    ("response", "error"),
    [
        ({}, "message_append_response_invalid"),
        ("PRIVATE-APPEND-RESPONSE", "message_append_response_invalid"),
        (
            {"message_id": "00000000-0000-4000-8000-00000000000b"},
            "message_append_response_context_mismatch",
        ),
    ],
)
@pytest.mark.asyncio
async def test_memory_store_client_rejects_malformed_or_mismatched_append_response(
    response,
    error,
):
    client = MemoryStoreClient("http://memory.local", "key")

    async def fake_post(path, *, request_id=None, json):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match=f"^{error}$") as exc:
        await client.add_message(
            conversation_id="conversation-1",
            owner_id="owner",
            role="user",
            content="PRIVATE-APPEND-CONTENT",
            client_id="client",
            message_id="00000000-0000-4000-8000-00000000000a",
        )
    assert "PRIVATE-APPEND" not in str(exc.value)


def _runtime_turn_response(**overrides):
    response = {
        "runtime_session": {
            "runtime_session_id": "session-1",
            "owner_id": "owner",
            "conversation_id": "conversation-1",
            "surface": "web",
        },
        "runtime_turn": {
            "runtime_turn_id": "turn-1",
            "runtime_session_id": "session-1",
            "input_message_id": "00000000-0000-4000-8000-00000000000a",
            "turn_status": "received",
        },
        "event": {
            "runtime_session_id": "session-1",
            "runtime_turn_id": "turn-1",
            "event_type": "turn_started",
        },
    }
    for key, value in overrides.items():
        target, field = key.split("__", 1)
        response[target][field] = value
    return response


@pytest.mark.asyncio
async def test_runtime_client_start_turn_sends_and_validates_admitted_identity():
    client = RuntimeClient("http://runtime.local", "key")
    captured = {}

    async def fake_post(path, *, json):
        captured.update({"path": path, "json": json})
        return _runtime_turn_response()

    client._post = fake_post  # type: ignore[method-assign]
    response = await client.start_turn(
        request_id="request-1",
        owner_id="owner",
        conversation_id="conversation-1",
        surface="web",
        input_message_id="00000000-0000-4000-8000-00000000000a",
        intent_class="question",
    )

    assert response["runtime_turn"]["runtime_turn_id"] == "turn-1"
    assert captured == {
        "path": "/v1/runtime/turns/start",
        "json": {
            "request_id": "request-1",
            "owner_id": "owner",
            "conversation_id": "conversation-1",
            "surface": "web",
            "input_message_id": "00000000-0000-4000-8000-00000000000a",
            "intent_class": "question",
        },
    }


@pytest.mark.parametrize(
    "response",
    [
        "PRIVATE-RUNTIME-RESPONSE",
        {},
        _runtime_turn_response(runtime_turn__runtime_turn_id=""),
        _runtime_turn_response(runtime_turn__turn_status="completed"),
    ],
)
@pytest.mark.asyncio
async def test_runtime_client_rejects_malformed_start_response(response):
    client = RuntimeClient("http://runtime.local", "key")

    async def fake_post(path, *, json):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="^runtime_turn_response_invalid$") as exc:
        await client.start_turn(
            request_id="request-1",
            owner_id="owner",
            conversation_id="conversation-1",
            surface="web",
            input_message_id="00000000-0000-4000-8000-00000000000a",
        )
    assert "PRIVATE-RUNTIME" not in str(exc.value)


@pytest.mark.parametrize(
    "response",
    [
        _runtime_turn_response(runtime_session__owner_id="other-owner"),
        _runtime_turn_response(runtime_session__conversation_id="other-conversation"),
        _runtime_turn_response(runtime_session__surface="voice"),
        _runtime_turn_response(runtime_turn__runtime_session_id="other-session"),
        _runtime_turn_response(
            runtime_turn__input_message_id="00000000-0000-4000-8000-00000000000b"
        ),
        _runtime_turn_response(event__runtime_turn_id="other-turn"),
    ],
)
@pytest.mark.asyncio
async def test_runtime_client_rejects_start_response_context_mismatch(response):
    client = RuntimeClient("http://runtime.local", "key")

    async def fake_post(path, *, json):
        return response

    client._post = fake_post  # type: ignore[method-assign]
    with pytest.raises(
        RuntimeError, match="^runtime_turn_response_context_mismatch$"
    ):
        await client.start_turn(
            request_id="request-1",
            owner_id="owner",
            conversation_id="conversation-1",
            surface="web",
            input_message_id="00000000-0000-4000-8000-00000000000a",
        )


@pytest.mark.asyncio
async def test_complete_persisted_orchestration_replay_corpus_passes_twice():
    for fixture in load_corpus():
        first = await run_scenario(fixture)
        second = await run_scenario(fixture)
        assert first == second
        expected = fixture["expected"]
        compare_snapshot(
            expected,
            project_snapshot(first, expected),
            fixture["scenario"],
        )


@pytest.mark.asyncio
async def test_runtime_unavailable_stops_at_admission_without_side_effects():
    snapshot = await _snapshot_for_scenario("runtime-unavailable")

    assert snapshot["outcome"] == {
        "status": "failed",
        "error_type": None,
        "error_code": None,
        "selected_model": "not_called",
        "answer_category": "other",
    }
    assert snapshot["call_order"] == ["conversation_resolution", "cr_turn_start"]
    assert snapshot["provider_attempt_count"] == 0
    assert snapshot["trace"]["persisted"] is False
    assert snapshot["runtime_terminal_status"] is None
    assert {
        "user_message_persistence",
        "assistant_message_persistence",
        "profile_resolution",
        "bms_retrieval",
        "provider_attempt",
        "trace_persistence",
        "cr_turn_complete",
    }.isdisjoint(snapshot["call_order"])
    assert_snapshot_privacy_safe(snapshot)


def test_changed_expected_output_produces_readable_structural_diff():
    expected = {"trace": {"persisted": True}}
    actual = {"trace": {"persisted": False}}
    with pytest.raises(AssertionError) as exc:
        compare_snapshot(expected, actual, "changed-fixture")
    message = str(exc.value)
    assert "changed-fixture:expected" in message
    assert "changed-fixture:actual" in message
    assert '-    "persisted": true' in message.lower()
    assert '+    "persisted": false' in message.lower()


def test_required_orchestration_replay_categories_are_present():
    categories = {fixture["category"] for fixture in load_corpus()}
    assert {
        "positive",
        "runtime_overlay_included",
        "runtime_overlay_omitted",
        "surface_variant",
        "missing_derivative",
        "stale_derivative",
        "malformed_retrieval",
        "vector_unavailable",
        "artifact_unavailable",
        "malformed_runtime",
        "runtime_unavailable",
        "provider_fallback",
        "truth_active_parked",
        "truth_active_stale",
        "truth_stale_only",
        "truth_missing_source",
        "truth_cross_owner",
        "truth_malformed_source_ref",
        "truth_incomplete_source_check",
        "truth_missing_provenance_identity",
        "truth_missing_provenance_type",
        "truth_unknown_durable_status",
        "truth_cr_unavailable",
        "truth_cr_malformed",
        "truth_cr_conflicting",
        "truth_policy_ceiling",
        "truth_corrected_relationship",
        "truth_corrected_invalid",
        "truth_relationship_authority",
        "truth_cr_consistency",
        "provider_exhaustion",
        "no_fallback",
        "request_id_mismatch",
        "bms_unavailable",
        "trace_persistence_failure",
        "wave3b_retrieval_suppressed",
        "wave3b_valid_containment",
        "wave3b_result_boundary_fallback",
        "wave3b_co3_unauthorized_artifact",
        "wave3b_co3_relationship_projection",
        "wave3b_co3_privacy_sanitization",
        "wave3b_co3_malformed_mandatory_response",
        "wave3c_capability_lifecycle",
        "wave3c_r_relationship_capability",
    } <= categories


@pytest.mark.asyncio
async def test_wave3c_r_smoke_report_includes_relationship_assertions():
    report = await run_wave3c_r_smoke_report()

    assert report["scenario_count"] == 9
    assert report["failed_count"] == 0
    assert report["relationship_gated_scenarios_included"] is True
    assert report["privacy_assertions_passed"] is True
    assert report["no_repeat_dispatch_assertions_passed"] is True
    assert report["descriptor_fingerprint_assertion_passed"] is True


def test_wave2d_prompt_budget_replay_corpus_is_complete():
    wave2d = [
        fixture["scenario"]
        for fixture in load_corpus()
        if fixture["category"] == "prompt_budget_wave2d"
    ]
    assert wave2d == [
        "wave2d-under-budget-no-truncation",
        "wave2d-request-history-overflow",
        "wave2d-recent-history-overflow",
        "wave2d-historical-before-current",
        "wave2d-current-relevance-tie",
        "wave2d-external-runtime-reduction",
        "wave2d-valid-profile-clamp",
        "wave2d-malformed-overlarge-profile-clamp",
        "wave2d-smaller-fallback-context",
        "wave2d-required-content-overflow",
        "wave2d-missing-primary-context",
        "wave2d-missing-fallback-context",
        "wave2d-primary-failure-fallback-success",
        "wave2d-repeat-deterministic",
        "wave2d-dropped-artifact-source",
    ]
    assert len(wave2d) == 15


@pytest.mark.asyncio
async def test_request_id_and_boundary_call_order_are_deterministic():
    fixture = next(item for item in load_corpus() if item["category"] == "positive")
    snapshot = await run_scenario(fixture)
    assert set(snapshot["request_ids"]) == {snapshot["request_id"]}
    order = snapshot["call_order"]
    required = [
        "conversation_resolution",
        "cr_turn_start",
        "user_message_persistence",
        "bms_retrieval",
        "cr_memory_hygiene",
        "cr_overlay",
        "prompt_assembly",
        "provider_attempt",
        "assistant_message_persistence",
        "cr_turn_complete",
        "trace_persistence",
    ]
    positions = [order.index(name) for name in required]
    assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_model_attempts_and_backward_compatible_summary_are_truthful():
    fallback_fixture = next(
        item for item in load_corpus() if item["category"] == "provider_fallback"
    )
    fallback = await run_scenario(fallback_fixture)
    attempts = fallback["trace"]["model_calls"]
    assert [attempt["status"] for attempt in attempts] == ["failed", "ok"]
    assert fallback["trace"]["model_call"]["status"] == "ok"
    assert fallback["trace"]["model_call"]["model"] == attempts[-1]["model"]
    assert "error_type" in attempts[0]
    assert "error_type" not in attempts[1]

    exhausted_fixture = next(
        item for item in load_corpus() if item["category"] == "provider_exhaustion"
    )
    exhausted = await run_scenario(exhausted_fixture)
    assert [attempt["status"] for attempt in exhausted["trace"]["model_calls"]] == [
        "failed",
        "failed",
    ]
    assert exhausted["trace"]["persisted"] is True
    assert exhausted["runtime_terminal_status"] == "abandoned"

    no_fallback_fixture = next(item for item in load_corpus() if item["category"] == "no_fallback")
    no_fallback = await run_scenario(no_fallback_fixture)
    assert len(no_fallback["trace"]["model_calls"]) == 1


@pytest.mark.asyncio
async def test_wave3c_capability_lifecycle_replay_smoke_report_is_complete():
    report = await run_wave3c_smoke_report()

    assert report == {
        "scenario_count": 11,
        "passed_count": 11,
        "failed_count": 0,
        "capability_lifecycle_scenarios": [
            "wave3c-world-state-read-lifecycle",
            "wave3c-local-draft-lifecycle",
            "wave3c-revalidation-lifecycle",
            "wave3c-confirmation-lifecycle",
            "wave3c-recursive-follow-up-blocked",
            "wave3c-fallback-same-descriptor-once",
            "wave3c-authorization-failures-no-fallback",
            "wave3c-selection-denial-zero-executor",
            "wave3c-hidden-capability-validation-zero-executor",
            "wave3c-multiple-call-validation-zero-executor",
            "wave3c-revalidation-failure-zero-executor",
        ],
        "privacy_assertions_passed": True,
        "no_repeat_dispatch_assertions_passed": True,
        "failures": [],
    }


@pytest.mark.asyncio
async def test_wave3c_replay_projects_bounded_privacy_safe_capability_trace():
    snapshot = await _snapshot_for_scenario("wave3c-world-state-read-lifecycle")
    capabilities = snapshot["trace"]["capabilities"]

    assert capabilities["exposure"]["exposed_capability_ids"] == [
        "runtime.world_state.read"
    ]
    assert capabilities["exposure"]["blocked_capability_ids"] == [
        "draft.local_message",
        "runtime.relationship_context.read",
    ]
    assert capabilities["validation"]["provider_tool_name"] == "runtime_world_state_read"
    assert capabilities["validation"]["capability_id"] == "runtime.world_state.read"
    assert capabilities["execution"]["executor_call_count"] == 1
    assert capabilities["follow_up"]["summary"]["result_summary"][
        "included_claim_count"
    ] == 1
    serialized = json.dumps(snapshot, sort_keys=True)
    assert "PRIVATE-WAVE3C-WORLD-VALUE" not in serialized
    assert "value_json" not in serialized
    assert "expected_value_digest" not in serialized
    assert "credentials" not in serialized
    assert_snapshot_privacy_safe(snapshot)


@pytest.mark.asyncio
async def test_wave3b_replay_restraint_records_zero_bms_retrieval():
    snapshot = await _snapshot_for_scenario("wave3b-retrieval-suppressed-zero-bms")
    assert "bms_retrieval" not in snapshot["call_order"]
    dispatch = snapshot["trace"]["retrieval_dispatch"]
    assert dispatch["bms_retrieval_call_issued"] is False
    assert dispatch["bms_retrieval_call_suppressed"] is True


@pytest.mark.asyncio
async def test_wave3b_replay_blocks_unauthorized_artifact_from_every_attempt():
    snapshot = await _snapshot_for_scenario("wave3b-co3-unauthorized-artifact-returned")
    assert snapshot["provider_attempt_count"] >= 1
    assert all(
        attempt["unauthorized_artifact_present"] is False
        for attempt in snapshot["provider_prompt_evidence"]
    )
    retained = snapshot["trace"]["result_boundary"]["retained_counts"]
    assert retained["artifact_refs"] == 0
    assert snapshot["sources_count"] == 0


@pytest.mark.asyncio
async def test_wave3b_replay_relationship_projection_includes_selected_only():
    snapshot = await _snapshot_for_scenario("wave3b-co3-relationship-projection-narrows")
    assert snapshot["provider_attempt_count"] >= 1
    assert all(
        attempt["selected_relationship_memory_present"] is True
        for attempt in snapshot["provider_prompt_evidence"]
    )
    assert all(
        attempt["excluded_relationship_memory_present"] is False
        for attempt in snapshot["provider_prompt_evidence"]
    )
    assert snapshot["trace"]["result_boundary"]["relationship_policy_applied"] is True


@pytest.mark.asyncio
async def test_wave3b_replay_fallback_attempt_identity_is_structural():
    snapshot = await _snapshot_for_scenario("wave3b-co2-fallback-same-prompt")
    assert snapshot["provider_attempt_count"] == 2
    assert len(set(snapshot["provider_fingerprints"])) == 1
    assert len(set(snapshot["provider_message_counts"])) == 1
    assert snapshot["provider_role_sequences"][0] == snapshot["provider_role_sequences"][1]
    model_calls = snapshot["trace"]["model_calls"]
    assert len(model_calls) == 2
    assert [call["status"] for call in model_calls] == ["failed", "ok"]
    assert [call["attempt_ordinal"] for call in model_calls] == [1, 2]
    assert model_calls[0]["prompt_fingerprint"] == model_calls[1]["prompt_fingerprint"]
    assert model_calls[0]["prompt_message_count"] == model_calls[1]["prompt_message_count"]
    assert model_calls[0]["prompt_role_sequence"] == model_calls[1]["prompt_role_sequence"]
    assert model_calls[0]["retained_semantic_message_ids"] == model_calls[1][
        "retained_semantic_message_ids"
    ]
    assert model_calls[0]["retained_artifact_ids"] == model_calls[1]["retained_artifact_ids"]
    assert model_calls[0]["retained_semantic_message_ids"]
    assert model_calls[0]["retained_artifact_ids"]
    assert all("memory-1" in call["retained_semantic_message_ids"] for call in model_calls)
    assert all("artifact-1" in call["retained_artifact_ids"] for call in model_calls)


@pytest.mark.asyncio
async def test_wave3b_replay_privacy_snapshot_has_no_retained_ids_or_sentinels():
    snapshot = await _snapshot_for_scenario("wave3b-co3-privacy-side-channels")
    assert all(
        attempt["privacy_replay_sentinel_present"] is False
        for attempt in snapshot["provider_prompt_evidence"]
    )
    assert snapshot["trace"]["references"] == []
    assert snapshot["trace"]["prompt_budget"]["retained_source_ids"] in (None, [])
    assert snapshot["trace"]["retrieval"].get("artifact_refs") in (None, [])
    assert_snapshot_privacy_safe(snapshot)


@pytest.mark.asyncio
async def test_wave3b_replay_malformed_mandatory_response_retains_no_ids():
    snapshot = await _snapshot_for_scenario("wave3b-co3-malformed-mandatory-response")
    retained = snapshot["trace"]["result_boundary"]["retained_counts"]
    assert retained["semantic"] == 0
    assert retained["artifact_refs"] == 0
    retrieval = snapshot["trace"]["retrieval"]
    assert retrieval["semantic_count"] == 0
    assert retrieval["artifact_count"] == 0
    assert retrieval["semantic"] == []
    assert retrieval["artifact_refs"] == []


@pytest.mark.asyncio
async def test_trace_contract_is_bounded_structural_and_privacy_safe():
    for fixture in load_corpus():
        snapshot = await run_scenario(fixture)
        assert_snapshot_privacy_safe(snapshot)
        if not snapshot["trace"]["persisted"]:
            continue
        trace = snapshot["trace"]
        assert trace["budget_enforcement"] == "enforced"
        assert isinstance(trace["prompt_layers"], list)
        assert isinstance(trace["artifacts"].get("artifact_count"), int)
        assert isinstance(trace["references"], list)
        assert "neutral request" not in str(trace)
        assert "neutral response" not in str(trace)


@pytest.mark.asyncio
async def test_failure_scenarios_do_not_claim_false_success():
    snapshots = {
        fixture["category"]: await run_scenario(fixture)
        for fixture in load_corpus()
        if fixture["category"]
        in {
            "request_id_mismatch",
            "bms_unavailable",
            "trace_persistence_failure",
            "runtime_unavailable",
        }
    }
    assert snapshots["request_id_mismatch"]["trace"]["persisted"] is False
    assert snapshots["bms_unavailable"]["trace"]["persisted"] is False
    assert snapshots["trace_persistence_failure"]["trace"]["persisted"] is False
    assert snapshots["trace_persistence_failure"]["runtime_terminal_status"] == "completed"
    assert snapshots["runtime_unavailable"]["trace"]["persisted"] is False
    assert snapshots["runtime_unavailable"]["call_order"] == [
        "conversation_resolution",
        "cr_turn_start",
    ]


async def _ordinary_presence(self, **request):
    state = "driving_or_active_task" if request["active_task_mode"] else "active_conversation"
    reason = "active_task_mode" if request["active_task_mode"] else "thread_active"
    if request["explicit_proactive_opt_out"]:
        state, reason = "do_not_intrude", "explicit_proactive_opt_out"
    return _replay_presence_response(request, state, reason)


def _replay_presence_response(request, state, reason):
    suppressed = request["proactive_output_suppressed"]
    return {
        **{key: value for key, value in request.items()
           if key not in {
               "active_task_mode", "proactive_output_suppressed", "explicit_proactive_opt_out",
               "surface_permission_status", "proactive_presence_allowed",
               "ambient_listening_allowed",
           }},
        "result": {
            "presence_state": state, "previous_presence_state": None, "state_changed": True,
            "proactive_output_suppressed": suppressed or state not in {
                "available", "active_conversation",
            },
            "required_help_allowed": state != "not_present",
            "reason_codes": [reason] + (["proactive_suppression_requested"] if suppressed else []),
            "policy_version": "runtime-presence.v1",
        },
    }


async def _absent_proactive_preference(self, *, owner_id):
    return {
        "owner_id": owner_id, "enabled": False,
        "allowed_surfaces_json": [], "rule_prefs_json": {},
        "created_at": None, "updated_at": None,
    }


async def _configured_surface_permission(self, *, owner_id, surface):
    return {
        "owner_id": owner_id, "surface": surface, "configured": True,
        "conversation_context_allowed": True, "proactive_presence_allowed": True,
        "ambient_listening_allowed": False, "created_at": "2026-10-05T00:00:00+00:00",
        "updated_at": "2026-10-05T00:00:00+00:00",
    }


@pytest.fixture(autouse=True)
def replay_presence_contract(monkeypatch):
    # Extend the existing boundary fake; historical corpus projections remain unchanged.
    monkeypatch.setattr(ReplayMemoryStore, "get_presence_surface_permission",
                        _configured_surface_permission, raising=False)
    monkeypatch.setattr(ReplayRuntime, "evaluate_presence", _ordinary_presence, raising=False)
    monkeypatch.setattr(ReplayRuntime, "evaluate_timing", _replay_timing, raising=False)
    monkeypatch.setattr(ReplayMemoryStore, "get_proactive_preferences",
                        _absent_proactive_preference, raising=False)


async def _run_presence_turn(
    monkeypatch, *, state=None, reason=None, failure=None, mutation=None,
    context=None, surface="chat", restraint=False, configured=True, provider_mode="success",
    preference=None, preference_error=None, admission_failure=False, owner_id="owner-replay",
):
    from services import orchestrate

    scenario = {"scenario": "presence", "provider": provider_mode}
    if admission_failure:
        scenario["runtime"] = "unavailable"
    calls = []
    memory = ReplayMemoryStore(scenario, calls)
    runtime = ReplayRuntime(scenario, calls)
    provider = ReplayProvider(scenario, calls)
    requests, messages = [], []
    original_restraint = runtime.evaluate_restraint
    original_chat = provider.chat
    original_situated = orchestrate.resolve_situated_presence

    async def get_proactive_preferences(*, owner_id):
        calls.append({"name": "proactive_preference", "owner_id": owner_id})
        if preference_error is not None:
            raise preference_error
        if preference is not None:
            return deepcopy(preference)
        return await _absent_proactive_preference(memory, owner_id=owner_id)

    async def evaluate_presence(**request):
        calls.append({"name": "cr_presence"})
        requests.append(deepcopy(request))
        if failure is not None:
            raise failure
        response = (
            _replay_presence_response(request, state, reason) if state
            else await _ordinary_presence(runtime, **request)
        )
        if mutation:
            mutation(response)
        return response

    async def evaluate_restraint(**request):
        response = await original_restraint(**request)
        response["result"]["proactive_output_suppressed"] = restraint
        return response

    async def situated(**request):
        calls.append({"name": "situated_presence"})
        return await original_situated(**request)

    async def chat(**request):
        messages.append(deepcopy(request["messages"]))
        return await original_chat(**request)

    monkeypatch.setattr(memory, "get_proactive_preferences", get_proactive_preferences)
    monkeypatch.setattr(runtime, "evaluate_presence", evaluate_presence)
    monkeypatch.setattr(runtime, "evaluate_restraint", evaluate_restraint)
    monkeypatch.setattr(provider, "chat", chat)
    monkeypatch.setattr(orchestrate, "resolve_situated_presence", situated)
    payload = _payload(scenario)
    payload.update(surface=surface, surface_context=context or {}, owner_id=owner_id)
    result = await orchestrate.orchestrate_chat(
        payload=payload, memory_store=memory, litellm=provider,
        runtime=runtime if configured else None,
        rules_path=str(RULES_PATH), model_registry_path=str(REGISTRY_PATH),
        allow_manual_override=False, enable_runtime_overlays=True,
        interaction_governance_enabled=True, persona_containment_enabled=True,
        restraint_enabled=True, request_id="presence-request",
        message_id_factory=lambda: "00000000-0000-4000-8000-000000000099",
    )
    return result, memory, runtime, calls, requests, messages


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [True, False, 1, "true", None])
@pytest.mark.parametrize("restraint", [True, False, 1, "true"])
async def test_presence_admitted_order_exact_scope_and_typed_projections(
    monkeypatch, active, restraint,
):
    result, memory, runtime, calls, requests, messages = await _run_presence_turn(
        monkeypatch, context={"active_task_mode": active}, restraint=restraint,
    )
    if active is not None and type(active) is not bool:
        assert result["status"] == "failed"
        assert messages == []
        assert runtime.terminal_status == "abandoned"
        assert memory.trace["retrieval"]["prompt_assembly"]["runtime_timing"][
            "failure_category"
        ] == "request_projection_invalid"
        return
    assert result["status"] == "ok"
    names = [call["name"] for call in calls]
    assert names.index("cr_turn_start") < names.index("cr_restraint")
    assert names.index("cr_restraint") < names.index("cr_presence")
    assert names.index("cr_presence") < names.index("situated_presence")
    assert names.index("situated_presence") < names.index("user_message_persistence")
    assert requests == [{
        "request_id": "presence-request", "owner_id": "owner-replay",
        "conversation_id": result["conversation_id"], "surface": "chat",
        "runtime_session_id": "runtime-session-1", "runtime_turn_id": "runtime-turn-1",
        "active_task_mode": active is True, "proactive_output_suppressed": restraint is True,
        "explicit_proactive_opt_out": False,
        "surface_permission_status": "configured", "proactive_presence_allowed": True,
        "ambient_listening_allowed": False,
    }]
    trace = memory.trace["retrieval"]["prompt_assembly"]
    presence = trace["runtime_presence"]
    assert presence["status"] == presence["runtime_call_status"] == "included"
    assert presence["attempted"] is True
    assert presence["required_help_allowed"] is True
    assert presence["fallback_status"] == "not_used"
    assert "situated_presence" in trace and "surface_presence" in trace
    assert runtime.terminal_status == "completed"
    assert messages


@pytest.mark.asyncio
@pytest.mark.parametrize("state,reason", [
    ("available", "session_available"), ("active_conversation", "thread_active"),
    ("low_attention", "attention_paused"), ("idle", "session_idle"),
    ("driving_or_active_task", "session_active_task_mode"),
])
async def test_presence_required_help_and_guidance_reach_every_provider_attempt(
    monkeypatch, state, reason,
):
    result, memory, _, _, _, messages = await _run_presence_turn(
        monkeypatch, state=state, reason=reason, provider_mode="fallback_success",
    )
    assert result["status"] == "degraded"
    assert len(messages) == 2
    suppressed = state not in {"available", "active_conversation"}
    trace = memory.trace["retrieval"]["prompt_assembly"]
    assert trace["runtime_presence"]["presence_state"] == state
    for attempt in messages:
        text = json.dumps(attempt)
        assert ("Omit optional proactive suggestions" in text) == suppressed
        if suppressed:
            assert "Preserve all information required" in text
            assert trace["response_shape"]["runtime_presence"]["applied"] is True
        for private in ["runtime-presence.v1", "reason_codes", "low_attention", "R" + "44"]:
            assert private not in text


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,mutation,category", [
    (httpx.ReadTimeout("PRIVATE"), None, "transport_timeout"),
    (httpx.ConnectError("PRIVATE"), None, "transport_failure"),
    (httpx.HTTPStatusError("PRIVATE", request=httpx.Request("POST", "http://runtime"),
                          response=httpx.Response(503)), None, "dependency_http_failure"),
    (Exception("PRIVATE"), None, "dependency_unavailable"),
    (None, lambda r: r.update(owner_id="PRIVATE"), "context_mismatch"),
    (None, lambda r: r["result"].update(private="PRIVATE"), "response_invalid"),
    (None, lambda r: r["result"].update(presence_state="ambient_listening"), "response_invalid"),
    (None, lambda r: r["result"].update(presence_state="returning_after_gap"), "response_invalid"),
    (None, lambda r: r["result"].update(presence_state="do_not_intrude"), "response_invalid"),
])
async def test_presence_dependency_failure_preserves_help_without_inferred_state(
    monkeypatch, failure, mutation, category,
):
    result, memory, _, _, _, messages = await _run_presence_turn(
        monkeypatch, failure=failure, mutation=mutation,
    )
    assert result["status"] == "ok"
    trace = memory.trace["retrieval"]["prompt_assembly"]
    presence = trace["runtime_presence"]
    assert presence == {
        "attempted": True, "status": "fallback", "runtime_call_status": "failed",
        "presence_state": None, "previous_presence_state": None, "state_changed": None,
        "proactive_output_suppressed": True, "required_help_allowed": True,
        "policy_version": None, "reason_codes": [], "fallback_status": "suppression_only",
        "failure_category": category,
    }
    assert "Omit optional proactive suggestions" in json.dumps(messages)
    assert "PRIVATE" not in json.dumps([trace, messages])


@pytest.mark.asyncio
async def test_presence_disabled_preserves_existing_response_behavior(monkeypatch):
    result, memory, _, calls, requests, messages = await _run_presence_turn(
        monkeypatch, configured=False,
    )
    assert result["status"] == "ok"
    assert requests == []
    trace = memory.trace["retrieval"]["prompt_assembly"]
    assert trace["runtime_presence"]["status"] == "disabled"
    assert trace["runtime_presence"]["runtime_call_status"] == "not_attempted"
    assert trace["runtime_presence"]["proactive_output_suppressed"] is False
    assert "runtime_presence" not in trace["response_shape"]
    assert "Omit optional proactive suggestions" not in json.dumps(messages)

    assert trace["turn_state"]["conversation_resolution"] == {
        "mode": "compatibility_create_new", "runtime_status": "unavailable",
        "retained_context_allowed": False,
    }
    assert "cr_continuation_selection" not in [call["name"] for call in calls]
    for private in ("allowed_surfaces_json", "rule_prefs_json", "ambient_listening_allowed",
                    "conversation_context_allowed", "2026-10-05T00:00:00+00:00"):
        assert private not in json.dumps([trace, messages, result])


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["car", "voice", "alexa"])
async def test_presence_never_infers_active_task_from_surface(monkeypatch, surface):
    _, _, _, _, requests, _ = await _run_presence_turn(monkeypatch, surface=surface)
    assert requests[0]["active_task_mode"] is False


@pytest.mark.asyncio
async def test_presence_disallowed_help_abandons_admitted_work_before_content_use(monkeypatch):
    result, memory, runtime, calls, _, messages = await _run_presence_turn(
        monkeypatch, state="not_present", reason="session_not_present",
    )
    assert result["status"] == "failed"
    assert result["selected_model"] == "not_called"
    assert "current interaction" in result["answer"]
    assert "not_present" not in json.dumps(result)
    assert "admitted" not in result["answer"]
    assert runtime.terminal_status == "abandoned"
    assert memory.work["state"] == "failed"
    assert messages == []
    names = [call["name"] for call in calls]
    assert "cr_turn_start" in names and "cr_presence" in names
    assert not set(names) & {
        "user_message_persistence", "assistant_message_persistence", "retrieval",
        "provider_attempt", "profile_resolution", "capability_execution",
    }
    assert memory.trace["retrieval"]["prompt_assembly"]["runtime_presence"][
        "required_help_allowed"
    ] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["web", "telegram", "car"])
@pytest.mark.parametrize("owner", ["owner-replay", "owner-other"])
@pytest.mark.parametrize("kind", ["absent", "enabled", "disabled"])
async def test_presence_projects_only_persisted_opt_out(monkeypatch, surface, owner, kind):
    preference = {
        "owner_id": owner, "enabled": kind == "enabled",
        "allowed_surfaces_json": ["PRIVATE-SURFACE"],
        "rule_prefs_json": {"PRIVATE-RULE": "PRIVATE-VALUE"},
        "created_at": "2001-02-03T04:05:06+00:00",
        "updated_at": "2002-03-04T05:06:07+00:00",
    }
    if kind == "absent":
        preference = await _absent_proactive_preference(None, owner_id=owner)
    result, memory, _, calls, requests, messages = await _run_presence_turn(
        monkeypatch, preference=preference, surface=surface, owner_id=owner,
        context={"allows_expansion": True},
    )
    assert result["status"] == "ok"
    assert messages
    opt_out = kind == "disabled"
    assert requests == [{
        "request_id": "presence-request", "owner_id": owner,
        "conversation_id": result["conversation_id"], "surface": surface,
        "runtime_session_id": "runtime-session-1", "runtime_turn_id": "runtime-turn-1",
        "active_task_mode": False, "proactive_output_suppressed": False,
        "explicit_proactive_opt_out": opt_out,
        "surface_permission_status": "configured", "proactive_presence_allowed": True,
        "ambient_listening_allowed": False,
    }]
    names = [call["name"] for call in calls]
    assert names.index("cr_turn_start") < names.index("cr_restraint")
    assert names.index("cr_restraint") < names.index("proactive_preference")
    assert names.index("proactive_preference") < names.index("cr_presence")
    assert names.index("cr_presence") < names.index("situated_presence")
    assert [call for call in calls if call["name"] == "proactive_preference"] == [
        {"name": "proactive_preference", "owner_id": owner},
    ]
    trace = memory.trace["retrieval"]["prompt_assembly"]
    presence = trace["runtime_presence"]
    assert presence["status"] == "included"
    assert presence["presence_state"] == ("do_not_intrude" if opt_out else "active_conversation")
    assert presence["required_help_allowed"] is True
    assert presence["proactive_output_suppressed"] is opt_out
    assert ("Omit optional proactive suggestions" in json.dumps(messages)) is opt_out
    if opt_out:
        shape = trace["response_shape"]["resolved_shape"]
        assert shape["allows_expansion"] is False
        assert shape["expansion_marker_allowed"] is False
        assert "Preserve all information required" in json.dumps(messages)
    for private in [
        "allowed_surfaces_json", "rule_prefs_json", "PRIVATE-SURFACE", "PRIVATE-RULE",
        "PRIVATE-VALUE", "2001-02-03", "2002-03-04", "proactive_consent",
    ]:
        assert private not in json.dumps([requests, messages, memory.trace, result])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [
    "timeout", "transport", "http", "exception", "malformed", "owner",
])
async def test_preference_failure_suppresses_without_fabricating_opt_out(monkeypatch, failure):
    preference = None
    error = None
    if failure == "timeout":
        error = httpx.ReadTimeout("PRIVATE-FAILURE")
    elif failure == "transport":
        error = httpx.ConnectError("PRIVATE-FAILURE")
    elif failure == "http":
        error = httpx.HTTPStatusError(
            "PRIVATE-FAILURE", request=httpx.Request("GET", "http://memory"),
            response=httpx.Response(503),
        )
    elif failure == "exception":
        error = Exception("PRIVATE-FAILURE")
    elif failure == "malformed":
        preference = {"PRIVATE-FAILURE": True}
    else:
        preference = await _absent_proactive_preference(None, owner_id="PRIVATE-FAILURE")
    result, memory, _, calls, requests, messages = await _run_presence_turn(
        monkeypatch, preference=preference, preference_error=error,
    )
    assert result["status"] == "ok"
    assert requests[0]["explicit_proactive_opt_out"] is False
    assert requests[0]["proactive_output_suppressed"] is True
    presence = memory.trace["retrieval"]["prompt_assembly"]["runtime_presence"]
    assert presence["status"] == "included"
    assert presence["presence_state"] == "active_conversation"
    assert presence["required_help_allowed"] is True
    assert presence["reason_codes"] == ["thread_active", "proactive_suppression_requested"]
    assert "Omit optional proactive suggestions" in json.dumps(messages)
    assert "Preserve all information required" in json.dumps(messages)
    assert "PRIVATE-FAILURE" not in json.dumps([calls, requests, messages, memory.trace, result])


@pytest.mark.asyncio
@pytest.mark.parametrize("configured,admission_failure", [(False, False), (True, True)])
async def test_preference_read_requires_configured_and_admitted_runtime(
    monkeypatch, configured, admission_failure,
):
    result, _, _, calls, requests, _ = await _run_presence_turn(
        monkeypatch, configured=configured, admission_failure=admission_failure,
    )
    assert requests == []
    assert "proactive_preference" not in [call["name"] for call in calls]
    assert result["status"] == ("failed" if admission_failure else "ok")


async def _replay_timing(self, **request):
    from clients.runtime import (
        RUNTIME_TIMING_BUDGET_MS,
        RUNTIME_TIMING_PROJECTIONS,
        RUNTIME_TIMING_REASON_POLICIES,
    )

    self.timing_calls = getattr(self, "timing_calls", [])
    self.timing_calls.append(deepcopy(request))
    if self.scenario.get("timing_record"):
        self._record("cr_timing", request["request_id"])
    if self.scenario.get("timing_failure"):
        raise httpx.ReadTimeout("private timing dependency failure")
    policy = self.scenario.get("timing_policy", "answer_now")
    reason = next(reason for reason, value in RUNTIME_TIMING_REASON_POLICIES.items()
                  if value == policy and reason != "dependency_blocking")
    if request["dependency_state"] == "blocking":
        policy, reason = "close_turn", "dependency_blocking"
    state, expansion, overlay = RUNTIME_TIMING_PROJECTIONS[policy]
    return {
        **{key: request[key] for key in (
            "request_id", "owner_id", "conversation_id", "surface",
            "runtime_session_id", "runtime_turn_id",
        )},
        "result": {
            "timing_policy": policy, "reason_codes": [reason] + (
                ["dependency_degraded"] if request["dependency_state"] == "degraded" else []
            ),
            "latency_budget_class": request["latency_budget_class"],
            "latency_budget_ms": RUNTIME_TIMING_BUDGET_MS[request["latency_budget_class"]],
            "expansion_allowed": expansion, "continuation_state": state,
            "degradation_mode": {"ready": "none", "degraded": "bounded", "blocking": "fail_closed"}[
                request["dependency_state"]
            ],
            "policy_version": "runtime-timing.v1", "prompt_overlay": overlay,
            "trace_ref": "rtrace-replay-timing",
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", [
    "answer_now", "defer_expansion", "ask_clarifying_question", "pause_or_wait",
    "yield_to_user", "close_turn", "resume_previous_thread",
])
async def test_timing_replay_fixtures_control_generation_and_keep_trace_private(
    monkeypatch, policy,
):
    scenario = {"scenario": "timing-fixture", "category": "timing", "provider": "success",
                "interaction_governance_enabled": True, "restraint_enabled": True,
                "timing_policy": policy, "timing_record": True}
    traces = []
    original_trace = ReplayMemoryStore.create_trace

    async def create_trace(self, **kwargs):
        traces.append(deepcopy(kwargs["payload"]))
        return await original_trace(self, **kwargs)

    monkeypatch.setattr(ReplayMemoryStore, "create_trace", create_trace)
    snapshot = await run_scenario(scenario)
    names = snapshot["call_order"]
    assert names.count("cr_timing") == 1
    timing_index = names.index("cr_timing")
    assert names.index("cr_interaction_governance") < timing_index
    assert names.index("cr_restraint") < timing_index
    if policy in {"ask_clarifying_question", "pause_or_wait", "yield_to_user", "close_turn"}:
        assert not any(name.startswith("provider_") for name in names)
    else:
        assert timing_index < next(
            i for i, name in enumerate(names) if name.startswith("provider_")
        )
    timing = traces[0]["retrieval"]["prompt_assembly"]["runtime_timing"]
    assert timing["result"]["timing_policy"] == policy
    assert set(timing["scope"]) == {
        "request_id", "owner_id", "conversation_id", "surface",
        "runtime_session_id", "runtime_turn_id",
    }
    assert "neutral request" not in json.dumps(timing)
    assert "neutral response" not in json.dumps(timing)
    assert_snapshot_privacy_safe(timing)


@pytest.mark.asyncio
@pytest.mark.parametrize("category", ["provider_fallback", "provider_exhaustion"])
async def test_timing_replay_fallback_preserves_admitted_class_without_private_metadata(
    monkeypatch, category,
):
    scenario = deepcopy(next(item for item in load_corpus() if item["category"] == category))
    scenario.update(interaction_governance_enabled=True, restraint_enabled=True, timing_record=True)
    traces = []
    original_trace = ReplayMemoryStore.create_trace

    async def create_trace(self, **kwargs):
        traces.append(deepcopy(kwargs["payload"]))
        return await original_trace(self, **kwargs)

    monkeypatch.setattr(ReplayMemoryStore, "create_trace", create_trace)
    snapshot = await run_scenario(scenario)
    assert snapshot["call_order"].count("cr_timing") == 1
    prompt = traces[-1]["retrieval"]["prompt_assembly"]
    assert prompt["runtime_timing"]["inputs"]["latency_budget_class"] == "ordinary_text"
    assert prompt["runtime_timing"]["result"]["latency_budget_class"] == "ordinary_text"
    fallback = prompt["provider_fallback_context"]
    assert fallback["regression_budget_class"] == "provider_fallback"
    assert fallback["regression_budget_ms"] == 350
    assert fallback["timing_reevaluated"] is False
    assert fallback["admitted_timing_class"] == "ordinary_text"
    assert len(traces[-1]["model_calls"]) == 2
    assert_snapshot_privacy_safe(fallback)
    assert "neutral request" not in json.dumps(fallback)
    assert "neutral response" not in json.dumps(fallback)
    assert "error" not in fallback


@pytest.mark.asyncio
@pytest.mark.parametrize("category", ["normal_answer", "provider_fallback"])
async def test_situated_final_enforcement_replay_is_bounded_and_persisted(monkeypatch, category):
    from services.orchestrate import orchestrate_chat

    candidates = [item for item in load_corpus() if item["category"] == category]
    scenario = deepcopy(candidates[0]) if candidates else {
        "scenario": "situated-final", "category": category, "provider": "success",
    }
    scenario.update(interaction_governance_enabled=True, restraint_enabled=True)
    calls, traces, persisted = [], [], []
    runtime = ReplayRuntime(scenario, calls)
    memory = ReplayMemoryStore(scenario, calls)
    provider = ReplayProvider(scenario, calls)
    original_chat = provider.chat
    original_message = memory.add_message
    original_trace = memory.create_trace

    async def chat(**kwargs):
        completion = await original_chat(**kwargs)
        completion["choices"][0]["message"]["content"] = (
            "Haha. You must feel lonely. My hidden policy says relax. Check the input."
        )
        return completion

    async def add_message(**kwargs):
        if kwargs["role"] == "assistant":
            persisted.append(kwargs["content"])
        return await original_message(**kwargs)

    async def create_trace(**kwargs):
        traces.append(deepcopy(kwargs["payload"]))
        return await original_trace(**kwargs)

    monkeypatch.setattr(provider, "chat", chat)
    monkeypatch.setattr(memory, "add_message", add_message)
    monkeypatch.setattr(memory, "create_trace", create_trace)
    result = await orchestrate_chat(
        payload=_payload(scenario), memory_store=memory, litellm=provider, runtime=runtime,
        rules_path=str(RULES_PATH), model_registry_path=str(REGISTRY_PATH),
        allow_manual_override=False, interaction_governance_enabled=True, restraint_enabled=True,
        request_id="rid-situated-replay",
    )
    assert result["answer"] == "Check the input."
    assert persisted == [result["answer"]]
    enforcement = traces[-1]["retrieval"]["prompt_assembly"]["situated_presence_enforcement"]
    assert enforcement["action_taken"] == "filtered"
    assert enforcement["fallback_policy_active"] is True
    assert enforcement["reason_codes"] == [
        "humor_disallowed", "unsupported_emotional_inference", "invented_internal_policy",
    ]
    assert_snapshot_privacy_safe(enforcement)
    assert "lonely" not in json.dumps(enforcement)
    assert "Check the input" not in json.dumps(enforcement)
    assert len([call for call in calls if call["name"] == "provider_attempt"]) == (
        2 if category == "provider_fallback" else 1
    )


@pytest.mark.asyncio
async def test_return_resume_provenance_is_structural_and_persistence_is_exact(tmp_path):
    from test_orchestrate_flow import ReturnMemoryStore, ReturnRuntime, _run_timing_turn

    out, runtime, provider, memory = await _run_timing_turn(
        tmp_path,
        policy="resume_previous_thread",
        runtime=ReturnRuntime(),
        memory=ReturnMemoryStore(),
    )
    trace = memory.trace_calls[-1]["payload"]["retrieval"]["prompt_assembly"]
    provenance = {
        "snapshot": trace["turn_state"]["return_after_gap"],
        "context": trace["return_resume_context"],
    }
    encoded = json.dumps(provenance)
    assert "backup procedure" not in encoded
    assert "current_user_text" not in encoded and "content" not in provenance["context"]
    assert provenance["context"]["source_count"] == 1
    assert provenance["snapshot"]["threshold_seconds"] == 300
    assert len(runtime.timing_calls) == len(provider.calls) == 1
    assert [m["content"] for m in memory.added_messages if m["role"] == "assistant"] == [
        out["answer"]
    ]
