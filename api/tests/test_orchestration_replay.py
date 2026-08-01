import json

import pytest
from clients.memory_store import MemoryStoreClient
from clients.runtime import RuntimeClient
from services.orchestration_replay import (
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
