import pytest
from clients.memory_store import MemoryStoreClient
from services.orchestration_replay import (
    assert_snapshot_privacy_safe,
    compare_snapshot,
    load_corpus,
    project_snapshot,
    run_scenario,
)


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
    } <= categories


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
        "user_message_persistence",
        "cr_turn_start",
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
    assert snapshots["runtime_unavailable"]["trace"]["persisted"] is True
    assert snapshots["runtime_unavailable"]["trace"]["runtime_overlay"]["status"] == "failed"
