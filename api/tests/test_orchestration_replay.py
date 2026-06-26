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

    async def fake_post(path, *, request_id=None, json):
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
    assert "-    \"persisted\": true" in message.lower()
    assert "+    \"persisted\": false" in message.lower()


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
        "truth_stale_only",
        "truth_missing_source",
        "truth_cross_owner",
        "truth_cr_unavailable",
        "truth_cr_malformed",
        "truth_cr_conflicting",
        "provider_exhaustion",
        "no_fallback",
        "request_id_mismatch",
        "bms_unavailable",
        "trace_persistence_failure",
    } <= categories


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

    no_fallback_fixture = next(
        item for item in load_corpus() if item["category"] == "no_fallback"
    )
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
        assert trace["budget_enforcement"] == "not_enforced"
        assert isinstance(trace["prompt_layers"], list)
        assert isinstance(trace["artifacts"].get("artifact_count"), int)
        assert isinstance(trace["references"], list)
        assert "content" not in str(trace)
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
