import pytest
from services.prompt_assembly import assemble_prompt
from services.prompt_budget import (
    ESTIMATOR_ID,
    PromptBudgetContract,
    PromptBudgetError,
    ProviderAttempt,
    estimate_message_tokens,
    estimate_messages_tokens,
    estimate_prompt_tokens,
)


def _attempt(limit: int = 4096, model: str = "primary") -> ProviderAttempt:
    return ProviderAttempt(
        model=model,
        provider="local",
        max_context_tokens=limit,
        role="primary" if model == "primary" else "fallback",
    )


def _contract(limit: int = 4096, *, profile_budget=None) -> PromptBudgetContract:
    return PromptBudgetContract(
        attempts=[_attempt(limit)],
        output_token_reserve=0,
        context_safety_margin=0,
        profile_prompt_budget=profile_budget,
    )


def test_estimator_formula_is_versioned_deterministic_and_utf8_aware():
    message = {"role": "user", "content": "abcd"}
    assert ESTIMATOR_ID == "co-local-utf8-v1"
    assert estimate_message_tokens(message) == 4 + len("user") + 1
    assert estimate_prompt_tokens([message]) == 2 + estimate_message_tokens(message)
    assert estimate_prompt_tokens([message]) == estimate_prompt_tokens([dict(message)])
    assert estimate_message_tokens({"role": "user", "content": "abcd🙂"}) > estimate_message_tokens(
        message
    )
    assert estimate_message_tokens(
        {"role": "user", "content": "abcdefgh"}
    ) > estimate_message_tokens(message)


def test_under_budget_preserves_roles_wording_and_reports_no_truncation():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={
            "bundle": {
                "recent": [{"role": "assistant", "content": "prior history"}],
                "semantic": [
                    {
                        "message_id": "m-1",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "role": "assistant",
                        "content": "semantic note",
                        "score": 0.7,
                    }
                ],
                "artifact_refs": [],
            }
        },
        current_messages=[{"role": "user", "content": "hi"}],
        prompt_budget_contract=_contract(4096),
    )
    assert [msg["role"] for msg in out.messages] == ["system", "system", "assistant", "user"]
    assert out.messages[0]["content"] == "profile text"
    assert out.trace["prompt_budget"]["status"] == "not_required"
    assert out.trace["prompt_budget"]["final_within_budget"] is True
    assert out.trace["truncation"] == {"applied": False, "reason": None}


def test_smaller_fallback_and_profile_clamp_constrain_effective_budget():
    out = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "short"}],
        prompt_budget_contract=PromptBudgetContract(
            attempts=[
                _attempt(2000, "primary"),
                ProviderAttempt("fallback", "local", 1000, "fallback"),
            ],
            output_token_reserve=100,
            context_safety_margin=50,
            profile_prompt_budget={"max_input_tokens": 500},
        ),
    )
    budget = out.trace["prompt_budget"]
    assert budget["effective_min_context_limit"] == 1000
    assert budget["effective_hard_input_budget"] == 500
    assert budget["profile_clamp"]["applied"] is True


def test_malformed_profile_clamp_cannot_expand_model_budget():
    out = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "short"}],
        prompt_budget_contract=PromptBudgetContract(
            attempts=[_attempt(100)],
            output_token_reserve=0,
            context_safety_margin=0,
            profile_prompt_budget={"max_input_tokens": "10000"},
        ),
    )
    budget = out.trace["prompt_budget"]
    assert budget["effective_hard_input_budget"] == 100
    assert budget["profile_clamp"]["valid"] is False
    assert budget["profile_clamp"]["warning"] == "invalid_profile_prompt_budget"


def test_reduction_order_preserves_final_current_turn_and_surviving_sources():
    out = assemble_prompt(
        profile={"prompt_overlay": "required profile"},
        retrieval_bundle={
            "bundle": {
                "recent": [
                    {"role": "assistant", "content": "old recent " * 20},
                    {"role": "assistant", "content": "new recent"},
                ],
                "semantic": [
                    {
                        "message_id": "hist-low",
                        "created_at": "2026-01-01",
                        "role": "assistant",
                        "content": "historical low " * 20,
                        "score": 0.1,
                        "memory_hygiene": {"framing": "stale_or_unverified"},
                    },
                    {
                        "message_id": "current-low",
                        "created_at": "2026-01-02",
                        "role": "assistant",
                        "content": "current low " * 20,
                        "score": 0.2,
                        "_truth_framing": "current",
                    },
                    {
                        "message_id": "current-high",
                        "created_at": "2026-01-03",
                        "role": "assistant",
                        "content": "current high",
                        "score": 0.9,
                        "_truth_framing": "current",
                    },
                ],
                "artifact_refs": [
                    {
                        "artifact_id": "artifact-low",
                        "file_path": "low.txt",
                        "snippet": "artifact low " * 20,
                        "relevance_score": 0.05,
                        "memory_hygiene": {"framing": "stale_or_unverified"},
                    },
                    {
                        "artifact_id": "artifact-high",
                        "file_path": "high.txt",
                        "snippet": "artifact high",
                        "relevance_score": 0.95,
                        "_truth_framing": "current",
                    },
                ],
            }
        },
        external_context_pack={
            "items": [
                {
                    "source_ref": "external-low",
                    "source_name": "docs",
                    "title": "low",
                    "text": "external low " * 20,
                    "relevance_score": 0.1,
                }
            ],
            "sources_used": ["docs"],
        },
        world_state=None,
        relationship_context=None,
        runtime_overlay=None,
        style_guidance=None,
        response_shape_guidance=None,
        current_messages=[
            {"role": "user", "content": "old request " * 20},
            {"role": "assistant", "content": "draft"},
            {"role": "user", "content": "final question"},
        ],
        prompt_budget_contract=_contract(150),
    )
    prompt_text = "\n".join(message["content"] for message in out.messages)
    assert "old request" not in prompt_text
    assert "old recent" not in prompt_text
    assert "historical low" not in prompt_text
    assert "artifact low" not in prompt_text
    assert "external low" not in prompt_text
    assert "current low" not in prompt_text
    assert "current high" in prompt_text
    assert out.messages[-1] == {"role": "user", "content": "final question"}
    assert out.trace["prompt_budget"]["status"] == "optional_context_reduced"
    assert out.trace["prompt_budget"]["final_within_budget"] is True
    assert out.trace["retained_source_ids"]["artifact_ids"] == ["artifact-high"]


def test_whole_optional_layers_reduce_after_retrieval_layers():
    out = assemble_prompt(
        profile={"prompt_overlay": "required profile"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "final question"}],
        world_state={"prompt_content": "world " * 20},
        relationship_context={"prompt_content": "relationship " * 20},
        runtime_overlay={"role": "system", "content": "runtime " * 20},
        style_guidance="style " * 20,
        response_shape_guidance="shape " * 20,
        prompt_budget_contract=_contract(110),
    )
    omitted = out.trace["prompt_budget"]["dropped_context"]["by_layer"]
    assert omitted["world_state"] == 1
    assert omitted["relationship_context"] == 1
    assert omitted["runtime_overlay"] == 1
    assert "style_guidance" not in omitted
    assert "response_shape" not in omitted


@pytest.mark.parametrize("limit", [0, -1, True])
def test_invalid_model_context_limit_blocks_budgeting(limit):
    with pytest.raises(PromptBudgetError) as exc:
        assemble_prompt(
            profile={"prompt_overlay": ""},
            retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
            current_messages=[{"role": "user", "content": "short"}],
            prompt_budget_contract=_contract(limit),
        )
    assert exc.value.reason == "model_context_limit_unavailable"


def test_required_content_overflow_raises_bounded_budget_error():
    with pytest.raises(PromptBudgetError) as exc:
        assemble_prompt(
            profile={"prompt_overlay": "required profile " * 50},
            retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
            current_messages=[{"role": "user", "content": "final user text " * 50}],
            prompt_budget_contract=_contract(20),
        )
    assert exc.value.reason == "required_prompt_content_exceeds_budget"
    assert "final user text" not in str(exc.value.trace)


def test_prompt_budget_trace_states_keep_truncation_and_failure_reasons_distinct():
    under_budget = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "short"}],
        prompt_budget_contract=_contract(4096),
    )
    assert under_budget.trace["truncation"] == {"applied": False, "reason": None}
    assert under_budget.trace["prompt_budget"]["failure_reason"] is None

    reduced = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={
            "bundle": {
                "recent": [{"role": "assistant", "content": "old recent " * 40}],
                "semantic": [],
                "artifact_refs": [],
            }
        },
        current_messages=[{"role": "user", "content": "final question"}],
        prompt_budget_contract=_contract(70),
    )
    assert reduced.trace["truncation"] == {
        "applied": True,
        "reason": "optional_context_reduced",
    }
    assert reduced.trace["prompt_budget"]["failure_reason"] is None

    with pytest.raises(PromptBudgetError) as exc:
        assemble_prompt(
            profile={"prompt_overlay": "required profile " * 50},
            retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
            current_messages=[{"role": "user", "content": "final user text"}],
            prompt_budget_contract=_contract(20),
        )
    assert exc.value.trace["failure_reason"] == "required_prompt_content_exceeds_budget"
    assert exc.value.trace["omission_or_truncation_occurred"] is False


def test_layer_accounting_reconciles_with_total_before_and_after_reduction():
    out = assemble_prompt(
        profile={"prompt_overlay": "required profile"},
        retrieval_bundle={
            "bundle": {
                "recent": [{"role": "assistant", "content": "old recent " * 20}],
                "semantic": [
                    {
                        "message_id": "current",
                        "created_at": "2026-01-03",
                        "role": "assistant",
                        "content": "current high",
                        "score": 0.9,
                        "_truth_framing": "current",
                    }
                ],
                "artifact_refs": [],
            }
        },
        current_messages=[
            {"role": "user", "content": "old request " * 20},
            {"role": "user", "content": "final question"},
        ],
        prompt_budget_contract=_contract(80),
    )
    budget = out.trace["prompt_budget"]
    before_sum = sum(layer["before_estimated_tokens"] for layer in budget["per_layer"])
    after_sum = sum(layer["after_estimated_tokens"] for layer in budget["per_layer"])
    assert before_sum == budget["estimated_tokens_before_budgeting"]
    assert after_sum == budget["estimated_tokens_after_budgeting"]
    assert budget["estimated_global_prompt_overhead_tokens"] == 2


@pytest.mark.parametrize("bad_limit", ["1000", True, 0, -5])
def test_malformed_model_context_limits_block_budgeting(bad_limit):
    with pytest.raises(PromptBudgetError) as exc:
        assemble_prompt(
            profile={"prompt_overlay": ""},
            retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
            current_messages=[{"role": "user", "content": "short"}],
            prompt_budget_contract=PromptBudgetContract(
                attempts=[ProviderAttempt("bad", "local", bad_limit, "primary")],
                output_token_reserve=0,
                context_safety_margin=0,
            ),
        )
    assert exc.value.reason == "model_context_limit_unavailable"


def test_missing_eligible_fallback_context_limit_blocks_budgeting():
    with pytest.raises(PromptBudgetError) as exc:
        assemble_prompt(
            profile={"prompt_overlay": ""},
            retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
            current_messages=[{"role": "user", "content": "short"}],
            prompt_budget_contract=PromptBudgetContract(
                attempts=[
                    ProviderAttempt("primary", "cloud", 1000, "primary"),
                    ProviderAttempt("fallback", "local", None, "fallback"),
                ],
                output_token_reserve=0,
                context_safety_margin=0,
            ),
        )
    assert exc.value.reason == "model_context_limit_unavailable"


def test_reserve_and_margin_exceeding_context_blocks_budgeting():
    with pytest.raises(PromptBudgetError) as exc:
        assemble_prompt(
            profile={"prompt_overlay": ""},
            retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
            current_messages=[{"role": "user", "content": "short"}],
            prompt_budget_contract=PromptBudgetContract(
                attempts=[_attempt(100)],
                output_token_reserve=80,
                context_safety_margin=20,
            ),
        )
    assert exc.value.reason == "effective_prompt_budget_unusable"


def test_overlarge_profile_clamp_does_not_expand_budget_and_warns():
    out = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "short"}],
        prompt_budget_contract=PromptBudgetContract(
            attempts=[_attempt(100)],
            output_token_reserve=0,
            context_safety_margin=0,
            profile_prompt_budget={"max_input_tokens": 1000},
        ),
    )
    assert out.trace["prompt_budget"]["effective_hard_input_budget"] == 100
    assert out.trace["prompt_budget"]["profile_clamp"]["warning"] == "profile_clamp_not_narrower"


def test_repeated_identical_budgeting_is_deterministic():
    kwargs = dict(
        profile={"prompt_overlay": "required profile"},
        retrieval_bundle={
            "bundle": {
                "recent": [{"role": "assistant", "content": "old recent " * 20}],
                "semantic": [],
                "artifact_refs": [],
            }
        },
        current_messages=[
            {"role": "user", "content": "old request " * 20},
            {"role": "user", "content": "final question"},
        ],
        prompt_budget_contract=_contract(60),
    )
    first = assemble_prompt(**kwargs)
    second = assemble_prompt(**kwargs)
    assert first.messages == second.messages
    assert first.trace["prompt_budget"] == second.trace["prompt_budget"]


def test_layer_estimates_exclude_global_overhead_per_layer():
    messages = [{"role": "system", "content": "abcd"}, {"role": "user", "content": "efgh"}]
    assert estimate_prompt_tokens(messages) == 2 + estimate_messages_tokens(messages)
