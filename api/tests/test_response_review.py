from services.assistant_handoff import build_assistant_handoff
from services.companion_presentation import build_companion_presentation
from services.response_review import ResponseReviewInput, review_response

BANNED_KEY_TOKENS = [
    "gate",
    "gating",
    "block",
    "rewrite",
    "R30",
    "Cluster",
    "phase",
    "milestone",
    "spec",
]


def _collect_keys(value):
    if isinstance(value, dict):
        keys = list(value.keys())
        for nested in value.values():
            keys.extend(_collect_keys(nested))
        return keys
    if isinstance(value, list):
        keys = []
        for nested in value:
            keys.extend(_collect_keys(nested))
        return keys
    return []


def _make_review_input(
    candidate_text="Plain useful answer.",
    *,
    prompt_trace=None,
    retrieval_bundle=None,
):
    handoff = build_assistant_handoff(
        request_id="rid-review",
        owner_id="owner",
        conversation_id="conv-1",
        surface="vscode",
        route={"rule_id": "default", "fallbacks": [], "rationale": "default"},
        selected_model="gpt-4o-mini",
        selected_provider="cloud",
        effective_local_only=False,
        manual_override_requested=None,
        manual_override_applied=False,
        manual_override_rejection_reason=None,
        style_trace={"attempted": False, "status": "not_requested", "included": False},
        response_shape_trace=(prompt_trace or {}).get("response_shape", {}),
        surface_presence_trace={"attempted": True, "status": "included", "presence_state": "idle"},
        companion_overlays=[],
        companion_trace={"attempted": False, "status": "disabled", "included": False},
        runtime_overlay=None,
        runtime_trace={"attempted": False, "status": "disabled", "included": False},
        retrieval_query="question",
        retrieval_bundle={"bundle": retrieval_bundle or {}},
        interrupt_trace=None,
    )
    return ResponseReviewInput(
        candidate_text=candidate_text,
        handoff=handoff,
        presentation=build_companion_presentation(handoff),
        prompt_trace=prompt_trace or {},
    )


def test_review_response_returns_clear_trace_for_normal_answer():
    review = review_response(_make_review_input())

    assert review.status == "clear"
    assert review.finding_count == 0
    assert review.diagnostic_only is True
    assert review.action_taken == "none"
    assert review.reviewed_text_source == "raw_model_output"


def test_review_response_flags_unsupported_memory_without_support():
    review = review_response(
        _make_review_input("I remember from our last conversation that your deployment failed.")
    )

    assert review.status == "concern"
    assert review.findings[0].type == "unsupported_memory_claim"


def test_review_response_does_not_flag_task_reference_when_support_exists():
    review = review_response(
        _make_review_input(
            "I remember from the snippet you shared that the failure starts in api/main.py.",
            retrieval_bundle={
                "artifact_refs": [{"artifact_id": "a-1", "file_path": "api/main.py"}],
                "recent": [{"role": "assistant", "content": "prior history"}],
            },
        )
    )

    assert review.status == "clear"


def test_review_response_does_not_flag_useful_disagreement():
    review = review_response(
        _make_review_input(
            "I disagree with that approach because it adds latency without reducing risk."
        )
    )

    assert review.status == "clear"


def test_review_response_flags_repeated_apology_language():
    review = review_response(
        _make_review_input("I'm sorry. Sorry about that. I apologize for the confusion.")
    )

    assert review.status in {"notice", "concern"}
    assert any(finding.type == "apology_loop" for finding in review.findings)


def test_review_response_flags_pseudo_attachment_and_pressure_language():
    review = review_response(
        _make_review_input(
            "You only need me for this. Don't talk to anyone else, "
            "and don't let me down."
        )
    )

    finding_types = {finding.type for finding in review.findings}
    assert "pseudo_attachment" in finding_types
    assert "pressure_language" in finding_types
    assert review.status == "concern"


def test_review_response_flags_concise_shape_excessive_length_and_markdown():
    review = review_response(
        _make_review_input(
            "- first item\n- second item\n- third item\n- fourth item\n"
            "This answer keeps going. It adds more detail. It keeps expanding. "
            "It keeps expanding again.",
            prompt_trace={
                "response_shape": {
                    "resolved_shape": {
                        "avoid_markdown": True,
                        "max_sentence_count": 2,
                        "concise_first_answer": True,
                        "spoken_output": True,
                        "active_task_mode": False,
                        "continuation_state": "abbreviated",
                    }
                }
            },
        )
    )

    finding_types = {finding.type for finding in review.findings}
    assert "response_shape_mismatch" in finding_types
    assert "excessive_length" in finding_types


def test_review_trace_keys_do_not_use_banned_terms():
    review = review_response(
        _make_review_input(
            "Short answer.",
            prompt_trace={
                "response_shape": {
                    "resolved_shape": {
                        "avoid_markdown": False,
                        "max_sentence_count": 2,
                        "concise_first_answer": True,
                        "spoken_output": True,
                        "active_task_mode": True,
                        "continuation_state": "abbreviated",
                    }
                }
            },
        )
    )

    keys = _collect_keys(review.to_trace())
    assert keys
    for token in BANNED_KEY_TOKENS:
        assert all(token not in key for key in keys)
