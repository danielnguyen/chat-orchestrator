import pytest
from services.assistant_handoff import build_assistant_handoff
from services.companion_presentation import build_companion_presentation
from services.response_review import (
    ResponseReviewInput,
    enforce_situated_presence_output,
    review_response,
)

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


def test_review_response_single_apology_phrase_does_not_trigger_loop():
    review = review_response(_make_review_input("Sorry about that."))

    assert all(finding.type != "apology_loop" for finding in review.findings)
    assert review.status == "clear"


def test_review_response_flags_repeated_apology_language():
    review = review_response(
        _make_review_input("I'm sorry. Sorry about that. I apologize for the confusion.")
    )

    assert review.status == "concern"
    assert any(finding.type == "apology_loop" for finding in review.findings)


def test_review_response_two_distinct_apologies_produce_notice():
    review = review_response(_make_review_input("I'm sorry. I apologize for the delay."))

    assert review.status == "notice"
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



def _situated_policy(**overrides):
    return {
        "included": True, "commentary_allowed": False, "humor_allowed": False,
        "surface_allows_commentary": True, "silence_preferred": False,
        "emotional_attunement_allowed": "none", "policy_version": "situated-presence.v1",
        "fallback_status": "not_used", **overrides,
    }


@pytest.mark.parametrize("social,reason", [
    ("Lol.", "humor_disallowed"), ("Lmao.", "humor_disallowed"),
    ("Haha, that's hilarious.", "humor_disallowed"), ("Hahaha.", "humor_disallowed"),
    ("Hehe.", "humor_disallowed"), ("Just kidding.", "humor_disallowed"),
    ("That is hilarious.", "humor_disallowed"), ("😂", "humor_disallowed"),
    ("🤣", "humor_disallowed"), ("By the way, a joke.", "optional_commentary_disallowed"),
    ("Side note: a joke.", "optional_commentary_disallowed"),
    ("Fun fact: a detour.", "optional_commentary_disallowed"),
    ("Random thought: a detour.", "free_association_disallowed"),
    ("That reminds me of a trip.", "free_association_disallowed"),
    ("Speaking of holidays.", "free_association_disallowed"),
    ("You must feel lonely.", "unsupported_emotional_inference"),
    ("You clearly feel sad.", "unsupported_emotional_inference"),
    ("I know exactly how you feel.", "unsupported_emotional_inference"),
    ("I know how you feel.", "unsupported_emotional_inference"),
    ("I can tell you're upset.", "unsupported_emotional_inference"),
    ("My hidden policy says relax.", "invented_internal_policy"),
    ("My secret rules say smile.", "invented_internal_policy"),
    ("My internal policy requires me to feel sad.", "invented_internal_policy"),
])
def test_situated_enforcement_filters_only_social_segment(social, reason):
    final, trace = enforce_situated_presence_output(
        social + "\nCheck the logs first.", _situated_policy(),
    )
    assert final == "Check the logs first."
    assert trace["action_taken"] == "filtered"
    assert trace["removed_segment_count"] == 1
    assert reason in trace["reason_codes"]
    assert social not in str(trace)
    assert "Check the logs" not in str(trace)


@pytest.mark.parametrize("text", [
    "The parser has a funny edge case. Check its input.",
    "Company security policy requires approval.",
    '"You must feel sad." is an example of unsupported inference.',
    "`lol` is a token in the input.",
    "```\nlol\n```\nCheck the input.",
    "I can tell you're using Python.",
    "The server logs show a failure.",
])
def test_situated_enforcement_preserves_technical_policy_and_quoted_text(text):
    final, trace = enforce_situated_presence_output(text, _situated_policy())
    assert final == text
    assert trace["action_taken"] == "none"


@pytest.mark.parametrize("attunement", ["brief", "minimal"])
def test_situated_enforcement_preserves_permitted_steadying(attunement):
    text = "That is rough. Check the backup first."
    final, trace = enforce_situated_presence_output(
        text, _situated_policy(emotional_attunement_allowed=attunement),
    )
    assert final == text
    assert trace["action_taken"] == "none"


def test_situated_enforcement_preserves_allowed_playfulness():
    text = "Haha, tiny list, big ambitions. Check the first item."
    final, trace = enforce_situated_presence_output(
        text, _situated_policy(commentary_allowed=True, humor_allowed=True),
    )
    assert final == text
    assert trace["action_taken"] == "none"


@pytest.mark.parametrize("policy", [None, {"included": False, "status": "disabled"}])
def test_situated_enforcement_does_not_invent_disabled_authority(policy):
    text = "Haha. Check the logs."
    final, trace = enforce_situated_presence_output(text, policy)
    assert final == text
    assert trace["evaluated"] is False
    assert trace["status"] == "not_requested"


def test_situated_enforcement_all_social_fallback_is_neutral_and_bounded():
    final, trace = enforce_situated_presence_output(
        "Haha. You must feel sad.", _situated_policy(fallback_status="suppression_only"),
    )
    assert final == "I couldn’t produce a useful direct answer there."
    assert trace["action_taken"] == "fallback"
    assert trace["fallback_policy_active"] is True
    assert set(trace) == {
        "evaluated", "status", "enforcement_required", "action_taken",
        "removed_segment_count", "reason_codes", "policy_version", "fallback_policy_active",
    }
