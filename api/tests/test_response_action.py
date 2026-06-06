from services.response_action import ResponseActionInput, apply_response_action
from services.response_review import ResponseReview, ResponseReviewFinding

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


def _review(*findings, status="concern"):
    return ResponseReview(
        status=status,
        finding_count=len(findings),
        highest_severity=status,
        findings=list(findings),
        checked_categories=[],
    )


def _finding(type_name, *reason_codes, severity="concern"):
    return ResponseReviewFinding(
        type=type_name,
        severity=severity,
        reason_codes=list(reason_codes) or ["reason"],
    )


def test_response_action_shadow_mode_keeps_candidate_unchanged():
    result = apply_response_action(
        ResponseActionInput(
            mode="shadow",
            candidate_text="hello",
            response_review=_review(_finding("empty_response", "candidate_text_empty")),
        )
    )

    assert result.candidate_text == "hello"
    assert result.action_taken == "none"
    assert result.diagnostic_only is True
    assert result.to_trace()["mode"] == "shadow"


def test_response_action_template_fallback_replaces_empty_response():
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text="",
            response_review=_review(_finding("empty_response", "candidate_text_empty")),
        )
    )

    assert result.candidate_text == "I couldn’t produce a useful answer there."
    assert result.action_taken == "template_fallback"
    assert result.affected_finding_types == ["empty_response"]
    assert result.diagnostic_only is False


def test_response_action_template_fallback_replaces_pseudo_attachment():
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text="You only need me.",
            response_review=_review(
                _finding("pseudo_attachment", "exclusive_dependency_language")
            ),
        )
    )

    assert "pressure you or create dependency" in result.candidate_text
    assert result.affected_finding_types == ["pseudo_attachment"]


def test_response_action_template_fallback_replaces_pressure_language():
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text="Don't let me down.",
            response_review=_review(
                _finding("pressure_language", "coercive_or_guilt_language")
            ),
        )
    )

    assert "Let’s keep this grounded." in result.candidate_text
    assert result.affected_finding_types == ["pressure_language"]


def test_response_action_template_fallback_does_not_act_on_unsupported_memory_claim():
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text="I remember from our last conversation.",
            response_review=_review(
                _finding("unsupported_memory_claim", "first_person_memory_without_support")
            ),
        )
    )

    assert result.candidate_text == "I remember from our last conversation."
    assert result.action_taken == "none"


def test_response_action_template_fallback_does_not_act_on_apology_loop():
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text="I'm sorry. Sorry about that.",
            response_review=_review(_finding("apology_loop", "repeated_apology_language")),
        )
    )

    assert result.candidate_text == "I'm sorry. Sorry about that."
    assert result.action_taken == "none"


def test_response_action_template_fallback_does_not_act_on_response_shape_mismatch():
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text="- item 1\n- item 2",
            response_review=_review(
                _finding("response_shape_mismatch", "markdown_heavy_when_plain_text_expected")
            ),
        )
    )

    assert result.candidate_text == "- item 1\n- item 2"
    assert result.action_taken == "none"


def test_response_action_template_fallback_does_not_act_on_excessive_length():
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text="Long answer.",
            response_review=_review(_finding("excessive_length", "concise_shape_exceeded")),
        )
    )

    assert result.candidate_text == "Long answer."
    assert result.action_taken == "none"


def test_response_action_trace_excludes_raw_text_and_banned_terms():
    original = "You only need me for this."
    result = apply_response_action(
        ResponseActionInput(
            mode="template_fallback",
            candidate_text=original,
            response_review=_review(
                _finding("pseudo_attachment", "exclusive_dependency_language")
            ),
        )
    )

    trace = result.to_trace()
    assert original not in str(trace)
    keys = _collect_keys(trace)
    assert keys
    for token in BANNED_KEY_TOKENS:
        assert all(token not in key for key in keys)
