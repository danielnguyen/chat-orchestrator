from copy import deepcopy

import pytest
from models import StyleEnvelope
from services.response_shape import (
    ResponseShape,
    build_response_shape_guidance_block,
    clamp_response_shape_for_runtime_presence,
    resolve_response_shape,
)

BANNED_TOKENS = ["R26", "R27", "Cluster11", "11C"]


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


def test_resolve_response_shape_defaults_without_emitting_guidance():
    shape, trace = resolve_response_shape(
        {"owner_id": "owner", "surface": "vscode", "messages": [{"role": "user", "content": "hi"}]},
        StyleEnvelope(),
        {"attempted": False, "status": "not_requested"},
    )

    assert shape.model_dump() == {
        "spoken_output": False,
        "active_task_mode": False,
        "concise_first_answer": False,
        "max_sentence_count": None,
        "avoid_markdown": False,
        "allows_expansion": False,
        "expansion_marker_allowed": False,
        "continuation_state": "none",
        "abbreviation_reason": None,
        "latency_preference": None,
        "confirmation_style": None,
    }
    assert trace["status"] == "not_requested"
    assert trace["included"] is False
    assert build_response_shape_guidance_block(shape, trace) == ""


def test_resolve_response_shape_text_surface_does_not_receive_spoken_constraints():
    shape, trace = resolve_response_shape(
        {
            "surface": "telegram",
            "surface_context": {"surface_type": "telegram", "interaction_mode": "text"},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": False, "status": "not_requested"},
    )

    guidance = build_response_shape_guidance_block(shape, trace)
    assert shape.spoken_output is False
    assert trace["included"] is False
    assert "spoken delivery" not in guidance


def test_resolve_response_shape_spoken_output_emits_speakable_guidance():
    shape, trace = resolve_response_shape(
        {
            "surface": "car",
            "surface_context": {"surface_type": "car", "spoken_output": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    guidance = build_response_shape_guidance_block(shape, trace)
    assert shape.spoken_output is True
    assert shape.concise_first_answer is True
    assert shape.max_sentence_count == 2
    assert shape.continuation_state == "abbreviated"
    assert "spoken delivery" in guidance
    assert "one or two short sentences" in guidance


def test_resolve_response_shape_voice_mediated_emits_spoken_guidance_without_explicit_spoken_output(
):
    shape, trace = resolve_response_shape(
        {
            "surface": "car",
            "surface_context": {"surface_type": "car", "interaction_mode": "voice_mediated"},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    guidance = build_response_shape_guidance_block(shape, trace)
    assert shape.spoken_output is True
    assert trace["guidance_flags"]["spoken_output"] is True
    assert "spoken delivery" in guidance


def test_resolve_response_shape_active_task_emits_concise_first_guidance():
    shape, trace = resolve_response_shape(
        {
            "surface": "vscode",
            "surface_context": {"active_task_mode": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    guidance = build_response_shape_guidance_block(shape, trace)
    assert shape.active_task_mode is True
    assert shape.concise_first_answer is True
    assert shape.continuation_state == "none"
    assert "Lead with the answer" in guidance
    assert "Keep cognitive load low" in guidance


def test_resolve_response_shape_allows_expansion_false_suppresses_marker_guidance():
    shape, trace = resolve_response_shape(
        {
            "surface": "car",
            "surface_context": {
                "spoken_output": True,
                "allows_expansion": False,
                "latency_preference": "low",
                "verbosity_target": "short",
            },
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    guidance = build_response_shape_guidance_block(shape, trace)
    assert shape.expansion_marker_allowed is False
    assert shape.continuation_state == "abbreviated"
    assert "more detail is available" not in guidance


def test_resolve_response_shape_allows_expansion_true_permits_but_does_not_force_expandable_state():
    expandable_shape, expandable_trace = resolve_response_shape(
        {
            "surface": "car",
            "surface_context": {
                "spoken_output": True,
                "allows_expansion": True,
                "latency_preference": "low",
                "verbosity_target": "short",
            },
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )
    default_shape, default_trace = resolve_response_shape(
        {
            "surface": "vscode",
            "surface_context": {"allows_expansion": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": False, "status": "not_requested"},
    )

    guidance = build_response_shape_guidance_block(expandable_shape, expandable_trace)
    assert expandable_shape.continuation_state == "expandable"
    assert expandable_shape.expansion_marker_allowed is True
    assert "more detail is available" in guidance
    assert default_shape.continuation_state == "none"
    assert default_trace["included"] is False


def test_response_shape_trace_keys_do_not_use_banned_identifiers():
    _, trace = resolve_response_shape(
        {
            "surface": "car",
            "surface_context": {
                "spoken_output": True,
                "active_task_mode": True,
                "allows_expansion": True,
                "latency_preference": "low",
            },
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    keys = _collect_keys(trace)
    assert keys
    for token in BANNED_TOKENS:
        assert all(token not in key for key in keys)


@pytest.mark.parametrize("presence", [None, {}, {"proactive_output_suppressed": False}])
def test_runtime_presence_without_suppression_preserves_shape(presence):
    shape, trace = resolve_response_shape({}, StyleEnvelope(), {})
    assert clamp_response_shape_for_runtime_presence(shape, trace, presence) == (shape, trace)


@pytest.mark.parametrize("continuation", ["none", "expandable", "abbreviated", "suppressed"])
@pytest.mark.parametrize("spoken", [False, True])
@pytest.mark.parametrize("fallback", [False, True])
def test_runtime_presence_only_narrows_expansion_and_preserves_required_detail(
    continuation, spoken, fallback,
):
    shape = ResponseShape(
        spoken_output=spoken, avoid_markdown=spoken, max_sentence_count=7 if spoken else None,
        allows_expansion=True, expansion_marker_allowed=True, continuation_state=continuation,
        confirmation_style="explicit",
    )
    trace = {"included": False, "guidance_flags": {"spoken_output": spoken}}
    before = deepcopy((shape, trace))
    presence = {"proactive_output_suppressed": True}
    if fallback:
        presence.update(status="fallback", fallback_status="suppression_only")
    narrowed, updated = clamp_response_shape_for_runtime_presence(shape, trace, presence)
    expected = shape.model_dump()
    expected.update(
        allows_expansion=False, expansion_marker_allowed=False,
        continuation_state="abbreviated" if continuation == "expandable" else continuation,
    )
    assert narrowed.model_dump() == expected
    assert (shape, trace) == before
    change = updated["runtime_presence"]
    assert change["applied"] is True
    assert set(change["changed_fields"]) == {
        key for key in change["before"] if change["before"][key] != change["after"][key]
    }
    guidance = build_response_shape_guidance_block(narrowed, updated)
    assert "Answer the user's request directly" in guidance
    assert "Omit optional proactive suggestions" in guidance
    assert "Preserve all information required" in guidance
    assert "more detail is available" not in guidance
    if not spoken:
        assert "two" not in guidance
    for internal in ["R" + "44", "Pha" + "se 6", "presence", "idle", "driving", "reason_codes"]:
        assert internal not in guidance


@pytest.mark.parametrize("surface", ["alexa", "car", "voice"])
def test_timing_projection_does_not_infer_speech_from_surface(surface):
    from services.response_shape import project_timing_facts

    assert project_timing_facts({"surface": surface}) == {
        "spoken_output": False, "active_task_mode": False, "requested_detail": "unspecified",
    }
    assert project_timing_facts({"surface_context": {"surface_type": "voice"}})[
        "spoken_output"
    ] is False


@pytest.mark.parametrize("context,expected", [
    ({"spoken_output": True}, True), ({"spoken_output": False}, False),
    ({"interaction_mode": "voice_mediated", "output_format": "speech"}, True),
    ({"interaction_mode": "text", "output_format": "plain_text"}, False),
])
def test_timing_projection_uses_only_consistent_typed_speech(context, expected):
    from services.response_shape import project_timing_facts

    assert project_timing_facts({"surface_context": context})["spoken_output"] is expected


@pytest.mark.parametrize("context", [
    {"spoken_output": "true"}, {"active_task_mode": 1},
    {"spoken_output": False, "output_format": "speech"},
    {"spoken_output": True, "interaction_mode": "text"},
    {"interaction_mode": "voice_mediated", "output_format": "markdown"},
])
def test_timing_projection_rejects_malformed_or_conflicting_typed_facts(context):
    from services.response_shape import project_timing_facts

    with pytest.raises(ValueError):
        project_timing_facts({"surface_context": context})


@pytest.mark.parametrize("verbosity,detail", [
    ("short", "brief"), ("normal", "normal"), ("detailed", "expanded"),
])
def test_timing_detail_and_active_task_are_explicit(verbosity, detail):
    from services.response_shape import project_timing_facts

    assert project_timing_facts({"surface_context": {
        "verbosity_target": verbosity, "active_task_mode": True,
    }}) == {"spoken_output": False, "active_task_mode": True, "requested_detail": detail}
    assert project_timing_facts({"response_mode": "brief"})["requested_detail"] == "brief"
    with pytest.raises(ValueError, match="timing_projection_conflict"):
        project_timing_facts({"response_mode": "brief", "surface_context": {
            "verbosity_target": "detailed",
        }})


def test_timing_defer_clamp_keeps_timing_and_presentation_continuation_distinct():
    from services.response_shape import clamp_response_shape_for_timing

    shape = ResponseShape(allows_expansion=True, expansion_marker_allowed=True,
                          continuation_state="expandable")
    narrowed, trace = clamp_response_shape_for_timing(shape, {}, {
        "timing_policy": "defer_expansion", "continuation_state": "deferred_expansion",
        "expansion_allowed": False,
    })
    assert narrowed.allows_expansion is False
    assert narrowed.expansion_marker_allowed is False
    assert narrowed.continuation_state == "abbreviated"
    assert trace["runtime_timing"]["expansion_allowed"] is False


@pytest.mark.parametrize("limit,expected", [(None, 2), (4, 2), (2, 2), (1, 1)])
def test_low_attention_timing_narrows_only_resolved_presentation(limit, expected):
    from services.response_shape import clamp_response_shape_for_timing

    shape = ResponseShape(
        max_sentence_count=limit,
        concise_first_answer=False,
        allows_expansion=True,
        expansion_marker_allowed=True,
    )
    narrowed, trace = clamp_response_shape_for_timing(
        shape,
        {},
        {
            "timing_policy": "defer_expansion",
            "expansion_allowed": False,
            "reason_codes": ["presence_low_attention", "dependency_degraded"],
        },
    )
    assert narrowed.max_sentence_count == expected
    assert narrowed.concise_first_answer is True
    assert not narrowed.allows_expansion and not narrowed.expansion_marker_allowed
    assert trace["resolved_shape"] == narrowed.model_dump()
    assert trace["runtime_timing"]["primary_reason"] == "presence_low_attention"
    assert "concise_first_answer" in trace["runtime_timing"]["changed_fields"]
    assert ("max_sentence_count" in trace["runtime_timing"]["changed_fields"]) == (
        limit != expected
    )
    assert "Lead with the answer" in build_response_shape_guidance_block(narrowed, trace)


@pytest.mark.parametrize("reason", ["presence_active_task", "restraint_defer_expansion"])
def test_other_defer_reasons_do_not_invent_sentence_boundaries(reason):
    from services.response_shape import clamp_response_shape_for_timing

    shape = ResponseShape(
        active_task_mode=reason == "presence_active_task",
        max_sentence_count=2 if reason == "presence_active_task" else None,
    )
    narrowed, trace = clamp_response_shape_for_timing(
        shape,
        {},
        {
            "timing_policy": "defer_expansion",
            "expansion_allowed": False,
            "reason_codes": [reason],
        },
    )
    assert narrowed.max_sentence_count == shape.max_sentence_count
    assert narrowed.concise_first_answer == shape.concise_first_answer
    assert not narrowed.allows_expansion
    assert "max_sentence_count" not in trace["runtime_timing"]["changed_fields"]


@pytest.mark.parametrize("state", ["active_conversation", "do_not_intrude"])
def test_proactive_suppression_with_normal_timing_does_not_cap_sentences(state):
    from services.response_shape import clamp_response_shape_for_timing

    shape, trace = clamp_response_shape_for_runtime_presence(
        ResponseShape(),
        {},
        {
            "presence_state": state,
            "proactive_output_suppressed": True,
        },
    )
    narrowed, _ = clamp_response_shape_for_timing(
        shape,
        trace,
        {
            "timing_policy": "answer_now",
            "expansion_allowed": True,
            "reason_codes": ["ordinary_ready"],
        },
    )
    assert narrowed.max_sentence_count is None
    assert not narrowed.concise_first_answer


def test_explicit_expanded_detail_timing_does_not_apply_low_attention_cap():
    from services.response_shape import clamp_response_shape_for_timing, project_timing_facts

    facts = project_timing_facts({"surface_context": {"verbosity_target": "detailed"}})
    assert facts["requested_detail"] == "expanded"
    # CR's existing expanded-detail precedence yields ordinary_ready, not presence_low_attention.
    shape, trace = clamp_response_shape_for_runtime_presence(
        ResponseShape(),
        {},
        {
            "presence_state": "low_attention",
            "proactive_output_suppressed": True,
        },
    )
    narrowed, _ = clamp_response_shape_for_timing(
        shape,
        trace,
        {
            "timing_policy": "answer_now",
            "expansion_allowed": True,
            "reason_codes": ["ordinary_ready"],
        },
    )
    assert narrowed.max_sentence_count is None
    assert not narrowed.concise_first_answer
