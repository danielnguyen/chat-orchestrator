from models import StyleEnvelope
from services.response_shape import resolve_response_shape
from services.surface_presence import apply_surface_presence_outcome, resolve_surface_presence

BANNED_TOKENS = ["R26", "R27", "Cluster11", "11D"]


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


def test_resolve_surface_presence_defaults_to_idle_for_completed_chat():
    shape, _ = resolve_response_shape(
        {"owner_id": "owner", "surface": "vscode", "messages": [{"role": "user", "content": "hi"}]},
        StyleEnvelope(),
        {"attempted": False, "status": "not_requested"},
    )

    trace = resolve_surface_presence(
        {"owner_id": "owner", "surface": "vscode", "messages": [{"role": "user", "content": "hi"}]},
        shape,
    )

    assert trace["presence_state"] == "idle"
    assert trace["reason"] == "default_completed_turn"
    assert trace["spoken_output"] is False
    assert trace["fallback_active"] is False


def test_resolve_surface_presence_marks_spoken_output_as_briefing():
    shape, _ = resolve_response_shape(
        {
            "surface": "car",
            "surface_context": {"surface_type": "car", "spoken_output": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    trace = resolve_surface_presence(
        {
            "surface": "car",
            "surface_context": {"surface_type": "car", "spoken_output": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        shape,
    )

    assert trace["presence_state"] == "briefing"
    assert trace["reason"] == "spoken_output_surface"
    assert trace["spoken_output"] is True


def test_resolve_surface_presence_keeps_active_task_text_chat_idle():
    shape, _ = resolve_response_shape(
        {
            "surface": "vscode",
            "surface_context": {"active_task_mode": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    trace = resolve_surface_presence(
        {
            "surface": "vscode",
            "surface_context": {"active_task_mode": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        shape,
    )

    assert trace["presence_state"] == "idle"
    assert trace["active_task_mode"] is True


def test_apply_surface_presence_outcome_marks_fallback_only_when_used():
    trace = apply_surface_presence_outcome(
        {
            "attempted": True,
            "status": "included",
            "included": True,
            "presence_state": "idle",
            "reason": "default_completed_turn",
            "source_fields": ["surface"],
            "surface_type": "vscode",
            "spoken_output": False,
            "active_task_mode": False,
            "fallback_active": False,
        },
        fallback_active=True,
    )

    assert trace["presence_state"] == "fallback"
    assert trace["fallback_active"] is True
    assert trace["reason"] == "provider_fallback_used"


def test_apply_surface_presence_outcome_marks_terminal_failure_unavailable():
    trace = apply_surface_presence_outcome(
        {
            "attempted": True,
            "status": "included",
            "included": True,
            "presence_state": "briefing",
            "reason": "spoken_output_surface",
            "source_fields": ["surface"],
            "surface_type": "car",
            "spoken_output": True,
            "active_task_mode": False,
            "fallback_active": False,
        },
        unavailable=True,
    )

    assert trace["presence_state"] == "unavailable"
    assert trace["fallback_active"] is False
    assert trace["reason"] == "request_failed"


def test_surface_presence_trace_keys_do_not_use_banned_identifiers():
    shape, _ = resolve_response_shape(
        {
            "surface": "car",
            "surface_context": {"surface_type": "car", "spoken_output": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        StyleEnvelope(),
        {"attempted": True, "status": "included"},
    )

    trace = resolve_surface_presence(
        {
            "surface": "car",
            "surface_context": {"surface_type": "car", "spoken_output": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        shape,
    )

    keys = _collect_keys(trace)
    assert keys
    for token in BANNED_TOKENS:
        assert all(token not in key for key in keys)
