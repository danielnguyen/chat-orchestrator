from models import ChatRequest
from services.situated_presence import derive_situated_surface_context
from services.style_envelope import (
    build_style_guidance_block,
    clamp_style_envelope,
    resolve_style_envelope,
)

BANNED_TOKENS = ["R27", "Cluster11", "11B"]


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


def test_chat_request_accepts_surface_context_and_ignores_unknown_style_fields():
    request = ChatRequest.model_validate(
        {
            "owner_id": "owner",
            "surface": "telegram",
            "messages": [{"role": "user", "content": "hi"}],
            "surface_context": {
                "surface_type": "telegram",
                "style_envelope": {
                    "directness": "high",
                    "unknown_style_key": "ignored",
                },
            },
        }
    )

    dumped = request.model_dump(exclude_none=True)
    assert dumped["surface_context"]["style_envelope"] == {"directness": "high"}


def test_chat_request_external_context_defaults_to_disabled():
    request = ChatRequest.model_validate(
        {
            "owner_id": "owner",
            "surface": "node_red",
            "messages": [{"role": "user", "content": "hi"}],
        }
    )

    dumped = request.model_dump()
    assert dumped["external_context_enabled"] is False
    assert dumped["external_context"] is None


def test_chat_request_accepts_nested_external_context_enablement():
    request = ChatRequest.model_validate(
        {
            "owner_id": "owner",
            "surface": "node_red",
            "messages": [{"role": "user", "content": "hi"}],
            "external_context": {
                "enabled": True,
                "source_ids": ["example_source"],
                "max_results": 5,
            },
        }
    )

    dumped = request.model_dump(exclude_none=True)
    assert dumped["external_context_enabled"] is False
    assert dumped["external_context"] == {
        "enabled": True,
        "source_ids": ["example_source"],
        "max_results": 5,
    }


def test_resolve_style_envelope_defaults_without_emitting_guidance():
    envelope, trace = resolve_style_envelope(
        {"owner_id": "owner", "surface": "vscode", "messages": [{"role": "user", "content": "hi"}]},
        {"response_style": {}},
    )

    assert envelope.model_dump() == {
        "directness": "balanced",
        "warmth": "medium",
        "playfulness_budget": "low",
        "challenge_sharpness": "balanced",
        "sentence_length": "flexible",
        "analogy_density": "low",
        "technical_density": "adaptive",
        "formality_range": "neutral",
        "repetition_sensitivity": "normal",
    }
    assert trace["status"] == "not_requested"
    assert trace["included"] is False
    assert build_style_guidance_block(envelope, trace) == ""



def test_resolve_style_envelope_telegram_surface_emits_text_guidance_only():
    envelope, trace = resolve_style_envelope(
        {
            "surface": "telegram",
            "surface_context": {"surface_type": "telegram", "interaction_mode": "text"},
            "messages": [{"role": "user", "content": "hi"}],
        },
        {"response_style": {}},
    )

    guidance = build_style_guidance_block(envelope, trace)
    assert trace["included"] is True
    assert trace["guidance_flags"]["text_compact"] is True
    assert trace["guidance_flags"]["spoken_output"] is False
    assert "compact and easy to scan in text" in guidance
    assert "spoken delivery" not in guidance



def test_resolve_style_envelope_voice_surface_emits_speakable_guidance():
    envelope, trace = resolve_style_envelope(
        {
            "surface": "car",
            "surface_context": {"surface_type": "car", "interaction_mode": "voice_mediated"},
            "messages": [{"role": "user", "content": "hi"}],
        },
        {"response_style": {}},
    )

    guidance = build_style_guidance_block(envelope, trace)
    assert envelope.sentence_length == "short"
    assert envelope.playfulness_budget == "none"
    assert envelope.analogy_density == "none"
    assert envelope.technical_density == "low"
    assert "spoken delivery" in guidance
    assert "compact and easy to scan in text" not in guidance



def test_resolve_style_envelope_active_task_emits_decisive_low_cognitive_load_guidance():
    envelope, trace = resolve_style_envelope(
        {
            "surface": "vscode",
            "surface_context": {"active_task_mode": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        {"response_style": {}},
    )

    guidance = build_style_guidance_block(envelope, trace)
    assert envelope.directness == "high"
    assert envelope.sentence_length == "short"
    assert "Lead with the answer, keep cognitive load low" in guidance
    assert "Be direct and decisive." in guidance



def test_resolve_style_envelope_request_override_wins_for_recognized_fields_only():
    envelope, trace = resolve_style_envelope(
        {
            "surface": "car",
            "surface_context": {
                "spoken_output": True,
                "style_envelope": {
                    "technical_density": "high",
                    "formality_range": "formal",
                    "not_a_field": "ignored",
                },
            },
            "messages": [{"role": "user", "content": "hi"}],
        },
        {"response_style": {"warmth": "high"}},
    )

    guidance = build_style_guidance_block(envelope, trace)
    assert envelope.warmth == "high"
    assert envelope.technical_density == "high"
    assert envelope.formality_range == "formal"
    assert trace["recognized_request_fields"] == ["formality_range", "technical_density"]
    assert "Include technical detail when it materially helps." in guidance



def test_style_trace_keys_do_not_use_banned_identifiers():
    _, trace = resolve_style_envelope(
        {
            "surface": "telegram",
            "surface_context": {"surface_type": "telegram", "active_task_mode": True},
            "messages": [{"role": "user", "content": "hi"}],
        },
        {"response_style": {"repetition_sensitivity": "high"}},
    )

    keys = _collect_keys(trace)
    assert keys
    for token in BANNED_TOKENS:
        assert all(token not in key for key in keys)


def test_situated_surface_context_is_conservative_and_uses_typed_facts_only():
    private, _ = derive_situated_surface_context(
        {"surface_context": {"surface_category": "telegram_private"}}
    )
    shared, _ = derive_situated_surface_context(
        {"surface_context": {"surface_category": "car_voice_possible_passenger"}}
    )
    public, _ = derive_situated_surface_context(
        {"surface_context": {"surface_category": "glasses_public_or_semi_public"}}
    )
    preview, _ = derive_situated_surface_context(
        {"surface_context": {"surface_category": "notification_preview"}}
    )
    task, _ = derive_situated_surface_context(
        {
            "surface_context": {
                "surface_category": "desktop_private",
                "active_task_mode": True,
            }
        }
    )
    no_expansion, _ = derive_situated_surface_context(
        {
            "surface_context": {
                "surface_category": "voice_private",
                "spoken_output": True,
                "allows_expansion": False,
            }
        }
    )
    unknown, trace = derive_situated_surface_context(
        {"surface": "telegram", "client_id": "telegram"}
    )

    assert private == {"visibility": "private", "constraint": "normal"}
    assert shared == {"visibility": "shared", "constraint": "normal"}
    assert public == {"visibility": "public", "constraint": "normal"}
    assert preview == {"visibility": "public", "constraint": "constrained"}
    assert task == {"visibility": "private", "constraint": "constrained"}
    assert no_expansion == {"visibility": "private", "constraint": "constrained"}
    assert unknown == {"visibility": "unknown", "constraint": "unknown"}
    assert trace["source_fields"] == []


def test_spoken_output_alone_does_not_make_private_surface_constrained():
    projection, _ = derive_situated_surface_context(
        {
            "surface_context": {
                "surface_category": "voice_private",
                "spoken_output": True,
            }
        }
    )
    assert projection == {"visibility": "private", "constraint": "normal"}


def test_situated_style_clamp_applies_after_explicit_overrides_without_loosening():
    envelope, trace = resolve_style_envelope(
        {
            "surface_context": {
                "style_envelope": {
                    "warmth": "high",
                    "technical_density": "high",
                    "playfulness_budget": "medium",
                    "challenge_sharpness": "soft",
                }
            }
        },
        {},
    )
    clamped, clamped_trace = clamp_style_envelope(
        envelope,
        trace,
        {
            "humor_allowed": False,
            "challenge_allowed": "medium",
            "response_posture": "tactical",
        },
        {"status": "included", "fallback_status": "not_used"},
    )

    assert clamped.playfulness_budget == "none"
    assert clamped.challenge_sharpness == "direct"
    assert clamped.directness == "high"
    assert clamped.sentence_length == "short"
    assert clamped.analogy_density == "none"
    assert clamped.warmth == "high"
    assert clamped.technical_density == "high"
    assert clamped_trace["situated_presence_clamp"]["before"] == envelope.model_dump()
    assert clamped_trace["situated_presence_clamp"]["after"] == clamped.model_dump()


def test_situated_playfulness_permission_does_not_raise_explicit_none():
    envelope, trace = resolve_style_envelope(
        {"surface_context": {"style_envelope": {"playfulness_budget": "none"}}},
        {},
    )
    clamped, _ = clamp_style_envelope(
        envelope,
        trace,
        {
            "humor_allowed": True,
            "challenge_allowed": "low",
            "response_posture": "playful",
        },
        {"status": "included"},
    )
    assert clamped.playfulness_budget == "none"
