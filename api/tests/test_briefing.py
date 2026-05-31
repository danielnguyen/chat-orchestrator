from services.briefing import generate_brief, normalize_surface


def test_tier_0_is_one_line_and_conclusion_first():
    result = generate_brief(
        content=(
            "Net: viable after the dependency lands. "
            "Risk: sequencing is tight. "
            "Next: rebase first."
        ),
        brief_type="project_status",
        depth_level=0,
        surface="chat",
    )

    assert "\n" not in result.rendered
    assert result.rendered.startswith("Net: viable after the dependency lands")


def test_tier_1_short_operational_brief():
    result = generate_brief(
        content=(
            "Status: implementation is ready. "
            "Risk: the trace contract could drift. "
            "Recommendation: keep metadata inside model_call.brief. "
            "Next: add focused tests."
        ),
        brief_type="project_status",
        depth_level=1,
        surface="chat",
    )

    assert result.rendered.splitlines()[0].startswith("Net:")
    assert "Risk: the trace contract could drift" in result.rendered
    assert "Recommendation: keep metadata inside model_call.brief" in result.rendered
    assert "Rationale:" not in result.rendered


def test_tier_2_keeps_conclusion_before_rationale():
    result = generate_brief(
        content=(
            "Net: ship the deterministic layer first. "
            "It preserves existing chat behavior. "
            "It avoids a second model call."
        ),
        brief_type="recommendation",
        depth_level=2,
        surface="chat",
    )

    lines = result.rendered.splitlines()
    assert lines[0] == "Net: ship the deterministic layer first"
    assert "Rationale:" in result.rendered


def test_tier_3_adds_action_framing():
    result = generate_brief(
        content=(
            "Net: implement in chat-orchestrator only. "
            "Recommendation: keep BMS unchanged. "
            "Next: wire brief mode after the model call."
        ),
        brief_type="implementation_plan",
        depth_level=3,
        surface="chat",
    )

    assert result.rendered.splitlines()[0].startswith("Net:")
    assert "Action framing:" in result.rendered
    assert "Start with: wire brief mode after the model call" in result.rendered


def test_missing_fields_are_not_invented():
    result = generate_brief(
        structured={"net_assessment": "No clear risk was provided."},
        brief_type="risk_review",
        depth_level=1,
        surface="chat",
    )

    brief = result.brief.to_dict()
    assert brief["top_risk"] is None
    assert brief["primary_recommendation"] is None
    assert "Top risk:" not in result.rendered
    assert "Mitigation:" not in result.rendered


def test_synthesizer_does_not_invent_absent_facts():
    result = generate_brief(
        content="This is exploratory context without an explicit risk or next step.",
        depth_level=1,
        surface="chat",
    )

    brief = result.brief.to_dict()
    assert brief["top_risk"] is None
    assert brief["next_step"] is None


def test_telegram_formatting_is_compact():
    result = generate_brief(
        content=(
            "Net: keep the first slice compact. "
            "Risk: public API sprawl. "
            "Recommendation: expose only generate. "
            "Next: test the surface formatter. "
            "Additional detail should be trimmed on mobile."
        ),
        depth_level=3,
        surface="mobile",
    )

    assert result.debug["surface"] == "telegram"
    assert len(result.rendered.splitlines()) <= 6


def test_voice_formatting_avoids_dense_bullets():
    result = generate_brief(
        content=(
            "Net: proceed with brief mode. "
            "Risk: overly rigid output. "
            "Next: keep it opt-in."
        ),
        depth_level=2,
        surface="voice",
    )

    assert result.debug["formatter"] == "voice"
    assert "\n" not in result.rendered
    assert "- " not in result.rendered


def test_compression_ratio_is_reported_for_source_content():
    result = generate_brief(
        content="Net: concise. Extra detail follows here and should make the source longer.",
        depth_level=0,
        surface="chat",
    )

    assert result.debug["compression_ratio"] is not None


def test_surface_normalization():
    assert normalize_surface("telegram") == "telegram"
    assert normalize_surface("mobile") == "telegram"
    assert normalize_surface("car") == "voice"
    assert normalize_surface("vscode") == "chat"
