from services.briefing import generate_brief
from services.prompt_assembly import assemble_prompt
from services.wave4_memory_composition import compose_wave4_context


def _bundle(*items):
    return {
        "request_id": "rid-wave4",
        "conversation_id": "conv-wave4",
        "bundle": {"recent": [], "semantic": list(items), "artifact_refs": []},
    }


def _memory(
    memory_id,
    content,
    *,
    state="promoted",
    score=0.9,
    salience=0.9,
    source_id=None,
):
    return {
        "message_id": f"msg-{memory_id}",
        "memory_id": memory_id,
        "owner_id": "owner",
        "conversation_id": "conv-wave4",
        "role": "assistant",
        "content": content,
        "score": score,
        "salience_score": salience,
        "promotion_state": state,
        "source_ref": {"ref_type": "memory_item", "ref_id": source_id or memory_id},
    }


def _recall(*decisions):
    return {
        "request_id": "rid-wave4",
        "owner_id": "owner",
        "decision_count": len(decisions),
        "decisions": list(decisions),
    }


def _decision(candidate_id, strategy, *, decision="mention", prompt_eligible=True):
    return {
        "candidate_id": candidate_id,
        "candidate_type": "memory_item",
        "decision": decision,
        "mention_strategy": strategy,
        "prompt_eligible": prompt_eligible,
        "reason": {"rule_id": f"{strategy}_test"},
    }


def _episodes(*decisions):
    return {
        "request_id": "rid-wave4",
        "owner_id": "owner",
        "decision_count": len(decisions),
        "decisions": list(decisions),
    }


def _episode(episode_id, *, eligible=True, strategy="light_callback", reason="ok"):
    return {
        "episode_id": episode_id,
        "decision": "include" if eligible else "suppress",
        "callback_strategy": strategy if eligible else "none",
        "callback_score": 0.8 if eligible else 0.1,
        "prompt_eligible": eligible,
        "reasons": [reason],
        "episode": {
            "episode_id": episode_id,
            "title": f"title {episode_id}",
            "summary": f"summary {episode_id}",
            "episode_type": "successful_mitigation",
            "source_refs": [{"ref_type": "message", "ref_id": f"msg-{episode_id}"}],
        },
    }


def test_promoted_memory_included_and_low_value_memory_suppressed():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(
            _memory("mem-promoted", "promoted fact"),
            _memory("mem-low", "low value fact"),
        ),
        recall_response=_recall(
            _decision("mem-promoted", "light_callback"),
            _decision("mem-low", "none", decision="suppress", prompt_eligible=False),
        ),
        episode_response=None,
    )

    retained = out.retrieval_bundle["bundle"]["semantic"]
    assert [item["memory_id"] for item in retained] == ["mem-promoted"]
    assert "promoted fact" in out.prompt_messages[0]["content"]
    assert "low value fact" not in out.prompt_messages[0]["content"]
    assert "mem-low" in out.trace["recall"]["suppressed_ids"]


def test_stale_memory_qualified_or_suppressed():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(_memory("mem-stale", "historical fact", state="stale")),
        recall_response=_recall(
            _decision("mem-stale", "implicit", decision="implicit_only", prompt_eligible=False)
        ),
        episode_response=None,
    )

    assert out.retrieval_bundle["bundle"]["semantic"][0]["memory_id"] == "mem-stale"
    assert out.brief_grounding["uncertainty"]
    assert "Use implicitly" in out.prompt_messages[0]["content"]


def test_corrected_fact_replaces_or_suppresses_old_memory():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(
            _memory("old", "old fact", state="demoted"),
            _memory("new", "corrected fact", state="corrected_replacement"),
        ),
        recall_response=_recall(_decision("new", "light_callback")),
        episode_response=None,
    )

    assert [item["memory_id"] for item in out.retrieval_bundle["bundle"]["semantic"]] == ["new"]
    assert "corrected fact" in out.prompt_messages[0]["content"]
    assert "old" in out.trace["recall"]["suppressed_ids"]


def test_meaningful_episode_callback_included():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(),
        recall_response=None,
        episode_response=_episodes(_episode("ep-meaningful")),
    )

    assert out.trace["episodes"]["included_episode_ids"] == ["ep-meaningful"]
    assert "summary ep-meaningful" in out.prompt_messages[0]["content"]


def test_awkward_episode_callback_suppressed():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(),
        recall_response=None,
        episode_response=_episodes(
            _episode("ep-awkward", eligible=False, reason="awkward_or_tangential")
        ),
    )

    assert out.prompt_messages == []
    assert out.brief_grounding["omissions"][0]["reason"] == "awkward_or_tangential"


def test_scene_inappropriate_episode_callback_suppressed():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(),
        recall_response=None,
        episode_response=_episodes(_episode("ep-scene", eligible=False, reason="scene_mismatch")),
    )

    assert out.prompt_messages == []
    assert out.brief_grounding["omissions"][0]["reason"] == "scene_mismatch"


def test_recall_implicit_used_without_explicit_mention():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(_memory("mem-implicit", "implicit context")),
        recall_response=_recall(
            _decision("mem-implicit", "implicit", decision="implicit_only", prompt_eligible=False)
        ),
        episode_response=None,
    )

    assert "Use implicitly" in out.prompt_messages[0]["content"]
    assert out.explicit_callbacks == []


def test_recall_light_explicit_mentioned_briefly():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(_memory("mem-light", "light callback context")),
        recall_response=_recall(_decision("mem-light", "light_callback")),
        episode_response=None,
    )

    assert out.explicit_callbacks == ["light callback context"]


def test_recall_strong_explicit_used_when_continuity_is_the_point():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(_memory("mem-strong", "strong continuity context")),
        recall_response=_recall(_decision("mem-strong", "explicit_callback")),
        episode_response=None,
    )

    assert out.explicit_callbacks == ["strong continuity context"]
    assert "Direct continuity callback allowed" in out.prompt_messages[0]["content"]


def test_recall_suppress_absent_from_prompt_and_answer():
    out = compose_wave4_context(
        retrieval_bundle=_bundle(_memory("mem-suppress", "do not surface")),
        recall_response=_recall(
            _decision("mem-suppress", "none", decision="suppress", prompt_eligible=False)
        ),
        episode_response=None,
    )

    assert out.retrieval_bundle["bundle"]["semantic"] == []
    assert out.prompt_messages == []
    assert out.explicit_callbacks == []


def test_source_grounded_brief_with_uncertainty_and_omissions():
    composition = compose_wave4_context(
        retrieval_bundle=_bundle(
            _memory("mem-current", "current fact"),
            _memory("mem-stale", "stale fact", state="stale"),
            _memory("mem-suppressed", "suppressed fact", state="suppressed"),
        ),
        recall_response=_recall(_decision("mem-current", "light_callback")),
        episode_response=_episodes(_episode("ep-brief")),
    )
    result = generate_brief(
        content="Net: continue. Recommendation: use grounded context. Next: verify.",
        grounding=composition.brief_grounding,
    )

    assert "Grounding:" in result.rendered
    assert "Uncertainty:" in result.rendered
    assert "Omissions:" in result.rendered
    assert result.debug["grounding"]["source_count"] >= 2


def test_provider_fallback_preserves_suppressed_context_boundary():
    composition = compose_wave4_context(
        retrieval_bundle=_bundle(_memory("mem-suppress", "fallback must not restore")),
        recall_response=_recall(
            _decision("mem-suppress", "none", decision="suppress", prompt_eligible=False)
        ),
        episode_response=None,
    )
    assembled = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle=composition.retrieval_bundle,
        current_messages=[{"role": "user", "content": "answer"}],
        wave4_memory_messages=composition.prompt_messages,
        wave4_memory_trace=composition.trace,
    )
    first_attempt = [msg["content"] for msg in assembled.messages]
    second_attempt = [msg["content"] for msg in assembled.messages]

    assert first_attempt == second_attempt
    assert all("fallback must not restore" not in content for content in second_attempt)
