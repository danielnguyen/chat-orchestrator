from services.prompt_assembly import assemble_prompt


def test_assemble_prompt_preserves_existing_layer_order_and_wording():
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
                "artifact_refs": [
                    {
                        "artifact_id": "a-1",
                        "repo_name": "repo",
                        "file_path": "api/main.py",
                        "snippet": "def entrypoint(): pass",
                        "relevance_score": 0.8,
                    }
                ],
            }
        },
        current_messages=[{"role": "user", "content": "hi"}],
    )

    assert [msg["role"] for msg in out.messages] == [
        "system",
        "system",
        "system",
        "assistant",
        "user",
    ]
    assert out.messages[0]["content"] == "profile text"
    assert out.messages[1]["content"] == (
        "Retrieved memory excerpts:\n"
        "- [2026-01-01T00:00:00+00:00] assistant: semantic note"
    )
    assert out.messages[2]["content"] == (
        "Retrieved file snippets:\n- [repo/api/main.py] def entrypoint(): pass"
    )
    assert out.trace["included_layers"] == [
        "profile_overlay",
        "retrieval_augmentation",
        "recent_history",
        "current_messages",
    ]
    assert out.trace["omitted_layers"] == ["runtime_overlay"]
    assert out.trace["truncation"] == {"applied": False, "reason": None}
    assert out.trace["runtime"] == {"attempted": False, "status": "not_requested"}
    snippets = out.trace["layers"][2]["metadata"]["snippets"]
    assert snippets["semantic"][0]["message_id"] == "m-1"
    assert snippets["artifact_refs"][0]["artifact_id"] == "a-1"


def test_assemble_prompt_marks_empty_layers_omitted():
    out = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
    )

    assert out.messages == [{"role": "user", "content": "hi"}]
    assert out.trace["omitted_layers"] == [
        "profile_overlay",
        "runtime_overlay",
        "retrieval_augmentation",
        "recent_history",
    ]


def test_assemble_prompt_includes_runtime_overlay_after_profile_before_retrieval():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={
            "bundle": {
                "recent": [],
                "semantic": [
                    {
                        "message_id": "m-1",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "role": "assistant",
                        "content": "semantic note",
                    }
                ],
                "artifact_refs": [],
            }
        },
        current_messages=[{"role": "user", "content": "hi"}],
        runtime_overlay={
            "runtime_state_id": "rtstate_1",
            "overlay_id": "rtoverlay_1",
            "overlay_type": "runtime_state",
            "role": "system",
            "content": (
                "Runtime context: scene=planning; interaction_mode=actionable; "
                "constraints=preserve_flow."
            ),
            "source_fields": ["active_scene", "interaction_mode", "temporary_constraints"],
        },
        runtime_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "runtime_state_id": "rtstate_1",
            "overlay_id": "rtoverlay_1",
        },
    )

    assert [msg["content"] for msg in out.messages[:3]] == [
        "profile text",
        "Runtime context: scene=planning; interaction_mode=actionable; "
        "constraints=preserve_flow.",
        "Retrieved memory excerpts:\n- [2026-01-01T00:00:00+00:00] assistant: semantic note",
    ]
    assert out.trace["included_layers"] == [
        "profile_overlay",
        "runtime_overlay",
        "retrieval_augmentation",
        "current_messages",
    ]
    runtime_layer = out.trace["layers"][1]
    assert runtime_layer["metadata"]["runtime_state_id"] == "rtstate_1"
    assert out.trace["runtime"]["status"] == "included"


def test_assemble_prompt_omits_runtime_overlay_with_non_system_role():
    out = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        runtime_overlay={
            "runtime_state_id": "rtstate_1",
            "overlay_id": "rtoverlay_1",
            "overlay_type": "runtime_state",
            "role": "user",
            "content": "Runtime context: scene=planning.",
            "source_fields": ["active_scene"],
        },
        runtime_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "runtime_state_id": "rtstate_1",
            "overlay_id": "rtoverlay_1",
        },
    )

    assert out.messages == [{"role": "user", "content": "hi"}]
    assert "runtime_overlay" in out.trace["omitted_layers"]
    runtime_layer = out.trace["layers"][1]
    assert runtime_layer["metadata"]["omission_reason"] == "invalid_runtime_overlay_role"
    assert out.trace["runtime"]["status"] == "omitted"
    assert out.trace["runtime"]["included"] is False
    assert out.trace["runtime"]["omission_reason"] == "invalid_runtime_overlay_role"
