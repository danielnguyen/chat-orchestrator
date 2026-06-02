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
    assert out.trace["omitted_layers"] == ["companion_policy", "runtime_overlay"]
    assert out.trace["truncation"] == {"applied": False, "reason": None}
    assert out.trace["runtime"] == {"attempted": False, "status": "not_requested"}
    snippets = out.trace["layers"][3]["metadata"]["snippets"]
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
        "companion_policy",
        "runtime_overlay",
        "retrieval_augmentation",
        "recent_history",
    ]
    assert "interrupt_policy" not in out.trace


def test_assemble_prompt_includes_interrupt_trace_without_changing_messages():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        interrupt_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "mode": "evaluate_only",
            "trigger_class": "repetitive_branching",
        },
    )

    assert out.messages == [
        {"role": "system", "content": "profile text"},
        {"role": "user", "content": "hi"},
    ]
    assert out.trace["interrupt_policy"]["mode"] == "evaluate_only"
    assert out.trace["interrupt_policy"]["trigger_class"] == "repetitive_branching"


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
    runtime_layer = out.trace["layers"][2]
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
    runtime_layer = out.trace["layers"][2]
    assert runtime_layer["metadata"]["omission_reason"] == "invalid_runtime_overlay_role"
    assert out.trace["runtime"]["status"] == "omitted"
    assert out.trace["runtime"]["included"] is False
    assert out.trace["runtime"]["omission_reason"] == "invalid_runtime_overlay_role"

def test_assemble_prompt_includes_companion_policy_before_runtime_overlay():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        companion_overlays=[
            {
                "overlay_id": "contract-1",
                "overlay_type": "interaction_contract",
                "role": "system",
                "content": "contract text",
            },
            {
                "overlay_id": "profile-1",
                "overlay_type": "companion_profile",
                "role": "system",
                "content": "profile companion text",
            },
            {
                "overlay_id": "scene-1",
                "overlay_type": "scene_policy",
                "role": "system",
                "content": "scene text",
            },
        ],
        companion_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "profile_id": "default_companion_profile",
            "profile_version": 1,
            "contract_id": "default_interaction_contract",
            "contract_version": 2,
            "contract_trace": {
                "contract_id": "default_interaction_contract",
                "contract_version": 2,
                "source": "default_compiled",
                "scope": "global_default",
                "selected_rule_groups": ["trust_rules", "repair_rules"],
                "warnings": ["default_contract_applied"],
            },
            "interaction_contract": {
                "contract_id": "default_interaction_contract",
                "contract_version": 2,
                "source": "default_compiled",
                "scope": "global_default",
            },
            "scene_id": "planning",
            "scene_confidence": 1.0,
            "scene_source": "requested_scene",
            "warnings": ["unknown_requested_scene", "default_contract_applied"],
        },
        runtime_overlay={
            "runtime_state_id": "rtstate_1",
            "overlay_id": "rtoverlay_1",
            "overlay_type": "runtime_state",
            "role": "system",
            "content": "Runtime context: scene=planning.",
            "source_fields": ["active_scene"],
        },
        runtime_trace={"attempted": True, "status": "included", "included": True},
    )

    assert [msg["content"] for msg in out.messages[:5]] == [
        "profile text",
        "contract text",
        "profile companion text",
        "scene text",
        "Runtime context: scene=planning.",
    ]
    assert out.trace["included_layers"] == [
        "profile_overlay",
        "companion_policy",
        "runtime_overlay",
        "current_messages",
    ]
    companion_layer = out.trace["layers"][1]
    assert companion_layer["metadata"]["scene_id"] == "planning"
    assert companion_layer["metadata"]["warnings"] == [
        "unknown_requested_scene",
        "default_contract_applied",
    ]
    assert companion_layer["metadata"]["contract_trace"]["source"] == "default_compiled"
    assert companion_layer["metadata"]["interaction_contract"]["scope"] == "global_default"
    assert out.trace["companion_policy"]["contract_trace"]["contract_version"] == 2
    assert [item["overlay_type"] for item in companion_layer["metadata"]["included_overlays"]] == [
        "interaction_contract",
        "companion_profile",
        "scene_policy",
    ]


def test_assemble_prompt_omits_companion_policy_with_non_system_role():
    out = assemble_prompt(
        profile={"prompt_overlay": ""},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        companion_overlays=[
            {
                "overlay_id": "contract-1",
                "overlay_type": "interaction_contract",
                "role": "user",
                "content": "contract text",
            },
            {
                "overlay_id": "profile-1",
                "overlay_type": "companion_profile",
                "content": "missing role should not default",
            },
            {
                "overlay_id": "scene-1",
                "overlay_type": "scene_policy",
                "role": "system",
                "content": "",
            },
            {
                "overlay_id": "scene-2",
                "overlay_type": "scene_policy",
                "role": "system",
                "content": 123,
            },
        ],
        companion_trace={"attempted": True, "status": "included", "included": True},
    )

    assert out.messages == [{"role": "user", "content": "hi"}]
    assert "companion_policy" in out.trace["omitted_layers"]
    companion_layer = out.trace["layers"][1]
    assert companion_layer["metadata"]["omission_reason"] == "invalid_companion_overlay_role"
    assert out.trace["companion_policy"]["status"] == "omitted"
    assert out.trace["companion_policy"]["included"] is False
    assert out.trace["companion_policy"]["included_overlays"] == []
    assert out.trace["companion_policy"]["invalid_overlay_types"] == [
        "interaction_contract",
        "companion_profile",
        "scene_policy",
        "scene_policy",
    ]
