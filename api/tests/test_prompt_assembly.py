from services.assistant_handoff import build_assistant_handoff
from services.prompt_assembly import assemble_prompt


def _build_handoff(**overrides):
    base = dict(
        request_id="rid-1",
        owner_id="owner",
        conversation_id="conv-1",
        surface="vscode",
        route={"rule_id": "default", "fallbacks": []},
        selected_model="gpt-4o-mini",
        selected_provider="cloud",
        effective_local_only=False,
        manual_override_requested=None,
        manual_override_applied=False,
        manual_override_rejection_reason=None,
        style_trace={"attempted": True, "status": "included", "included": True},
        response_shape_trace={"attempted": True, "status": "included", "included": True},
        surface_presence_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "presence_state": "idle",
            "reason": "default_completed_turn",
        },
        companion_overlays=[],
        companion_trace={"attempted": False, "status": "disabled", "included": False},
        runtime_overlay=None,
        runtime_trace={"attempted": False, "status": "disabled", "included": False},
        retrieval_query="hi",
        retrieval_bundle={
            "bundle": {
                "recent": [],
                "semantic": [],
                "artifact_refs": [],
                "observed_metadata": {"has_code_like_content": False},
            }
        },
        interrupt_trace=None,
        fallback_active=False,
        model_error=None,
    )
    base.update(overrides)
    return build_assistant_handoff(**base)


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
    assert out.trace["omitted_layers"] == [
        "style_guidance",
        "response_shape",
        "companion_policy",
        "runtime_overlay",
    ]
    assert out.trace["truncation"] == {"applied": False, "reason": None}
    assert out.trace["style"]["status"] == "not_requested"
    assert out.trace["response_shape"]["status"] == "not_requested"
    assert out.trace["surface_presence"] == {"attempted": False, "status": "not_requested"}
    assert out.trace["runtime"] == {"attempted": False, "status": "not_requested"}
    snippets = out.trace["layers"][5]["metadata"]["snippets"]
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
        "style_guidance",
        "response_shape",
        "companion_policy",
        "runtime_overlay",
        "retrieval_augmentation",
        "recent_history",
    ]
    assert "interrupt_policy" not in out.trace


def test_assemble_prompt_includes_style_guidance_after_profile_overlay():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        style_guidance="Style guidance:\n- Be direct and decisive.",
        style_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "source_fields": ["surface_context.active_task_mode"],
            "recognized_profile_fields": [],
            "recognized_request_fields": [],
            "guidance_flags": {"active_task_mode": True},
            "resolved_envelope": {"directness": "high"},
        },
    )

    assert out.messages[:2] == [
        {"role": "system", "content": "profile text"},
        {"role": "system", "content": "Style guidance:\n- Be direct and decisive."},
    ]
    assert out.trace["included_layers"] == [
        "profile_overlay",
        "style_guidance",
        "current_messages",
    ]
    style_layer = out.trace["layers"][1]
    assert style_layer["metadata"]["source_fields"] == ["surface_context.active_task_mode"]
    assert style_layer["metadata"]["resolved_envelope"] == {"directness": "high"}
    assert out.trace["style"]["status"] == "included"


def test_assemble_prompt_includes_surface_presence_in_top_level_trace_only():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        surface_presence_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "presence_state": "idle",
            "reason": "default_completed_turn",
            "source_fields": ["surface", "response_shape.resolved_shape"],
            "surface_type": "vscode",
            "spoken_output": False,
            "active_task_mode": False,
            "fallback_active": False,
        },
    )

    assert out.messages == [
        {"role": "system", "content": "profile text"},
        {"role": "user", "content": "hi"},
    ]
    assert out.trace["surface_presence"]["presence_state"] == "idle"
    assert "surface_presence" not in out.trace["included_layers"]
    assert "surface_presence" not in out.trace["omitted_layers"]


def test_assemble_prompt_includes_response_shape_after_style_guidance():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        style_guidance="Style guidance:\n- Prefer short sentences.",
        style_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "source_fields": ["surface_context.spoken_output"],
            "guidance_flags": {"spoken_output": True},
            "resolved_envelope": {"sentence_length": "short"},
        },
        response_shape_guidance=(
            "Response shape guidance:\n"
            "- Write for spoken delivery with plain, speakable text."
        ),
        response_shape_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "source_fields": ["surface_context.spoken_output"],
            "guidance_flags": {"spoken_output": True, "concise_first_answer": True},
            "resolved_shape": {"spoken_output": True, "concise_first_answer": True},
            "continuation_state": "abbreviated",
            "abbreviation_reason": "spoken_output",
        },
    )

    assert out.messages[:3] == [
        {"role": "system", "content": "profile text"},
        {"role": "system", "content": "Style guidance:\n- Prefer short sentences."},
        {
            "role": "system",
            "content": (
                "Response shape guidance:\n"
                "- Write for spoken delivery with plain, speakable text."
            ),
        },
    ]
    assert out.trace["included_layers"] == [
        "profile_overlay",
        "style_guidance",
        "response_shape",
        "current_messages",
    ]
    response_shape_layer = out.trace["layers"][2]
    assert response_shape_layer["metadata"]["continuation_state"] == "abbreviated"
    assert response_shape_layer["metadata"]["abbreviation_reason"] == "spoken_output"
    assert out.trace["response_shape"]["status"] == "included"


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


def test_assemble_prompt_includes_runtime_overlay_after_response_shape_before_retrieval():
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
        style_guidance="Style guidance:\n- Prefer short sentences.",
        style_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "source_fields": ["surface_context.spoken_output"],
            "guidance_flags": {"spoken_output": True},
            "resolved_envelope": {"sentence_length": "short"},
        },
        response_shape_guidance=(
            "Response shape guidance:\n"
            "- Lead with the answer before any supporting detail."
        ),
        response_shape_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "source_fields": ["surface_context.active_task_mode"],
            "guidance_flags": {"active_task_mode": True, "concise_first_answer": True},
            "resolved_shape": {"active_task_mode": True, "concise_first_answer": True},
            "continuation_state": "abbreviated",
            "abbreviation_reason": "active_task_mode",
        },
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

    assert [msg["content"] for msg in out.messages[:5]] == [
        "profile text",
        "Style guidance:\n- Prefer short sentences.",
        "Response shape guidance:\n- Lead with the answer before any supporting detail.",
        "Runtime context: scene=planning; interaction_mode=actionable; constraints=preserve_flow.",
        "Retrieved memory excerpts:\n- [2026-01-01T00:00:00+00:00] assistant: semantic note",
    ]
    assert out.trace["included_layers"] == [
        "profile_overlay",
        "style_guidance",
        "response_shape",
        "runtime_overlay",
        "retrieval_augmentation",
        "current_messages",
    ]
    runtime_layer = out.trace["layers"][4]
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
    runtime_layer = out.trace["layers"][4]
    assert runtime_layer["metadata"]["omission_reason"] == "invalid_runtime_overlay_role"
    assert out.trace["runtime"]["status"] == "omitted"
    assert out.trace["runtime"]["included"] is False
    assert out.trace["runtime"]["omission_reason"] == "invalid_runtime_overlay_role"



def test_assemble_prompt_includes_companion_policy_after_response_shape_before_runtime_overlay():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        style_guidance="Style guidance:\n- Use analogies sparingly.",
        style_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "source_fields": ["profile.response_style"],
            "recognized_profile_fields": ["analogy_density"],
            "recognized_request_fields": [],
            "guidance_flags": {},
            "resolved_envelope": {"analogy_density": "low"},
        },
        response_shape_guidance=(
            "Response shape guidance:\n"
            "- Lead with the answer before any supporting detail."
        ),
        response_shape_trace={
            "attempted": True,
            "status": "included",
            "included": True,
            "source_fields": ["surface_context.active_task_mode"],
            "guidance_flags": {"active_task_mode": True, "concise_first_answer": True},
            "resolved_shape": {"active_task_mode": True, "concise_first_answer": True},
            "continuation_state": "abbreviated",
            "abbreviation_reason": "active_task_mode",
        },
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

    assert [msg["content"] for msg in out.messages[:7]] == [
        "profile text",
        "Style guidance:\n- Use analogies sparingly.",
        "Response shape guidance:\n- Lead with the answer before any supporting detail.",
        "contract text",
        "profile companion text",
        "scene text",
        "Runtime context: scene=planning.",
    ]
    assert out.trace["included_layers"] == [
        "profile_overlay",
        "style_guidance",
        "response_shape",
        "companion_policy",
        "runtime_overlay",
        "current_messages",
    ]
    companion_layer = out.trace["layers"][3]
    assert companion_layer["metadata"]["scene_id"] == "planning"
    assert companion_layer["metadata"]["warnings"] == [
        "unknown_requested_scene",
        "default_contract_applied",
    ]
    assert companion_layer["metadata"]["contract_trace"]["source"] == "default_compiled"
    assert companion_layer["metadata"]["interaction_contract"]["scope"] == "global_default"
    assert companion_layer["metadata"]["companion_profile_id"] == "default_companion_profile"
    assert companion_layer["metadata"]["interaction_contract_id"] == (
        "default_interaction_contract"
    )
    assert companion_layer["metadata"]["companion_policy_warnings"] == [
        "unknown_requested_scene",
        "default_contract_applied",
    ]
    assert companion_layer["metadata"]["companion_overlay_ids"] == [
        "contract-1",
        "profile-1",
        "scene-1",
    ]
    assert companion_layer["metadata"]["runtime_overlay_ids"] == ["rtoverlay_1"]
    assert companion_layer["metadata"]["cognitive_runtime_compile_status"] == "included"
    assert companion_layer["metadata"]["cognitive_runtime_compile_error"] is None
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
    companion_layer = out.trace["layers"][3]
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


def test_assemble_prompt_adds_summary_only_handoff_trace_in_parallel():
    out = assemble_prompt(
        profile={"prompt_overlay": "profile text"},
        retrieval_bundle={"bundle": {"recent": [], "semantic": [], "artifact_refs": []}},
        current_messages=[{"role": "user", "content": "hi"}],
        handoff=_build_handoff(
            companion_overlays=[
                {
                    "overlay_id": "contract-1",
                    "overlay_type": "interaction_contract",
                    "role": "system",
                    "content": "contract text",
                }
            ],
            companion_trace={
                "attempted": True,
                "status": "included",
                "included": True,
                "cognitive_runtime_compile_status": "included",
                "cognitive_runtime_compile_endpoint": "/v1/companion/profile/compile",
            },
            runtime_overlay={
                "overlay_id": "runtime-1",
                "overlay_type": "runtime_state",
                "role": "system",
                "content": "Runtime context",
                "source_fields": ["active_scene"],
            },
            runtime_trace={
                "attempted": True,
                "status": "included",
                "included": True,
                "runtime_state_id": "rtstate_1",
            },
            retrieval_bundle={
                "bundle": {
                    "recent": [{"role": "assistant", "content": "prior history"}],
                    "semantic": [
                        {
                            "message_id": "m-1",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "role": "assistant",
                            "content": "semantic note",
                        }
                    ],
                    "artifact_refs": [
                        {
                            "artifact_id": "a-1",
                            "repo_name": "repo",
                            "file_path": "api/main.py",
                            "snippet": "def entrypoint(): pass",
                        }
                    ],
                    "observed_metadata": {"has_code_like_content": False},
                }
            },
        ),
    )

    assert "handoff" in out.trace
    assert out.trace["handoff"]["request"]["request_id"] == "rid-1"
    assert out.trace["handoff"]["companion"]["overlay_refs"] == [
        {"overlay_id": "contract-1", "overlay_type": "interaction_contract"}
    ]
    assert out.trace["handoff"]["runtime"]["overlay_ref"] == {
        "overlay_id": "runtime-1",
        "overlay_type": "runtime_state",
    }
    assert out.trace["handoff"]["retrieval"]["semantic_count"] == 1
    assert out.trace["handoff"]["retrieval"]["artifact_ref_count"] == 1
    assert out.trace["handoff"]["companion"].get("content") is None
    assert out.trace["handoff"]["runtime"].get("content") is None
    assert out.trace["handoff"]["retrieval"]["semantic_refs"] == [
        {
            "message_id": "m-1",
            "created_at": "2026-01-01T00:00:00+00:00",
            "role": "assistant",
        }
    ]
    assert out.trace["handoff"]["retrieval"]["artifact_refs"] == [
        {
            "artifact_id": "a-1",
            "file_path": "api/main.py",
            "repo_name": "repo",
        }
    ]
