from copy import deepcopy
from typing import get_args

import pytest
from models import StyleEnvelope
from services.response_shape import resolve_response_shape
from services.surface_presence import (
    PresenceState,
    apply_surface_presence_outcome,
    resolve_surface_presence,
)

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


def _surface_turn(surface="web"):
    payload = {"owner_id": "owner", "conversation_id": "conversation", "surface": surface}
    shape, _ = resolve_response_shape(payload, StyleEnvelope(), {})
    admitted = {
        "runtime_session": {"owner_id": "owner", "conversation_id": "conversation",
                            "surface": surface, "status": "active",
                            "runtime_session_id": "session"},
        "runtime_turn": {"runtime_session_id": "session", "runtime_turn_id": "turn",
                         "turn_status": "responding"},
        "event": {"event_type": "turn_started", "runtime_session_id": "session",
                  "runtime_turn_id": "turn", "event_payload_json": {"request_id": "request"}},
    }
    return payload, shape, admitted


def _processing(surface="web"):
    payload, shape, admitted = _surface_turn(surface)
    return resolve_surface_presence(payload, shape, admitted_turn=admitted, request_id="request")


def test_surface_presence_vocabulary_is_distinct_from_runtime_presence():
    assert set(get_args(PresenceState)) == {
        "idle", "listening", "thinking", "briefing", "alerting", "awaiting_confirmation",
        "suppressed", "unavailable", "fallback",
    }
    for state in ("ambient_listening", "active_conversation", "do_not_intrude", "unknown"):
        result = apply_surface_presence_outcome({"presence_state": state})
        assert result["presence_state"] == "fallback"
        assert result["reason"] == "unknown_presence_state"


@pytest.mark.parametrize("surface,completed", [("web", "idle"), ("car", "briefing")])
def test_admitted_processing_transitions_to_completed_presentation(surface, completed):
    trace = _processing(surface)
    assert trace["presence_state"] == "thinking"
    assert trace["previous_state"] is None
    assert trace["transition_status"] == "initialized"
    assert trace["source_fields"][-1] == "runtime_turn.turn_status"
    result = apply_surface_presence_outcome(trace, outcome="completed")
    assert result["presence_state"] == completed
    assert result["previous_state"] == "thinking"
    assert result["transition_status"] == "accepted"
    assert result["source_fields"][-1] == "response.completed"
    assert trace["presence_state"] == "thinking"


@pytest.mark.parametrize("field,value", [
    ("owner_id", "other"), ("conversation_id", "other"), ("surface", "other"),
    ("status", "closed"), ("closed_at", "2026-01-01T00:00:00Z"),
    ("runtime_session_id", ""),
])
def test_processing_rejects_mismatched_or_invalid_session(field, value):
    payload, shape, admitted = _surface_turn()
    admitted["runtime_session"][field] = value
    before = deepcopy(admitted)
    result = resolve_surface_presence(payload, shape, admitted_turn=admitted, request_id="request")
    assert result["presence_state"] == "fallback"
    assert result["reason"] == "invalid_runtime_binding"
    assert admitted == before


@pytest.mark.parametrize("target,field,value", [
    ("runtime_turn", "turn_status", "completed"),
    ("runtime_turn", "turn_status", {}),
    ("runtime_turn", "completed_at", "2026-01-01T00:00:00Z"),
    ("runtime_turn", "runtime_turn_id", ""),
    ("runtime_turn", "runtime_session_id", "other"),
    ("event", "runtime_turn_id", "other"),
    ("event", "event_type", "turn_completed"),
    ("event", "event_payload_json", {"request_id": "stale-request"}),
    ("event", "event_payload_json", None),
])
def test_processing_rejects_stale_malformed_and_wrong_turn_events(target, field, value):
    payload, shape, admitted = _surface_turn()
    admitted[target][field] = value
    assert resolve_surface_presence(
        payload, shape, admitted_turn=admitted, request_id="request",
    )["presence_state"] == "fallback"


@pytest.mark.parametrize("surface", ["web", "voice", "alexa", "car"])
def test_client_state_and_voice_labels_never_establish_capture(surface):
    payload, shape, _ = _surface_turn(surface)
    payload.update(presence_state="listening", outcome="intervention",
                   runtime_presence={"presence_state": "ambient_listening"})
    payload["surface_context"] = {"listening": True, "capture_active": True}
    assert resolve_surface_presence(payload, shape)["presence_state"] in {"idle", "briefing"}
    trace = _processing(surface)
    trace["presence_state"] = "listening"
    result = apply_surface_presence_outcome(trace, outcome="completed")
    assert result["presence_state"] == "fallback"
    assert result["reason"] == "listening_signal_unavailable"


@pytest.mark.parametrize("outcome,state,source", [
    ("completed", "idle", "response.completed"),
    ("confirmation_pending", "awaiting_confirmation", "capabilities.pending_confirmation"),
    ("intervention", "alerting", "interrupt_policy.response_selected"),
    ("suppressed", "suppressed", "runtime_timing.result"),
    ("privacy_withheld", "suppressed", "privacy_context.enforcement"),
])
def test_server_outcome_projection_is_bounded_and_does_not_mutate_input(outcome, state, source):
    trace = _processing()
    before = deepcopy(trace)
    result = apply_surface_presence_outcome(trace, outcome=outcome)
    assert result["presence_state"] == state
    assert result["previous_state"] == "thinking"
    assert result["source_fields"][-1] == source
    assert trace == before
    repeated = apply_surface_presence_outcome(result, outcome=outcome)
    assert repeated["presence_state"] == state
    assert repeated["transition_status"] == "unchanged"
    assert len(repeated["source_fields"]) == len(set(repeated["source_fields"]))


@pytest.mark.parametrize("outcome", ["completed", "confirmation_pending", "intervention"])
def test_suppression_and_terminal_failure_cannot_be_reactivated(outcome):
    for state in ("suppressed", "unavailable"):
        trace = apply_surface_presence_outcome(
            _processing(), outcome="privacy_withheld", unavailable=state == "unavailable",
        )
        result = apply_surface_presence_outcome(trace, outcome=outcome)
        assert result["presence_state"] == state
        assert result["transition_status"] == "rejected"
        assert result["reason"] == "invalid_transition"


@pytest.mark.parametrize("outcome", ["listening", "ambient_listening", "private-detail", {}, []])
def test_unknown_and_malformed_transitions_fail_without_exposing_input(outcome):
    result = apply_surface_presence_outcome(_processing(), outcome=outcome)
    assert result["presence_state"] == "fallback"
    assert result["reason"] == "invalid_presence_signal"
    assert "private-detail" not in str(result)


@pytest.mark.parametrize("change", [
    {"presence_state": "PRIVATE-UNKNOWN"}, {"reason": "PRIVATE-REASON"},
    {"source_fields": ["PRIVATE-SOURCE"]}, {"source_fields": [None]},
    {"spoken_output": "false"}, {"fallback_active": 1}, {"fallback_active": True},
])
def test_malformed_trace_falls_back_without_raw_content(change):
    result = apply_surface_presence_outcome({**_processing(), **change})
    assert result["presence_state"] == "fallback"
    assert "PRIVATE-" not in str(result)


def test_failure_and_privacy_take_precedence_over_provider_fallback():
    trace = _processing()
    unavailable = apply_surface_presence_outcome(trace, fallback_active=True, unavailable=True)
    assert unavailable["presence_state"] == "unavailable"
    assert unavailable["fallback_active"] is False
    suppressed = apply_surface_presence_outcome(
        trace, fallback_active=True, outcome="privacy_withheld",
    )
    assert suppressed["presence_state"] == "suppressed"
    assert suppressed["fallback_active"] is True
    repeated = apply_surface_presence_outcome(suppressed, outcome="completed")
    assert repeated["presence_state"] == "suppressed"
    assert repeated["fallback_active"] is True


def test_completed_provider_fallback_is_not_rewritten_as_ordinary_success():
    trace = apply_surface_presence_outcome(_processing(), fallback_active=True)
    result = apply_surface_presence_outcome(trace, outcome="completed")
    assert result["presence_state"] == "fallback" and result["fallback_active"] is True
    assert result["reason"] == "provider_fallback_used"


@pytest.mark.asyncio
@pytest.mark.parametrize("policy,expected", [
    ("answer_now", "idle"), ("pause_or_wait", "suppressed"),
    ("yield_to_user", "suppressed"), ("close_turn", "unavailable"),
])
async def test_normal_orchestration_consumes_current_timing_outcome(tmp_path, policy, expected):
    from test_orchestrate_flow import _run_timing_turn

    result, runtime, provider, memory = await _run_timing_turn(tmp_path, policy=policy)
    trace = memory.trace_calls[0]["payload"]
    presence = trace["retrieval"]["prompt_assembly"]["surface_presence"]
    assert presence["presence_state"] == expected
    assert presence["previous_state"] == "thinking"
    assert trace["owner_id"] == "owner" and trace["conversation_id"] == "conv-1"
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []
    if policy != "answer_now":
        assert provider.calls == []
        assert "not_called" == result["selected_model"]


@pytest.mark.asyncio
async def test_only_selected_intervention_projects_alerting(tmp_path):
    from test_orchestrate_flow import EnforceInterruptRuntime, _run_timing_turn

    result, runtime, provider, memory = await _run_timing_turn(
        tmp_path, runtime=EnforceInterruptRuntime(), interrupt_policy_mode="enforce",
    )
    trace = memory.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert trace["surface_presence"]["presence_state"] == "alerting"
    assert trace["interrupt_policy"]["response_selected"] is True
    assert result["answer"] == runtime.intervention_text
    assert provider.calls == []
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


@pytest.mark.asyncio
async def test_pending_action_presentation_does_not_confirm_or_dispatch(tmp_path, monkeypatch):
    from test_orchestrate_flow import (
        ActionConnectorRegistry,
        DisplaySettingConnector,
        DisplaySettingOperations,
        DisplaySettingRuntime,
        FakeMemoryStore,
        SequenceLiteLLM,
        _display_chat_payload,
        _install_display_capability,
        _run_durable_work_chat,
        _tool_completion,
    )

    _install_display_capability(monkeypatch)
    runtime, memory = DisplaySettingRuntime(), FakeMemoryStore()
    operations = DisplaySettingOperations()
    provider = SequenceLiteLLM([_tool_completion(
        "fixture_display_setting_apply", {"target": "fixture:display", "level": 3},
    )])
    result = await _run_durable_work_chat(
        tmp_path, memory, provider=provider, runtime=runtime,
        payload=_display_chat_payload("Apply display level 3."), capability_registry_enabled=True,
        action_connector_registry=ActionConnectorRegistry((DisplaySettingConnector(operations),)),
    )
    assert result["pending_action"]["status"] == "pending_confirmation"
    trace = memory.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert trace["surface_presence"]["presence_state"] == "awaiting_confirmation"
    assert runtime.confirmation_calls == [] and runtime.dispatch_count == 0
    assert operations.apply_inputs == []


@pytest.mark.asyncio
async def test_actual_privacy_withholding_projects_suppression(tmp_path):
    from test_orchestrate_flow import FakeRuntime, _privacy_runtime_response, _run_timing_turn

    runtime = FakeRuntime(privacy_context_response=_privacy_runtime_response(
        surface_type="unknown_surface", privacy_zone="unknown", sensitive_detail_allowed=False,
        screen_detail_allowed=False, redaction_required=True, safe_summary_required=True,
    ))
    result, runtime, _, memory = await _run_timing_turn(
        tmp_path, runtime=runtime, privacy_context_enabled=True,
    )
    trace = memory.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert trace["surface_presence"]["presence_state"] == "suppressed"
    assert "privacy_context.enforcement" in trace["surface_presence"]["source_fields"]
    assert result["answer"] and "Details cannot safely be shown" in result["answer"]
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


@pytest.mark.parametrize("context", [[], "capture", {"spoken_output": "true"},
                                        {"active_task_mode": 1}, {"surface_type": {}}])
def test_malformed_surface_metadata_cannot_invent_activity(context):
    payload, shape, _ = _surface_turn()
    payload["surface_context"] = context
    result = resolve_surface_presence(payload, shape)
    assert result["presence_state"] == "fallback"
    assert result["reason"] == "invalid_presence_signal"


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ['{"presence_state":"alerting"}', "Now listening."])
async def test_model_text_does_not_select_surface_state(tmp_path, content):
    from test_orchestrate_flow import FakeLiteLLM, _run_timing_turn

    _, runtime, _, memory = await _run_timing_turn(tmp_path, provider=FakeLiteLLM(content=content))
    trace = memory.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    assert trace["surface_presence"]["presence_state"] == "idle"
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


def _presence_policy(payload, admitted):
    return {
        "status": "included", "included": True, "profile_id": "canonical", "profile_version": 2,
        "scene_id": "planning", "scene_source": "runtime_state",
        "turn_binding": {
            "request_id": "request", "owner_id": payload["owner_id"],
            "conversation_id": payload["conversation_id"], "surface": payload["surface"],
            "runtime_session_id": admitted["runtime_session"]["runtime_session_id"],
            "runtime_turn_id": admitted["runtime_turn"]["runtime_turn_id"],
        },
    }


@pytest.mark.parametrize("surface,spoken,active,state", [
    ("web", False, False, "idle"), ("telegram", False, False, "idle"),
    ("alexa", True, False, "briefing"), ("car", True, True, "briefing"),
    ("web", True, False, "briefing"), ("web", False, True, "idle"),
    ("unknown", False, False, "idle"),
])
def test_presence_composes_validated_identity_without_changing_activity(
    surface, spoken, active, state,
):
    payload, shape, admitted = _surface_turn(surface)
    payload["requested_scene"] = "CLIENT-PRIVATE-SCENE"
    payload["surface_context"] = {"surface_type": "alexa", "profile_id": "client-profile"}
    shape = shape.model_copy(update={"spoken_output": spoken, "active_task_mode": active})
    policy = _presence_policy(payload, admitted)
    trace = resolve_surface_presence(
        payload, shape, admitted_turn=admitted, request_id="request", companion_policy=policy,
    )
    result = apply_surface_presence_outcome(trace, outcome="completed")
    assert result["presence_state"] == state
    assert result["companion"] == {
        "profile_id": "canonical", "profile_version": 2,
        "scene_id": "planning", "scene_source": "runtime_state",
    }
    assert result["turn_context"] == policy["turn_binding"]
    assert result["surface_type"] == (None if surface == "unknown" else surface)
    assert result["active_task_mode"] is active
    assert "CLIENT-PRIVATE" not in str(result) and "client-profile" not in str(result)


@pytest.mark.parametrize("field", [
    "request_id", "owner_id", "conversation_id", "surface", "runtime_session_id", "runtime_turn_id",
])
def test_wrong_turn_companion_context_is_omitted(field):
    payload, shape, admitted = _surface_turn()
    policy = _presence_policy(payload, admitted)
    policy["turn_binding"][field] = "PRIVATE-WRONG-BINDING"
    result = resolve_surface_presence(
        payload, shape, admitted_turn=admitted, request_id="request", companion_policy=policy,
    )
    assert result["presence_state"] == "thinking" and "companion" not in result
    assert "PRIVATE-WRONG" not in str(result)


@pytest.mark.parametrize("change", [
    {"included": False}, {"status": "denied"}, {"profile_version": True},
    {"profile_version": 0}, {"profile_id": None}, {"scene_id": {}},
    {"scene_source": "PRIVATE-CLIENT-SOURCE"}, {"turn_binding": None},
])
def test_missing_or_malformed_companion_does_not_invent_identity(change):
    payload, shape, admitted = _surface_turn()
    policy = {**_presence_policy(payload, admitted), **change}
    result = resolve_surface_presence(
        payload, shape, admitted_turn=admitted, request_id="request", companion_policy=policy,
    )
    assert "companion" not in result and "PRIVATE-CLIENT" not in str(result)
    assert result["presence_state"] == "thinking"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "unavailable", "denied", "malformed", "wrong_owner"])
async def test_orchestration_binds_only_validated_current_companion_policy(tmp_path, failure):
    from test_orchestrate_flow import FakeRuntime, _run_timing_turn

    runtime = FakeRuntime(companion_error=RuntimeError(failure) if failure in {
        "unavailable", "denied",
    } else None)
    if failure == "malformed":
        runtime.companion_response["profile_version"] = False
    if failure == "wrong_owner":
        runtime.companion_response["runtime_state"]["owner_id"] = "other-private-owner"
    result, runtime, provider, memory = await _run_timing_turn(
        tmp_path, runtime=runtime, companion_policy_enabled=True,
    )
    prompt = memory.trace_calls[0]["payload"]["retrieval"]["prompt_assembly"]
    presence, policy = prompt["surface_presence"], prompt["companion_policy"]
    assert presence["presence_state"] == "idle"
    assert result["answer"] == memory.added_messages[-1]["content"] == "hello"
    assert len(provider.calls) == 1
    if failure is None:
        assert presence["turn_context"] == policy["turn_binding"]
        assert presence["companion"] == {key: policy[key] for key in (
            "profile_id", "profile_version", "scene_id", "scene_source",
        )}
        system = [m["content"] for m in provider.calls[0]["messages"] if m["role"] == "system"]
        assert "scene text" in system and "profile companion text" in system
    else:
        assert "companion" not in presence and policy["included"] is False
    assert result.get("pending_action") is None
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


@pytest.mark.parametrize("outcome,state", [
    ("privacy_withheld", "suppressed"), ("suppressed", "suppressed"),
    ("confirmation_pending", "awaiting_confirmation"), ("intervention", "alerting"),
])
def test_identity_scene_and_surface_never_override_authoritative_outcome(outcome, state):
    payload, shape, admitted = _surface_turn()
    for profile, version, scene, source in [
        ("canonical", 1, "planning", "requested_scene"),
        ("canonical", 2, "work_triage", "runtime_interaction_mode"),
    ]:
        policy = {**_presence_policy(payload, admitted), "profile_id": profile,
                  "profile_version": version, "scene_id": scene, "scene_source": source}
        trace = resolve_surface_presence(
            payload, shape, admitted_turn=admitted, request_id="request", companion_policy=policy,
        )
        assert trace["presence_state"] == "thinking"
        result = apply_surface_presence_outcome(trace, outcome=outcome)
        assert result["presence_state"] == state
        if outcome == "privacy_withheld":
            assert "companion" not in result
            assert "companion_policy.validated" not in result["source_fields"]
            failed = apply_surface_presence_outcome(trace, outcome=outcome, unavailable=True)
            assert "companion" not in failed and failed["presence_state"] == "unavailable"
        else:
            assert result["companion"]["profile_version"] == version
        fallback = apply_surface_presence_outcome(trace, fallback_active=True)
        unavailable = apply_surface_presence_outcome(trace, unavailable=True)
        assert fallback["presence_state"] == "fallback"
        assert unavailable["presence_state"] == "unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("assistant_failure", [False, True])
async def test_server_owned_private_continuation_redacts_persisted_presence(
    tmp_path, assistant_failure,
):
    from test_orchestrate_flow import (
        ExactAnswerMemory,
        ExactAnswerRuntime,
        _base_payload,
        _privacy_runtime_response,
        _run_timing_turn,
    )

    class Memory(ExactAnswerMemory):
        async def add_message(self, **kwargs):
            if assistant_failure and kwargs["role"] == "assistant":
                raise RuntimeError("assistant persistence unavailable")
            return await super().add_message(**kwargs)

    runtime = ExactAnswerRuntime()
    runtime.privacy_context_response = _privacy_runtime_response(
        surface_type="unknown_surface", privacy_zone="unknown", sensitive_detail_allowed=False,
        screen_detail_allowed=False, redaction_required=True, safe_summary_required=True,
    )
    result, runtime, provider, memory = await _run_timing_turn(
        tmp_path, runtime=runtime, memory=Memory(), companion_policy_enabled=True,
        privacy_context_enabled=True, payload=_base_payload(
            conversation_id="conv-1", surface="web",
            messages=[{"role": "user", "content": "Tell me more."}],
        ),
    )
    trace = memory.trace_calls[-1]["payload"]
    prompt = trace["retrieval"]["prompt_assembly"]
    presence = prompt["surface_presence"]
    assert len(runtime.companion_calls) == len(runtime.privacy_context_calls) == 1
    assert (runtime.call_order.index("companion_policy")
            < runtime.call_order.index("privacy_context"))
    assert prompt["return_resume_context"]["status"] == "unavailable"
    assert presence["presence_state"] == ("unavailable" if assistant_failure else "suppressed")
    assert "companion" not in presence
    assert "companion_policy.validated" not in presence["source_fields"]
    assert "profile_id" not in str(presence) and "scene_id" not in str(presence)
    assert presence["turn_context"]["request_id"] == result["request_id"]
    assert presence["turn_context"]["owner_id"] == trace["owner_id"] == "owner"
    assert presence["turn_context"]["conversation_id"] == trace["conversation_id"] == "conv-1"
    for field in ("runtime_session_id", "runtime_turn_id"):
        assert presence["turn_context"][field] == prompt["turn_state"][field]
    assert provider.calls == []
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []
    assistants = [m["content"] for m in memory.added_messages if m["role"] == "assistant"]
    if assistant_failure:
        assert result["status"] == "failed" and assistants == []
        assert runtime.turn_complete_calls[-1]["turn_status"] == "abandoned"
    else:
        assert result["answer"] == (
            "I don't have enough retained context to continue that safely. "
            "Tell me what you'd like me to continue from."
        )
        assert assistants == [result["answer"]]
        assert runtime.turn_complete_calls[-1]["turn_status"] == "completed"
    assert "backup procedure" not in str(trace)


@pytest.mark.asyncio
@pytest.mark.parametrize("policy,state", [
    ("yield_to_user", "suppressed"), ("close_turn", "unavailable"),
])
async def test_early_timing_stop_before_privacy_resolution_is_safe(tmp_path, policy, state):
    from test_orchestrate_flow import _run_timing_turn

    _, runtime, provider, memory = await _run_timing_turn(
        tmp_path, policy=policy, companion_policy_enabled=True, privacy_context_enabled=True,
    )
    presence = memory.trace_calls[-1]["payload"]["retrieval"]["prompt_assembly"]["surface_presence"]
    assert presence["presence_state"] == state and provider.calls == []
    assert runtime.privacy_context_calls == []
    assert "privacy_context.enforcement" not in presence["source_fields"]
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


@pytest.mark.asyncio
async def test_resolved_privacy_is_not_reused_by_another_request(tmp_path):
    from test_orchestrate_flow import (
        ExactAnswerMemory,
        ExactAnswerRuntime,
        _privacy_runtime_response,
        _run_timing_turn,
    )

    runtime = ExactAnswerRuntime()
    runtime.privacy_context_response = _privacy_runtime_response(
        surface_type="unknown_surface", privacy_zone="unknown", sensitive_detail_allowed=False,
        screen_detail_allowed=False, redaction_required=True, safe_summary_required=True,
    )
    _, _, _, first = await _run_timing_turn(
        tmp_path, runtime=runtime, memory=ExactAnswerMemory(),
        privacy_context_enabled=True, companion_policy_enabled=True,
    )
    presence = first.trace_calls[-1]["payload"]["retrieval"]["prompt_assembly"]["surface_presence"]
    assert presence["presence_state"] == "suppressed" and "companion" not in presence
    _, _, provider, second = await _run_timing_turn(
        tmp_path, runtime=runtime, policy="yield_to_user",
        privacy_context_enabled=True, companion_policy_enabled=True,
    )
    assert len(runtime.privacy_context_calls) == 1 and provider.calls == []
    presence = second.trace_calls[-1]["payload"]["retrieval"]["prompt_assembly"]["surface_presence"]
    assert presence["presence_state"] == "suppressed" and "companion" in presence
    assert "privacy_context.enforcement" not in presence["source_fields"]
    result, _, provider, third = await _run_timing_turn(
        tmp_path, runtime=runtime, memory=ExactAnswerMemory(), companion_policy_enabled=True,
    )
    presence = third.trace_calls[-1]["payload"]["retrieval"]["prompt_assembly"]["surface_presence"]
    assert presence["presence_state"] == "idle" and "companion" in presence
    assert "privacy_context.enforcement" not in presence["source_fields"]
    assert len(runtime.privacy_context_calls) == 1 and len(provider.calls) == 1
    assert result["answer"] == third.added_messages[-1]["content"] == "hello"
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []
