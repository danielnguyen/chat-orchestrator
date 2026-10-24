from copy import deepcopy
from typing import get_args

import pytest
from models import StyleEnvelope
from services.response_shape import resolve_response_shape
from services.surface_presence import (
    PresenceState,
    apply_surface_presence_outcome,
    resolve_surface_presence,
    stream_surface_presence,
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
async def test_presence_publication_timeout_cancels_observation_without_changing_chat(tmp_path):
    import asyncio

    from test_orchestrate_flow import FakeRuntime, _run_timing_turn

    class Runtime(FakeRuntime):
        cancelled = 0

        async def publish_surface_presence(self, **body):
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled += 1

    result, runtime, _, memory = await asyncio.wait_for(
        _run_timing_turn(tmp_path, runtime=Runtime()), timeout=4,
    )
    assert runtime.cancelled == 2
    assert memory.work["state"] == "completed"
    assert result["answer"] == memory.added_messages[-1]["content"] == "hello"
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


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


@pytest.mark.asyncio
async def test_presence_observes_server_owned_pause_without_entering_cognition(tmp_path):
    from test_orchestrate_flow import (
        FakeLiteLLM,
        PauseMemory,
        PauseRuntime,
        _base_payload,
        _run_durable_work_chat,
    )

    class Runtime(PauseRuntime):
        publications = []

        async def publish_surface_presence(self, **body):
            self.publications.append(body)

    memory, runtime, provider = PauseMemory(), Runtime(), FakeLiteLLM()
    result = await _run_durable_work_chat(
        tmp_path, memory, runtime=runtime, provider=provider,
        payload=_base_payload(conversation_id="conv-1", session_transition="pause"),
    )
    assert result["status"] == "ok" and memory.work["state"] == "completed"
    assert [p["state"] for p in runtime.publications] == ["thinking", "idle"]
    assert [p["stage"] for p in runtime.publications] == ["processing", "terminal"]
    assert all(p["request_id"] == result["request_id"] for p in runtime.publications)
    assert result["answer"] == memory.added_messages[-1]["content"]
    assert provider.calls == memory.retrieve_calls == []
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


def _presence_batch(state="thinking", cursor="pe_" + "a" * 32, events=()):
    from models import PresenceProjection, PresenceRuntimeBatch

    return PresenceRuntimeBatch(
        owner_id="owner", conversation_id="conv-1", surface="web", runtime_session_id="rtsession_1",
        current=PresenceProjection(state=state, cursor=cursor) if state else None,
        cursor=cursor, events=[PresenceProjection(**e) for e in events], has_more=False,
    )


@pytest.mark.asyncio
async def test_presence_stream_lifetime_bounds_blocked_transport_and_closes_producer(monkeypatch):
    import asyncio

    from test_chat_api import _load_main

    main = _load_main(monkeypatch)
    closed = asyncio.Event()

    async def body():
        try:
            yield "bounded frame"
        finally:
            closed.set()

    async def blocked_send(message):
        if message["type"] == "http.response.body":
            await asyncio.Event().wait()

    response = main._PresenceStreamingResponse(body())
    response.max_duration = .01
    await asyncio.wait_for(response.stream_response(blocked_send), timeout=.5)
    assert closed.is_set()


def _presence_conversation():
    return {"owner_id": "owner", "conversation_id": "conv-1", "client_id": "client",
            "title": None, "lifecycle_state": "open", "superseded_by_conversation_id": None,
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}


@pytest.mark.asyncio
async def test_public_presence_scope_auth_allowlist_and_no_writable_update(monkeypatch):
    import httpx
    from test_chat_api import _load_main

    main = _load_main(monkeypatch)

    class Memory:
        async def get_conversation(self, **scope):
            return _presence_conversation()

    class Runtime:
        result = _presence_batch()

        async def read_surface_presence(self, **scope):
            return self.result

    runtime = Runtime()
    monkeypatch.setattr(main, "runtime", runtime)
    monkeypatch.setattr(main, "memory_store", Memory())
    monkeypatch.setattr(main.settings, "orch_api_key", "presence-test-key")
    params = {"owner_id": "owner", "conversation_id": "conv-1", "surface": "web"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app), base_url="http://test",
    ) as c:
        denied = await c.get("/v1/presence/current", params=params)
        assert denied.status_code == 401 and denied.headers["cache-control"] == "no-store"
        headers = {"X-API-Key": "presence-test-key"}
        for endpoint in ("current", "stream"):
            for missing in params:
                incomplete = {k: v for k, v in params.items() if k != missing}
                response = await c.get("/v1/presence/" + endpoint, params=incomplete,
                                       headers=headers)
                assert response.status_code == 400
                assert response.json() == {"detail": "presence_scope_or_cursor_invalid"}
                assert response.headers["cache-control"] == "no-store"
        response = await c.get("/v1/presence/current", params=params, headers=headers)
        assert response.status_code == 200
        assert response.json() == {"state": "thinking", "cursor": "pe_" + "a" * 32}
        assert response.headers["cache-control"] == "no-store"
        runtime.result = _presence_batch(state=None, cursor="p0_" + "a" * 32)
        response = await c.get("/v1/presence/current", params=params, headers=headers)
        assert response.json()["state"] == "unavailable"
        runtime.result = _presence_batch().model_copy(update={"owner_id": "private-owner"})
        response = await c.get("/v1/presence/current", params=params, headers=headers)
        assert response.status_code == 503 and "private-owner" not in response.text
        response = await c.post("/v1/presence/update", headers=headers,
                                json={**params, "state": "alerting"})
        assert response.status_code in (404, 405)


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["retired", "owner", "missing", "runtime", "malformed"])
async def test_public_presence_dependency_and_scope_failures_are_bounded(monkeypatch, defect):
    import httpx
    from test_chat_api import _load_main

    main = _load_main(monkeypatch)

    class Memory:
        async def get_conversation(self, **scope):
            if defect == "missing":
                raise RuntimeError("PRIVATE CONTENT")
            result = _presence_conversation()
            if defect == "owner":
                result["owner_id"] = "PRIVATE OWNER"
            if defect == "retired":
                result["lifecycle_state"] = "closed"
            return result

    class Runtime:
        async def read_surface_presence(self, **scope):
            if defect == "malformed":
                return {"content": "PRIVATE CONTENT", "state": "listening"}
            raise RuntimeError("PRIVATE ERROR")

    monkeypatch.setattr(main, "runtime", Runtime())
    monkeypatch.setattr(main, "memory_store", Memory())
    monkeypatch.setattr(main.settings, "orch_api_key", "presence-test-key")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app), base_url="http://test",
    ) as c:
        response = await c.get("/v1/presence/current", headers={"X-API-Key": "presence-test-key"},
                               params={"owner_id": "owner", "conversation_id": "conv-1",
                                       "surface": "web"})
    assert response.status_code == 503 and "PRIVATE" not in response.text
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_presence_stream_snapshot_race_replay_and_bounded_public_shape(monkeypatch):
    import httpx
    from test_chat_api import _load_main

    main = _load_main(monkeypatch)
    import services.surface_presence as service

    class Memory:
        async def get_conversation(self, **scope):
            return _presence_conversation()

    a, b = "pe_" + "a" * 32, "pe_" + "b" * 32

    class Runtime:
        async def read_surface_presence(self, **scope):
            if scope["after"] is None:
                return _presence_batch(cursor=a)
            if scope["after"] == a:
                return _presence_batch(state="suppressed", cursor=b, events=[{
                    "state": "suppressed", "cursor": b,
                }])
            if scope["after"] == b:
                return _presence_batch(state="suppressed", cursor=b)
            raise ValueError("presence_cursor_invalid")

    monkeypatch.setattr(main, "runtime", Runtime())
    monkeypatch.setattr(main, "memory_store", Memory())
    monkeypatch.setattr(main.settings, "orch_api_key", "presence-test-key")
    monkeypatch.setattr(service, "PRESENCE_STREAM_SECONDS", .03)
    monkeypatch.setattr(service, "PRESENCE_STREAM_POLL_SECONDS", .001)
    params = {"owner_id": "owner", "conversation_id": "conv-1", "surface": "web"}
    headers = {"X-API-Key": "presence-test-key"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=main.app), base_url="http://test",
    ) as c:
        first = await c.get("/v1/presence/stream", params=params, headers=headers)
        assert first.status_code == 200 and first.headers["cache-control"] == "no-store"
        assert first.text.count("event: snapshot") == first.text.count("event: transition") == 1
        assert first.text.index(a) < first.text.index(b)
        resumed = await c.get("/v1/presence/stream", params=params,
                              headers={**headers, "Last-Event-ID": a})
        assert "event: snapshot" not in resumed.text and a not in resumed.text
        assert resumed.text.count("event: transition") == 1 and b in resumed.text
        for cursor in ("malformed-private", "pe_" + "c" * 32):
            bad = await c.get("/v1/presence/stream", params={**params, "cursor": cursor},
                              headers=headers)
            assert bad.status_code == 400 and cursor not in bad.text
        conflict = await c.get("/v1/presence/stream", params={**params, "cursor": b},
                               headers={**headers, "Last-Event-ID": a})
        assert conflict.status_code == 400
    import json
    for line in first.text.splitlines():
        if line.startswith("data: "):
            assert set(json.loads(line[6:])) == {"state", "cursor"}
    assert all(secret not in first.text for secret in ("owner", "conv-1", "rtsession", "profile"))


@pytest.mark.asyncio
async def test_presence_stream_disconnect_failure_and_backpressure_limit(monkeypatch):
    import services.surface_presence as service

    class Request:
        closed = False

        async def is_disconnected(self):
            return self.closed

    request, calls = Request(), []

    async def failed(cursor):
        calls.append(cursor)
        raise RuntimeError("PRIVATE FAILURE")

    monkeypatch.setattr(service, "PRESENCE_STREAM_POLL_SECONDS", .001)
    stream = stream_surface_presence(request=request, initial=_presence_batch(),
                                     read=failed, after=None)
    assert "snapshot" in await anext(stream)
    request.closed = True
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert calls == []
    request.closed = False
    frames = [f async for f in stream_surface_presence(
        request=request, initial=_presence_batch(), read=failed, after=None,
    )]
    assert len(frames) == 2 and "event: unavailable" in frames[-1]
    assert "PRIVATE" not in str(frames)
    monkeypatch.setattr(service, "PRESENCE_STREAM_MAX_EVENTS", 3)
    events = [{"state": "idle", "cursor": "pe_" + str(i) * 32} for i in range(5)]
    initial = _presence_batch(events=events)
    frames = [f async for f in stream_surface_presence(
        request=request, initial=initial, read=failed, after="p0_" + "a" * 32,
    )]
    assert len(frames) == 3


@pytest.mark.asyncio
async def test_presence_publication_is_allowlisted_and_failure_does_not_change_chat(tmp_path):
    from test_orchestrate_flow import FakeRuntime, _run_timing_turn

    class Runtime(FakeRuntime):
        publications = []

        async def publish_surface_presence(self, **body):
            self.publications.append(body)
            raise RuntimeError("publication unavailable")

    result, runtime, _, memory = await _run_timing_turn(tmp_path, runtime=Runtime())
    assert result["answer"] == memory.added_messages[-1]["content"] == "hello"
    assert memory.work["state"] == "completed"
    assert [p["stage"] for p in runtime.publications] == ["processing", "terminal"]
    assert [p["state"] for p in runtime.publications] == ["thinking", "idle"]
    assert all(set(p) == {"owner_id", "conversation_id", "surface", "request_id",
                          "runtime_session_id", "runtime_turn_id", "stage", "state"}
               for p in runtime.publications)
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("current_state", [None, "idle"])
async def test_public_stream_cap_qualifies_history_without_skipping_reconnect(
    monkeypatch, current_state,
):
    import json

    import httpx
    import services.surface_presence as service
    from test_chat_api import _load_main

    main = _load_main(monkeypatch)
    history = [{"cursor": f"pe_{i:032x}", "state": "thinking" if i % 2 else "idle"}
               for i in range(1, 201)]
    positions = {event["cursor"]: index for index, event in enumerate(history)}

    class Memory:
        async def get_conversation(self, **scope):
            return _presence_conversation()

    class Runtime:
        fail_after_current = False

        async def read_surface_presence(self, **scope):
            after = scope["after"]
            if self.fail_after_current and after == history[-1]["cursor"]:
                raise RuntimeError("read after cap must not occur")
            if after is None:
                events, more, cursor = [], False, history[-1]["cursor"]
            else:
                if after not in positions:
                    raise ValueError("presence_cursor_invalid")
                start = positions[after] + 1
                events = history[start:start + 50]
                more = start + len(events) < len(history)
                cursor = events[-1]["cursor"] if events else after
            return _presence_batch(state=current_state, cursor=history[-1]["cursor"],
                                   events=events).model_copy(
                                       update={"cursor": cursor, "has_more": more})

    monkeypatch.setattr(main, "runtime", Runtime())
    monkeypatch.setattr(main, "memory_store", Memory())
    monkeypatch.setattr(main.settings, "orch_api_key", "presence-test-key")
    monkeypatch.setattr(service, "PRESENCE_STREAM_SECONDS", .06)
    monkeypatch.setattr(service, "PRESENCE_STREAM_POLL_SECONDS", .001)
    params = {"owner_id": "owner", "conversation_id": "conv-1", "surface": "web"}
    headers = {"X-API-Key": "presence-test-key"}

    def frames(response):
        return [{line.split(": ", 1)[0]: line.split(": ", 1)[1]
                 for line in frame.splitlines()} for frame in response.text.strip().split("\n\n")]

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                base_url="http://test") as client:
        response = await client.get("/v1/presence/current", params=params, headers=headers)
        assert response.json()["state"] == (current_state or "unavailable")
        # Starting at the second ledger event makes the final historical frame
        # thinking at the 100-frame boundary (99 transitions + qualification).
        cursor, delivered = history[1]["cursor"], []
        for attempt in range(3):
            response = await client.get("/v1/presence/stream", params=params,
                                        headers={**headers, "Last-Event-ID": cursor})
            assert response.status_code == 200
            observed = frames(response)
            transitions = [frame for frame in observed if frame["event"] == "transition"]
            delivered.extend(json.loads(frame["data"]) for frame in transitions)
            cursor = transitions[-1]["id"]
            if attempt == 0:
                assert len(observed) == 100
                assert json.loads(transitions[-1]["data"])["state"] == "thinking"
                assert observed[-1]["event"] == "unavailable" and "id" not in observed[-1]
                assert json.loads(observed[-1]["data"]) == {
                    "state": "unavailable", "cursor": cursor,
                }
            if cursor == history[-1]["cursor"]:
                break
        assert delivered == history[2:]
        if current_state is not None:
            main.runtime.fail_after_current = True
            response = await client.get("/v1/presence/stream", params=params,
                headers={**headers, "Last-Event-ID": history[99]["cursor"]})
            final = frames(response)
            assert len(final) == 100
            assert all(frame["event"] == "transition" for frame in final)
            assert [json.loads(frame["data"]) for frame in final] == history[100:]
        for invalid in ("", "malformed", "pe_" + "f" * 32):
            response = await client.get("/v1/presence/stream", params={**params, "cursor": invalid},
                                        headers=headers)
            assert response.status_code == 400


@pytest.mark.asyncio
async def test_observed_admission_removes_processing_round_trip_and_retains_terminal_order(
    tmp_path,
):
    from test_orchestrate_flow import FakeRuntime, _run_timing_turn

    class Runtime(FakeRuntime):
        publications = []

        async def start_turn_with_presence(self, **scope):
            return await self.start_turn(**scope)

        async def publish_surface_presence(self, **body):
            # The terminal write remains after acknowledged BMS finalization.
            assert memory.work["state"] == "completed"
            self.publications.append(body)

    from test_orchestrate_flow import FakeMemoryStore
    memory = FakeMemoryStore()
    result, runtime, provider, memory = await _run_timing_turn(
        tmp_path, runtime=Runtime(), memory=memory,
    )
    assert result["answer"] == memory.added_messages[-1]["content"] == "hello"
    assert len(provider.calls) == 1
    assert [p["stage"] for p in runtime.publications] == ["terminal"]
    assert [p["state"] for p in runtime.publications] == ["idle"]
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


@pytest.mark.asyncio
async def test_terminal_observation_still_requires_current_open_conversation():
    from services.surface_presence import publish_surface_presence

    payload, shape, admitted = _surface_turn()
    trace = apply_surface_presence_outcome(resolve_surface_presence(
        payload, shape, admitted_turn=admitted, request_id="request",
    ))

    class Memory:
        async def get_conversation(self, **scope):
            return {**scope, "lifecycle_state": "closed"}

    class Runtime:
        async def publish_surface_presence(self, **body):
            raise AssertionError("closed conversation must not publish")

    assert not await publish_surface_presence(
        runtime=Runtime(), memory_store=Memory(), trace=trace, stage="terminal",
    )


@pytest.mark.asyncio
async def test_trace_failure_after_canonical_cr_completion_requires_terminal_unavailable(tmp_path):
    from test_orchestrate_flow import FakeMemoryStore, FakeRuntime, _run_timing_turn

    class Memory(FakeMemoryStore):
        async def create_trace(self, **kwargs):
            raise RuntimeError("required trace persistence unavailable")

    class Runtime(FakeRuntime):
        publications = []

        async def start_turn_with_presence(self, **scope):
            return await self.start_turn(**scope)

        async def publish_surface_presence(self, **body):
            self.publications.append(body)

    memory, runtime = Memory(), Runtime()
    with pytest.raises(RuntimeError, match="required trace persistence unavailable"):
        await _run_timing_turn(tmp_path, runtime=runtime, memory=memory)
    assert runtime.turn_complete_calls[-1]["turn_status"] == "completed"
    assert any(message["role"] == "assistant" for message in memory.added_messages)
    assert memory.work["state"] == "failed"
    assert [p["state"] for p in runtime.publications] == ["unavailable"]
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []


# Reuse the existing API/canonical-work fixture; no separate lifecycle harness.
@pytest.fixture
def presence_delivery(monkeypatch, tmp_path):
    from test_chat_api import delivery_flow

    return delivery_flow.__wrapped__(monkeypatch, tmp_path)



@pytest.mark.asyncio
@pytest.mark.parametrize("deferred", [False, True])
async def test_public_answer_does_not_wait_for_slow_terminal_observation(
    presence_delivery, monkeypatch, deferred,
):
    import asyncio
    import json
    from time import perf_counter

    import httpx
    from test_chat_api import _full_chat_payload

    main, memory, provider, _, release, _, _ = presence_delivery
    validations, publications = [], []
    get_conversation = memory.get_conversation
    runtime = main.runtime
    monkeypatch.setattr(runtime, "start_turn_with_presence", runtime.start_turn, raising=False)

    async def slow_validation(**scope):
        if getattr(memory, "work", {}).get("state") == "completed":
            validations.append(scope)
            await asyncio.sleep(.9)
        return await get_conversation(**scope)

    async def slow_publication(**binding):
        assert memory.work["state"] == "completed" and memory.trace_calls
        publications.append(binding)
        await asyncio.sleep(.9)

    monkeypatch.setattr(memory, "get_conversation", slow_validation)
    monkeypatch.setattr(runtime, "publish_surface_presence", slow_publication, raising=False)
    release.set()
    started = perf_counter()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                    base_url="http://test") as client:
            response = await client.post("/v1/chat", headers={"X-API-Key": "orch-test"},
                json=_full_chat_payload(conversation_id="conv-1", allow_deferred=deferred,
                    delivery_wait_ms=5000 if deferred else None))
        delivered_ms = (perf_counter() - started) * 1000
        assert response.status_code == 200, response.text
        assert response.json()["answer"] == memory.added_messages[-1]["content"]
        assert memory.work["state"] == "completed" and len(provider.calls) == 1
        assert delivered_ms < 500
        assert main._owned_presence_tasks
        await asyncio.gather(*main._owned_presence_tasks)
        observed_ms = (perf_counter() - started) * 1000
        assert len(validations) == len(publications) == 1
        assert publications[0]["stage"] == "terminal" and publications[0]["state"] == "idle"
        assert observed_ms - delivered_ms >= 1700
        print(json.dumps({"scenario": "owned_slow_observation", "deferred": deferred,
            "answer_ms": delivered_ms, "observation_completed_ms": observed_ms,
            "blocking_observation_http_calls": 0, "canonical_equal": True}))
    finally:
        tasks = tuple(main._owned_presence_tasks | main._owned_chat_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert not main._owned_presence_tasks


@pytest.mark.asyncio
async def test_presence_observer_cap_timeout_cleanup_and_snapshot(monkeypatch):
    import asyncio

    from test_chat_api import _load_main

    main = _load_main(monkeypatch)
    active, maximum, observed = 0, 0, []
    monkeypatch.setattr(main, "_PRESENCE_OBSERVER_SECONDS", .03)

    async def blocked(**kwargs):
        nonlocal active, maximum
        observed.append(kwargs["trace"])
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1

    monkeypatch.setattr(main, "publish_surface_presence", blocked)
    trace = {"presence_state": "suppressed", "turn_context": {"request_id": "original"}}
    for _ in range(main._PRESENCE_OBSERVER_LIMIT + 10):
        main._schedule_presence_observation(trace)
    assert len(main._owned_presence_tasks) == main._PRESENCE_OBSERVER_LIMIT
    trace["turn_context"]["request_id"] = "changed"
    await asyncio.gather(*main._owned_presence_tasks)
    await asyncio.sleep(0)
    assert maximum == len(observed) == main._PRESENCE_OBSERVER_LIMIT
    assert all(item["turn_context"]["request_id"] == "original" for item in observed)
    assert active == 0 and not main._owned_presence_tasks


@pytest.mark.asyncio
async def test_shutdown_cancels_observers_before_closing_clients_and_rejects_new_tasks(monkeypatch):
    import asyncio

    from test_chat_api import _load_main

    main = _load_main(monkeypatch)
    entered, cancelled = asyncio.Event(), asyncio.Event()
    closed = []

    class Client:
        async def open(self):
            pass

        async def reconcile_interrupted_turns(self, request_id):
            pass

        async def reconcile_interrupted_work(self):
            pass

        async def close(self):
            assert cancelled.is_set()
            closed.append(True)

    async def blocked(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(main, "runtime", Client())
    monkeypatch.setattr(main, "memory_store", Client())
    monkeypatch.setattr(main, "publish_surface_presence", blocked)
    async with main.lifespan(main.app):
        main._schedule_presence_observation({"presence_state": "suppressed"})
        await entered.wait()
    assert cancelled.is_set() and len(closed) == 2
    assert not main._owned_presence_tasks
    main._schedule_presence_observation({"presence_state": "idle"})
    assert not main._owned_presence_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize("withheld,trace_failure", [(False, False), (True, False), (False, True)])
async def test_owned_terminal_observer_preserves_finalization_and_privacy(
    tmp_path, monkeypatch, withheld, trace_failure,
):
    import asyncio

    from test_chat_api import _load_main
    from test_orchestrate_flow import (
        ExactAnswerMemory,
        ExactAnswerRuntime,
        _base_payload,
        _privacy_runtime_response,
        _run_timing_turn,
    )

    main = _load_main(monkeypatch)
    memory, runtime, captured = ExactAnswerMemory(), ExactAnswerRuntime(), []
    if withheld:
        runtime.privacy_context_response = _privacy_runtime_response(
            surface_type="unknown_surface", privacy_zone="unknown", sensitive_detail_allowed=False,
            screen_detail_allowed=False, redaction_required=True, safe_summary_required=True,
        )

    async def fail_trace(**kwargs):
        raise RuntimeError("trace persistence unavailable")

    async def publish(**body):
        assert memory.work["state"] == ("failed" if trace_failure else "completed")
        captured.append(body)

    if trace_failure:
        monkeypatch.setattr(memory, "create_trace", fail_trace)
    monkeypatch.setattr(runtime, "start_turn_with_presence", runtime.start_turn, raising=False)
    monkeypatch.setattr(runtime, "publish_surface_presence", publish, raising=False)
    monkeypatch.setattr(main, "runtime", runtime)
    monkeypatch.setattr(main, "memory_store", memory)
    call = _run_timing_turn(tmp_path, runtime=runtime, memory=memory,
        companion_policy_enabled=True, privacy_context_enabled=withheld,
        payload=_base_payload(conversation_id="conv-1", surface="web",
            messages=[{"role": "user", "content": "Tell me more."}]),
        on_presence_observation=main._schedule_presence_observation)
    if trace_failure:
        with pytest.raises(RuntimeError, match="trace persistence unavailable"):
            await call
    else:
        result, _, provider, _ = await call
        assert result["answer"] == memory.added_messages[-1]["content"]
        if withheld:
            assert provider.calls == []
    await asyncio.gather(*main._owned_presence_tasks)
    assert [body["state"] for body in captured] == [
        "unavailable" if trace_failure else "suppressed" if withheld else "idle",
    ]
    assert all(set(body) == {"owner_id", "conversation_id", "surface", "request_id",
        "runtime_session_id", "runtime_turn_id", "stage", "state"} for body in captured)
    assert runtime.capability_authority_calls == runtime.capability_flow_calls == []
