from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

CAPABILITY_DESCRIPTOR_VERSION = "co.capability-descriptor.v1"
CAPABILITY_ARGUMENT_SCHEMA_VERSION = "co.capability-args.v1"
MAX_ARGUMENT_BYTES = 4096

_SAFE_ID = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
_SAFE_PROVIDER_TOOL_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9_.:@/-]{1,120}$")
_WORLD_STATE_DOMAINS = {
    "active_project",
    "active_repository",
    "active_artifact",
    "active_tool_session",
    "active_external_system",
    "pending_action",
    "runtime_surface",
}
_WORLD_STATE_OUTPUT_MODES = {"summary", "structured"}
_DRAFT_TONES = {"neutral", "warm", "direct"}
_DRAFT_FORMATS = {"plain_text", "markdown"}


class CapabilityValidationError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True)
class CapabilityEntry:
    capability_id: str
    provider_tool_name: str
    operation_class: str
    capability_domain: str
    supported_surfaces: tuple[str, ...]
    executor_binding: str
    descriptor_metadata: dict[str, Any]
    privacy_classification: str
    authorization_requirements: dict[str, Any]
    argument_schema: dict[str, Any]


@dataclass(frozen=True)
class ParsedCapabilityRequest:
    capability_id: str
    provider_tool_name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class CapabilityValidationResult:
    capability_id: str
    schema_version: str
    normalized_arguments: dict[str, Any]
    argument_digest: str
    trace: dict[str, Any]


@dataclass(frozen=True)
class CapabilityExecutionResult:
    response_text: str
    trace: dict[str, Any]


@dataclass(frozen=True)
class RevalidatorEntry:
    revalidator_id: str
    verifier_id: str
    verification_source_type: str
    verification_source_ref: str
    supported_domains: tuple[str, ...] = ()
    supported_attributes: tuple[str, ...] = ()
    resulting_authority: str = "verified_tool_output"
    resulting_confidence: float = 1.0
    resulting_freshness_state: str = "fresh"
    ttl_seconds: int | None = None
    revalidation_interval_seconds: int | None = None


@dataclass(frozen=True)
class RevalidationOutput:
    claim_id: str
    expected_value_digest: str
    observed_at: str
    verified_at: str
    source_type: str | None = None
    source_ref: str | None = None
    resulting_authority: str | None = None
    confidence: float | None = None
    freshness_state: str | None = None
    ttl_seconds: int | None = None
    revalidation_interval_seconds: int | None = None
    status: str = "verified"
    reason_code: str | None = None


@dataclass(frozen=True)
class Revalidator:
    entry: RevalidatorEntry
    verify: Any


@dataclass(frozen=True)
class _AuthorizationResult:
    trace: dict[str, Any]
    revalidation_selector: dict[str, Any] | None = None


@dataclass(frozen=True)
class _RevalidationResult:
    trace: dict[str, Any]
    selection: _AuthorizationResult | None = None


PRODUCTION_CAPABILITIES: tuple[CapabilityEntry, ...] = (
    CapabilityEntry(
        capability_id="runtime.world_state.read",
        provider_tool_name="runtime_world_state_read",
        operation_class="read",
        capability_domain="software_architecture",
        supported_surfaces=("dev", "vscode"),
        executor_binding="cr_world_state_read",
        descriptor_metadata={
            "display_name": "Read runtime world state",
            "description": "Read bounded runtime world-state claims as structured context.",
        },
        privacy_classification="runtime_context",
        authorization_requirements={
            "authorization_phases": ["exposure", "selection", "dispatch"],
            "relationship_requirements": [],
            "world_state_requirements": [],
        },
        argument_schema={
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "requested_domains": {
                    "type": "array",
                    "items": {"type": "string", "enum": sorted(_WORLD_STATE_DOMAINS)},
                    "maxItems": 4,
                },
                "entity_id": {"type": "string", "maxLength": 120},
                "attribute": {"type": "string", "maxLength": 64},
                "output_mode": {
                    "type": "string",
                    "enum": sorted(_WORLD_STATE_OUTPUT_MODES),
                },
            },
        },
    ),
    CapabilityEntry(
        capability_id="draft.local_message",
        provider_tool_name="draft_local_message",
        operation_class="draft",
        capability_domain="software_architecture",
        supported_surfaces=("dev", "vscode"),
        executor_binding="local_message_draft",
        descriptor_metadata={
            "display_name": "Draft local message",
            "description": "Create a local unsent message draft with no delivery side effect.",
        },
        privacy_classification="local_unsent_draft",
        authorization_requirements={
            "authorization_phases": ["exposure", "selection", "dispatch"],
            "relationship_requirements": [],
            "world_state_requirements": [],
        },
        argument_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["body"],
            "properties": {
                "recipient_label": {"type": "string", "maxLength": 80},
                "subject": {"type": "string", "maxLength": 120},
                "body": {"type": "string", "minLength": 1, "maxLength": 2000},
                "tone": {"type": "string", "enum": sorted(_DRAFT_TONES)},
                "format": {"type": "string", "enum": sorted(_DRAFT_FORMATS)},
            },
        },
    ),
)


def production_capability_registry() -> tuple[CapabilityEntry, ...]:
    validate_production_registry(PRODUCTION_CAPABILITIES)
    return PRODUCTION_CAPABILITIES


def production_revalidator_registry() -> dict[str, Revalidator]:
    return {}


def validate_production_registry(entries: tuple[CapabilityEntry, ...]) -> None:
    ids = [entry.capability_id for entry in entries]
    if ids != ["runtime.world_state.read", "draft.local_message"]:
        raise RuntimeError("production_capability_registry_unexpected_ids")
    for entry in entries:
        if not entry.executor_binding:
            raise RuntimeError(f"missing_executor_binding:{entry.capability_id}")
        if not _SAFE_ID.fullmatch(entry.capability_id):
            raise RuntimeError(f"invalid_capability_id:{entry.capability_id}")
        if not _SAFE_PROVIDER_TOOL_NAME.fullmatch(entry.provider_tool_name):
            raise RuntimeError(f"invalid_provider_tool_name:{entry.capability_id}")
        if entry.provider_tool_name == entry.capability_id:
            raise RuntimeError(f"provider_tool_name_not_distinct:{entry.capability_id}")
    provider_names = [entry.provider_tool_name for entry in entries]
    if len(provider_names) != len(set(provider_names)):
        raise RuntimeError("duplicate_provider_tool_name")


def capability_by_id(capability_id: str) -> CapabilityEntry | None:
    for entry in production_capability_registry():
        if entry.capability_id == capability_id:
            return entry
    return None


def capability_by_provider_tool_name(provider_tool_name: str) -> CapabilityEntry | None:
    for entry in production_capability_registry():
        if entry.provider_tool_name == provider_tool_name:
            return entry
    return None


def provider_descriptor(entry: CapabilityEntry) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": entry.provider_tool_name,
            "description": entry.descriptor_metadata["description"],
            "parameters": entry.argument_schema,
        },
        "metadata": {
            "capability_id": entry.capability_id,
            "provider_tool_name": entry.provider_tool_name,
            "operation_class": entry.operation_class,
            "capability_domain": entry.capability_domain,
            "privacy_classification": entry.privacy_classification,
            "descriptor_version": CAPABILITY_DESCRIPTOR_VERSION,
            "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
            "local_only": entry.capability_id == "draft.local_message",
        },
    }


def provider_descriptors(entries: list[CapabilityEntry]) -> list[dict[str, Any]]:
    return [
        provider_descriptor(entry)
        for entry in sorted(entries, key=lambda item: item.capability_id)
    ]


def descriptor_fingerprint(descriptors: list[dict[str, Any]]) -> str:
    material = _canonical_json(descriptors)
    return f"capdesc_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


async def filter_capability_descriptors_for_exposure(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    active_persona_id: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates = list(production_capability_registry())
    candidate_ids = [entry.capability_id for entry in candidates]
    trace: dict[str, Any] = {
        "status": "failed_closed",
        "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
        "descriptor_version": CAPABILITY_DESCRIPTOR_VERSION,
        "candidate_capability_ids": candidate_ids,
        "exposed_capability_ids": [],
        "blocked_capability_ids": candidate_ids,
        "blocked_reasons": {
            capability_id: "exposure_authorization_unavailable"
            for capability_id in candidate_ids
        },
        "descriptor_count": 0,
        "descriptor_fingerprint": descriptor_fingerprint([]),
    }
    if (
        runtime is None
        or not runtime_session_id
        or not runtime_turn_id
        or not active_persona_id
        or not hasattr(runtime, "authorize_capability")
    ):
        trace["status_reason"] = "runtime_context_unavailable"
        return [], trace

    exposed: list[CapabilityEntry] = []
    blocked_reasons: dict[str, str] = {}
    for entry in candidates:
        try:
            response = await runtime.authorize_capability(
                request_id=f"{request_id}:{entry.capability_id}:exposure",
                owner_id=owner_id,
                conversation_id=conversation_id,
                surface=surface,
                runtime_session_id=runtime_session_id,
                runtime_turn_id=runtime_turn_id,
                active_persona_id=active_persona_id,
                authorization_phase="exposure",
                capability_id=entry.capability_id,
                capability_domain=entry.capability_domain,
                operation_class=entry.operation_class,
                argument_digest=None,
                supported_surfaces=list(entry.supported_surfaces),
                relationship_requirements=entry.authorization_requirements.get(
                    "relationship_requirements",
                    [],
                ),
                selected_relationship_ids=[],
                world_state_requirements=entry.authorization_requirements.get(
                    "world_state_requirements",
                    [],
                ),
                selected_world_state_claim_ids=[],
                confirmation_challenge_ref=None,
            )
        except Exception:
            trace["status_reason"] = "exposure_authorization_unavailable"
            return [], trace
        result = response.get("result") if isinstance(response, dict) else None
        if not isinstance(result, dict) or not isinstance(result.get("allowed"), bool):
            trace["status_reason"] = "malformed_exposure_authorization"
            return [], trace
        if result["allowed"]:
            exposed.append(entry)
        else:
            blocked_reasons[entry.capability_id] = _bounded_reason(result)

    descriptors = provider_descriptors(exposed)
    exposed_ids = [entry.capability_id for entry in exposed]
    blocked_ids = [
        entry.capability_id
        for entry in candidates
        if entry.capability_id not in exposed_ids
    ]
    trace.update(
        {
            "status": "ok",
            "status_reason": "exposure_authorization_complete",
            "exposed_capability_ids": exposed_ids,
            "blocked_capability_ids": blocked_ids,
            "blocked_reasons": blocked_reasons,
            "descriptor_count": len(descriptors),
            "descriptor_fingerprint": descriptor_fingerprint(descriptors),
        }
    )
    return descriptors, trace


def parse_provider_capability_request(completion: dict[str, Any]) -> ParsedCapabilityRequest | None:
    message = _completion_message(completion)
    if message is None:
        return None
    requests: list[ParsedCapabilityRequest] = []
    tool_calls = message.get("tool_calls")
    if tool_calls is not None:
        if not isinstance(tool_calls, list):
            raise CapabilityValidationError("malformed_capability_call")
        for call in tool_calls:
            if not isinstance(call, dict):
                raise CapabilityValidationError("malformed_capability_call")
            function = call.get("function")
            if not isinstance(function, dict):
                raise CapabilityValidationError("malformed_capability_call")
            requests.append(
                _request_from_parts(function.get("name"), function.get("arguments"))
            )
    capability_request = message.get("capability_request")
    if capability_request is not None:
        if not isinstance(capability_request, dict):
            raise CapabilityValidationError("malformed_capability_call")
        requests.append(
            _request_from_parts(
                capability_request.get("capability_id"),
                capability_request.get("arguments"),
            )
        )
    if not requests:
        return None
    if len(requests) != 1:
        raise CapabilityValidationError("multiple_capability_calls")
    return requests[0]


def validate_and_digest_capability_request(
    *,
    request: ParsedCapabilityRequest,
    exposed_capability_ids: list[str],
) -> CapabilityValidationResult:
    if request.capability_id not in exposed_capability_ids:
        if capability_by_id(request.capability_id) is None:
            raise CapabilityValidationError("unknown_capability_id")
        raise CapabilityValidationError("capability_not_exposed")
    entry = capability_by_id(request.capability_id)
    if entry is None:
        raise CapabilityValidationError("unknown_capability_id")
    normalized = normalize_arguments(entry, request.arguments)
    digest = argument_digest(entry.capability_id, normalized)
    return CapabilityValidationResult(
        capability_id=entry.capability_id,
        schema_version=CAPABILITY_ARGUMENT_SCHEMA_VERSION,
        normalized_arguments=normalized,
        argument_digest=digest,
        trace={
            "capability_id": entry.capability_id,
            "provider_tool_name": entry.provider_tool_name,
            "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
            "validation_status": "ok",
            "argument_digest": digest,
            "reason_code": "validated",
        },
    )


def argument_digest(capability_id: str, normalized_arguments: dict[str, Any]) -> str:
    digest_material = {
        "capability_id": capability_id,
        "arguments": normalized_arguments,
    }
    return (
        f"capargs_"
        f"{hashlib.sha256(_canonical_json(digest_material).encode('utf-8')).hexdigest()}"
    )


async def authorize_and_execute_capability(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    active_persona_id: str | None,
    validation_result: CapabilityValidationResult,
    revalidators: dict[str, Revalidator] | None = None,
) -> CapabilityExecutionResult:
    entry = capability_by_id(validation_result.capability_id)
    trace = {
        **validation_result.trace,
        "authorization": {
            "selection": _authorization_empty_trace("not_requested"),
            "dispatch": _authorization_empty_trace("not_requested"),
        },
        "revalidation": _revalidation_empty_trace("not_required"),
        "executor_binding": entry.executor_binding if entry else None,
        "executor_called": False,
        "executor_call_count": 0,
        "executor_result_status": "not_called",
        "failure_reason_code": None,
        "response_status": "not_executed",
    }
    if (
        runtime is None
        or entry is None
        or not runtime_session_id
        or not runtime_turn_id
        or not active_persona_id
        or not hasattr(runtime, "authorize_capability")
    ):
        return _capability_not_executed(
            trace,
            "authorization_context_unavailable",
            "I could not use that capability request safely.",
        )

    selection = await _authorize_capability_phase(
        runtime=runtime,
        request_id=f"{request_id}:{entry.capability_id}:selection",
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        runtime_turn_id=runtime_turn_id,
        active_persona_id=active_persona_id,
        entry=entry,
        phase="selection",
        argument_digest_value=validation_result.argument_digest,
        confirmation_challenge_ref=None,
    )
    trace["authorization"]["selection"] = selection.trace
    if selection.trace["status"] == "revalidation_required":
        configured_revalidators = (
            revalidators if revalidators is not None else production_revalidator_registry()
        )
        revalidation = await _perform_revalidation(
            runtime=runtime,
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            active_persona_id=active_persona_id,
            entry=entry,
            argument_digest_value=validation_result.argument_digest,
            selector=selection.revalidation_selector,
            revalidators=configured_revalidators,
        )
        trace["revalidation"] = revalidation.trace
        if revalidation.selection is not None:
            trace["authorization"]["selection"] = revalidation.selection.trace
        if revalidation.trace["status"] != "verified" or revalidation.selection is None:
            return _capability_not_executed(
                trace,
                revalidation.trace.get("reason_code") or "revalidation_failed",
                _revalidation_failure_text(revalidation.trace),
            )
        selection = revalidation.selection
    if selection.trace["status"] != "allowed":
        return _capability_not_executed(
            trace,
            _authorization_failure_reason(selection.trace),
            _authorization_failure_text(selection.trace),
        )

    dispatch_digest = argument_digest(
        validation_result.capability_id,
        validation_result.normalized_arguments,
    )
    if dispatch_digest != validation_result.argument_digest:
        return _capability_not_executed(
            trace,
            "argument_digest_mismatch",
            (
                "I could not use that capability request because its arguments changed "
                "before execution."
            ),
        )

    dispatch = await _authorize_capability_phase(
        runtime=runtime,
        request_id=f"{request_id}:{entry.capability_id}:dispatch",
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        runtime_turn_id=runtime_turn_id,
        active_persona_id=active_persona_id,
        entry=entry,
        phase="dispatch",
        argument_digest_value=dispatch_digest,
        confirmation_challenge_ref=selection.trace.get("confirmation_challenge_ref"),
    )
    trace["authorization"]["dispatch"] = dispatch.trace
    if dispatch.trace["status"] != "allowed":
        return _capability_not_executed(
            trace,
            _authorization_failure_reason(dispatch.trace),
            _authorization_failure_text(dispatch.trace),
        )

    trace["executor_called"] = True
    trace["executor_call_count"] = 1
    try:
        executor_result = await _execute_capability(
            runtime=runtime,
            request_id=f"{request_id}:{entry.capability_id}:execute",
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            active_persona_id=active_persona_id,
            entry=entry,
            normalized_arguments=validation_result.normalized_arguments,
        )
    except Exception:
        return _capability_not_executed(
            trace,
            "executor_failed",
            "I could not complete that capability request.",
            executor_failed=True,
        )
    trace["executor_result_status"] = executor_result["status"]
    trace["executor_result"] = executor_result["trace"]
    if executor_result["status"] != "ok":
        return _capability_not_executed(
            trace,
            executor_result["reason_code"],
            "I could not complete that capability request.",
            executor_failed=True,
        )
    trace["response_status"] = "executed"
    return CapabilityExecutionResult(
        response_text=executor_result["response_text"],
        trace=trace,
    )


def capability_validation_failure_trace(
    reason_code: str,
    capability_id: str | None = None,
    provider_tool_name: str | None = None,
) -> dict[str, Any]:
    trace = {
        "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
        "validation_status": "rejected",
        "reason_code": reason_code,
    }
    if capability_id:
        trace["capability_id"] = capability_id[:120]
    if provider_tool_name:
        trace["provider_tool_name"] = provider_tool_name[:80]
    return trace


def normalize_arguments(entry: CapabilityEntry, arguments: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(arguments, dict):
        raise CapabilityValidationError("malformed_arguments")
    try:
        encoded = _canonical_json(arguments)
    except TypeError as exc:
        raise CapabilityValidationError("malformed_arguments") from exc
    if len(encoded.encode("utf-8")) > MAX_ARGUMENT_BYTES:
        raise CapabilityValidationError("oversized_arguments")
    if any(key not in entry.argument_schema["properties"] for key in arguments):
        raise CapabilityValidationError("schema_invalid_arguments")
    if entry.capability_id == "runtime.world_state.read":
        return _normalize_world_state_arguments(arguments)
    if entry.capability_id == "draft.local_message":
        return _normalize_draft_arguments(arguments)
    raise CapabilityValidationError("unknown_capability_id")


def provider_text(completion: dict[str, Any]) -> str:
    message = _completion_message(completion)
    if message is None:
        return ""
    content = message.get("content")
    return content if isinstance(content, str) else ""


def _normalize_world_state_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    if "requested_domains" in arguments:
        domains = arguments["requested_domains"]
        if not isinstance(domains, list) or len(domains) > 4:
            raise CapabilityValidationError("schema_invalid_arguments")
        cleaned: list[str] = []
        for domain in domains:
            if not isinstance(domain, str) or domain not in _WORLD_STATE_DOMAINS:
                raise CapabilityValidationError("schema_invalid_arguments")
            if domain not in cleaned:
                cleaned.append(domain)
        normalized["requested_domains"] = sorted(cleaned)
    for key in ("entity_id", "attribute"):
        if key in arguments:
            value = arguments[key]
            if not isinstance(value, str) or not value or not _SAFE_LABEL.fullmatch(value):
                raise CapabilityValidationError("schema_invalid_arguments")
            normalized[key] = value
    if "output_mode" in arguments:
        value = arguments["output_mode"]
        if not isinstance(value, str) or value not in _WORLD_STATE_OUTPUT_MODES:
            raise CapabilityValidationError("schema_invalid_arguments")
        normalized["output_mode"] = value
    return {key: normalized[key] for key in sorted(normalized)}


def _normalize_draft_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    body = arguments.get("body")
    if not isinstance(body, str) or not body.strip() or len(body) > 2000:
        raise CapabilityValidationError("schema_invalid_arguments")
    normalized: dict[str, Any] = {"body": body.strip()}
    for key, max_length in (("recipient_label", 80), ("subject", 120)):
        if key in arguments:
            value = arguments[key]
            if not isinstance(value, str) or not value.strip() or len(value) > max_length:
                raise CapabilityValidationError("schema_invalid_arguments")
            normalized[key] = value.strip()
    for key, allowed in (("tone", _DRAFT_TONES), ("format", _DRAFT_FORMATS)):
        if key in arguments:
            value = arguments[key]
            if not isinstance(value, str) or value not in allowed:
                raise CapabilityValidationError("schema_invalid_arguments")
            normalized[key] = value
    return {key: normalized[key] for key in sorted(normalized)}


def _completion_message(completion: dict[str, Any]) -> dict[str, Any] | None:
    choices = completion.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0]
    if not isinstance(first, dict):
        return None
    message = first.get("message")
    return message if isinstance(message, dict) else None


def _request_from_parts(provider_tool_name: Any, arguments: Any) -> ParsedCapabilityRequest:
    if not isinstance(provider_tool_name, str) or not _SAFE_PROVIDER_TOOL_NAME.fullmatch(
        provider_tool_name
    ):
        raise CapabilityValidationError("unknown_capability_id")
    entry = capability_by_provider_tool_name(provider_tool_name)
    if entry is None:
        raise CapabilityValidationError("unknown_capability_id")
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError as exc:
            raise CapabilityValidationError("malformed_arguments") from exc
    if not isinstance(arguments, dict):
        raise CapabilityValidationError("malformed_arguments")
    return ParsedCapabilityRequest(
        capability_id=entry.capability_id,
        provider_tool_name=provider_tool_name,
        arguments=arguments,
    )


def _bounded_reason(result: dict[str, Any]) -> str:
    reason_codes = result.get("reason_codes")
    if isinstance(reason_codes, list):
        for reason in reason_codes:
            if isinstance(reason, str) and reason:
                return reason[:80]
    decision_code = result.get("decision_code")
    return (
        decision_code[:80]
        if isinstance(decision_code, str) and decision_code
        else "authorization_denied"
    )


async def _authorize_capability_phase(
    *,
    runtime: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
    active_persona_id: str,
    entry: CapabilityEntry,
    phase: str,
    argument_digest_value: str,
    confirmation_challenge_ref: str | None,
) -> _AuthorizationResult:
    try:
        response = await runtime.authorize_capability(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            active_persona_id=active_persona_id,
            authorization_phase=phase,
            capability_id=entry.capability_id,
            capability_domain=entry.capability_domain,
            operation_class=entry.operation_class,
            argument_digest=argument_digest_value,
            supported_surfaces=list(entry.supported_surfaces),
            relationship_requirements=entry.authorization_requirements.get(
                "relationship_requirements",
                [],
            ),
            selected_relationship_ids=[],
            world_state_requirements=entry.authorization_requirements.get(
                "world_state_requirements",
                [],
            ),
            selected_world_state_claim_ids=[],
            confirmation_challenge_ref=confirmation_challenge_ref,
        )
    except Exception:
        return _AuthorizationResult(_authorization_empty_trace("unavailable"))
    result = response.get("result") if isinstance(response, dict) else None
    if not isinstance(result, dict) or not isinstance(result.get("allowed"), bool):
        return _AuthorizationResult(_authorization_empty_trace("malformed"))
    decision_code = _bounded_string(result.get("decision_code"), "authorization_denied", 80)
    reason_codes = _bounded_string_list(result.get("reason_codes"), 8, 80)
    status = "allowed" if result["allowed"] else decision_code
    trace = {
        "status": status,
        "phase": phase,
        "allowed": bool(result["allowed"]),
        "decision_code": decision_code,
        "reason_codes": reason_codes,
        "confirmation_challenge_ref": _bounded_optional_string(result.get("challenge_ref"), 120),
        "revalidation_selector": _revalidation_selector_summary(
            result.get("revalidation_selector")
        ),
        "relationship_id_count": len(result.get("relationship_ids_used") or []),
        "world_state_claim_id_count": len(result.get("world_state_claim_ids_used") or []),
    }
    selector = result.get("revalidation_selector")
    return _AuthorizationResult(
        trace=trace,
        revalidation_selector=selector if isinstance(selector, dict) else None,
    )


def _authorization_empty_trace(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "allowed": False,
        "decision_code": status,
        "reason_codes": [status],
        "confirmation_challenge_ref": None,
        "revalidation_selector": None,
        "relationship_id_count": 0,
        "world_state_claim_id_count": 0,
    }


def _revalidation_empty_trace(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "revalidator_id": None,
        "selected_claim_count": 0,
        "configured_revalidator_matched": False,
        "verification_call_count": 0,
        "verification_success_count": 0,
        "verification_failure_count": 0,
        "rerun_selection_status": None,
        "reason_code": status,
    }


async def _perform_revalidation(
    *,
    runtime: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
    active_persona_id: str,
    entry: CapabilityEntry,
    argument_digest_value: str,
    selector: dict[str, Any] | None,
    revalidators: dict[str, Revalidator],
) -> _RevalidationResult:
    parsed_selector, reason = _parse_revalidation_selector(selector)
    trace = _revalidation_empty_trace("required")
    if parsed_selector is None:
        trace.update({"status": "malformed", "reason_code": reason})
        return _RevalidationResult(trace)
    revalidator_id = parsed_selector["revalidator_id"]
    claim_ids = parsed_selector["claim_ids"]
    trace.update(
        {
            "revalidator_id": revalidator_id,
            "selected_claim_count": len(claim_ids),
            "reason_code": "revalidation_required",
        }
    )
    revalidator = revalidators.get(revalidator_id)
    if revalidator is None:
        trace.update({"status": "blocked", "reason_code": "unknown_revalidator_id"})
        return _RevalidationResult(trace)
    if revalidator.entry.revalidator_id != revalidator_id:
        trace.update({"status": "blocked", "reason_code": "mismatched_revalidator_id"})
        return _RevalidationResult(trace)
    trace["configured_revalidator_matched"] = True
    outputs: list[RevalidationOutput] = []
    try:
        raw_outputs = revalidator.verify(tuple(claim_ids))
        if hasattr(raw_outputs, "__await__"):
            raw_outputs = await raw_outputs
    except Exception:
        trace.update({"status": "failed", "reason_code": "revalidator_unavailable"})
        return _RevalidationResult(trace)
    if not isinstance(raw_outputs, list) or len(raw_outputs) != len(claim_ids):
        trace.update({"status": "malformed", "reason_code": "malformed_revalidator_output"})
        return _RevalidationResult(trace)
    for item in raw_outputs:
        output = _coerce_revalidation_output(item)
        if output is None or output.claim_id not in claim_ids:
            trace.update({"status": "malformed", "reason_code": "malformed_revalidator_output"})
            return _RevalidationResult(trace)
        if output.status != "verified":
            trace.update(
                {
                    "status": "failed",
                    "reason_code": output.reason_code or "revalidator_failed",
                }
            )
            return _RevalidationResult(trace)
        outputs.append(output)
    if sorted(output.claim_id for output in outputs) != sorted(claim_ids):
        trace.update({"status": "blocked", "reason_code": "revalidator_claim_mismatch"})
        return _RevalidationResult(trace)
    if not hasattr(runtime, "world_state_claim_verify"):
        trace.update({"status": "failed", "reason_code": "verification_unavailable"})
        return _RevalidationResult(trace)
    for index, output in enumerate(sorted(outputs, key=lambda item: item.claim_id)):
        verification_payload = _verification_payload(
            output=output,
            entry=revalidator.entry,
            request_id=f"{request_id}:{entry.capability_id}:verify:{index}",
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
        )
        if verification_payload is None:
            trace.update(
                {"status": "blocked", "reason_code": "inadequate_revalidator_output"}
            )
            return _RevalidationResult(trace)
        trace["verification_call_count"] += 1
        try:
            response = await runtime.world_state_claim_verify(**verification_payload)
        except Exception:
            trace["verification_failure_count"] += 1
            trace.update({"status": "failed", "reason_code": "verification_failed"})
            return _RevalidationResult(trace)
        if not _verification_response_matches_request(response, verification_payload):
            trace["verification_failure_count"] += 1
            trace.update({"status": "blocked", "reason_code": "verification_claim_mismatch"})
            return _RevalidationResult(trace)
        trace["verification_success_count"] += 1
    rerun = await _authorize_capability_phase(
        runtime=runtime,
        request_id=f"{request_id}:{entry.capability_id}:selection:rerun",
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        runtime_turn_id=runtime_turn_id,
        active_persona_id=active_persona_id,
        entry=entry,
        phase="selection",
        argument_digest_value=argument_digest_value,
        confirmation_challenge_ref=None,
    )
    trace["rerun_selection_status"] = rerun.trace["status"]
    if rerun.trace["status"] == "allowed":
        trace.update({"status": "verified", "reason_code": "verified"})
        return _RevalidationResult(trace, selection=rerun)
    if rerun.trace["status"] == "revalidation_required":
        trace.update({"status": "blocked", "reason_code": "revalidation_loop_blocked"})
    elif rerun.trace["status"] == "confirmation_required":
        trace.update({"status": "blocked", "reason_code": "confirmation_required"})
    else:
        trace.update(
            {
                "status": "blocked",
                "reason_code": _authorization_failure_reason(rerun.trace),
            }
        )
    return _RevalidationResult(trace, selection=rerun)


def _parse_revalidation_selector(value: dict[str, Any] | None) -> tuple[dict[str, Any] | None, str]:
    if not isinstance(value, dict) or set(value) != {"revalidator_id", "world_state_claim_ids"}:
        return None, "malformed_revalidation_selector"
    revalidator_id = value.get("revalidator_id")
    claim_ids = value.get("world_state_claim_ids")
    if not isinstance(revalidator_id, str) or not _SAFE_LABEL.fullmatch(revalidator_id):
        return None, "malformed_revalidation_selector"
    if not isinstance(claim_ids, list) or not claim_ids or len(claim_ids) > 64:
        return None, "malformed_revalidation_selector"
    cleaned: list[str] = []
    for claim_id in claim_ids:
        if not isinstance(claim_id, str) or not _SAFE_LABEL.fullmatch(claim_id):
            return None, "malformed_revalidation_selector"
        if claim_id not in cleaned:
            cleaned.append(claim_id)
    if len(cleaned) != len(claim_ids):
        return None, "malformed_revalidation_selector"
    return {"revalidator_id": revalidator_id, "claim_ids": cleaned}, "ok"


def _coerce_revalidation_output(value: Any) -> RevalidationOutput | None:
    if isinstance(value, RevalidationOutput):
        return value
    if not isinstance(value, dict):
        return None
    allowed = {
        "claim_id",
        "expected_value_digest",
        "observed_at",
        "verified_at",
        "source_type",
        "source_ref",
        "resulting_authority",
        "confidence",
        "freshness_state",
        "ttl_seconds",
        "revalidation_interval_seconds",
        "status",
        "reason_code",
    }
    if set(value) - allowed:
        return None
    try:
        return RevalidationOutput(
            claim_id=value["claim_id"],
            expected_value_digest=value["expected_value_digest"],
            observed_at=value["observed_at"],
            verified_at=value["verified_at"],
            source_type=value.get("source_type"),
            source_ref=value.get("source_ref"),
            resulting_authority=value.get("resulting_authority"),
            confidence=value.get("confidence"),
            freshness_state=value.get("freshness_state"),
            ttl_seconds=value.get("ttl_seconds"),
            revalidation_interval_seconds=value.get("revalidation_interval_seconds"),
            status=value.get("status", "verified"),
            reason_code=value.get("reason_code"),
        )
    except KeyError:
        return None


def _verification_payload(
    *,
    output: RevalidationOutput,
    entry: RevalidatorEntry,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
) -> dict[str, Any] | None:
    if not isinstance(output.claim_id, str) or not _SAFE_LABEL.fullmatch(output.claim_id):
        return None
    if not isinstance(output.expected_value_digest, str) or not output.expected_value_digest:
        return None
    source_type = output.source_type or entry.verification_source_type
    source_ref = output.source_ref or entry.verification_source_ref
    authority = output.resulting_authority or entry.resulting_authority
    freshness = output.freshness_state or entry.resulting_freshness_state
    confidence = output.confidence if output.confidence is not None else entry.resulting_confidence
    if (
        not isinstance(source_type, str)
        or not _SAFE_LABEL.fullmatch(source_type)
        or not isinstance(source_ref, str)
        or not source_ref
        or len(source_ref) > 240
        or not isinstance(authority, str)
        or not isinstance(freshness, str)
        or not isinstance(entry.verifier_id, str)
        or not _SAFE_LABEL.fullmatch(entry.verifier_id)
        or not isinstance(confidence, int | float)
        or isinstance(confidence, bool)
        or confidence < 0.0
        or confidence > 1.0
        or _parse_timestamp(output.observed_at) is None
        or _parse_timestamp(output.verified_at) is None
    ):
        return None
    payload: dict[str, Any] = {
        "request_id": request_id,
        "owner_id": owner_id,
        "conversation_id": conversation_id,
        "surface": surface,
        "runtime_session_id": runtime_session_id,
        "runtime_turn_id": runtime_turn_id,
        "world_state_claim_id": output.claim_id,
        "expected_value_digest": output.expected_value_digest,
        "verifier_id": entry.verifier_id,
        "verification_source_type": source_type,
        "verification_source_ref": source_ref,
        "observed_at": output.observed_at,
        "verified_at": output.verified_at,
        "resulting_authority": authority,
        "resulting_confidence": float(confidence),
        "resulting_freshness_state": freshness,
    }
    ttl_seconds = output.ttl_seconds if output.ttl_seconds is not None else entry.ttl_seconds
    interval_seconds = (
        output.revalidation_interval_seconds
        if output.revalidation_interval_seconds is not None
        else entry.revalidation_interval_seconds
    )
    if ttl_seconds is not None:
        if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
            return None
        payload["resulting_ttl_seconds"] = ttl_seconds
    if interval_seconds is not None:
        if not isinstance(interval_seconds, int) or interval_seconds <= 0:
            return None
        payload["resulting_revalidation_interval_seconds"] = interval_seconds
    return payload


def _verification_response_matches_request(
    response: Any,
    verification_payload: dict[str, Any],
) -> bool:
    if not isinstance(response, dict):
        return False
    claim = response.get("claim")
    if not isinstance(claim, dict):
        return False
    if claim.get("world_state_claim_id") != verification_payload["world_state_claim_id"]:
        return False

    optional_exact_matches = {
        "verification_verifier_id": "verifier_id",
        "verification_source_type": "verification_source_type",
        "verification_source_ref": "verification_source_ref",
        "last_verified_runtime_session_id": "runtime_session_id",
        "last_verified_runtime_turn_id": "runtime_turn_id",
    }
    for claim_key, payload_key in optional_exact_matches.items():
        if claim_key in claim and claim[claim_key] != verification_payload[payload_key]:
            return False

    if "confidence" in claim and not _bounded_confidence(claim["confidence"]):
        return False
    if "state_authority" in claim and (
        not isinstance(claim["state_authority"], str)
        or not _SAFE_LABEL.fullmatch(claim["state_authority"])
    ):
        return False
    for freshness_key in ("freshness_state", "effective_freshness_state"):
        if freshness_key in claim and (
            not isinstance(claim[freshness_key], str)
            or not _SAFE_LABEL.fullmatch(claim[freshness_key])
        ):
            return False
    for source_key in ("verification_source_type", "verification_source_ref"):
        if source_key in claim and not isinstance(claim[source_key], str):
            return False
    if "verification_source_ref" in claim and len(claim["verification_source_ref"]) > 240:
        return False
    return True


def _bounded_confidence(value: Any) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and value >= 0.0
        and value <= 1.0
    )


def _parse_timestamp(value: str) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _revalidation_failure_text(trace: dict[str, Any]) -> str:
    if trace.get("rerun_selection_status") == "revalidation_required":
        return "That capability still requires revalidation before execution."
    return (
        "That capability requires revalidation before execution, but revalidation "
        "could not be completed safely."
    )


def _authorization_failure_reason(summary: dict[str, Any]) -> str:
    status = summary.get("status")
    if status in {"confirmation_required", "revalidation_required"}:
        return str(status)
    reason_codes = summary.get("reason_codes")
    if isinstance(reason_codes, list):
        for reason in reason_codes:
            if isinstance(reason, str) and reason:
                return reason[:80]
    return _bounded_string(summary.get("decision_code"), "authorization_denied", 80)


def _authorization_failure_text(summary: dict[str, Any]) -> str:
    status = summary.get("status")
    if status == "confirmation_required":
        return "That capability needs confirmation before execution."
    if status == "revalidation_required":
        return "That capability requires revalidation before execution."
    return "I could not use that capability request safely."


def _capability_not_executed(
    trace: dict[str, Any],
    reason_code: str,
    response_text: str,
    *,
    executor_failed: bool = False,
) -> CapabilityExecutionResult:
    trace["failure_reason_code"] = reason_code
    trace["response_status"] = "executor_failed" if executor_failed else "not_executed"
    if executor_failed:
        trace["executor_result_status"] = "failed"
    return CapabilityExecutionResult(response_text=response_text, trace=trace)


async def _execute_capability(
    *,
    runtime: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    active_persona_id: str,
    entry: CapabilityEntry,
    normalized_arguments: dict[str, Any],
) -> dict[str, Any]:
    if entry.capability_id == "runtime.world_state.read":
        return await _execute_world_state_read(
            runtime=runtime,
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            active_persona_id=active_persona_id,
            normalized_arguments=normalized_arguments,
        )
    if entry.capability_id == "draft.local_message":
        return _execute_local_message_draft(normalized_arguments)
    return {
        "status": "failed",
        "reason_code": "executor_binding_unavailable",
        "trace": {"status": "failed", "reason_code": "executor_binding_unavailable"},
        "response_text": "",
    }


async def _execute_world_state_read(
    *,
    runtime: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    active_persona_id: str,
    normalized_arguments: dict[str, Any],
) -> dict[str, Any]:
    if not hasattr(runtime, "world_state_resolve"):
        return _executor_failure("world_state_read_unavailable")
    response = await runtime.world_state_resolve(
        request_id=request_id,
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        active_persona_id=active_persona_id,
        requested_domains=normalized_arguments.get("requested_domains"),
    )
    if not isinstance(response, dict):
        return _executor_failure("malformed_world_state_response")
    trace = response.get("trace")
    included_claims = response.get("included_claims")
    excluded_claims = response.get("excluded_claim_summaries", [])
    if not isinstance(trace, dict) or not isinstance(included_claims, list):
        return _executor_failure("malformed_world_state_response")
    filtered_claims = _filter_world_state_claims(
        included_claims,
        entity_id=normalized_arguments.get("entity_id"),
        attribute=normalized_arguments.get("attribute"),
    )
    domains = sorted(
        {
            claim.get("domain")
            for claim in filtered_claims
            if isinstance(claim, dict) and isinstance(claim.get("domain"), str)
        }
    )
    output_mode = normalized_arguments.get("output_mode", "summary")
    result_trace = {
        "status": "ok",
        "output_mode": output_mode,
        "included_claim_count": len(filtered_claims),
        "excluded_claim_count": len(excluded_claims) if isinstance(excluded_claims, list) else 0,
        "domain_count": len(domains),
        "domains": domains[:8],
        "stale_count": trace.get("stale_count", 0),
        "aging_count": trace.get("aging_count", 0),
        "expired_count": trace.get("expired_count", 0),
        "conflicted_count": trace.get("conflicted_count", 0),
        "confirmation_required": bool(trace.get("confirmation_required", False)),
    }
    return {
        "status": "ok",
        "reason_code": "executed",
        "trace": result_trace,
        "response_text": (
            "I read bounded runtime world state and found "
            f"{len(filtered_claims)} matching claim(s)."
        ),
    }


def _execute_local_message_draft(normalized_arguments: dict[str, Any]) -> dict[str, Any]:
    body = normalized_arguments.get("body")
    if not isinstance(body, str) or not body:
        return _executor_failure("draft_construction_failed")
    material = _canonical_json(
        {
            "body": body,
            "recipient_label": normalized_arguments.get("recipient_label"),
            "subject": normalized_arguments.get("subject"),
        }
    )
    draft_id = f"draft_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"
    result_trace = {
        "status": "ok",
        "draft_id": draft_id,
        "local": True,
        "sent": False,
        "recipient_present": bool(normalized_arguments.get("recipient_label")),
        "subject_present": bool(normalized_arguments.get("subject")),
        "body_char_count": len(body),
        "tone": _bounded_optional_string(normalized_arguments.get("tone"), 40),
        "format": _bounded_optional_string(
            normalized_arguments.get("format", "plain_text"),
            40,
        ),
    }
    return {
        "status": "ok",
        "reason_code": "executed",
        "trace": result_trace,
        "response_text": "I created a local unsent draft. Nothing was sent.",
    }


def _executor_failure(reason_code: str) -> dict[str, Any]:
    return {
        "status": "failed",
        "reason_code": reason_code,
        "trace": {"status": "failed", "reason_code": reason_code},
        "response_text": "",
    }


def _filter_world_state_claims(
    claims: list[Any],
    *,
    entity_id: str | None,
    attribute: str | None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        if entity_id is not None and claim.get("entity_id") != entity_id:
            continue
        if attribute is not None and claim.get("attribute") != attribute:
            continue
        filtered.append(claim)
    return filtered


def _revalidation_selector_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    claim_ids = value.get("world_state_claim_ids")
    return {
        "revalidator_id": _bounded_optional_string(value.get("revalidator_id"), 120),
        "world_state_claim_id_count": len(claim_ids) if isinstance(claim_ids, list) else 0,
    }


def _bounded_optional_string(value: Any, max_length: int) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value[:max_length]


def _bounded_string(value: Any, default: str, max_length: int) -> str:
    if not isinstance(value, str) or not value:
        return default
    return value[:max_length]


def _bounded_string_list(value: Any, max_items: int, max_length: int) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str) and item:
            out.append(item[:max_length])
        if len(out) >= max_items:
            break
    return out


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
