from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from services.action_connectors import (
    ConnectorArguments,
    ConnectorAvailabilityRequest,
    ConnectorAvailabilityResult,
    ConnectorClaimObservation,
    ConnectorContinuationDescription,
    ConnectorExecutionRequest,
    ConnectorExecutionResult,
    ConnectorInputError,
    ConnectorPresentation,
    ConnectorRevalidationRequest,
    ConnectorRevalidationResult,
    ConnectorRevalidationSpec,
    ConnectorVerificationRequest,
    ConnectorVerificationResult,
    ExecutionStatus,
    RevalidationStatus,
    VerificationStatus,
)

JELLYFIN_CAPABILITY_ID = "jellyfin_restart"
JELLYFIN_PROVIDER_TOOL_NAME = "jellyfin_safe_restart"
JELLYFIN_TARGET = "service:jellyfin"
JELLYFIN_REVALIDATOR_ID = "jellyfin_status"
JELLYFIN_EFFECT_MODES = {"simulated", "live"}


@dataclass(frozen=True)
class JellyfinOperations:
    effect_mode: str
    status: Any
    restart: Any


@dataclass(frozen=True)
class _StatusResult:
    status: str
    reason_code: str
    observed_at: str
    verified_at: str


class JellyfinActionConnector:
    capability_id = JELLYFIN_CAPABILITY_ID
    revalidation_spec = ConnectorRevalidationSpec(
        revalidator_id=JELLYFIN_REVALIDATOR_ID,
        verifier_id=JELLYFIN_REVALIDATOR_ID,
        source_type="tool_output",
        source_ref=JELLYFIN_REVALIDATOR_ID,
    )
    presentation = ConnectorPresentation(
        pending_confirmation=(
            "Restarting service:jellyfin requires confirmation. No action was taken."
        ),
        confirmation_rejected=(
            "The restart of service:jellyfin was rejected. No action was taken."
        ),
        execution_failed=(
            "I could not restart service:jellyfin safely. No action was taken."
        ),
        execution_unknown=(
            "The restart outcome for service:jellyfin is unknown. I did not retry it."
        ),
        partially_executed=(
            "The restart of service:jellyfin was only partially completed. "
            "I did not retry it."
        ),
        executed="I restarted service:jellyfin once.",
        executed_verified=(
            "I restarted service:jellyfin once and verified it is healthy."
        ),
        executed_unverified=(
            "The restart was attempted, but service:jellyfin could not be verified "
            "healthy. I did not retry it."
        ),
    )

    def __init__(self, operations: JellyfinOperations | None) -> None:
        self._operations = operations

    @property
    def effect_mode(self) -> str | None:
        operations = self._operations
        if operations is None or operations.effect_mode not in JELLYFIN_EFFECT_MODES:
            return None
        return operations.effect_mode

    def normalize_arguments(self, arguments: Mapping[str, Any]) -> ConnectorArguments:
        if set(arguments) != {"target"} or arguments.get("target") != JELLYFIN_TARGET:
            raise ConnectorInputError("schema_invalid_arguments")
        return ConnectorArguments({"target": JELLYFIN_TARGET})

    def describe_continuation(
        self,
        arguments: ConnectorArguments,
    ) -> ConnectorContinuationDescription:
        if arguments.as_dict() != {"target": JELLYFIN_TARGET}:
            raise ConnectorInputError("schema_invalid_arguments")
        return ConnectorContinuationDescription(
            target=JELLYFIN_TARGET,
            confirmation_text=(
                "Confirm Restart Jellyfin. This may be difficult to reverse."
            ),
        )

    def restore_continuation(
        self,
        description: ConnectorContinuationDescription,
    ) -> ConnectorArguments:
        expected = self.describe_continuation(ConnectorArguments({"target": JELLYFIN_TARGET}))
        if description != expected:
            raise ConnectorInputError("continuation_mismatch")
        return ConnectorArguments({"target": JELLYFIN_TARGET})

    def check_availability(
        self,
        request: ConnectorAvailabilityRequest,
    ) -> ConnectorAvailabilityResult:
        operations = self._operations
        if (
            operations is None
            or operations.effect_mode not in JELLYFIN_EFFECT_MODES
            or not callable(operations.status)
            or not callable(operations.restart)
        ):
            return ConnectorAvailabilityResult(False, "jellyfin_operations_unavailable")
        if not request.selected_claims:
            return ConnectorAvailabilityResult(False, "restart_safe_claim_unavailable")
        return ConnectorAvailabilityResult(True, "available")

    async def revalidate(
        self,
        request: ConnectorRevalidationRequest,
    ) -> ConnectorRevalidationResult:
        operations = self._operations
        if operations is None or not callable(operations.status):
            return ConnectorRevalidationResult(
                RevalidationStatus.UNAVAILABLE,
                "revalidator_unavailable",
            )
        claim_digests = {
            item.claim_id: item.value_digest for item in request.selected_claims
        }
        requested = [
            {
                "claim_id": claim_id,
                "value_digest": claim_digests.get(claim_id, ""),
            }
            for claim_id in request.requested_claim_ids
        ]
        if any(not item["value_digest"] for item in requested):
            return ConnectorRevalidationResult(
                RevalidationStatus.FAILED,
                "revalidator_claim_mismatch",
            )
        try:
            raw = operations.status(
                {
                    "request_id": request.request_id,
                    "capability_id": JELLYFIN_CAPABILITY_ID,
                    "target": JELLYFIN_TARGET,
                    "purpose": "revalidation",
                    "claims": requested,
                }
            )
            if hasattr(raw, "__await__"):
                raw = await raw
        except Exception:
            return ConnectorRevalidationResult(
                RevalidationStatus.UNAVAILABLE,
                "revalidator_unavailable",
                external_call_count=1,
            )
        parsed, reason = _parse_status_result(
            raw,
            purpose="revalidation",
            expected_claims=requested,
        )
        if parsed is None:
            return ConnectorRevalidationResult(
                RevalidationStatus.FAILED,
                reason or "malformed_status_result",
                external_call_count=1,
            )
        if parsed.status != "safe":
            return ConnectorRevalidationResult(
                RevalidationStatus.FAILED,
                f"restart_state_{parsed.status}",
                external_call_count=1,
            )
        return ConnectorRevalidationResult(
            RevalidationStatus.SUCCESSFUL,
            "revalidated",
            observations=tuple(
                ConnectorClaimObservation(
                    claim_id=item["claim_id"],
                    expected_value_digest=item["value_digest"],
                    observed_at=parsed.observed_at,
                    verified_at=parsed.verified_at,
                )
                for item in requested
            ),
            external_call_count=1,
        )

    async def execute(
        self,
        request: ConnectorExecutionRequest,
    ) -> ConnectorExecutionResult:
        operations = self._operations
        if (
            operations is None
            or operations.effect_mode not in JELLYFIN_EFFECT_MODES
            or not callable(operations.restart)
            or request.arguments.as_dict() != {"target": JELLYFIN_TARGET}
        ):
            return ConnectorExecutionResult(
                ExecutionStatus.FAILED,
                "jellyfin_operations_unavailable",
                "jellyfin_operations_unavailable",
                0,
            )
        try:
            raw = operations.restart(
                {
                    "request_id": request.request_id,
                    "runtime_session_id": request.runtime_session_id,
                    "runtime_turn_id": request.runtime_turn_id,
                    "capability_id": JELLYFIN_CAPABILITY_ID,
                    "target": JELLYFIN_TARGET,
                }
            )
            if hasattr(raw, "__await__"):
                raw = await raw
        except Exception:
            return ConnectorExecutionResult(
                ExecutionStatus.FAILED,
                "executor_failed",
                "restart_unavailable",
                1,
                effect_mode=operations.effect_mode,
                target_label=JELLYFIN_TARGET,
            )
        if not isinstance(raw, dict) or set(raw) != {"status", "reason_code"}:
            return _malformed_execution_result(operations.effect_mode)
        status = raw.get("status")
        external_reason = raw.get("reason_code")
        allowed_statuses = {
            ExecutionStatus.COMPLETED.value,
            ExecutionStatus.FAILED.value,
            ExecutionStatus.UNKNOWN.value,
        }
        if status not in allowed_statuses or not _safe_label(external_reason):
            return _malformed_execution_result(operations.effect_mode)
        parsed_status = ExecutionStatus(status)
        reason_code = {
            ExecutionStatus.COMPLETED: "executed",
            ExecutionStatus.FAILED: "restart_failed",
            ExecutionStatus.UNKNOWN: "restart_unknown",
        }[parsed_status]
        return ConnectorExecutionResult(
            parsed_status,
            reason_code,
            external_reason,
            1,
            effect_mode=operations.effect_mode,
            target_label=JELLYFIN_TARGET,
        )

    async def verify(
        self,
        request: ConnectorVerificationRequest,
    ) -> ConnectorVerificationResult:
        operations = self._operations
        if (
            operations is None
            or operations.effect_mode not in JELLYFIN_EFFECT_MODES
            or not callable(operations.status)
        ):
            return ConnectorVerificationResult(
                VerificationStatus.UNKNOWN,
                "verification_unavailable",
                "verification_unavailable",
                0,
            )
        try:
            raw = operations.status(
                {
                    "request_id": request.request_id,
                    "capability_id": JELLYFIN_CAPABILITY_ID,
                    "target": JELLYFIN_TARGET,
                    "purpose": "post_restart",
                    "claims": [],
                }
            )
            if hasattr(raw, "__await__"):
                raw = await raw
        except Exception:
            return ConnectorVerificationResult(
                VerificationStatus.UNKNOWN,
                "verification_unavailable",
                "verification_unavailable",
                1,
                effect_mode=operations.effect_mode,
                target_label=JELLYFIN_TARGET,
            )
        parsed, reason = _parse_status_result(
            raw,
            purpose="post_restart",
            expected_claims=[],
        )
        if parsed is None:
            return ConnectorVerificationResult(
                VerificationStatus.FAILED,
                reason or "verification_result_malformed",
                "verification_result_malformed",
                1,
                effect_mode=operations.effect_mode,
                target_label=JELLYFIN_TARGET,
            )
        status, reason_code = {
            "healthy": (VerificationStatus.PASSED, "service_healthy"),
            "unhealthy": (VerificationStatus.FAILED, "service_unhealthy"),
            "unknown": (VerificationStatus.UNKNOWN, "verification_unknown"),
            "failed": (VerificationStatus.FAILED, "verification_failed"),
        }[parsed.status]
        return ConnectorVerificationResult(
            status,
            reason_code,
            parsed.reason_code,
            1,
            effect_mode=operations.effect_mode,
            target_label=JELLYFIN_TARGET,
        )


def _malformed_execution_result(effect_mode: str) -> ConnectorExecutionResult:
    return ConnectorExecutionResult(
        ExecutionStatus.FAILED,
        "malformed_restart_result",
        "malformed_restart_result",
        1,
        effect_mode=effect_mode,
        target_label=JELLYFIN_TARGET,
    )


def _safe_label(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= 80
        and all(
            character.isalnum() or character in "_.:@/-"
            for character in value
        )
    )


def _parse_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _parse_status_result(
    value: Any,
    *,
    purpose: str,
    expected_claims: list[dict[str, str]],
) -> tuple[_StatusResult | None, str | None]:
    expected_fields = {
        "status",
        "reason_code",
        "observed_at",
        "verified_at",
        "claims",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        return None, "malformed_status_result"
    allowed_statuses = (
        {"safe", "unsafe", "unknown", "failed"}
        if purpose == "revalidation"
        else {"healthy", "unhealthy", "unknown", "failed"}
    )
    status = value.get("status")
    reason_code = value.get("reason_code")
    observed_at = value.get("observed_at")
    verified_at = value.get("verified_at")
    claims = value.get("claims")
    if (
        status not in allowed_statuses
        or not _safe_label(reason_code)
        or not _parse_timestamp(observed_at)
        or not _parse_timestamp(verified_at)
        or not isinstance(claims, list)
    ):
        return None, "malformed_status_result"
    parsed_claims: list[dict[str, str]] = []
    for claim in claims:
        if not isinstance(claim, dict) or set(claim) != {"claim_id", "value_digest"}:
            return None, "malformed_status_result"
        claim_id = claim.get("claim_id")
        value_digest = claim.get("value_digest")
        if not isinstance(claim_id, str) or not isinstance(value_digest, str):
            return None, "malformed_status_result"
        parsed_claims.append({"claim_id": claim_id, "value_digest": value_digest})
    if sorted(parsed_claims, key=lambda item: item["claim_id"]) != sorted(
        expected_claims,
        key=lambda item: item["claim_id"],
    ):
        return None, "status_claim_mismatch"
    return _StatusResult(status, reason_code, observed_at, verified_at), None
