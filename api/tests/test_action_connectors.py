from __future__ import annotations

from dataclasses import dataclass

import pytest
from services.action_connectors import (
    ActionConnector,
    ActionConnectorRegistry,
    ConnectorArguments,
    ConnectorAvailabilityRequest,
    ConnectorAvailabilityResult,
    ConnectorClaimObservation,
    ConnectorClaimRef,
    ConnectorExecutionRequest,
    ConnectorExecutionResult,
    ConnectorInputError,
    ConnectorRevalidationRequest,
    ConnectorRevalidationResult,
    ConnectorVerificationRequest,
    ConnectorVerificationResult,
    ExecutionStatus,
    RevalidationStatus,
    VerificationStatus,
    is_valid_capability_id,
)
from services.jellyfin_action_connector import (
    JELLYFIN_CAPABILITY_ID,
    JELLYFIN_TARGET,
    JellyfinActionConnector,
    JellyfinOperations,
)

_CLAIMS = (ConnectorClaimRef("claim_jellyfin_safe", "wsvalue_safe"),)


@dataclass
class DisplaySettingOperations:
    result_state: str = "completed"

    def __post_init__(self) -> None:
        self.apply_inputs = []

    async def apply_setting(self, value):
        self.apply_inputs.append(value)
        return self.result_state


class DisplaySettingConnector:
    capability_id = "fixture.display_setting_apply"
    effect_mode = "simulated"
    revalidation_spec = None

    def __init__(self, operations: DisplaySettingOperations) -> None:
        self.operations = operations

    def normalize_arguments(self, arguments):
        if set(arguments) != {"target", "level"}:
            raise ConnectorInputError("schema_invalid_arguments")
        if arguments.get("target") != "fixture:display":
            raise ConnectorInputError("schema_invalid_arguments")
        level = arguments.get("level")
        if not isinstance(level, int) or isinstance(level, bool) or not 0 <= level <= 10:
            raise ConnectorInputError("schema_invalid_arguments")
        return ConnectorArguments({"target": "fixture:display", "level": level})

    def check_availability(self, request):
        return ConnectorAvailabilityResult(True, "available")

    async def revalidate(self, request):
        return ConnectorRevalidationResult(
            RevalidationStatus.UNAVAILABLE,
            "revalidation_unavailable",
        )

    async def execute(self, request):
        external_status = await self.operations.apply_setting(
            {
                "request_id": request.request_id,
                "target": request.arguments.values["target"],
                "level": request.arguments.values["level"],
            }
        )
        status = ExecutionStatus(external_status)
        return ConnectorExecutionResult(
            status,
            "executed" if status is ExecutionStatus.COMPLETED else f"setting_{status.value}",
            f"fixture_{status.value}",
            1,
            effect_mode="simulated",
            target_label="fixture:display",
        )

    async def verify(self, request):
        return ConnectorVerificationResult(
            VerificationStatus.NOT_SUPPORTED,
            "verification_not_supported",
            "verification_not_supported",
            0,
            effect_mode="simulated",
            target_label="fixture:display",
        )


class JellyfinFixtureOperations:
    def __init__(self, *, safety="safe", restart="completed", health="healthy") -> None:
        self.safety = safety
        self.restart_state = restart
        self.health = health
        self.status_inputs = []
        self.restart_inputs = []

    async def status(self, value):
        self.status_inputs.append(value)
        state = self.safety if value["purpose"] == "revalidation" else self.health
        if isinstance(state, BaseException):
            raise state
        if state == "malformed":
            return {"status": "unknown"}
        claims = value["claims"]
        if state == "mismatched":
            state = "safe" if value["purpose"] == "revalidation" else "healthy"
            claims = [{"claim_id": "claim_other", "value_digest": "wsvalue_other"}]
        return {
            "status": state,
            "reason_code": f"simulated_{state}",
            "observed_at": "2026-07-12T00:00:00+00:00",
            "verified_at": "2026-07-12T00:00:01+00:00",
            "claims": claims,
        }

    async def restart(self, value):
        self.restart_inputs.append(value)
        state = self.restart_state
        if isinstance(state, BaseException):
            raise state
        if state == "malformed":
            return {"status": "completed", "reason_code": "simulated", "extra": "no"}
        return {"status": state, "reason_code": f"simulated_{state}"}

    def binding(self, effect_mode="simulated"):
        return JellyfinOperations(effect_mode, self.status, self.restart)


def _availability(claims=_CLAIMS):
    return ConnectorAvailabilityRequest(
        surface="dev",
        active_persona_id="technical_architect",
        selected_claims=claims,
    )


def _revalidation_request(claims=_CLAIMS, claim_ids=("claim_jellyfin_safe",)):
    return ConnectorRevalidationRequest(
        request_id="rid_jellyfin_status",
        arguments=ConnectorArguments({"target": JELLYFIN_TARGET}),
        selected_claims=claims,
        requested_claim_ids=claim_ids,
    )


def _execution_request(arguments=None):
    return ConnectorExecutionRequest(
        request_id="rid_jellyfin_execute",
        runtime_session_id="rtsession_jellyfin",
        runtime_turn_id="rtturn_jellyfin",
        arguments=ConnectorArguments(arguments or {"target": JELLYFIN_TARGET}),
    )


def _verification_request(execution=None):
    return ConnectorVerificationRequest(
        request_id="rid_jellyfin_verify",
        arguments=ConnectorArguments({"target": JELLYFIN_TARGET}),
        execution=execution
        or ConnectorExecutionResult(
            ExecutionStatus.COMPLETED,
            "executed",
            "simulated_completed",
            1,
            effect_mode="simulated",
            target_label=JELLYFIN_TARGET,
        ),
    )


def test_registry_known_unknown_and_order_independent_lookup():
    display = DisplaySettingConnector(DisplaySettingOperations())
    jellyfin = JellyfinActionConnector(None)
    first = ActionConnectorRegistry((display, jellyfin))
    second = ActionConnectorRegistry((jellyfin, display))

    assert first.capability_ids() == second.capability_ids() == (
        "fixture.display_setting_apply",
        "jellyfin_restart",
    )
    assert first.get(display.capability_id) is display
    assert first.get("fixture.unknown_action") is None
    assert isinstance(first.get(JELLYFIN_CAPABILITY_ID), ActionConnector)


def test_registry_rejects_duplicate_and_malformed_identities():
    display = DisplaySettingConnector(DisplaySettingOperations())
    with pytest.raises(ValueError, match="duplicate_connector_capability_id"):
        ActionConnectorRegistry((display, display))

    class BadIdentity(DisplaySettingConnector):
        capability_id = "https://invalid/action"

    with pytest.raises(ValueError, match="invalid_connector_capability_id"):
        ActionConnectorRegistry((BadIdentity(DisplaySettingOperations()),))

    assert is_valid_capability_id("runtime.world_state.read") is True
    assert is_valid_capability_id("jellyfin_restart") is True
    assert is_valid_capability_id("plain") is False


def test_registry_lookup_and_normalization_do_not_execute_connector():
    operations = DisplaySettingOperations()
    connector = DisplaySettingConnector(operations)
    registry = ActionConnectorRegistry((connector,))

    looked_up = registry.get(connector.capability_id)
    normalized = looked_up.normalize_arguments(
        {"target": "fixture:display", "level": 4}
    )

    assert normalized.as_dict() == {"level": 4, "target": "fixture:display"}
    assert operations.apply_inputs == []
    assert not any(
        hasattr(registry, field)
        for field in ("risk_level", "authority_level", "confirmation_state")
    )


@pytest.mark.asyncio
async def test_neutral_connector_uses_different_operation_and_no_verification():
    operations = DisplaySettingOperations()
    connector = DisplaySettingConnector(operations)
    arguments = connector.normalize_arguments({"target": "fixture:display", "level": 7})
    execution = await connector.execute(
        ConnectorExecutionRequest(
            request_id="rid_display",
            runtime_session_id="rtsession_display",
            runtime_turn_id="rtturn_display",
            arguments=arguments,
        )
    )
    verification = await connector.verify(
        ConnectorVerificationRequest("rid_display_verify", arguments, execution)
    )

    assert execution.status is ExecutionStatus.COMPLETED
    assert execution.external_call_count == 1
    assert operations.apply_inputs == [
        {"request_id": "rid_display", "target": "fixture:display", "level": 7}
    ]
    assert verification.status is VerificationStatus.NOT_SUPPORTED
    assert verification.external_call_count == 0
    assert not hasattr(operations, "status")
    assert not hasattr(operations, "restart")


@pytest.mark.parametrize("status", list(ExecutionStatus))
def test_execution_result_accepts_bounded_statuses(status):
    result = ConnectorExecutionResult(status, "executed", "fixture_result", 1)
    assert result.status is status


@pytest.mark.parametrize("status", list(VerificationStatus))
def test_verification_result_accepts_bounded_statuses(status):
    result = ConnectorVerificationResult(status, "verified", "fixture_result", 0)
    assert result.status is status


def test_revalidation_results_accept_success_failure_and_unavailable():
    observation = ConnectorClaimObservation(
        "claim_fixture",
        "wsvalue_fixture",
        "2026-07-12T00:00:00+00:00",
        "2026-07-12T00:00:01+00:00",
    )
    successful = ConnectorRevalidationResult(
        RevalidationStatus.SUCCESSFUL,
        "revalidated",
        (observation,),
        1,
    )
    failed = ConnectorRevalidationResult(RevalidationStatus.FAILED, "state_unsafe", (), 1)
    unavailable = ConnectorRevalidationResult(
        RevalidationStatus.UNAVAILABLE,
        "revalidation_unavailable",
    )
    assert successful.external_call_count == 1
    assert failed.status is RevalidationStatus.FAILED
    assert unavailable.external_call_count == 0


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ConnectorExecutionResult("completed", "executed", "fixture_result", 1),
        lambda: ConnectorExecutionResult(ExecutionStatus.COMPLETED, "Bad code", "safe", 1),
        lambda: ConnectorExecutionResult(ExecutionStatus.COMPLETED, "executed", "safe", -1),
        lambda: ConnectorVerificationResult("passed", "verified", "fixture_result", 0),
        lambda: ConnectorRevalidationResult(RevalidationStatus.SUCCESSFUL, "revalidated"),
        lambda: ConnectorExecutionResult(
            ExecutionStatus.COMPLETED,
            "executed",
            "fixture_result",
            1,
            effect_mode="unsupported",
        ),
    ],
)
def test_shared_results_reject_malformed_values(factory):
    with pytest.raises(ValueError):
        factory()


def test_shared_results_do_not_accept_raw_body_or_metadata_fields():
    with pytest.raises(TypeError):
        ConnectorExecutionResult(
            ExecutionStatus.COMPLETED,
            "executed",
            "fixture_result",
            1,
            raw_body="private",
        )
    with pytest.raises(TypeError):
        ConnectorVerificationResult(
            VerificationStatus.PASSED,
            "verified",
            "fixture_result",
            1,
            metadata={"private": True},
        )


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"target": "service:other"},
        {"target": "https://service.invalid"},
        {"target": JELLYFIN_TARGET, "extra": True},
    ],
)
def test_jellyfin_connector_normalizes_only_exact_target(arguments):
    connector = JellyfinActionConnector(None)
    with pytest.raises(ConnectorInputError, match="schema_invalid_arguments"):
        connector.normalize_arguments(arguments)


def test_jellyfin_connector_exact_target_normalizes_without_external_call():
    operations = JellyfinFixtureOperations()
    connector = JellyfinActionConnector(operations.binding())
    result = connector.normalize_arguments({"target": JELLYFIN_TARGET})
    assert result.as_dict() == {"target": JELLYFIN_TARGET}
    assert operations.status_inputs == []
    assert operations.restart_inputs == []


def test_jellyfin_connector_availability_is_bounded():
    assert JellyfinActionConnector(None).check_availability(_availability()).reason_code == (
        "jellyfin_operations_unavailable"
    )
    invalid = JellyfinFixtureOperations()
    invalid_connector = JellyfinActionConnector(invalid.binding("unsupported"))
    assert invalid_connector.check_availability(_availability()).available is False
    valid = JellyfinActionConnector(JellyfinFixtureOperations().binding())
    missing_claim = valid.check_availability(_availability(()))
    assert missing_claim.reason_code == "restart_safe_claim_unavailable"
    assert valid.check_availability(_availability()).available is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "expected_status", "expected_reason"),
    [
        ("safe", RevalidationStatus.SUCCESSFUL, "revalidated"),
        ("unsafe", RevalidationStatus.FAILED, "restart_state_unsafe"),
        ("unknown", RevalidationStatus.FAILED, "restart_state_unknown"),
        ("failed", RevalidationStatus.FAILED, "restart_state_failed"),
        (RuntimeError("private"), RevalidationStatus.UNAVAILABLE, "revalidator_unavailable"),
        ("malformed", RevalidationStatus.FAILED, "malformed_status_result"),
        ("mismatched", RevalidationStatus.FAILED, "status_claim_mismatch"),
    ],
)
async def test_jellyfin_revalidation_maps_bounded_results_once(
    state,
    expected_status,
    expected_reason,
):
    operations = JellyfinFixtureOperations(safety=state)
    result = await JellyfinActionConnector(operations.binding()).revalidate(
        _revalidation_request()
    )
    assert result.status is expected_status
    assert result.reason_code == expected_reason
    assert result.external_call_count == len(operations.status_inputs) == 1


@pytest.mark.asyncio
async def test_jellyfin_revalidation_missing_digest_calls_nothing():
    operations = JellyfinFixtureOperations()
    result = await JellyfinActionConnector(operations.binding()).revalidate(
        _revalidation_request(claim_ids=("claim_other",))
    )
    assert result.status is RevalidationStatus.FAILED
    assert result.reason_code == "revalidator_claim_mismatch"
    assert result.external_call_count == 0
    assert operations.status_inputs == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "expected_status", "expected_reason"),
    [
        ("completed", ExecutionStatus.COMPLETED, "executed"),
        ("failed", ExecutionStatus.FAILED, "restart_failed"),
        ("unknown", ExecutionStatus.UNKNOWN, "restart_unknown"),
        (RuntimeError("private"), ExecutionStatus.FAILED, "executor_failed"),
        ("malformed", ExecutionStatus.FAILED, "malformed_restart_result"),
    ],
)
async def test_jellyfin_execution_maps_bounded_results_once(
    state,
    expected_status,
    expected_reason,
):
    operations = JellyfinFixtureOperations(restart=state)
    result = await JellyfinActionConnector(operations.binding()).execute(
        _execution_request()
    )
    assert result.status is expected_status
    assert result.reason_code == expected_reason
    assert result.external_call_count == len(operations.restart_inputs) == 1


@pytest.mark.asyncio
async def test_jellyfin_execution_with_missing_binding_calls_nothing():
    result = await JellyfinActionConnector(None).execute(_execution_request())
    assert result.status is ExecutionStatus.FAILED
    assert result.external_call_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "expected_status", "expected_reason"),
    [
        ("healthy", VerificationStatus.PASSED, "service_healthy"),
        ("unhealthy", VerificationStatus.FAILED, "service_unhealthy"),
        ("unknown", VerificationStatus.UNKNOWN, "verification_unknown"),
        ("failed", VerificationStatus.FAILED, "verification_failed"),
        (RuntimeError("private"), VerificationStatus.UNKNOWN, "verification_unavailable"),
        ("malformed", VerificationStatus.FAILED, "malformed_status_result"),
        ("mismatched", VerificationStatus.FAILED, "status_claim_mismatch"),
    ],
)
async def test_jellyfin_verification_maps_bounded_results_once(
    state,
    expected_status,
    expected_reason,
):
    operations = JellyfinFixtureOperations(health=state)
    result = await JellyfinActionConnector(operations.binding()).verify(
        _verification_request()
    )
    assert result.status is expected_status
    assert result.reason_code == expected_reason
    assert result.external_call_count == len(operations.status_inputs) == 1
