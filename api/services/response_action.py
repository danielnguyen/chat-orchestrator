from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from services.response_review import ResponseReview

ResponseActionMode = Literal["shadow", "template_fallback"]

_EMPTY_RESPONSE_FALLBACK = "I couldn’t produce a useful answer there."
_DEPENDENCY_PRESSURE_FALLBACK = (
    "I can help with the task, but I should not pressure you or create dependency. "
    "Let’s keep this grounded."
)


@dataclass(frozen=True)
class ResponseActionInput:
    mode: ResponseActionMode
    candidate_text: str
    response_review: ResponseReview


@dataclass(frozen=True)
class ResponseActionResult:
    mode: ResponseActionMode
    action_taken: str
    action_reason_codes: list[str]
    action_source: str
    affected_finding_types: list[str]
    diagnostic_only: bool
    original_review_status: str
    candidate_text: str

    def to_trace(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "action_taken": self.action_taken,
            "action_reason_codes": list(self.action_reason_codes),
            "action_source": self.action_source,
            "affected_finding_types": list(self.affected_finding_types),
            "diagnostic_only": self.diagnostic_only,
            "original_review_status": self.original_review_status,
        }


def _unique(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        if value not in out:
            out.append(value)
    return out


def apply_response_action(action_input: ResponseActionInput) -> ResponseActionResult:
    review = action_input.response_review
    actionable_findings = [
        finding
        for finding in review.findings
        if finding.type in {"empty_response", "pseudo_attachment", "pressure_language"}
    ]
    affected_finding_types = _unique([finding.type for finding in actionable_findings])
    action_reason_codes = _unique(
        [
            reason_code
            for finding in actionable_findings
            for reason_code in finding.reason_codes
        ]
    )

    if action_input.mode == "shadow" or not actionable_findings or review.status != "concern":
        return ResponseActionResult(
            mode=action_input.mode,
            action_taken="none",
            action_reason_codes=action_reason_codes,
            action_source="response_review",
            affected_finding_types=affected_finding_types,
            diagnostic_only=True,
            original_review_status=review.status,
            candidate_text=action_input.candidate_text,
        )

    if "empty_response" in affected_finding_types:
        candidate_text = _EMPTY_RESPONSE_FALLBACK
    else:
        candidate_text = _DEPENDENCY_PRESSURE_FALLBACK

    return ResponseActionResult(
        mode=action_input.mode,
        action_taken="template_fallback",
        action_reason_codes=action_reason_codes,
        action_source="response_review",
        affected_finding_types=affected_finding_types,
        diagnostic_only=False,
        original_review_status=review.status,
        candidate_text=candidate_text,
    )
