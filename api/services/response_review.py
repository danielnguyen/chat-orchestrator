from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from services.assistant_handoff import AssistantHandoff
from services.companion_presentation import CompanionPresentation

ResponseReviewSeverity = Literal["clear", "notice", "concern"]

CHECKED_CATEGORIES = [
    "empty_response",
    "unsupported_memory_claim",
    "apology_loop",
    "pseudo_attachment",
    "pressure_language",
    "response_shape_mismatch",
    "excessive_length",
]

_PUNCT_ONLY_RE = re.compile(r"[\s\W_]+", re.UNICODE)
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_WORD_RE = re.compile(r"\b[\w']+\b")
_MARKDOWN_RE = re.compile(r"(^|\n)\s*(?:[-*] |\d+\. |#{1,6}\s|```)", re.MULTILINE)
_MEMORY_CLAIMS = [
    "i remember",
    "i still remember",
    "i recall",
    "i remember from last time",
    "i remember from our last conversation",
    "as i remember",
]
_TASK_RELEVANT_REFERENCES = [
    "from the snippet",
    "from your message",
    "from what you shared",
    "based on the code",
    "based on your notes",
    "based on the context",
    "in the file",
    "in the trace",
]
_APOLOGY_PATTERNS = [
    "i'm sorry",
    "i am sorry",
    "sorry about that",
    "sorry for that",
    "i apologize",
    "sorry",
]
_APOLOGY_RE = re.compile(
    r"\b(?:"
    + "|".join(re.escape(pattern) for pattern in _APOLOGY_PATTERNS)
    + r")\b"
)
_PSEUDO_ATTACHMENT_PATTERNS = [
    "you only need me",
    "only need me",
    "don't talk to anyone else",
    "do not talk to anyone else",
    "keep this between us",
    "i'm all you need",
]
_PRESSURE_PATTERNS = [
    "you owe me",
    "if you cared",
    "you'd disappoint me",
    "do this for me",
    "don't let me down",
    "prove you care",
]
_SEVERITY_ORDER: dict[ResponseReviewSeverity, int] = {
    "clear": 0,
    "notice": 1,
    "concern": 2,
}


@dataclass(frozen=True)
class ResponseReviewFinding:
    type: str
    severity: ResponseReviewSeverity
    reason_codes: list[str]

    def to_trace(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "severity": self.severity,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True)
class ResponseReview:
    status: ResponseReviewSeverity
    finding_count: int
    highest_severity: ResponseReviewSeverity
    findings: list[ResponseReviewFinding]
    checked_categories: list[str]
    diagnostic_only: bool = True
    action_taken: str = "none"
    reviewed_text_source: str = "raw_model_output"

    def to_trace(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "finding_count": self.finding_count,
            "highest_severity": self.highest_severity,
            "findings": [finding.to_trace() for finding in self.findings],
            "checked_categories": list(self.checked_categories),
            "diagnostic_only": self.diagnostic_only,
            "action_taken": self.action_taken,
            "reviewed_text_source": self.reviewed_text_source,
        }


@dataclass(frozen=True)
class ResponseReviewInput:
    candidate_text: str
    handoff: AssistantHandoff
    presentation: CompanionPresentation
    prompt_trace: dict[str, Any]


def _normalize_text(text: str) -> str:
    return " ".join((text or "").strip().split())


def _split_sentences(text: str) -> list[str]:
    normalized = _normalize_text(text)
    if not normalized:
        return []
    parts = [part.strip() for part in _SENTENCE_RE.split(normalized) if part.strip()]
    return parts or [normalized]


def _word_count(text: str) -> int:
    return len(_WORD_RE.findall(text or ""))


def _contains_any(text: str, patterns: list[str]) -> bool:
    lowered = (text or "").lower()
    return any(pattern in lowered for pattern in patterns)


def _retrieval_support_present(handoff: AssistantHandoff) -> bool:
    retrieval_bundle = handoff.retrieval.get("bundle") or {}
    semantic = retrieval_bundle.get("semantic") or []
    recent = retrieval_bundle.get("recent") or []
    artifact_refs = retrieval_bundle.get("artifact_refs") or []
    return bool(semantic or recent or artifact_refs)


def _highest_severity(findings: list[ResponseReviewFinding]) -> ResponseReviewSeverity:
    highest: ResponseReviewSeverity = "clear"
    for finding in findings:
        if _SEVERITY_ORDER[finding.severity] > _SEVERITY_ORDER[highest]:
            highest = finding.severity
    return highest


def _empty_response_finding(text: str) -> ResponseReviewFinding | None:
    if not text or not _PUNCT_ONLY_RE.sub("", text):
        return ResponseReviewFinding(
            type="empty_response",
            severity="concern",
            reason_codes=["candidate_text_empty"],
        )
    return None


def _unsupported_memory_finding(
    text: str,
    handoff: AssistantHandoff,
) -> ResponseReviewFinding | None:
    lowered = (text or "").lower()
    if not _contains_any(lowered, _MEMORY_CLAIMS):
        return None
    if _contains_any(lowered, _TASK_RELEVANT_REFERENCES):
        return None
    if _retrieval_support_present(handoff):
        return None
    return ResponseReviewFinding(
        type="unsupported_memory_claim",
        severity="concern",
        reason_codes=["first_person_memory_without_support"],
    )


def _apology_loop_finding(text: str) -> ResponseReviewFinding | None:
    lowered = (text or "").lower()
    matches = len(list(_APOLOGY_RE.finditer(lowered)))
    if matches < 2:
        return None
    severity: ResponseReviewSeverity = "notice"
    reason_codes = ["repeated_apology_language"]
    if matches >= 3 and _word_count(text) <= 60:
        severity = "concern"
        reason_codes.append("apology_language_dominant")
    return ResponseReviewFinding(
        type="apology_loop",
        severity=severity,
        reason_codes=reason_codes,
    )


def _pseudo_attachment_finding(text: str) -> ResponseReviewFinding | None:
    if not _contains_any(text, _PSEUDO_ATTACHMENT_PATTERNS):
        return None
    return ResponseReviewFinding(
        type="pseudo_attachment",
        severity="concern",
        reason_codes=["exclusive_dependency_language"],
    )


def _pressure_language_finding(text: str) -> ResponseReviewFinding | None:
    if not _contains_any(text, _PRESSURE_PATTERNS):
        return None
    return ResponseReviewFinding(
        type="pressure_language",
        severity="concern",
        reason_codes=["coercive_or_guilt_language"],
    )


def _response_shape_mismatch_finding(
    prompt_trace: dict[str, Any],
    text: str,
) -> ResponseReviewFinding | None:
    response_shape = (prompt_trace or {}).get("response_shape") or {}
    resolved_shape = response_shape.get("resolved_shape") or {}
    reasons: list[str] = []
    sentences = _split_sentences(text)
    max_sentence_count = resolved_shape.get("max_sentence_count")
    if resolved_shape.get("avoid_markdown") and _MARKDOWN_RE.search(text or ""):
        reasons.append("markdown_heavy_when_plain_text_expected")
    if isinstance(max_sentence_count, int) and max_sentence_count > 0:
        if len(sentences) >= max_sentence_count + 2:
            reasons.append("sentence_count_exceeds_shape")
    if not reasons:
        return None
    return ResponseReviewFinding(
        type="response_shape_mismatch",
        severity="notice",
        reason_codes=reasons,
    )


def _excessive_length_finding(
    prompt_trace: dict[str, Any],
    text: str,
) -> ResponseReviewFinding | None:
    response_shape = (prompt_trace or {}).get("response_shape") or {}
    resolved_shape = response_shape.get("resolved_shape") or {}
    sentences = _split_sentences(text)
    words = _word_count(text)
    concise_signal = bool(
        resolved_shape.get("concise_first_answer")
        or resolved_shape.get("spoken_output")
        or resolved_shape.get("active_task_mode")
        or resolved_shape.get("continuation_state") in {"abbreviated", "expandable"}
    )
    if not concise_signal:
        return None
    max_sentence_count = resolved_shape.get("max_sentence_count")
    if isinstance(max_sentence_count, int) and max_sentence_count > 0:
        if len(sentences) >= max_sentence_count + 2:
            return ResponseReviewFinding(
                type="excessive_length",
                severity="notice",
                reason_codes=["concise_shape_exceeded"],
            )
    if words >= 120 and len(sentences) >= 5:
        return ResponseReviewFinding(
            type="excessive_length",
            severity="notice",
            reason_codes=["concise_shape_word_count_high"],
        )
    return None


def review_response(review_input: ResponseReviewInput) -> ResponseReview:
    text = review_input.candidate_text or ""
    findings = [
        finding
        for finding in [
            _empty_response_finding(text),
            _unsupported_memory_finding(text, review_input.handoff),
            _apology_loop_finding(text),
            _pseudo_attachment_finding(text),
            _pressure_language_finding(text),
            _response_shape_mismatch_finding(review_input.prompt_trace, text),
            _excessive_length_finding(review_input.prompt_trace, text),
        ]
        if finding is not None
    ]
    highest_severity = _highest_severity(findings)
    return ResponseReview(
        status=highest_severity,
        finding_count=len(findings),
        highest_severity=highest_severity,
        findings=findings,
        checked_categories=list(CHECKED_CATEGORIES),
    )
