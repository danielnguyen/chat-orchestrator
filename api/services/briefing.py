from __future__ import annotations

from dataclasses import dataclass, field
from re import split
from typing import Any, Literal

BriefType = Literal[
    "project_status",
    "risk_review",
    "recommendation",
    "implementation_plan",
    "general",
]
Surface = Literal["chat", "telegram", "voice"]
BriefSource = Literal["explicit_user_request", "proactive_suggestion", "general_chat_context"]

VALID_BRIEF_TYPES = {
    "project_status",
    "risk_review",
    "recommendation",
    "implementation_plan",
    "general",
}

STATUS_TEMPLATE_LIBRARY: dict[str, dict[str, str]] = {
    "project_status": {
        "status": "Status",
        "net_assessment": "Net",
        "top_risk": "Risk",
        "primary_recommendation": "Recommendation",
        "next_step": "Next",
    },
    "risk_review": {
        "status": "Status",
        "net_assessment": "Assessment",
        "top_risk": "Top risk",
        "primary_recommendation": "Mitigation",
        "next_step": "Next",
    },
    "recommendation": {
        "status": "Status",
        "net_assessment": "Net",
        "top_risk": "Tradeoff",
        "primary_recommendation": "Recommendation",
        "next_step": "Next",
    },
    "implementation_plan": {
        "status": "Status",
        "net_assessment": "Net",
        "top_risk": "Risk",
        "primary_recommendation": "Approach",
        "next_step": "Next",
    },
    "general": {
        "status": "Status",
        "net_assessment": "Net",
        "top_risk": "Risk",
        "primary_recommendation": "Recommendation",
        "next_step": "Next",
    },
}


@dataclass(frozen=True)
class BriefSchema:
    status: str | None = None
    net_assessment: str | None = None
    top_risk: str | None = None
    primary_recommendation: str | None = None
    next_step: str | None = None
    optional_depth_sections: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "net_assessment": self.net_assessment,
            "top_risk": self.top_risk,
            "primary_recommendation": self.primary_recommendation,
            "next_step": self.next_step,
            "optional_depth_sections": self.optional_depth_sections,
        }


@dataclass(frozen=True)
class BriefResult:
    rendered: str
    brief: BriefSchema
    debug: dict[str, Any]


def normalize_surface(surface: str | None) -> Surface:
    value = (surface or "chat").strip().lower()
    if value in {"telegram", "mobile", "notification", "compact"}:
        return "telegram"
    if value in {"voice", "alexa", "car", "spoken"}:
        return "voice"
    return "chat"


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    out = " ".join(str(value).strip().split())
    return out or None


def _sentences(content: str | None) -> list[str]:
    text = _clean_text(content) or ""
    if not text:
        return []
    parts = split(r"(?<=[.!?])\s+", text)
    return [p.strip(" -\n\t") for p in parts if p.strip(" -\n\t")]


def _trim_terminal(value: str) -> str:
    return value.rstrip(" .")


def _strip_label(value: str) -> str:
    if ":" not in value:
        return value
    label, rest = value.split(":", 1)
    if len(label.split()) <= 4:
        return rest.strip() or value
    return value


def _first_sentence(content: str | None) -> str | None:
    sentences = _sentences(content)
    if not sentences:
        return None
    return _clean_text(sentences[0])


def _find_labeled(content: str | None, labels: tuple[str, ...]) -> str | None:
    for sentence in _sentences(content):
        lowered = sentence.lower()
        for label in labels:
            prefix = f"{label.lower()}:"
            if lowered.startswith(prefix):
                cleaned = _clean_text(sentence[len(prefix) :])
                return _trim_terminal(cleaned) if cleaned else None
    return None


def _find_sentence(content: str | None, markers: tuple[str, ...]) -> str | None:
    for sentence in _sentences(content):
        lowered = sentence.lower()
        matched_markers = [marker for marker in markers if marker in lowered]
        if matched_markers and ("without" in lowered or " no " in f" {lowered} "):
            continue
        if matched_markers:
            return _clean_text(_strip_label(sentence))
    return None


class BriefSynthesizer:
    def synthesize(
        self,
        *,
        content: str | None = None,
        structured: dict[str, Any] | None = None,
    ) -> BriefSchema:
        structured = structured or {}
        status = _clean_text(structured.get("status")) or _find_labeled(
            content,
            ("status",),
        )
        net_assessment = _clean_text(structured.get("net_assessment")) or _find_labeled(
            content,
            ("net", "assessment", "net assessment"),
        )
        top_risk = _clean_text(structured.get("top_risk")) or _find_labeled(
            content,
            ("risk", "top risk"),
        )
        if top_risk is None:
            top_risk = _find_sentence(content, ("risk", "blocker", "concern", "tradeoff"))

        primary_recommendation = _clean_text(
            structured.get("primary_recommendation")
        ) or _find_labeled(content, ("recommendation", "approach"))
        if primary_recommendation is None:
            primary_recommendation = _find_sentence(
                content,
                ("recommend", "should", "approach", "best next"),
            )

        next_step = _clean_text(structured.get("next_step")) or _find_labeled(
            content,
            ("next", "next step"),
        )
        if next_step is None:
            next_step = _find_sentence(content, ("next", "start by", "first", "then"))

        if status is None:
            status = _first_sentence(content)
        if net_assessment is None:
            net_assessment = _first_sentence(content)

        depth_sections = structured.get("optional_depth_sections") or []
        optional_depth_sections = [
            {"title": _clean_text(section.get("title")) or "Detail", "content": section_content}
            for section in depth_sections
            if isinstance(section, dict)
            for section_content in [_clean_text(section.get("content"))]
            if section_content
        ]
        if not optional_depth_sections:
            remaining = _sentences(content)[1:5]
            if remaining:
                optional_depth_sections = [
                    {"title": "Rationale", "content": " ".join(remaining[:3])}
                ]
            if len(remaining) > 3:
                optional_depth_sections.append(
                    {"title": "Additional context", "content": " ".join(remaining[3:])}
                )

        return BriefSchema(
            status=status,
            net_assessment=net_assessment,
            top_risk=top_risk,
            primary_recommendation=primary_recommendation,
            next_step=next_step,
            optional_depth_sections=optional_depth_sections,
        )


class DepthExpander:
    def render(self, brief: BriefSchema, *, brief_type: str, depth_level: int) -> str:
        labels = STATUS_TEMPLATE_LIBRARY.get(brief_type, STATUS_TEMPLATE_LIBRARY["general"])
        lead = (
            brief.net_assessment
            or brief.status
            or "No clear conclusion available from the input."
        )

        if depth_level == 0:
            return f"{labels['net_assessment']}: {lead}"

        lines = [f"{labels['net_assessment']}: {lead}"]
        if brief.status and brief.status != lead:
            lines.append(f"{labels['status']}: {brief.status}")
        if brief.top_risk:
            lines.append(f"{labels['top_risk']}: {brief.top_risk}")
        if brief.primary_recommendation:
            lines.append(f"{labels['primary_recommendation']}: {brief.primary_recommendation}")
        if brief.next_step:
            lines.append(f"{labels['next_step']}: {brief.next_step}")

        if depth_level >= 2 and brief.optional_depth_sections:
            lines.append("")
            lines.append("Rationale:")
            for section in brief.optional_depth_sections[:2]:
                lines.append(f"- {section['content']}")

        if depth_level >= 3:
            lines.append("")
            lines.append("Action framing:")
            if brief.next_step:
                lines.append(f"- Start with: {brief.next_step}")
            if brief.primary_recommendation:
                lines.append(f"- Use this approach: {brief.primary_recommendation}")
            if brief.top_risk:
                lines.append(f"- Watch: {brief.top_risk}")

        return "\n".join(lines)


class SurfaceFormatter:
    def format(self, text: str, *, surface: str) -> tuple[str, str]:
        normalized = normalize_surface(surface)
        if normalized == "telegram":
            return self._telegram(text), "telegram"
        if normalized == "voice":
            return self._voice(text), "voice"
        return text.strip(), "chat"

    def _telegram(self, text: str) -> str:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        compact = lines[:6]
        return "\n".join(line[:220] for line in compact).strip()

    def _voice(self, text: str) -> str:
        lines = [line.strip("- ").strip() for line in text.splitlines() if line.strip()]
        spoken = ". ".join(line.rstrip(".") for line in lines[:4])
        return f"{spoken}.".strip()


def _compression_ratio(source: str | None, rendered: str) -> float | None:
    source_len = len(_clean_text(source) or "")
    if source_len == 0:
        return None
    return round(len(_clean_text(rendered) or "") / source_len, 3)


def generate_brief(
    *,
    content: str | None = None,
    structured: dict[str, Any] | None = None,
    brief_type: str = "general",
    depth_level: int = 1,
    surface: str = "chat",
    source: BriefSource = "explicit_user_request",
    explicit_request: bool = True,
) -> BriefResult:
    selected_type = brief_type if brief_type in VALID_BRIEF_TYPES else "general"
    selected_depth = min(max(int(depth_level), 0), 3)
    selected_surface = normalize_surface(surface)

    brief = BriefSynthesizer().synthesize(content=content, structured=structured)
    expanded = DepthExpander().render(
        brief,
        brief_type=selected_type,
        depth_level=selected_depth,
    )
    rendered, formatter = SurfaceFormatter().format(expanded, surface=selected_surface)
    debug = {
        "enabled": True,
        "brief_type": selected_type,
        "depth_level": selected_depth,
        "surface": selected_surface,
        "formatter": formatter,
        "compression_ratio": _compression_ratio(content, rendered),
        "source": source,
        "explicit_request": explicit_request,
    }
    return BriefResult(rendered=rendered, brief=brief, debug=debug)
