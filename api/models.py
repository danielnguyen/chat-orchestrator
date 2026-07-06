from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

Role = Literal["user", "assistant", "system", "tool"]
BriefType = Literal[
    "project_status",
    "risk_review",
    "recommendation",
    "implementation_plan",
    "general",
]
ResponseMode = Literal["normal", "brief"]
BriefDepth = Literal[0, 1, 2, 3]
InterruptPolicyMode = Literal["off", "evaluate_only"]
InteractionMode = Literal["text", "voice_mediated"]
LatencyPreference = Literal["normal", "low", "lowest"]
VerbosityTarget = Literal["short", "normal", "detailed"]
OutputFormat = Literal["plain_text", "markdown", "speech"]
StyleDirectness = Literal["low", "balanced", "high"]
StyleWarmth = Literal["low", "medium", "high"]
StylePlayfulnessBudget = Literal["none", "low", "medium"]
StyleChallengeSharpness = Literal["soft", "balanced", "direct"]
StyleSentenceLength = Literal["short", "medium", "flexible"]
StyleAnalogyDensity = Literal["none", "low", "medium"]
StyleTechnicalDensity = Literal["low", "adaptive", "high"]
StyleFormalityRange = Literal["casual", "neutral", "formal"]
StyleRepetitionSensitivity = Literal["normal", "high"]


class MessageIn(BaseModel):
    role: Role
    content: str


class RetrievalOptions(BaseModel):
    k: int = Field(default=8, ge=1, le=50)
    min_score: float = Field(default=0.25, ge=0.0, le=1.0)
    scope: Literal["conversation", "client", "owner"] = "conversation"
    time_window: Literal["7d", "30d", "90d", "all"] = "all"
    retrieval_mode: Literal["recent", "balanced", "historical"] = "balanced"


class StyleEnvelope(BaseModel):
    directness: StyleDirectness = "balanced"
    warmth: StyleWarmth = "medium"
    playfulness_budget: StylePlayfulnessBudget = "low"
    challenge_sharpness: StyleChallengeSharpness = "balanced"
    sentence_length: StyleSentenceLength = "flexible"
    analogy_density: StyleAnalogyDensity = "low"
    technical_density: StyleTechnicalDensity = "adaptive"
    formality_range: StyleFormalityRange = "neutral"
    repetition_sensitivity: StyleRepetitionSensitivity = "normal"


class StyleEnvelopeOverride(BaseModel):
    model_config = ConfigDict(extra="ignore")

    directness: Optional[StyleDirectness] = None
    warmth: Optional[StyleWarmth] = None
    playfulness_budget: Optional[StylePlayfulnessBudget] = None
    challenge_sharpness: Optional[StyleChallengeSharpness] = None
    sentence_length: Optional[StyleSentenceLength] = None
    analogy_density: Optional[StyleAnalogyDensity] = None
    technical_density: Optional[StyleTechnicalDensity] = None
    formality_range: Optional[StyleFormalityRange] = None
    repetition_sensitivity: Optional[StyleRepetitionSensitivity] = None


class SurfaceContext(BaseModel):
    model_config = ConfigDict(extra="ignore")

    surface_type: Optional[str] = Field(default=None, min_length=1, max_length=80)
    surface_category: Optional[str] = Field(default=None, min_length=1, max_length=80)
    sensitivity_level: Optional[str] = Field(default=None, min_length=1, max_length=32)
    sensitivity_domains: List[str] = Field(default_factory=list, max_length=8)
    interaction_mode: Optional[InteractionMode] = None
    spoken_output: Optional[bool] = None
    active_task_mode: Optional[bool] = None
    latency_preference: Optional[LatencyPreference] = None
    verbosity_target: Optional[VerbosityTarget] = None
    allows_expansion: Optional[bool] = None
    output_format: Optional[OutputFormat] = None
    style_envelope: StyleEnvelopeOverride = Field(default_factory=StyleEnvelopeOverride)


class ExternalContextRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    enabled: Optional[bool] = None
    source_ids: Optional[List[str]] = None
    domain_tags: Optional[List[str]] = None
    allowed_sensitivity: Optional[str] = None
    max_results: Optional[int] = Field(default=None, ge=1, le=20)


class CapabilityConfirmationInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    challenge_ref: Optional[str] = Field(default=None, min_length=1, max_length=120)
    capability_id: Optional[str] = Field(default=None, min_length=1, max_length=120)
    argument_digest: Optional[str] = Field(default=None, min_length=1, max_length=120)
    confirmed: Optional[bool] = None


class ChatRequest(BaseModel):
    owner_id: str
    client_id: Optional[str] = None
    conversation_id: Optional[str] = None
    surface: str = "unknown"
    surface_context: Optional[SurfaceContext] = None
    messages: List[MessageIn]
    requested_profile: Optional[str] = None
    requested_scene: Optional[str] = Field(default=None, max_length=64)
    external_context_enabled: bool = False
    external_context: Optional[ExternalContextRequest] = None
    model_override: Optional[str] = None
    sensitivity: Literal["public", "private", "local_only"] = "private"
    retrieval: Optional[RetrievalOptions] = None
    response_mode: ResponseMode = "normal"
    brief_depth: Optional[BriefDepth] = None
    brief_type: BriefType = "general"
    interrupt_policy_mode: InterruptPolicyMode = "off"
    capability_confirmation: Optional[CapabilityConfirmationInput] = None


class ChatResponse(BaseModel):
    request_id: str
    conversation_id: str
    profile_name: str
    selected_model: str
    answer: str
    status: Literal["ok", "degraded", "failed"]
    sources: List[Dict[str, Any]] = Field(default_factory=list)


class BriefStructuredInput(BaseModel):
    status: Optional[str] = None
    net_assessment: Optional[str] = None
    top_risk: Optional[str] = None
    primary_recommendation: Optional[str] = None
    next_step: Optional[str] = None
    optional_depth_sections: List[Dict[str, str]] = Field(default_factory=list)


class BriefGenerateRequest(BaseModel):
    content: Optional[str] = None
    structured: Optional[BriefStructuredInput] = None
    brief_type: BriefType = "general"
    depth_level: BriefDepth = 1
    surface: str = "chat"
    source_context: Dict[str, Any] = Field(default_factory=dict)


class BriefGenerateResponse(BaseModel):
    rendered: str
    brief: Dict[str, Any]
    debug: Dict[str, Any]


class RouteDecision(BaseModel):
    selected_model: str
    provider: str
    rule_id: str
    rationale: str
    fallbacks: List[Dict[str, str]] = Field(default_factory=list)


class RetrievalBundleResponse(BaseModel):
    request_id: str
    conversation_id: str
    bundle: Dict[str, Any]


class ProfileResolveResponse(BaseModel):
    profile_name: str
    source: str
    profile_version: int
    effective_profile_ref: str
    prompt_overlay: str
    retrieval_policy: Dict[str, Any]
    routing_policy: Dict[str, Any]
    response_style: Dict[str, Any]
    safety_policy: Dict[str, Any]
    tool_policy: Dict[str, Any]
