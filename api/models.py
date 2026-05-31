from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

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


class MessageIn(BaseModel):
    role: Role
    content: str


class RetrievalOptions(BaseModel):
    k: int = Field(default=8, ge=1, le=50)
    min_score: float = Field(default=0.25, ge=0.0, le=1.0)
    scope: Literal["conversation", "client", "owner"] = "conversation"
    time_window: Literal["7d", "30d", "90d", "all"] = "all"
    retrieval_mode: Literal["recent", "balanced", "historical"] = "balanced"


class ChatRequest(BaseModel):
    owner_id: str
    client_id: Optional[str] = None
    conversation_id: Optional[str] = None
    surface: str = "unknown"
    messages: List[MessageIn]
    requested_profile: Optional[str] = None
    requested_scene: Optional[str] = Field(default=None, max_length=64)
    model_override: Optional[str] = None
    sensitivity: Literal["public", "private", "local_only"] = "private"
    retrieval: Optional[RetrievalOptions] = None
    response_mode: ResponseMode = "normal"
    brief_depth: Optional[BriefDepth] = None
    brief_type: BriefType = "general"


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
