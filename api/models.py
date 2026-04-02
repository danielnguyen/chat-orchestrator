from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

Role = Literal["user", "assistant", "system", "tool"]


class MessageIn(BaseModel):
    role: Role
    content: str


class RetrievalOptions(BaseModel):
    k: int = Field(default=8, ge=1, le=50)
    min_score: float = Field(default=0.25, ge=0.0, le=1.0)
    scope: Literal["conversation", "client", "owner"] = "conversation"


class ChatRequest(BaseModel):
    owner_id: str
    client_id: Optional[str] = None
    conversation_id: Optional[str] = None
    surface: str = "unknown"
    messages: List[MessageIn]
    requested_profile: Optional[str] = None
    model_override: Optional[str] = None
    sensitivity: Literal["public", "private", "local_only"] = "private"
    retrieval: Optional[RetrievalOptions] = None


class ChatResponse(BaseModel):
    request_id: str
    conversation_id: str
    profile_name: str
    selected_model: str
    answer: str
    status: Literal["ok", "degraded", "failed"]
    sources: List[Dict[str, Any]] = Field(default_factory=list)


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
