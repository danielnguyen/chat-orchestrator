from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    orch_api_key: str = Field(..., alias="ORCH_API_KEY")

    memory_store_base_url: str = Field(..., alias="MEMORY_STORE_BASE_URL")
    memory_store_api_key: str = Field(..., alias="MEMORY_STORE_API_KEY")

    cognitive_runtime_base_url: str | None = Field(default=None, alias="COGNITIVE_RUNTIME_BASE_URL")
    cognitive_runtime_api_key: str | None = Field(default=None, alias="COGNITIVE_RUNTIME_API_KEY")
    cognitive_runtime_timeout_ms: int = Field(
        default=1500,
        alias="COGNITIVE_RUNTIME_TIMEOUT_MS",
        ge=100,
        le=30000,
    )
    enable_runtime_overlays: bool = Field(default=False, alias="ENABLE_RUNTIME_OVERLAYS")
    cognitive_runtime_companion_enabled: bool = Field(
        default=False,
        alias="COGNITIVE_RUNTIME_COMPANION_ENABLED",
    )
    cognitive_runtime_interaction_governance_enabled: bool = Field(
        default=False,
        alias="COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED",
    )
    cognitive_runtime_persona_containment_enabled: bool = Field(
        default=False,
        alias="COGNITIVE_RUNTIME_PERSONA_CONTAINMENT_ENABLED",
    )
    cognitive_runtime_restraint_enabled: bool = Field(
        default=False,
        alias="COGNITIVE_RUNTIME_RESTRAINT_ENABLED",
    )
    cognitive_runtime_memory_hygiene_enabled: bool = Field(
        default=False,
        alias="COGNITIVE_RUNTIME_MEMORY_HYGIENE_ENABLED",
    )
    cognitive_runtime_privacy_context_enabled: bool = Field(
        default=False,
        alias="COGNITIVE_RUNTIME_PRIVACY_CONTEXT_ENABLED",
    )
    cognitive_runtime_capability_registry_enabled: bool = Field(
        default=False,
        alias="COGNITIVE_RUNTIME_CAPABILITY_REGISTRY_ENABLED",
    )
    claim_record_capture_enabled: bool = Field(
        default=False,
        alias="CLAIM_RECORD_CAPTURE_ENABLED",
    )
    evidence_acquisition_enabled: bool = Field(
        default=False,
        alias="EVIDENCE_ACQUISITION_ENABLED",
    )
    dsa_base_url: str = Field(default="http://localhost:5174", alias="DSA_BASE_URL")
    dsa_timeout_ms: int = Field(default=5000, alias="DSA_TIMEOUT_MS", ge=100, le=30000)
    dsa_enabled: bool = Field(default=False, alias="DSA_ENABLED")
    dsa_api_key: str | None = Field(default=None, alias="DSA_API_KEY")

    litellm_base_url: str = Field(..., alias="LITELLM_BASE_URL")
    litellm_api_key: str | None = Field(default=None, alias="LITELLM_API_KEY")

    router_rules_path: str = Field(default="/app/api/router/rules.yaml", alias="ROUTER_RULES_PATH")
    model_registry_path: str = Field(
        default="/app/api/router/model_registry.yaml",
        alias="MODEL_REGISTRY_PATH",
    )

    allow_manual_override: bool = Field(default=True, alias="ALLOW_MANUAL_OVERRIDE")
    default_profile_name: str = Field(default="dev", alias="DEFAULT_PROFILE_NAME")
    response_action_mode: Literal["shadow", "template_fallback"] = Field(
        default="shadow",
        alias="RESPONSE_ACTION_MODE",
    )
    prompt_output_token_reserve: int = Field(
        default=2048,
        alias="PROMPT_OUTPUT_TOKEN_RESERVE",
        ge=0,
    )
    prompt_context_safety_margin: int = Field(
        default=256,
        alias="PROMPT_CONTEXT_SAFETY_MARGIN",
        ge=0,
    )

    offline_provider: str = Field(default="litellm-local", alias="OFFLINE_PROVIDER")
    ollama_base_url: str | None = Field(default=None, alias="OLLAMA_BASE_URL")
    request_timeout_ms: int = Field(default=30000, alias="REQUEST_TIMEOUT_MS")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @model_validator(mode="after")
    def validate_claim_capture_runtime(self) -> Settings:
        if self.claim_record_capture_enabled and not self.cognitive_runtime_base_url:
            raise ValueError("claim record capture requires Cognitive Runtime")
        if self.evidence_acquisition_enabled:
            if not self.cognitive_runtime_base_url:
                raise ValueError("evidence acquisition requires Cognitive Runtime")
            if not self.cognitive_runtime_interaction_governance_enabled:
                raise ValueError("evidence acquisition requires interaction governance")
            if not self.dsa_enabled:
                raise ValueError("evidence acquisition requires Data Source Aggregator")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
