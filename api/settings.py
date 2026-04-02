from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    orch_api_key: str = Field(..., alias="ORCH_API_KEY")

    memory_store_base_url: str = Field(..., alias="MEMORY_STORE_BASE_URL")
    memory_store_api_key: str = Field(..., alias="MEMORY_STORE_API_KEY")

    litellm_base_url: str = Field(..., alias="LITELLM_BASE_URL")
    litellm_api_key: str | None = Field(default=None, alias="LITELLM_API_KEY")

    router_rules_path: str = Field(default="/app/api/router/rules.yaml", alias="ROUTER_RULES_PATH")
    model_registry_path: str = Field(
        default="/app/api/router/model_registry.yaml",
        alias="MODEL_REGISTRY_PATH",
    )

    allow_manual_override: bool = Field(default=True, alias="ALLOW_MANUAL_OVERRIDE")
    default_profile_name: str = Field(default="dev", alias="DEFAULT_PROFILE_NAME")

    offline_provider: str = Field(default="litellm-local", alias="OFFLINE_PROVIDER")
    ollama_base_url: str | None = Field(default=None, alias="OLLAMA_BASE_URL")
    request_timeout_ms: int = Field(default=30000, alias="REQUEST_TIMEOUT_MS")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
