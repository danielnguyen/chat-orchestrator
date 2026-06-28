SHELL := /usr/bin/env bash

DEV_COMPOSE := docker-compose.yml

.PHONY: dev-up dev-down dev-reset dev-logs dev-test dev-install dev-lint replay-test prompt-budget-test prompt-budget-smoke composed-smoke smoke dev-start dev-start-reload

dev-up:
	@docker compose -f $(DEV_COMPOSE) up -d

dev-down:
	@docker compose -f $(DEV_COMPOSE) down

# Full reset: wipes containers (and any anonymous volumes), then boots clean.
dev-reset:
	@docker compose -f $(DEV_COMPOSE) down -v --remove-orphans
	@docker compose -f $(DEV_COMPOSE) up -d

dev-logs:
	@docker compose -f $(DEV_COMPOSE) logs -f --tail=200

dev-test:
	@cd api && ./.venv/bin/python -m pytest -q

dev-install:
	@cd api && ./.venv/bin/python -m pip install -r requirements.txt

dev-lint:
	@cd api && RUFF_CACHE_DIR="$${RUFF_CACHE_DIR:-/tmp/chat-orchestrator-ruff-cache}" ./.venv/bin/python -m ruff check .

replay-test:
	@cd api && ./.venv/bin/python -m pytest -q tests/test_orchestration_replay.py

prompt-budget-test:
	@cd api && ./.venv/bin/python -m pytest -q tests/test_prompt_budget.py tests/test_prompt_budget_smoke.py tests/test_orchestrate_flow.py tests/test_offline_fallback.py tests/test_orchestration_replay.py

prompt-budget-smoke:
	@cd api && ./.venv/bin/python -m pytest -q tests/test_prompt_budget_smoke.py

composed-smoke:
	@./scripts/composed_smoke.sh

smoke:
	@set -euo pipefail; \
	ORCH_BASE="$${ORCH_BASE:-http://127.0.0.1:4361}"; \
	ORCH_KEY="$${ORCH_API_KEY:-dev-key}"; \
	MS_BASE="$${MEMORY_STORE_BASE_URL:-http://127.0.0.1:4321}"; \
	MS_KEY="$${MEMORY_STORE_API_KEY:-dev-local}"; \
	CHAT_PAYLOAD_JSON="$${CHAT_PAYLOAD_JSON:-{\"owner_id\":\"daniel\",\"client_id\":\"vscode\",\"surface\":\"vscode\",\"messages\":[{\"role\":\"user\",\"content\":\"smoke check\"}]}}"; \
	EXPECT_GOVERNANCE_STATUS="$${EXPECT_GOVERNANCE_STATUS:-}"; \
	EXPECT_GOVERNANCE_POSTURE="$${EXPECT_GOVERNANCE_POSTURE:-}"; \
	EXPECT_PERSONA_STATUS="$${EXPECT_PERSONA_STATUS:-}"; \
	EXPECT_PERSONA_DOMAIN="$${EXPECT_PERSONA_DOMAIN:-}"; \
	EXPECT_PERSONA_RETRIEVAL_SCOPE_REASON="$${EXPECT_PERSONA_RETRIEVAL_SCOPE_REASON:-}"; \
	EXPECT_RESTRAINT_STATUS="$${EXPECT_RESTRAINT_STATUS:-}"; \
	EXPECT_RESTRAINT_POLICY="$${EXPECT_RESTRAINT_POLICY:-}"; \
	echo "==> POST $$ORCH_BASE/v1/chat"; \
	RESP="$$(curl -sS -X POST "$$ORCH_BASE/v1/chat" \
	  -H "X-API-Key: $$ORCH_KEY" \
	  -H "Content-Type: application/json" \
	  -d "$$CHAT_PAYLOAD_JSON")"; \
	echo "$$RESP" | jq . >/dev/null; \
	RID="$$(echo "$$RESP" | jq -r '.request_id // empty')"; \
	test -n "$$RID"; \
	STATUS="$$(echo "$$RESP" | jq -r '.status // empty')"; \
	echo "request_id=$$RID status=$$STATUS"; \
	if [ "$$STATUS" = "failed" ]; then \
	  echo "Smoke returned valid failure JSON with request_id."; \
	  exit 0; \
	fi; \
	echo "==> GET $$MS_BASE/v1/traces/$$RID"; \
	TRACE="$$(curl -sS "$$MS_BASE/v1/traces/$$RID" -H "X-API-Key: $$MS_KEY")"; \
	echo "$$TRACE" | jq -e '.request_id == "'$$RID'"' >/dev/null; \
	if [ -n "$$EXPECT_GOVERNANCE_STATUS" ]; then \
	  echo "==> CHECK governance trace summary"; \
	  echo "$$TRACE" | jq -e '.payload.retrieval.prompt_assembly.interaction_governance.status == "'$$EXPECT_GOVERNANCE_STATUS'"' >/dev/null; \
	fi; \
	if [ -n "$$EXPECT_GOVERNANCE_POSTURE" ]; then \
	  echo "==> CHECK governance posture"; \
	  echo "$$TRACE" | jq -e '.payload.retrieval.prompt_assembly.interaction_governance.response_posture == "'$$EXPECT_GOVERNANCE_POSTURE'"' >/dev/null; \
	fi; \
	if [ -n "$$EXPECT_PERSONA_STATUS" ]; then \
	  echo "==> CHECK persona containment trace summary"; \
	  echo "$$TRACE" | jq -e '.payload.retrieval.prompt_assembly.persona_containment.status == "'$$EXPECT_PERSONA_STATUS'"' >/dev/null; \
	fi; \
	if [ -n "$$EXPECT_PERSONA_DOMAIN" ]; then \
	  echo "==> CHECK persona containment capability domain"; \
	  echo "$$TRACE" | jq -e '.payload.retrieval.prompt_assembly.persona_containment.capability_domain == "'$$EXPECT_PERSONA_DOMAIN'"' >/dev/null; \
	fi; \
	if [ -n "$$EXPECT_PERSONA_RETRIEVAL_SCOPE_REASON" ]; then \
	  echo "==> CHECK persona containment retrieval scope note"; \
	  echo "$$TRACE" | jq -e '.payload.retrieval.prompt_assembly.persona_containment.retrieval_scope_reason == "'$$EXPECT_PERSONA_RETRIEVAL_SCOPE_REASON'"' >/dev/null; \
	fi; \
	if [ -n "$$EXPECT_RESTRAINT_STATUS" ]; then \
	  echo "==> CHECK restraint trace summary"; \
	  echo "$$TRACE" | jq -e '.payload.retrieval.prompt_assembly.restraint.status == "'$$EXPECT_RESTRAINT_STATUS'"' >/dev/null; \
	fi; \
	if [ -n "$$EXPECT_RESTRAINT_POLICY" ]; then \
	  echo "==> CHECK restraint policy"; \
	  echo "$$TRACE" | jq -e '.payload.retrieval.prompt_assembly.restraint.restraint_policy == "'$$EXPECT_RESTRAINT_POLICY'"' >/dev/null; \
	fi; \
	echo "Smoke passed."

# Non-docker local defaults:
# - memory-store expected on http://127.0.0.1:4321
# - orchestrator served on APP_PORT (default 4361)
dev-start:
	@cd api && ./.venv/bin/uvicorn main:app --host 0.0.0.0 --port "$${APP_PORT:-4361}"

dev-start-reload:
	@cd api && ./.venv/bin/uvicorn main:app --host 0.0.0.0 --port "$${APP_PORT:-4361}" --reload
