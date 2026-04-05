SHELL := /usr/bin/env bash

DEV_COMPOSE := docker-compose.yml

.PHONY: dev-up dev-down dev-reset dev-logs dev-test dev-install dev-lint smoke dev-start dev-start-reload

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
	@cd api && ./.venv/bin/python -m ruff check .

smoke:
	@set -euo pipefail; \
	ORCH_BASE="$${ORCH_BASE:-http://127.0.0.1:4361}"; \
	ORCH_KEY="$${ORCH_API_KEY:-dev-key}"; \
	MS_BASE="$${MEMORY_STORE_BASE_URL:-http://127.0.0.1:4321}"; \
	MS_KEY="$${MEMORY_STORE_API_KEY:-dev-key}"; \
	echo "==> POST $$ORCH_BASE/v1/chat"; \
	RESP="$$(curl -sS -X POST "$$ORCH_BASE/v1/chat" \
	  -H "X-API-Key: $$ORCH_KEY" \
	  -H "Content-Type: application/json" \
	  -d '{"owner_id":"daniel","client_id":"vscode","surface":"vscode","messages":[{"role":"user","content":"smoke check"}]}')"; \
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
	curl -sS "$$MS_BASE/v1/traces/$$RID" -H "X-API-Key: $$MS_KEY" | jq -e '.request_id == "'$$RID'"' >/dev/null; \
	echo "Smoke passed."

# Non-docker local defaults:
# - memory-store expected on http://127.0.0.1:4321
# - orchestrator served on APP_PORT (default 4361)
dev-start:
	@cd api && ./.venv/bin/uvicorn main:app --host 0.0.0.0 --port "$${APP_PORT:-4361}"

dev-start-reload:
	@cd api && ./.venv/bin/uvicorn main:app --host 0.0.0.0 --port "$${APP_PORT:-4361}" --reload
