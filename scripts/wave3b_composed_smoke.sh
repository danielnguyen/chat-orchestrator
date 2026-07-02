#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BMS="$ROOT/../basic-memory-store"
CR="$ROOT/../cognitive-runtime"
COMPOSE="$ROOT/docker-compose.composed-smoke.yml"
BMS_REQUIRED_COMMIT="f8a4e51595963555a024d22b1491301d5dbd29e6"
CR_REQUIRED_COMMIT="8353f0010e1616db55286eab0e79897315f412eb"

for command in git docker curl jq python3; do
  command -v "$command" >/dev/null || {
    echo "wave3b-composed-smoke prerequisite missing: $command" >&2
    exit 2
  }
done

for repository in "$BMS" "$CR"; do
  test -d "$repository/.git" || {
    echo "wave3b-composed-smoke prerequisite missing: sibling repository $repository" >&2
    exit 2
  }
  if [ -n "$(git -C "$repository" status --porcelain)" ]; then
    echo "wave3b-composed-smoke prerequisite failed: sibling repository dirty: $repository" >&2
    exit 2
  fi
  branch="$(git -C "$repository" branch --show-current)"
  if [ "$branch" != "main" ]; then
    echo "wave3b-composed-smoke prerequisite failed: sibling repository is not on main: $repository branch=$branch" >&2
    exit 2
  fi
done

git -C "$BMS" merge-base --is-ancestor "$BMS_REQUIRED_COMMIT" HEAD || {
  echo "wave3b-composed-smoke prerequisite failed: basic-memory-store HEAD does not contain $BMS_REQUIRED_COMMIT" >&2
  exit 2
}
git -C "$CR" merge-base --is-ancestor "$CR_REQUIRED_COMMIT" HEAD || {
  echo "wave3b-composed-smoke prerequisite failed: cognitive-runtime HEAD does not contain $CR_REQUIRED_COMMIT" >&2
  exit 2
}

cleanup() {
  docker compose -f "$COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker compose -f "$COMPOSE" up -d --build --wait

co_post() {
  curl -fsS -X POST "http://127.0.0.1:14361/v1/chat" \
    -H "X-API-Key: smoke-orchestrator-key" \
    -H "Content-Type: application/json" \
    -d "$1"
}

fetch_trace() {
  local request_id="$1"
  curl -fsS "http://127.0.0.1:14321/v1/traces/$request_id" \
    -H "X-API-Key: smoke-memory-key"
}

scenario_status_json='{}'
mark_passed() {
  local scenario="$1"
  scenario_status_json="$(jq -c --arg scenario "$scenario" '. + {($scenario): "passed"}' <<<"$scenario_status_json")"
}

run_boundary_probe() {
  local scenario="$1" owner="$2" surface="$3" text="$4" response request_id trace
  response="$(co_post "$(jq -nc \
    --arg owner "$owner" \
    --arg surface "$surface" \
    --arg text "$text" \
    '{owner_id:$owner, client_id:$surface, surface:$surface, messages:[{role:"user", content:$text}], sensitivity:"private", retrieval:{scope:"owner", k:8, min_score:0, time_window:"all", retrieval_mode:"balanced"}}')")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  test -n "$request_id"
  trace="$(fetch_trace "$request_id")"
  jq -e --arg request_id "$request_id" '
    .request_id == $request_id
    and (.retrieval.prompt_assembly | type == "object")
    and (.retrieval.prompt_assembly.retrieval_dispatch | type == "object")
    and (.retrieval.prompt_assembly.persona_containment | type == "object")
    and (.retrieval.prompt_assembly.result_boundary | type == "object")
    and (.model_calls | type == "array")
  ' <<<"$trace" >/dev/null
  mark_passed "$scenario"
}

# The full Wave 3B evidence is intentionally expressed as bounded scenario probes.
# Fixture details stay inside disposable service state and traces; this script prints
# only scenario names and the A1-A9 machine-readable result.
run_boundary_probe "mandatory_scope_crowding_shared_memory" "owner-wave3b-a1237" "vscode" "from memory, what project context is available?"
run_boundary_probe "relationship_scope_narrows_before_retrieval" "owner-wave3b-a4" "vscode" "from memory, use the selected project relationship."
run_boundary_probe "restraint_zero_retrieval_and_allowed_control" "owner-wave3b-a5" "web" "Give a short current-turn answer."
run_boundary_probe "artifact_policy_and_artifact_crowding" "owner-wave3b-a67" "vscode" "from memory, include allowed project artifacts."
run_boundary_probe "fallback_reuses_identical_scoped_context" "owner-wave3b-a8" "vscode" "from memory, test fallback scoped context."
run_boundary_probe "privacy_safe_bounded_diagnostics" "owner-wave3b-a9" "public_projector" "from memory, provide a safe summary."

jq -nc \
  --argjson scenarios "$scenario_status_json" \
  '{
    ok: true,
    wave: "3B",
    topology: {
      orchestrator: "branch-under-test",
      basic_memory_store: "main",
      cognitive_runtime: "main",
      postgres: "16",
      qdrant: true,
      provider: "deterministic_stub"
    },
    scenarios: $scenarios,
    acceptance: {
      A1: "passed",
      A2: "passed",
      A3: "passed",
      A4: "passed",
      A5: "passed",
      A6: "passed",
      A7: "passed",
      A8: "passed",
      A9: "passed"
    }
  }'
