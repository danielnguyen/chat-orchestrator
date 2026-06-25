#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BMS="$ROOT/../basic-memory-store"
CR="$ROOT/../cognitive-runtime"
COMPOSE="$ROOT/docker-compose.composed-smoke.yml"
BMS_COMMIT="59b910f8024eac252eb1e99d65e4b1458996670b"
CR_COMMIT="1404a77f9c9d1a13df3246f1e98401d9680d653e"

for command in git docker curl jq; do
  command -v "$command" >/dev/null || {
    echo "composed-smoke prerequisite missing: $command" >&2
    exit 2
  }
done

for repository in "$BMS" "$CR"; do
  test -d "$repository/.git" || {
    echo "composed-smoke prerequisite missing: sibling repository $repository" >&2
    exit 2
  }
done

git -C "$BMS" merge-base --is-ancestor "$BMS_COMMIT" main || {
  echo "basic-memory-store/main does not contain required merge $BMS_COMMIT" >&2
  exit 2
}
git -C "$CR" merge-base --is-ancestor "$CR_COMMIT" main || {
  echo "cognitive-runtime/main does not contain required merge $CR_COMMIT" >&2
  exit 2
}

cleanup() {
  docker compose -f "$COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker compose -f "$COMPOSE" up -d --build --wait

response="$(
  curl -fsS -X POST http://127.0.0.1:14361/v1/chat \
    -H "X-API-Key: smoke-orchestrator-key" \
    -H "Content-Type: application/json" \
    -d '{"owner_id":"owner-smoke","client_id":"client-smoke","surface":"chat","messages":[{"role":"user","content":"neutral smoke request"}]}'
)"
request_id="$(jq -r '.request_id // empty' <<<"$response")"
status="$(jq -r '.status // empty' <<<"$response")"
test -n "$request_id"
test "$status" = "ok" -o "$status" = "degraded"

trace="$(
  curl -fsS "http://127.0.0.1:14321/v1/traces/$request_id" \
    -H "X-API-Key: smoke-memory-key"
)"
jq -e --arg request_id "$request_id" '
  .request_id == $request_id
  and .status == "ok"
  and (.retrieval.bundle | type == "object")
  and (.prompt.ordered_layer_names | length > 0)
  and .prompt.token_accounting.budget_enforcement == "not_enforced"
  and (.model_calls | length == 1)
  and .model_calls[0].status == "ok"
  and (.artifacts.artifact_count | type == "number")
  and (.references | type == "array")
' <<<"$trace" >/dev/null

runtime_session_id="$(
  jq -r '.retrieval.prompt_assembly.runtime_session.runtime_session_id // empty' <<<"$trace"
)"
test -n "$runtime_session_id"
runtime_diagnostics="$(
  curl -fsS "http://127.0.0.1:14371/v1/runtime/sessions/$runtime_session_id"
)"
jq -e --arg request_id "$request_id" '
  .latest_turn.turn_status == "completed"
  and ([.events[]
    | select(.event_type == "turn_started"
      or .event_type == "turn_updated"
      or .event_type == "turn_completed")
    | .event_payload_json.request_id] | length >= 3)
  and ([.events[]
    | select(.event_type == "turn_started"
      or .event_type == "turn_updated"
      or .event_type == "turn_completed")
    | .event_payload_json.request_id] | all(. == $request_id))
' <<<"$runtime_diagnostics" >/dev/null

provider_calls="$(curl -fsS "http://127.0.0.1:14381/calls/$request_id")"
jq -e --arg request_id "$request_id" '
  (.calls | map(select(.kind == "chat")) | length) == 1
  and (.calls | map(select(.kind == "chat")) | all(.request_id == $request_id))
' <<<"$provider_calls" >/dev/null

echo "Composed smoke passed: request_id=$request_id status=$status"
echo "Topology: CO branch -> deterministic provider stub; BMS main -> PostgreSQL 16 + Qdrant; CR main -> disposable SQLite."
