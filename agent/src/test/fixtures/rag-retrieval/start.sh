#!/usr/bin/env bash
set -euo pipefail
fixture_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project=suts-agent-rag-retrieval-it
marker=agent-rag-integration-only
redis_marker=suts:agent:test:rag-retrieval-fixture
compose=(docker compose --project-name "$project" -f "$fixture_dir/compose.yaml")
for required in docker curl timeout; do
  command -v "$required" >/dev/null || { printf 'Required command is unavailable: %s\n' "$required" >&2; exit 1; }
done
startup_seconds="${RAG_FIXTURE_START_TIMEOUT_SECONDS:-600}"
[[ "$startup_seconds" =~ ^[0-9]+$ ]] && (( startup_seconds >= 30 && startup_seconds <= 1800 )) \
  || { printf '%s\n' 'RAG_FIXTURE_START_TIMEOUT_SECONDS must be between 30 and 1800.' >&2; exit 1; }
deadline=$((SECONDS + startup_seconds))
remaining() {
  local left=$((deadline - SECONDS))
  (( left > 0 )) || { printf '%s\n' 'RAG fixture startup deadline exceeded.' >&2; exit 1; }
  printf '%s' "$left"
}
timeout --foreground "$(remaining)s" docker info >/dev/null
for port in 13326 16393 19200; do
  if timeout --foreground 2s bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$port" 2>/dev/null; then
    printf 'Refusing occupied fixture port 127.0.0.1:%s; release it explicitly before starting this fixture.\n' "$port" >&2
    exit 1
  fi
done
# One deadline covers pulls, creation and health checks. Never change the host's sysctl settings.
timeout --foreground "$(remaining)s" "${compose[@]}" up -d --wait --wait-timeout "$(remaining)" mysql redis elasticsearch
mysql_marker="$(timeout --foreground "$(remaining)s" "${compose[@]}" exec -T -e MYSQL_PWD="$marker" mysql \
  mysql -h127.0.0.1 -uroot -N -B agent_rag_it -e 'SELECT marker FROM suts_agent_test_fixture')"
[[ "$mysql_marker" == "$marker" ]] || { printf '%s\n' 'MySQL RAG fixture marker mismatch.' >&2; exit 1; }
timeout --foreground "$(remaining)s" "${compose[@]}" exec -T redis redis-cli --no-auth-warning -a "$marker" \
  SET "$redis_marker" "$marker" NX >/dev/null
actual_redis_marker="$(timeout --foreground "$(remaining)s" "${compose[@]}" exec -T redis redis-cli --no-auth-warning -a "$marker" GET "$redis_marker")"
[[ "$actual_redis_marker" == "$marker" ]] || { printf '%s\n' 'Redis RAG fixture marker mismatch.' >&2; exit 1; }
curl --connect-timeout 2 --max-time "$(remaining)" -fsS -X PUT \
  'http://127.0.0.1:19200/suts-rag-fixture-marker/_doc/fixture?refresh=wait_for' \
  -H 'Content-Type: application/json' -d '{"marker":"agent-rag-integration-only"}' >/dev/null
printf '%s\n' 'Isolated RAG fixture ready: MySQL 13326, Redis HNSW 16393, Elasticsearch 19200.'
