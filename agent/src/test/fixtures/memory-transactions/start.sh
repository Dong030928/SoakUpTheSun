#!/usr/bin/env bash
set -euo pipefail
fixture_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
compose=(docker compose -f "$fixture_dir/compose.yaml")
"${compose[@]}" up -d --wait mysql redis nameserver broker
"${compose[@]}" exec -T redis redis-cli --no-auth-warning -a agent-transactions-integration-only \
  SETNX suts:agent:memory:transactions:generation agent-transactions-integration-only
for attempt in {1..30}; do
  result="$("${compose[@]}" exec -T broker sh mqadmin updateTopic -b 127.0.0.1:10931 \
    -t SutsMemoryArchiveIT -r 2 -w 2 -a '+message.type=TRANSACTION' 2>&1)"
  if [[ "$result" == *"create topic to 127.0.0.1:10931 success."* ]]; then
    printf '%s\n' 'Isolated MySQL/Redis/RocketMQ transaction fixture is ready.'
    exit 0
  fi
  sleep 1
done
printf '%s\n' "$result" >&2
exit 1
