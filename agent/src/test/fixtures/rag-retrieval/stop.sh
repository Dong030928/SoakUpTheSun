#!/usr/bin/env bash
set -euo pipefail
fixture_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
timeout --foreground 90s docker compose --project-name suts-agent-rag-retrieval-it \
  -f "$fixture_dir/compose.yaml" down --volumes --remove-orphans --timeout 20
