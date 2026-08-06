#!/usr/bin/env bash
# Controlled rollback for a forward-only SQLite migration.
#
# This script is deliberately not invoked by Docker Compose.  It requires a
# named, integrity-checked pre-Rust snapshot and an explicit confirmation,
# then performs only the rollback sequence documented in TASKs.md.
set -euo pipefail

if [[ "${1:-}" != "--confirm" || "${2:-}" == "" ]]; then
  echo "usage: $0 --confirm data/app.db.bak-before-rust-YYYYMMDD-HHMMSS" >&2
  exit 64
fi

snapshot=$2
image=${ROLLBACK_IMAGE:-proxy-fleet-python:pre-rust-20260805}
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
data_dir="$project_dir/data"
database="$data_dir/app.db"

[[ -f "$snapshot" ]] || { echo "snapshot not found: $snapshot" >&2; exit 66; }
docker image inspect "$image" >/dev/null

echo "Stopping Rust service and restoring $snapshot"
(cd "$project_dir" && docker compose down)
rm -f "$database-wal" "$database-shm"
cp -- "$snapshot" "$database"

docker run -d --name config-orchestrator --network host \
  --ulimit nofile=65535:65535 \
  -e HOME=/home/app \
  -v "$data_dir:/app/data" \
  -v "$project_dir/config/config.yml:/app/config/config.yml:ro" \
  -v /home/h-mousavi/.ssh/id_ed25519:/run/secrets/github_ssh_key:ro \
  -v /home/h-mousavi/.ssh/known_hosts:/home/app/.ssh/known_hosts:ro \
  "$image"

for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null; then
    echo "Rollback health smoke passed."
    exit 0
  fi
  sleep 1
done

docker logs --tail 100 config-orchestrator >&2 || true
echo "Rollback container failed health smoke" >&2
exit 1
