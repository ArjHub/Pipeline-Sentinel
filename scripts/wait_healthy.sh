#!/usr/bin/env bash
# Wait until every long-running service is healthy (or running, if it has no
# healthcheck) and every one-off init container exited 0. Replaces `compose up --wait`,
# which treats a successfully-exited init container as a failure.
set -euo pipefail
cd "$(dirname "$0")/.."

ONE_OFF="airflow-init redpanda-init"
timeout=${1:-300}
deadline=$((SECONDS + timeout))

while true; do
  pending=()
  for svc in $(docker compose config --services); do
    cid=$(docker compose ps -a -q "$svc" 2>/dev/null | head -1)
    [[ -z "$cid" ]] && continue  # profile-only services (e.g. tools) aren't started
    if [[ " $ONE_OFF " == *" $svc "* ]]; then
      read -r status code < <(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$cid")
      if [[ "$status" == "exited" && "$code" != "0" ]]; then
        echo "FAILED: $svc exited with code $code"; docker compose logs --tail=40 "$svc"; exit 1
      fi
      [[ "$status" == "exited" ]] || pending+=("$svc")
    else
      health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid")
      [[ "$health" == "healthy" || "$health" == "running" ]] || pending+=("$svc($health)")
    fi
  done
  if [[ ${#pending[@]} -eq 0 ]]; then
    echo "all services ready"; exit 0
  fi
  if (( SECONDS > deadline )); then
    echo "timed out after ${timeout}s waiting for: ${pending[*]}"; exit 1
  fi
  sleep 3
done
