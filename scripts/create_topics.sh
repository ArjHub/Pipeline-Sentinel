#!/usr/bin/env bash
# Idempotently create the four event topics on Redpanda (RPK_BROKERS is set by compose).
set -euo pipefail

for t in job_logs job_metrics dq_results deploy_events; do
  if rpk topic describe "$t" >/dev/null 2>&1; then
    echo "topic exists: $t"
  else
    rpk topic create "$t" --partitions 3 --replicas 1 --topic-config retention.ms=604800000
  fi
done
rpk topic list
