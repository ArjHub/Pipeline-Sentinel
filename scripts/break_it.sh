#!/usr/bin/env bash
# Inject one of the Phase 1 faults for one business date on the live stack, then wait
# for the affected DAG runs to finish (red). Every command it runs is echoed (+ ...).
#
#   bash scripts/break_it.sh missing_amount  2026-10-16
#   bash scripts/break_it.sh volume_drop     2026-10-18
#   bash scripts/break_it.sh stale_customers 2026-10-22
#
# Then: make detect && make query-incidents. To fix: make run-all DATE=<date> (it
# regenerates the raw files and re-runs every DAG for that date), then make detect.
set -euo pipefail

FAULT=${1:?usage: break_it.sh missing_amount|volume_drop|stale_customers YYYY-MM-DD}
D=${2:?usage: break_it.sh FAULT YYYY-MM-DD}
RAW=data/lake/raw
AF=(docker compose exec -T airflow-scheduler airflow)

run() { echo "+ $*" >&2; "$@"; }
quiet() { echo "+ $*" >&2; "$@" >/dev/null 2>&1; } # Airflow's CLI prints warnings

wait_dag() { # wait until the DAG run for $D is no longer queued/running
  local dag=$1 state
  for _ in $(seq 1 120); do
    state=$("${AF[@]}" dags state "$dag" "${D}T00:00:00+00:00" 2>/dev/null | tail -1)
    case "$state" in success|failed) echo "  $dag ds=$D: $state"; return 0 ;; esac
    sleep 5
  done
  echo "  $dag ds=$D: still '$state' after 10 min" >&2
  return 1
}

rerun_orders_and_revenue() {
  # Re-run ONLY ingest_orders and what follows it. Clearing generate_orders would
  # regenerate a correct file and undo the fault.
  quiet "${AF[@]}" tasks clear orders_ingest -t '^ingest_orders$' -s "$D" -e "$D" -d -y
  # Re-run the downstream aggregate too, so its sensors see the new upstream state.
  quiet "${AF[@]}" tasks clear daily_revenue_agg -s "$D" -e "$D" -y
  wait_dag orders_ingest
  wait_dag daily_revenue_agg
}

case "$FAULT" in
  missing_amount | volume_drop)
    run make --no-print-directory run-all DATE="$D" >/dev/null # start from a green run
    F="$RAW/orders/ds=$D/part-0000.csv"
    if [[ $FAULT == missing_amount ]]; then
      run cut -d, -f1-3,5- "$F" >"$F.tmp" # column 4 is amount
    else
      run awk 'NR==1 || NR%3==0' "$F" >"$F.tmp" # keep 1 row in 3, spread over the day
    fi
    run mv "$F.tmp" "$F"
    echo "  $(head -1 "$F"); $(($(wc -l <"$F") - 1)) rows"
    rerun_orders_and_revenue
    ;;

  stale_customers)
    # The customers dim only ever moves forward (MERGE updates a row only if the new
    # updated_at is newer), so the date must be after every customers snapshot loaded
    # so far, and generate_customers must not run for it (it would write a fresh one).
    # Two days later, so the lag (~48 h) is clearly past the 24 h SLA.
    latest=$(ls "$RAW/customers" | sed 's/ds=//' | sort | tail -1)
    earliest=$(python3 -c "import datetime as d; print(d.date.fromisoformat('$latest') + d.timedelta(2))")
    if [[ "$D" < "$earliest" ]]; then
      echo "customers are loaded up to ds=$latest; pick DATE=$earliest or later" >&2
      exit 2
    fi
    echo "  newest customers snapshot: ds=$latest (it will be served as ds=$D)"
    quiet "${AF[@]}" dags pause customers_dim
    quiet "${AF[@]}" dags trigger customers_dim -r "run_all__$D" -e "${D}T00:00:00+00:00"
    # Mark the export step done without running it, then put the old snapshot in place.
    quiet "${AF[@]}" tasks run -m customers_dim generate_customers "run_all__$D"
    run mkdir -p "$RAW/customers/ds=$D"
    run cp "$RAW/customers/ds=$latest/part-0000.csv" "$RAW/customers/ds=$D/part-0000.csv"
    quiet "${AF[@]}" dags unpause customers_dim
    for dag in orders_ingest payments_etl daily_revenue_agg; do
      quiet "${AF[@]}" dags unpause "$dag"
      quiet "${AF[@]}" dags trigger "$dag" -r "run_all__$D" -e "${D}T00:00:00+00:00"
    done
    for dag in customers_dim orders_ingest payments_etl daily_revenue_agg; do wait_dag "$dag"; done
    ;;

  *)
    echo "unknown fault: $FAULT (missing_amount | volume_drop | stale_customers)" >&2
    exit 2
    ;;
esac

echo
echo "Fault '$FAULT' injected for ds=$D. Next: make detect && make query-incidents"
echo "Fix: make run-all DATE=$D && make detect"
