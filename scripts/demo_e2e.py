"""End-to-end demo on the live stack (make demo-e2e DEMO_DATE=YYYY-MM-DD):

  1. make break-it FAULT=missing_amount      -> ingest_orders fails on the live stack
  2. sentinel-detector                        -> new primary incident (within ~60 s)
  3. diagnosis (Claude if ANTHROPIC_API_KEY is set, else the labelled rule-based MOCK)
  4. mock Slack message with Retry / Dismiss
  5. operator fixes the data (regenerates the orders file)
  6. click Retry          -> refused: the diagnosis says retry_safe=false
  7. click "retry anyway" -> accepted: Airflow clears the failed tasks, audit record
  8. the re-run succeeds  -> detector marks the incident resolved

Host-side, stdlib only. Clicks go to http://localhost:8000/mock-slack/click, which signs
them and runs them through the real Slack handler.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sentinel.airflow_client import AirflowClient

API = "http://localhost:8000"
EXEC = ["docker", "compose", "exec", "-T"]


def step(n: int, text: str) -> None:
    print(f"\n=== {n}. {text}", flush=True)


def lake() -> dict:
    out = subprocess.run(
        [*EXEC, "sentinel-detector", "/opt/venv/jobs/bin/python", "scripts/lake_json.py"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)


def wait(what: str, fn, timeout: float, every: float = 5):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        got = fn()
        if got:
            print(f"   {what}: after {time.monotonic() - t0:.0f}s")
            return got
        time.sleep(every)
    raise SystemExit(f"TIMEOUT waiting for {what} ({timeout:.0f}s)")


def click(message_id: str, action: str, value: str) -> dict:
    body = urllib.parse.urlencode(
        {"message_id": message_id, "action_id": action, "value": value}
    ).encode()
    req = urllib.request.Request(
        f"{API}/mock-slack/click", body, {"Accept": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def messages() -> list[dict]:
    with urllib.request.urlopen(f"{API}/mock-slack/messages", timeout=10) as r:
        return json.loads(r.read())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-05")
    args = ap.parse_args()
    d = args.date
    have_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
    before = {i["incident_id"] for i in lake()["incidents"]}
    t_start = time.monotonic()

    step(1, f"inject missing_amount for ds={d}")
    subprocess.run(
        ["make", "--no-print-directory", "break-it", "FAULT=missing_amount", f"DATE={d}"],
        check=True,
    )

    step(2, "wait for sentinel-detector to open a new primary incident")

    def new_incident():
        for i in lake()["incidents"]:
            ev = json.loads(i["evidence"])
            if (
                i["incident_id"] not in before
                and i["role"] == "primary"
                and i["pipeline"] == "orders_ingest"
                and d in ev["affected_ds"]
                and i["status"] == "open"
            ):
                return i
        return None

    inc = wait("incident", new_incident, 150)
    iid = inc["incident_id"]
    print(f"   {iid}  {inc['type']}  {inc['title']}")

    step(3, "diagnosis")

    def diagnosed():
        return next((x for x in reversed(lake()["diagnoses"]) if x["incident_id"] == iid), None)

    if have_key:
        print("   Claude (real API) via the detector's diagnosis worker")
        dx = wait("diagnosis", diagnosed, 180)
    else:
        print("   ANTHROPIC_API_KEY not set: using the rule-based MOCK (labelled, $0)")
        subprocess.run(
            [
                *EXEC,
                "sentinel-detector",
                "/opt/venv/jobs/bin/python",
                "-m",
                "sentinel.diagnose",
                "--incident",
                iid,
                "--mock",
            ],
            check=True,
            capture_output=True,
        )
        dx = diagnosed()
    result = json.loads(dx["result"])
    top = result["root_causes"][0]
    print(
        f"   {dx['model']} status={dx['status']} top={top['category']} "
        f"conf={top['confidence']} retry_safe={top['retry_safe']} "
        f"tokens={dx['input_tokens']}+{dx['output_tokens']} cost=${dx['cost_usd']:.5f}"
    )
    print(f"   evidence: {top['evidence'][0]['quote'][:120] if top['evidence'] else '-'}")

    step(4, "mock Slack message")
    msg = wait(
        "Slack message",
        lambda: next(
            (
                m
                for m in reversed(messages())
                if m.get("incident_id") == iid
                and "No diagnosis available" not in json.dumps(m["blocks"])
            ),
            None,
        ),
        60,
        2,
    )
    print(f"   message {msg['id']}: {msg['text']}")
    print(f"   buttons: {[e['action_id'] for e in msg['blocks'][-1]['elements']]}")
    print(f"   view it: {API}/mock-slack")
    value = msg["blocks"][-1]["elements"][0]["value"]

    step(5, "operator fixes the input (regenerates the orders file for the date)")
    subprocess.run(
        [
            *EXEC,
            "-e",
            "SENTINEL_EVENT_SINK=none",
            "airflow-scheduler",
            "/opt/venv/jobs/bin/python",
            "-m",
            "pipelines.generators",
            "--dataset",
            "orders",
            "--ds",
            d,
        ],
        check=True,
        capture_output=True,
    )
    print("   orders file restored")

    step(6, "click Retry DAG")
    r = click(msg["id"], "retry", value)
    print(f"   {r['decision']}: {r['message']}")

    if r["decision"] != "accepted":
        step(7, "click 'I fixed it, retry anyway' (explicit confirmation)")
        r = click(msg["id"], "retry_confirmed", value)
        print(f"   {r['decision']}: {r['message']}")
        if r["decision"] != "accepted":
            return 1

    step(8, "wait for the re-run and for the detector to resolve the incident")
    af = AirflowClient.from_env()

    def runs_done():
        states = {
            dag: af.find_run(dag, f"{d}T00:00:00+00:00")["state"]
            for dag in ("orders_ingest", "daily_revenue_agg")
        }
        return states if all(s in ("success", "failed") for s in states.values()) else None

    states = wait("DAG runs finished", runs_done, 300)
    print(f"   {states}")
    status = wait(
        "incident resolved",
        lambda: next(
            (
                i["status"]
                for i in lake()["incidents"]
                if i["incident_id"] == iid and i["status"] == "resolved"
            ),
            None,
        ),
        150,
    )
    print(f"   {iid}: {status}")

    print("\n=== audit records")
    for a in lake()["audit"]:
        if a["incident_id"] == iid:
            print(
                f"   {a['at'][:19]} {a['source']:<10} {a['actor']:<22} {a['action']:<15} "
                f"{a['decision']:<8} {a['reason'][:90]}"
            )
    ok = all(s == "success" for s in states.values()) and status == "resolved"
    print(f"\nDEMO {'PASSED' if ok else 'FAILED'} in {time.monotonic() - t_start:.0f}s")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
