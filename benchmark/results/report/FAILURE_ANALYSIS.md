# Failure analysis: diagnosis_v1, benchmark split (Phase 5b)

Every diagnosis whose top category was wrong, every `unknown`, and every diagnosis where the
category was right but the root-cause judge said the stated cause was wrong. Haiku 5.5 ran 3
repeats (78 diagnoses, 75 fault-level); Sonnet 5.5 ran 1 (26, 25 fault-level). The numbers
are in `metrics.json` and `diagnoses.csv`. Nothing was re-run, and `prompts/diagnosis_v1.md`
was not edited.

Cause labels: **tool result gap** (the evidence the agent needed never reaches its tools, or
reaches them in a misleading form), **prompt gap** (the prompt's categories or rules push to
the wrong answer), **ambiguous fault** (a reasonable expert could pick either answer),
**detector miss**, **model error** (the evidence was there and the model didn't use it).

## Summary

| Group | Haiku (of 75) | Sonnet (of 25) | Cause |
|---|---|---|---|
| OOM → `unknown` | 9 (3 faults × 3) | 3 | tool result gap |
| Late upstream DAG → `late_data` | 3 | 1 | prompt gap / ambiguous fault |
| Source endpoint unreachable → `unknown` | 1 (1 of 3 repeats) | 0 | prompt gap |
| Bad deploy → `data_quality` (deploy ranked 2nd) | 1 (1 of 3 repeats) | 0 | model error |
| **Wrong top-1 total** | **14** | **4** | |
| Correct `unknown` (business dip, permission denied) | 6 | 2 | — (right answer) |
| Category right, cause wrong (DQ "nulls") | 6 | 2 | tool result gap |
| Detector misses | 0 | 0 | — |

On **retry_safe**, every unsafe "safe to retry" (Haiku 9, Sonnet 3) was on an OOM. The agent
called the JVM crash "transient", which follows from the same tool gap. No diagnosis said
"unsafe" when the frozen ground truth (`benchmark/sealed/retry_safe.toml`) said "safe".

## 1. OOM: the agent never sees the OutOfMemoryError (tool result gap)

Faults F-XZR2R6 (ingest_orders), F-R36B8K (aggregate_revenue), F-ZR22VD (payments glue job).
Haiku 0/9, Sonnet 0/3, all `unknown` at 0.35–0.6. Your manual trial of F-ZR22VD: also
`unknown`, after 506 s.

The JVM prints `java.lang.OutOfMemoryError: Cannot reserve ... bytes of direct buffer memory`
early in the task's output. The Airflow failure callback keeps the last 60 lines, and by then
PySpark has filled them with the Py4J teardown. Checked directly on F-XZR2R6:
`get_error_logs` returns 4 items and 5,408 characters. They contain `Py4JNetworkError` and
`ConnectionRefusedError`, and **none of** `OutOfMemoryError`, `direct buffer` or
`MaxDirectMemorySize`. No other tool shows the task environment either.

Given that evidence, the agents' explanations are accurate: "the JVM gateway went away; the
tools don't show why" (Haiku), and "a silent JVM crash from memory pressure is possible but
unproven" (Sonnet). Under the prompt's rule "prefer `unknown` to guessing", `unknown` is the
defensible output. It still scores as wrong, because the label is `oom`.

The real harm is **retry_safe = true** on all 12. The agents called the crash transient, but
the memory limit is still set, so a retry fails again. A human approving that retry from
Slack would get a second failure. It wouldn't corrupt data: the job fails before it writes.

Fix options (not built): keep the first ERROR/`Exception`/`Error:` line of the full task log
next to the 60-line tail, or add a tool for the task's environment and resource settings.

## 2. Late upstream DAG → `late_data` (prompt gap / ambiguous fault)

F-ED2JNB: customers_dim started 65 min late, and daily_revenue_agg's sensor timed out after
30 min. The label is `upstream_outage`. Haiku said `late_data` 3/3 (0.5–0.8), and so did
Sonnet. You said `unknown`, in 68 s.

The mechanism was diagnosed correctly. Haiku r0/r1 and Sonnet all found that the upstream
finished at 01:10 instead of about 00:05 and that the sensor gave up at 00:35. All of them
said retry_safe = true, which is correct (the upstream has since finished). The miss is
categorical. The prompt defines `late_data` as "input data is stale or incomplete in time"
and `upstream_outage` as "an upstream pipeline failed or didn't deliver". A late upstream
run fits both definitions. The other upstream DAG fault, where the run never started
(F-7YQ53N), was `upstream_outage` 3/3 and 1/1. Haiku r2 also failed to retrieve the upstream
completion time that r0 and r1 found, so it was less certain (0.5).

## 3. Source endpoint unreachable → `unknown` (prompt gap)

F-QXQHZ5: the customers source's export host didn't resolve, and generate_customers failed
after its retries. Haiku r0/r1 said `upstream_outage` (0.6); **Haiku r2 said `unknown`
(0.6)**, arguing that "it is not upstream_outage in the lineage sense, since no upstream
pipeline reported a status". That is a literal reading of the prompt's definition ("an
upstream *pipeline* failed"). A source system isn't a pipeline in that sense, and the
category list has nowhere else to put it. This is one of the two faults where the Haiku
repeats disagree.

## 4. Bad deploy read as data quality (model error)

F-5RX3GD r0 (Haiku): deploy 6505154 cast `amount` to DECIMAL(4,2), so amounts of 100 or more
became NULL. Haiku r0 put `data_quality` 0.6 first and `bad_deploy` 0.3 second (top-3 still
correct). It noted that "the tools do not show whether the nulls come from the generator's
output or were introduced by the ingest transform". Yet the generator's `rows_out` matched,
and `get_recent_deploys` (which it called) shows a deploy of orders_ingest.py that day. r1
(0.45) and r2 ranked `bad_deploy` first. Sonnet was right (1/1). Your trial of the other two
deploy faults went `data_quality` and `schema_drift`, so this is the hard family for humans
too.

## 5. Ambiguous faults where `unknown` is correct

- **Business dip (F-P56PRB):** order volume really fell about 36%. The label is `unknown`
  (the evidence can't separate a real dip from a partial extract). On the fault-level orders
  incident, Haiku and Sonnet said `unknown` 4/4 (0.5–0.6). The night also raised a second
  primary incident on payments, which isn't in accuracy. On it, **Haiku r0 said
  `data_quality` 0.5** over `unknown` 0.4, and r1/r2 and Sonnet said `unknown`. The prompt
  lists "volume far off normal" under `data_quality`, which invites this (prompt gap).
- **Permission denied (F-H3FBP3):** the label is `unknown`. Haiku 3/3 and Sonnet 1/1 said
  `unknown` and named the permission error. Haiku's confidence on that `unknown` was 0.3,
  0.35 and 0.6 for the same evidence (see "Calibration" in the README). You said `unknown`
  too, in 175 s.
- **IST written as UTC (F-7BAWGS):** labelled `data_quality`, with `unknown` accepted. All 4
  said `data_quality`. The root-cause judge rated them `partial`: the agents saw the
  freshness failure but didn't name the +5:30 shift.

## 6. Category right, cause wrong: "nulls" that aren't nulls (tool result gap)

F-8EHKFX (the source sent `'UPI-COLLECT'` instead of `'upi'`) and F-CGQXGR (about 3% negative
amounts). Haiku 6/6 and Sonnet 2/2 have the right category, `data_quality`, but say the
column is **NULL** ("about 40% of rows have a null payment method", "64 unexpected nulls").
`get_dq_failures` returns `"observed_value": "null"` for these column-map expectations:
Great Expectations has no single observed value for them, and the tool serialises `None` as
`"null"`. No sample of the unexpected values is included, so the models read it as NULL
data. A fix that "backfills the nulls" would miss the real defect. The root-cause judge
rated all 8 `mismatch`.

## 7. Phase 4 known issues, measured on the benchmark

| Phase 4 issue | Haiku | Sonnet | Verdict |
|---|---|---|---|
| Says `unknown` too early | 0 `unknown`s after ≤ 2 tool calls; every `unknown` made 3–6 calls (Sonnet 4–6) | 0 | Not reproduced. The new `unknown`s are the OOM tool gap, plus one reading of a category definition |
| Repeated tool calls | 12 of 78 diagnoses repeat a tool (14 extra calls) | 0 of 26 | Still present for Haiku, at a lower rate. Arguments aren't logged, so these are same-*tool* repeats (an upper bound on identical calls) |
| Irrelevant deploys cited | deploy evidence in the top cause on 18 of 63 non-deploy diagnoses; `bad_deploy` ranked anywhere on 8 of 63, never first | 5 of 21; 1 of 21 | Mostly cited to *rule a deploy out* ("ran cleanly for two days before"), which is correct reasoning. `bad_deploy` as a low-ranked alternative (0.1–0.2) still happens |
| `rows_out` undocumented | Not triggered: no benchmark fault depends on reading customers_dim's `rows_out` | — | Still a gap; untested here |
| Verbose output | median 2,309 output tokens | median 955 | Haiku is still about 2.4× wordier, but cheaper and only ~3.5 s slower |
| Two runs of the same ds (break-it) | — | — | Removed by the harness design (Phase 5a) |

## 8. Detector

25/25 injected faults raised a primary incident on the expected pipeline, and the 5 clean
runs raised none. There were no detector misses, so end-to-end accuracy equals
accuracy-on-detected. Caveat: the catalog was built in Phase 5a with a check that each
scenario raises its expected incident ("0 expectation mismatches"). These faults are ones
the four detectors are designed to catch, so 25/25 says the detectors work on the faults
they were built for, not that they catch every fault.
