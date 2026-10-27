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

Found later, in the blind human sample: the same gap hits the **duplicate load** F-J8PS66.
The uniqueness check also reports `"null"`, and a Haiku diagnosis said "payment_id is null
for all 4166 rows". The judge rated it `partial`, because "volume doubles" was right, and
you rated it `mismatch`. So the 8 above is a lower bound.

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

## 9. How far to trust the judges (judge vs. human, 15 blind items)

Fix score: 14/15 exact, 15/15 within one, quadratic kappa 0.94 (bootstrap 95% CI
0.59–1.00). Root-cause verdict: 12/15 exact, Cohen's kappa 0.61 (0.38–1.00). All 3
root-cause disagreements are the judge saying `partial` where the human said `mismatch`.
These are the two OOM `unknown`s and F-J8PS66 above. Every `match` agreed (10/10).

So the counts above that rely on `match` are supported. The judge's `partial` is lenient,
and should be read with `mismatch` as "not the stated root cause". The sample has no fix
scored 0, and only two scored 1, so the low end of the fix rubric is the least validated
part. Details are in the README ("Judge vs. human agreement").


## 10. Phase 5c: post-hoc tool fixes (not held-out)

Everything above is v1 and stays the headline. Phase 5c is **post-hoc**: the three fixes
were chosen after reading the failures above, and measured on the **same 25 faults**. So
the "after" numbers show these fixes remove these failures. They are not an estimate of
accuracy on unseen faults. `prompts/diagnosis_v1.md` was not edited. Haiku 5.5 only, 3
repeats, run label `posthoc-tool-fixes`, on scenario lakes rebuilt with the fixed
instrumentation (`data/benchmark-posthoc-tool-fixes`; the v1 lakes and results are
untouched). Numbers: `report/posthoc/`.

The fixes:
- **(a) Logs:** the failure callback keeps terminal JVM error lines from anywhere in the
  task log, ahead of the 60-line tail. `get_error_logs` returns them in a `terminal_errors`
  field that truncation can't cut.
- **(b) DQ:** the checkpoint keeps GX's sample of unexpected values and the null count.
  `get_dq_failures` reports those instead of `"observed_value": "null"`.
- **(c) Guardrail:** if the top cause is `unknown`, `retry_safe` is forced to false in the
  `Diagnosis` model.

| Failure (v1 section) | v1 | post-hoc | What happened |
|---|---|---|---|
| 1. OOM → `unknown` | 0/9 right, 9 `unknown`, 9 unsafe "safe" | **9/9 `oom`**, 0 `unknown`, **2** unsafe "safe" | (a) works: every repeat quotes the `OutOfMemoryError` line. Root cause is still mostly `partial` (2/9 `match`): no tool shows the task's `JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize` setting, so the model can name the OOM but not why the limit was so low (**remaining tool gap**). 2 repeats called the retry safe, "if the memory pressure was transient" (**model error / prompt gap**). The guardrail doesn't apply, because the top cause is now `oom`, not `unknown` |
| 2. Late upstream DAG → `late_data` | 0/3 | 0/3 | Untouched by the fixes; still the category-boundary prompt gap |
| 3. Unreachable source → `unknown` | 2/3 | 3/3 | Not a fix: this is the fault that flipped between repeats in v1 |
| 4. Bad deploy read as DQ | 2/3 (F-5RX3GD) | 2/3 | Same pattern, one repeat |
| 5. Business dip (truth `unknown`) | 3/3 | **2/3**: r1 said `data_quality` | New miss on the orders incident. Same prompt gap as v1's payments incident ("volume far off normal" is listed under `data_quality`); run-to-run variation, not caused by a fix |
| 6. DQ "nulls" | root `mismatch` 6/6 | F-CGQXGR **3/3 `match`**; F-8EHKFX **3/3 `partial`** | (b) works for the negative amounts (`-247.57, -225.13, ...`). For the invalid code it introduced **a new tool bug**: string samples are JSON-encoded twice, so the model saw `"\"upi-collect\""` and blamed "literal quote characters / serialisation". Duplicate load F-J8PS66: 1/3 `match` (v1 0/3) |

**The guardrail, separated from the tool fixes.** Applied offline to v1's 66 scored
diagnoses (no API), it alone makes `retry_safe` 66/66 correct: every v1 error was an
`unknown` OOM. In the post-hoc run it fired twice, both on the business dip, whose
`retry_safe` is unscored ("either"). The OOM fixes turned "honest `unknown` + safe", which
the guardrail catches, into "`oom` + safe" in 2 of 9 repeats, which it doesn't.

**Noise floor.** Faults the fixes don't touch also moved: late_data root `match` went 7/9
→ 5/9, and a few schema / deploy repeats went `match` → `partial`. So root-verdict changes
of about ±2 per category are run-to-run (model + judge) variation. The OOM category gain
(0 → 9 of 9) and F-CGQXGR (0 → 3 of 3 `match`) are well beyond that; the rest is not.

**Not done, proposed:**
- **Fix the double encoding:** pass sample values through as JSON values, not
  `json.dumps` strings.
- **A task-environment tool, or the OOM line's own `limit:` figure:** the OOM `limit`
  figure is in the quoted line, but the setting that caused it is not.
- **Collapse near-duplicate `terminal_errors`:** the same OOM appears up to 5 times, with
  different timestamps or a `Caused by:` prefix.
- **Extend the guardrail to `oom`:** an OOM is never retry-safe without a config change.

Each of these would be another post-hoc change on the same faults. Phase 5d (section 11) built the first, third and
fourth. The task-environment tool stays future work.

## 11. Phase 5d: post-hoc round 2 (not held-out; the last round)

Round 2 is also **post-hoc**. Its three changes were chosen after reading round 1's
failures (section 10), on the **same 25 faults**. Rounds 1 and 2 are not held-out. **No
further tuning will be done on these 25 faults.** v1 stays the headline, and v1's and
round 1's results are unchanged (both re-score to their committed metrics). The prompt was
not edited. Haiku 5.5 only, 3 repeats, run label `posthoc-round-2`, on its own lakes
(`data/benchmark-posthoc-round-2`). Numbers: `report/posthoc-round-2/` (`rounds.csv`,
`faults.csv`, `metrics.json`, `charts/rounds.svg`).

The changes:
- **DQ double encoding fixed:** sample values go through as JSON values (a string once
  encoded, numbers typed).
- **Guardrail extended to `oom`:** a top cause of `oom` forces `retry_safe = false`, in
  `Diagnosis` validation, not the prompt. The `unknown` rule is kept.
- **Near-duplicate terminal errors collapse:** one line per error, the first occurrence,
  with a count (the callback annotates `[sentinel: Nx in this log]`; the tool returns
  `{"line", "count"}`).

| Failure | v1 | round 1 | round 2 | What happened |
|---|---|---|---|---|
| OOM `retry_safe` | 0/9 | 7/9 | **9/9** | The oom guardrail fired once (F-ZR22VD r2). The other 8 repeats said "not safe" by themselves. Applied offline to v1, both guardrails give 66/66 |
| OOM root cause | 0/5/4 | 2/7/0 | 4/5/0 | Within the ±2 noise floor: the collapse removes repetition but adds no new fact. The memory **setting** is still invisible; that is the task-environment tool (future work, not built) |
| Invalid payment code (F-8EHKFX) root | mismatch 3/3 | partial 3/3 | match 1, partial 2 | The encoding bug is gone: all 3 repeats name `upi-collect` as an unexpected value, and none mentions quote characters. The remaining `partial`s say "a new or renamed method, or corrupted values; the tools can't tell which", which is honest |
| Late upstream DAG (F-ED2JNB) | `late_data` 3/3 | `late_data` 3/3 | `late_data` 2, `unknown` 1 | The prompt gap from v1 is unchanged. The `unknown` repeat was forced to `retry_safe = false` by the **5c unknown rule**, but a retry is safe here. That is the single `retry_safe` miss in round 2, and the first time the unknown rule cost a correct answer: it trades a missed safe retry for never offering an unsafe one |
| Business dip (truth `unknown`) | 3/3 | 2/3 | **1/3** | 2 repeats said `data_quality`, one judged root `mismatch`. None of the changes touches this night's evidence. Same prompt gap as v1 and round 1 ("volume far off normal" is listed under `data_quality`), moving with run-to-run variation |
| Bad deploy read as DQ (F-5RX3GD) | 2/3 | 2/3 | 3/3 | Not caused by a change; variation |

**Overall (Haiku ×3):**
- Top-1: v1 61/75, round 1 70/75, round 2 69/75.
- Root match/partial/mismatch: 42/23/10, then 46/29/0, then 47/26/2.
- `retry_safe` correct: 57/66, then 64/66, then 65/66. Round 2 called 0 unsafe retries "safe" and 1 safe retry "unsafe".
- Brier: 0.127, then 0.106, then 0.092.
- Mean cost: $0.0025, then $0.0023, then $0.0024. Median latency: 12.8 s, then 12.6 s, then 12.2 s.

Round 2 differs from round 1 on top-1 by one fault-repeat, which is noise. Its real
effects are narrow: `retry_safe` on OOM, and the DQ misreading removed.

**Remaining, not built:**
- A task-environment tool, the fix for the OOM `partial`s. It is listed as future work in
  the README and is to be measured on new faults.
- The v2 prompt for the late-DAG and business-dip category boundaries (still a proposal).

