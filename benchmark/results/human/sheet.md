# Fix-quality scoring sheet (blind)

Score each suggested fix 0-3 with the rubric below, in `scores.csv` (`fix_score`).
Optionally, also say whether the explanation names the true root cause
(`root_verdict`: match / partial / mismatch). The model, its confidence and the
judge's scores are hidden. Don't open `key.json` until you're done.

## Rubric (the same text the LLM judge gets)

- `3` correct and actionable: it addresses the true cause with concrete steps specific to
  this incident (it names the column, deploy, source, setting or file to fix) and says how
  to recover the affected run (re-run or backfill after the fix, or wait for the
  upstream).
- `2` right direction: it addresses the true cause, but it is generic, misses a needed
  step (such as the recovery re-run or contacting the source owner), or buries the right
  step among unneeded ones.
- `1` weak: mostly investigation steps or generic advice that would not by itself fix the
  true cause, or it fixes a symptom only.
- `0` wrong or harmful: it addresses a different cause, or following it would make things
  worse (re-running a job on input that is still broken and publishing bad data, deleting
  data, rolling back an unrelated deploy).

When the ground truth says the cause is outside the known categories or is a real
business change (no data defect), a fix that says to confirm with the right owner before
changing anything, and does not change the pipeline, scores `3`.

## S01

**Ground truth:** The orders raw file landed without read permission (mode 000), so the job cannot read it: an access problem outside the six categories.

**Category given:** unknown

**Explanation:** The log shows SparkFileNotFoundException with "(Permission denied)" on the day's input CSV. That points to file ownership or mode on the landed partition, or to the identity the job runs as. The evidence rules out the other categories. There is no schema diff. The pipeline has no upstreams. The last deploys only touched logging and comments, and the 2026-08-01 and 2026-08-02 runs used the same code version 46ff16e as this run. The tools don't show who or what set the permissions.

**Suggested fix:** Check owner and mode of /opt/sentinel/data/lake/raw/orders/ds=2026-08-03/ and part-0000.csv, and compare them with the ds=2026-08-02 partition. Find the process that wrote the file and fix its umask or ACLs. Then chmod or chown the file so the Spark user can read it, and re-run ingest_orders.

**Retry safe:** False (The file is still unreadable, so a re-run would fail the same way until the permissions are fixed.)

## S02

**Ground truth:** The orders extract stopped early: it has only events before 14:00 UTC, so the latest order_ts is 10 h before the end of the day (SLA 2 h) and volume is low.

**Category given:** late_data

**Explanation:** The orders input for ds=2026-07-21 ends at 13:59:43 UTC, which is 10 h short of the expected 22:00 freshness threshold. Rows_in is 1116 against a typical ~1900 for a Tuesday. That fits data that cuts off partway through the day. The schema is identical to the previous run. Code version 1fdde5d has been live since 07-15 and ran fine on earlier dates. The validate_orders failure is a symptom of this staleness. The tools don't show why the source stopped, and no upstream pipeline is recorded in lineage.

**Suggested fix:** Find out why the orders source stopped at about 13:59 UTC on 07-21. Check the generate_orders task and the source system for an outage or a stalled feed. Once the missing orders after 13:59 are backfilled, re-run ingest_orders and validate_orders for ds=2026-07-21.

**Retry safe:** False (The input data is still incomplete, so re-running now would reproduce the same stale data and the same freshness failure.)

## S03

**Ground truth:** The Spark JVM of run_payments_glue_job ran out of memory: its environment set JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize=64k, the job hit java.lang.OutOfMemoryError (direct buffer memory) and the JVM died.

**Category given:** unknown

**Explanation:** The log shows 'Answer from Java side is empty' followed by 'ConnectionRefusedError: [Errno 111] Connection refused' on the py4j socket. That means the Spark JVM exited or was unreachable. The logs show no OutOfMemoryError, exit 137 or container-kill message, so I can't call it OOM. Upstream: the lineage lists no upstream pipelines. Input: generate_payments succeeded with 1754 rows, which is in the normal range. Deploys: code version 6e7fbc9 has run successfully since 2026-06-19, so a bad deploy is unlikely. It is the first and only occurrence, so a transient JVM or driver crash is plausible, but nothing confirms it.

**Suggested fix:** Check the Spark driver and executor logs, and the container or host events, for ds=2026-06-24 around 00:05:43-00:05:50 UTC. Look for a JVM crash, an OOM kill or a node restart. If nothing is found, re-run run_payments_glue_job and watch it. If it fails again, look at the Spark and JVM environment (memory limits, port or gateway startup).

**Retry safe:** True (The input is normal and the code is unchanged from recent successful runs, so a transient JVM failure should clear on re-run. The cause is not confirmed, so watch the retry.)

## S04

**Ground truth:** Deploy 18fa2a1 of daily_revenue_agg changed pipelines/jobs/daily_revenue_agg.py: it drops currency from the aggregate's grouping, so the output loses its currency column and validate_daily_revenue fails.

**Category given:** bad_deploy

**Explanation:** Code version 18fa2a1 changed pipelines/jobs/daily_revenue_agg.py at 2026-07-23 16:34 UTC, about 7.5 hours before the failing run. The ds=2026-07-22 run on 77641a7 wrote 18 rows, and the 07-23 run on 18fa2a1 wrote 6. The three failed expectations (currency null, compound uniqueness, and the column set) match a grouping change that dropped currency from the output. Input rows_in was normal (1951), all upstreams succeeded, and the input schema diff is empty, so the regression is most likely in the transformation. The link to the grouping change is inferred from the diff summary and the output shape, because the job source was not retrieved.

**Suggested fix:** Review the 18fa2a1 change to pipelines/jobs/daily_revenue_agg.py, specifically the grouping keys, and confirm whether currency was dropped from the GROUP BY or select list. Restore the previous grouping (for example, revert to 77641a7 or fix the grouping to keep currency and the original key set), then re-run the ds=2026-07-23 validate and aggregation tasks. Before re-running, check whether a bad daily_revenue output for 2026-07-23 was already published to any downstream consumer, and if so, quarantine or backfill it. Add a DQ expectation or row-count guard against the 18-row baseline so a repeat is caught before publish.

**Retry safe:** False (The deployed code 18fa2a1 is still live and will reproduce the same 6-row, currency-less output, so re-running without a code fix or revert is expected to fail again.)

## S05

**Ground truth:** The orders extract stopped early: it has only events before 14:00 UTC, so the latest order_ts is 10 h before the end of the day (SLA 2 h) and volume is low.

**Category given:** late_data

**Explanation:** The orders feed for 2026-07-21 stopped early. Its newest order_ts is about 14:00 UTC, while the freshness SLA needs data up to about 22:00 UTC. The generate_orders job still reported success, but it wrote only 1116 rows against a same-weekday norm of about 1914 and a 2026-07-20 volume of 1997. The schema matches the previous run exactly, so this is not schema drift. The code version (1fdde5d) is the same as the prior day's run, so no deploy explains it. The validate_orders failure is a symptom of the stale data.

**Suggested fix:** Check the upstream order source or extract for 2026-07-21 to see why it stopped around 14:00 UTC and whether a complete file exists. Once a complete extract is available, re-land it and re-run generate_orders and orders_ingest for ds=2026-07-21, then confirm order_ts reaches the freshness floor and the row count is back near the expected volume. Do not treat the 1116-row output as final.

**Retry safe:** False (The input is still incomplete (latest order_ts about 10 h stale, volume down about 42%). Re-running on the same data would reproduce the failure unless the source has been re-delivered first.)

## S06

**Ground truth:** The Spark JVM of aggregate_revenue ran out of memory: its environment set JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize=32k, the job hit java.lang.OutOfMemoryError (direct buffer memory) and the JVM died.

**Category given:** unknown

**Explanation:** The job failed at orders.count() when the py4j gateway got connection refused. The traceback shows the JVM side was gone: 'Answer from Java side is empty', then connection refused. No log line states why. There is no OOM or exit-137 message, though the traceback is truncated, so OOM is not ruled out. All three upstreams (orders_ingest, customers_dim, payments_etl) succeeded. The last deploy, a62c8ef, ran successfully for 7 days (ds 2026-09-15 to 2026-09-21), so it doesn't line up with this failure. The failure happened once, on try 1, about 9s into the run. This looks like a transient Spark driver/JVM or environment failure, but that is not confirmed.

**Suggested fix:** Check the Spark driver/executor host logs and container events around 2026-09-23 00:06 UTC. Look for a JVM crash, OOM kill, or a restart of the Spark service. If the environment is healthy, re-run aggregate_revenue for ds=2026-09-22 and watch it. If it fails again, escalate and check the JVM memory settings.

**Retry safe:** True (This was a single failure with no sign of broken inputs, and the same code version succeeded on the previous 7 days. A re-run is likely to succeed if the Spark environment has recovered, but the cause isn't confirmed.)

## S07

**Ground truth:** The orders source renamed column `amount` to `order_amount`; orders_ingest requires `amount` and fails.

**Category given:** schema_drift

**Explanation:** The observed input columns replaced `amount:double` with `order_amount:double`. The type is unchanged, so this looks like a rename at the source. The job's required-column check failed as a result. No upstream pipeline is listed in lineage, so the rename probably came from an external producer of the orders data.

**Suggested fix:** Confirm with the orders data producer whether `amount` was renamed to `order_amount` on purpose. If it was, update the ingest job to accept `order_amount` (or map it back to `amount`), then re-run ds=2026-07-11. If it wasn't, ask the producer to restore the old column name. Also check commit 154c5e0 in pipelines/dags/orders_ingest.py to rule out a side effect on column handling. Its message says it only logs row counts, so this is unlikely.

**Retry safe:** False (The input still has `order_amount` instead of `amount`, so a re-run without a fix would fail the same way.)

## S08

**Ground truth:** The orders raw file landed without read permission (mode 000), so the job cannot read it: an access problem outside the six categories.

**Category given:** unknown

**Explanation:** The failure is a read permission error on the raw input file /opt/sentinel/data/lake/raw/orders/ds=2026-08-03/part-0000.csv, not a schema, freshness, memory or data-content problem. The upstream generate_orders job succeeded for the same ds with 2087 rows, so the file was produced. The tools do not show why the file or directory became unreadable by the job. Deploy 46ff16e was the code version for 2026-08-01 through 2026-08-03; the same version ran successfully on 2026-08-01 and 2026-08-02, and its only changed file is logging.py, so a code change is unlikely to be the cause. The remaining explanation is an ownership or permission change on the lake path outside this pipeline's code.

**Suggested fix:** 1) Check ownership and mode of /opt/sentinel/data/lake/raw/orders/ds=2026-08-03/ and part-0000.csv, and the permissions of the airflow/Spark user, and find what changed them. 2) Confirm generate_orders wrote the file completely (2087 rows expected). 3) Restore read access for the job user, then re-run ingest_orders for ds=2026-08-03. 4) Check whether other ds partitions under raw/orders were affected by the same change.

**Retry safe:** False (The file is still unreadable as far as the evidence shows, so a retry would most likely fail the same way until read access is restored.)

## S09

**Ground truth:** The customers source system's export endpoint (extract-api.erp.internal) was unreachable; generate_customers failed after its retries and nothing was delivered.

**Category given:** upstream_outage

**Explanation:** The source export that generate_customers reads from (an external ERP API, not a monitored upstream pipeline) was unreachable. Every attempt failed at DNS resolution (Errno -2, 'Name or service not known'), before any HTTP response. The 2026-07-17 export therefore never landed, and the job failed after three attempts. The code did not change: version 7cda97c succeeded on 2026-07-16 and 2026-07-15. The deploy history shows only comment and docstring changes since then. Upstream status lists no upstream pipelines, so the outage is inferred from the source error, not confirmed by a monitored upstream failure.

**Suggested fix:** Check whether extract-api.erp.internal resolves from the Airflow workers now, and whether the ERP customers export for 2026-07-17 exists on the source side. Confirm with the ERP or platform owner whether the endpoint or its DNS record was moved or retired. If the host resolves again and the export is present, clear the failed generate_customers task for ds=2026-07-17 and rerun it, then check that rows_out is near the usual ~9,900. If the endpoint is gone, fix the source configuration before any rerun. Downstream customers_dim tasks for that ds should be checked after the rerun.

**Retry safe:** False (The host still failed name resolution on every attempt, so a rerun will probably fail the same way until DNS or the endpoint is fixed. Once the endpoint resolves and serves the export, a rerun should be safe, because the job reads one ds and writes one output.)

## S10

**Ground truth:** The orders source extract stopped including the `amount` column; orders_ingest requires it and fails.

**Category given:** schema_drift

**Explanation:** The input for ds=2026-08-25 is missing the 'amount' column (type double) that was present in the baseline run for ds=2026-08-24. No columns were added or retyped. The job's require_columns check raised SchemaMismatchError, and the Airflow task failed as a consequence. Both runs used code version ba63d01, so the change is in the incoming data, not in the pipeline code.

**Suggested fix:** Confirm with the producer of the orders source feed whether 'amount' was removed or renamed on purpose, and check whether the upstream export for 2026-08-25 is partial. If the removal is intended, update the expected column list and the transform in a reviewed change, then re-run. If it is a producer bug, get the feed restored with 'amount' and re-run ingest_orders for ds=2026-08-25. Do not relax the required-column check to get the run through.

**Retry safe:** False (The input for ds=2026-08-25 still lacks 'amount', so re-running the same task would fail the same check until the source feed is fixed or the expected schema is changed.)

## S11

**Ground truth:** Deploy 6505154 of orders_ingest changed pipelines/jobs/orders_ingest.py: it casts amount to DECIMAL(4,2), so every amount of 100 or more overflows to NULL and validate_orders' not-null check fails.

**Category given:** bad_deploy

**Explanation:** ds=2026-08-17 is the first run on new code (10b6d01). It is also the first failure, and ds=2026-08-16 ran 4ac07ad. Two deploys landed on 08-17 before this run. Commit 6505154, "tighten amount precision for the finance export", changed the orders job and plausibly produces nulls in `amount` (for example a cast or round that fails). The 10b6d01 refactor also touched the DAG. The schema is identical to baseline, so a type change is not the cause. I have no log or metric that directly ties the nulls to either commit.

**Suggested fix:** Diff 6505154 and 10b6d01 for how `amount` is computed in pipelines/jobs/orders_ingest.py and pipelines/dags/orders_ingest.py. Check whether the precision change turns values into nulls. Revert or fix, then re-run ds=2026-08-17. Also compare the raw input's null rate for `amount` against the output.

**Retry safe:** False (The suspect code is still live, so a re-run would likely produce the same null amounts.)

## S12

**Ground truth:** The customers source system's export endpoint (extract-api.erp.internal) was unreachable; generate_customers failed after its retries and nothing was delivered.

**Category given:** upstream_outage

**Explanation:** The generate_customers task pulls its input from an external ERP export endpoint (extract-api.erp.internal). Every attempt failed at name resolution, so the source was unavailable for this run, and the downstream customers_dim job never ran. The source is not a monitored pipeline, so get_upstream_status returned nothing to confirm an upstream failure directly.

**Suggested fix:** Confirm that extract-api.erp.internal resolves from the Airflow worker (for example with a DNS lookup) and that the ERP export endpoint for 2026-07-17 is up. Check with the owners of the ERP extract service whether it had an outage or DNS change around 2026-07-18 00:05 UTC. Once the endpoint resolves, re-run generate_customers and customers_dim for ds=2026-07-17 and confirm that rows_out is in the usual range (about 9,700 to 9,900 for recent days).

**Retry safe:** True (The failure is a name-resolution error with no sign of bad input or a code problem, and the same code version succeeded the previous two days, so a re-run should succeed once the endpoint resolves. The source's current state was not verified, so confirm DNS and endpoint health before retrying.)

## S13

**Ground truth:** The payments extract contains every row twice (a duplicate load): the unique-id check fails and volume doubles.

**Category given:** data_quality

**Explanation:** The payments dataset for 2026-09-06 arrived on time with the expected schema, but payment_id is null for all 4166 rows. The uniqueness check therefore fails on every row, and the volume spike is consistent with a malformed or duplicated extract. The tools do not show why the source produced null IDs or the extra rows, so the exact upstream fault is not confirmed.

**Suggested fix:** Inspect the generate_payments output for ds=2026-09-06 (the 4166-row file) and confirm whether payment_id is genuinely null at the source or lost in transformation. Check whether the rows are duplicates of an earlier extract, since volume is about double the normal ~2000-2300. If the source is faulty, get it regenerated and re-validated before anything downstream consumes it. If the data is confirmed to be correct, review the uniqueness expectation for this dataset.

**Retry safe:** False (The input for ds=2026-09-06 is still bad: the same null payment_id rows would be read again and fail the uniqueness check, so a plain rerun would not succeed until the source is corrected.)

## S14

**Ground truth:** Deploy 18fa2a1 of daily_revenue_agg changed pipelines/jobs/daily_revenue_agg.py: it drops currency from the aggregate's grouping, so the output loses its currency column and validate_daily_revenue fails.

**Category given:** bad_deploy

**Explanation:** Commit 18fa2a1 changed daily_revenue_agg.py about 8 hours before the run and was the first code version to run for ds=2026-07-23. The output columns are now country, ds, gross_revenue, order_count, paid_revenue and unpaid_orders, so currency is gone. That also explains the null-currency and compound-uniqueness failures, since the grouping key changed. The input schema diff is empty and all upstreams succeeded, so the inputs did not cause this.

**Suggested fix:** Review the diff of 18fa2a1 in pipelines/jobs/daily_revenue_agg.py and restore currency in the groupBy and the output columns, or revert to 77641a7. Then rerun ds=2026-07-23 and confirm the DQ checks pass. Check whether any bad output was already published downstream.

**Retry safe:** False (The faulty code version 18fa2a1 is still live, so a rerun would produce the same output without currency.)

## S15

**Ground truth:** The payments source wrote local IST times labelled as UTC (+5:30), so late-evening paid_ts values fall after the end of the day and the freshness upper bound fails.

**Category given:** data_quality

**Explanation:** The paid_ts column contains a value later than the business date allows. The max is 2026-08-14 05:29:59+00:00 for ds=2026-08-13, about 5 hours past the end of the day. The schema diff is empty and rows_out (1731) is in the normal 1663-2326 range, so this looks like bad timestamp contents rather than missing data or a volume problem. The tools do not show how many rows are affected (unexpected_count is null), so whether this is a few bad rows or systematic is unconfirmed.

**Suggested fix:** Have the data owner inspect the payments source for ds=2026-08-13 and find the rows where paid_ts is later than 2026-08-14 00:00 UTC (or whatever the expected bound is). Check whether these are timezone-conversion errors, bad clock values, or rows from a later load. Fix or quarantine those rows at the source, or confirm the expectation bound is correct before re-running validate_payments. Do not simply widen the check without confirming the timestamps are legitimate.

**Retry safe:** False (The out-of-range paid_ts values are still in the input for ds=2026-08-13, so a re-run is expected to fail the same check unless the source data is corrected first.)
