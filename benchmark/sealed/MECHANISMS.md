# How each fault kind is produced (sealed until the manual trials are done)

Reading this tells you what each kind of fault looks like in the evidence, which would bias
a manual diagnosis. It moves to the README in Phase 5b.

| Kind | Mechanism |
|---|---|
| source problems (schema drift, late data, data quality, a business dip) | The generate task (the "source system" extract) goes through a source shim that delivers the faulty data: dropped / renamed / retyped / added columns, a stale or previous-day extract, an extract that stops early, NULLs, a duplicated load, out-of-range or invalid values, shifted timestamps. Because the change happens before the file is written, the generate task's own metrics and logs describe what was really delivered. |
| bad deploy | A **real code regression** applied to the build copy of one job (for example a cast or a required-column list), plus the `deploy_event` that shipped it: git SHA, changed file, author, commit message, a few hours before the nightly run. The regressed job then runs. |
| OOM | A **real Spark failure**: the task runs with `JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize=<n>k`, the JVM really hits `java.lang.OutOfMemoryError: Cannot reserve ... bytes of direct buffer memory` and dies, and PySpark fails the way it does when its JVM goes away (`Py4JNetworkError`, `ConnectionRefusedError`). Found by experiment: a larger input alone doesn't OOM these jobs (Spark streamed a 400x orders file in 512 MB), so a memory *limit* is the realistic trigger here. |
| upstream outage | An upstream DAG run never starts or starts late (the downstream sensor times out with Airflow's real message), or the source's export endpoint is unreachable (a real failed HTTP call, retried, then raised). |
| novel | A delivered file with no read permission (a real Spark read error), a gzip file under a `.csv` name. |
| clean | Nothing injected. |


OOM detail: the JVM's own `OutOfMemoryError` line is printed early in the task's output,
so it can scroll out of the 60-line log tail the Airflow callback keeps, which is all the
agent's `get_error_logs` sees. That is how the real stack behaves; it was not changed.
