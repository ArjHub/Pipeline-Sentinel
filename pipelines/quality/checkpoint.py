"""Run a suite against a pandas DataFrame through a GX Checkpoint whose action emits a
`dq_result` event. The action is the only place DQ results leave GX."""

from __future__ import annotations

import math
import uuid
from typing import Any, Literal

import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint.actions import ValidationAction

from pipelines.quality.suites import expectations_for, suite_name
from sentinel.events.models import DQResult, ExpectationOutcome, RunContext
from sentinel.events.sinks import EventSink

_MAX_LIST = 20


def _jsonable(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in list(value)[:_MAX_LIST]]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


def to_dq_result(dataset: str, row_count: int, checkpoint_result: Any) -> DQResult:
    outcomes: list[ExpectationOutcome] = []
    suite_name = ""
    for vr in checkpoint_result.run_results.values():
        suite_name = vr.suite_name
        for r in vr.results:
            res = r.result or {}
            outcomes.append(
                ExpectationOutcome(
                    expectation_type=r.expectation_config.type,
                    column=r.expectation_config.kwargs.get("column"),
                    success=bool(r.success),
                    observed_value=_jsonable(res.get("observed_value")),
                    unexpected_count=res.get("unexpected_count"),
                    unexpected_percent=_jsonable(res.get("unexpected_percent")),
                )
            )
    failed = sum(not o.success for o in outcomes)
    return DQResult(
        dataset=dataset,
        suite_name=suite_name,
        success=failed == 0,
        evaluated=len(outcomes),
        failed=failed,
        row_count=row_count,
        results=outcomes,
    )


# GX copies/serializes actions, so non-serializable objects (sink, context) can't live on
# the action itself. They're registered here under a per-run binding id instead.
_BINDINGS: dict[str, tuple[RunContext, EventSink]] = {}
_RESULTS: dict[str, DQResult] = {}


class SentinelEventAction(ValidationAction):
    type: Literal["sentinel_event"] = "sentinel_event"
    binding_id: str
    dataset: str
    row_count: int

    def run(self, checkpoint_result: Any, action_context: Any = None) -> dict:
        result = to_dq_result(self.dataset, self.row_count, checkpoint_result)
        _RESULTS[self.binding_id] = result
        ctx, sink = _BINDINGS[self.binding_id]
        sink.emit(ctx.event(result))
        return {"sentinel_event": result.success}


def validate(
    df: pd.DataFrame,
    dataset: str,
    ds: str,
    ctx: RunContext,
    sink: EventSink,
) -> DQResult:
    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name(dataset)))
    for expectation in expectations_for(dataset, ds):
        suite.add_expectation(expectation)
    batch_def = (
        context.data_sources.add_pandas(f"{dataset}_src")
        .add_dataframe_asset(dataset)
        .add_batch_definition_whole_dataframe("whole")
    )
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{dataset}_vd", data=batch_def, suite=suite)
    )
    binding_id = uuid.uuid4().hex
    _BINDINGS[binding_id] = (ctx, sink)
    action = SentinelEventAction(
        name="emit_dq_result", binding_id=binding_id, dataset=dataset, row_count=len(df)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=f"{dataset}_checkpoint", validation_definitions=[vd], actions=[action])
    )
    try:
        checkpoint.run(batch_parameters={"dataframe": df})
        return _RESULTS.pop(binding_id)
    finally:
        _BINDINGS.pop(binding_id, None)
