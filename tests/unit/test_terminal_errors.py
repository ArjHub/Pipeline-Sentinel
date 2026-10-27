"""The fatal-JVM-error line scanner shared by the failure callback and the log tool."""

from __future__ import annotations

import pytest

from sentinel.instrumentation.terminal_errors import MAX_LINE_CHARS, terminal_error_lines


@pytest.mark.parametrize(
    "line",
    [
        ": java.lang.OutOfMemoryError: Cannot reserve 8192 bytes of direct buffer memory",
        "Caused by: java.lang.OutOfMemoryError: Java heap space",
        'Exception in thread "main" java.lang.StackOverflowError',
        "# There is insufficient memory for the Java Runtime Environment to continue.",
        "# A fatal error has been detected by the Java Runtime Environment:",
        "Killed",
    ],
)
def test_terminal_errors_are_found(line) -> None:
    assert terminal_error_lines(["ok", line, "ok"]) == [line.strip()]


@pytest.mark.parametrize(
    "line",
    [
        "py4j.protocol.Py4JNetworkError: Answer from Java side is empty",
        "ConnectionRefusedError: [Errno 111] Connection refused",
        "pipelines.jobs.spark.SchemaMismatchError: orders: missing required columns",
        "java.lang.IllegalArgumentException: bad path",  # an Exception, not an Error
        "WARN MemoryStore: Not enough space to cache rdd_3_0 in memory",
        "INFO killed 0 tasks",
    ],
)
def test_ordinary_errors_are_not_terminal(line) -> None:
    assert terminal_error_lines([line]) == []


def test_first_distinct_lines_in_log_order_capped() -> None:
    oom = "java.lang.OutOfMemoryError: Java heap space"
    lines = [oom, "x", oom, "java.lang.StackOverflowError", "Killed"]
    assert terminal_error_lines(lines, limit=2) == [oom, "java.lang.StackOverflowError"]
    assert terminal_error_lines(["java.lang.OutOfMemoryError " + "z" * 900]) == [
        ("java.lang.OutOfMemoryError " + "z" * 900)[:MAX_LINE_CHARS]
    ]
