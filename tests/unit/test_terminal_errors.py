"""The fatal-JVM-error line scanner shared by the failure callback and the log tool."""

from __future__ import annotations

import pytest

from sentinel.instrumentation.terminal_errors import (
    MAX_LINE_CHARS,
    TerminalError,
    terminal_error_lines,
    terminal_errors,
)


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
    assert terminal_error_lines(lines, limit=2) == [
        f"{oom} [sentinel: 2x in this log]",
        "java.lang.StackOverflowError",
    ]
    assert terminal_error_lines(["java.lang.OutOfMemoryError " + "z" * 900]) == [
        ("java.lang.OutOfMemoryError " + "z" * 900)[:MAX_LINE_CHARS]
    ]


# Phase 5d: the shapes one OOM was re-raised in, in the benchmark's real task logs.
PREFIX = "[2026-09-23T00:06:10.0{}+0000] {{subprocess.py:106}} INFO - "
RERAISED = [
    PREFIX.format(74) + "java.lang.OutOfMemoryError: Cannot reserve 8 bytes of direct "
    "buffer memory (allocated: 32769, limit: 32768)",
    PREFIX.format(75) + "Caused by: java.lang.OutOfMemoryError: Cannot reserve 8 bytes of "
    "direct buffer memory (allocated: 32769, limit: 32768)",
    PREFIX.format(89) + "26/09/23 00:06:09 WARN TaskSetManager: Lost task 42.0 in stage 2.0 "
    "(TID 2) (9fd65d6328ca executor driver): java.lang.OutOfMemoryError: Cannot reserve 8 "
    "bytes of direct buffer memory (allocated: 32769, limit: 32768)",
    PREFIX.format(94) + "Caused by: java.lang.OutOfMemoryError: Cannot reserve 16 bytes of "
    "direct buffer memory (allocated: 32777, limit: 32768)",  # numbers differ: still the same
]


def test_near_duplicate_reraises_collapse_to_the_first_line_with_a_count() -> None:
    errs = terminal_errors(["start", *RERAISED, "py4j teardown", "Killed"])
    assert errs == [TerminalError(RERAISED[0].strip(), 4), TerminalError("Killed", 1)]
    assert terminal_error_lines(RERAISED) == [RERAISED[0] + " [sentinel: 4x in this log]"]


def test_an_annotated_line_keeps_its_count_and_its_copies_are_not_added() -> None:
    # What the tool sees: the callback's collapsed line ahead of a tail that still holds
    # two of the same re-raises. Those are among the 4, not 2 more.
    head = terminal_error_lines(RERAISED)
    errs = terminal_errors([*head, "teardown", *RERAISED[2:]])
    assert errs == [TerminalError(RERAISED[0].strip(), 4)]
    assert terminal_errors(RERAISED[:1] * 7) == [TerminalError(RERAISED[0].strip(), 7)]
