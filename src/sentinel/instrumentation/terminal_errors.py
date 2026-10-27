"""Lines that say why a JVM (or the process hosting it) died.

PySpark prints the JVM's fatal error early, then fills the rest of the task log with the
Py4J teardown, so a fixed-size log tail can lose the one line that explains the failure.
The failure callback keeps these lines from anywhere in the log, and the diagnosis tool
returns them in their own field. Phase 5c (post-hoc): the benchmark's OOM faults were all
diagnosed `unknown` because of this.

Phase 5d (post-hoc): one OOM is re-raised several times ("Caused by: ...", "Lost task
... : ...", different log prefixes), which filled all five slots with the same error.
Near-duplicates (the same error text from the match on, ignoring numbers) now collapse to
the first line, with a count.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

TERMINAL_ERROR_RE = re.compile(
    r"java\.lang\.\w*Error\b"  # OutOfMemoryError, StackOverflowError, InternalError, ...
    r"|There is insufficient memory for the Java Runtime Environment"
    r"|A fatal error has been detected by the Java Runtime Environment"
    r"|Container killed .* exceeding memory limits"
    r"|^\s*Killed\b"  # the kernel OOM killer, as bash reports it
)
# How the failure callback marks a collapsed line, so the tool can recover the count.
COUNT_SUFFIX_RE = re.compile(r" \[sentinel: (\d+)x in this log\]$")
MAX_LINES = 5
MAX_LINE_CHARS = 300


@dataclass
class TerminalError:
    line: str  # the first occurrence, verbatim (stripped, capped)
    count: int  # occurrences of this error in the log, near-duplicates included

    def annotated(self) -> str:
        return (
            self.line if self.count == 1 else f"{self.line} [sentinel: {self.count}x in this log]"
        )


def _key(line: str, start: int) -> str:
    return re.sub(r"\d+", "#", line[start:].strip())


def terminal_errors(lines: Iterable[str], limit: int = MAX_LINES) -> list[TerminalError]:
    """The first `limit` distinct terminal errors, in log order (the first is the cause;
    later ones are usually the same error re-raised), each with how often it occurred."""
    found: dict[str, TerminalError] = {}
    plain: dict[str, int] = {}  # unannotated occurrences per error
    tagged_max: dict[str, int] = {}  # the count a collapsed (annotated) line carries
    for raw in lines:
        tagged = COUNT_SUFFIX_RE.search(raw.rstrip())
        line = raw.rstrip()[: tagged.start()] if tagged else raw
        m = TERMINAL_ERROR_RE.search(line)
        if not m:
            continue
        key = _key(line, m.start())
        if key not in found:
            if len(found) >= limit:
                continue
            found[key] = TerminalError(line.strip()[:MAX_LINE_CHARS], 0)
        if tagged:
            tagged_max[key] = max(tagged_max.get(key, 0), int(tagged.group(1)))
        else:
            plain[key] = plain.get(key, 0) + 1
    for key, e in found.items():
        # An annotated line already counts every occurrence in the original log, so the
        # plain copies of it (e.g. in a kept tail) are a subset, not extra occurrences.
        e.count = max(tagged_max.get(key, 0), plain.get(key, 0))
    return list(found.values())


def terminal_error_lines(lines: Iterable[str], limit: int = MAX_LINES) -> list[str]:
    """The lines of terminal_errors(), annotated with their count when it is above one."""
    return [e.annotated() for e in terminal_errors(lines, limit)]
