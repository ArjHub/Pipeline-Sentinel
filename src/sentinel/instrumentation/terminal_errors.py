"""Lines that say why a JVM (or the process hosting it) died.

PySpark prints the JVM's fatal error early, then fills the rest of the task log with the
Py4J teardown, so a fixed-size log tail can lose the one line that explains the failure.
The failure callback keeps these lines from anywhere in the log, and the diagnosis tool
returns them in their own field. Phase 5c (post-hoc): the benchmark's OOM faults were all
diagnosed `unknown` because of this.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

TERMINAL_ERROR_RE = re.compile(
    r"java\.lang\.\w*Error\b"  # OutOfMemoryError, StackOverflowError, InternalError, ...
    r"|There is insufficient memory for the Java Runtime Environment"
    r"|A fatal error has been detected by the Java Runtime Environment"
    r"|Container killed .* exceeding memory limits"
    r"|^\s*Killed\b"  # the kernel OOM killer, as bash reports it
)
MAX_LINES = 5
MAX_LINE_CHARS = 300


def terminal_error_lines(lines: Iterable[str], limit: int = MAX_LINES) -> list[str]:
    """The first `limit` distinct matching lines, in log order (the first is the cause;
    later ones are usually the same error re-raised)."""
    out: list[str] = []
    for line in lines:
        text = line.strip()[:MAX_LINE_CHARS]
        if TERMINAL_ERROR_RE.search(line) and text not in out:
            out.append(text)
            if len(out) >= limit:
                break
    return out
