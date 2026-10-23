"""Process exit for command-line entry points that use deltalake."""

from __future__ import annotations

import os
import sys
from typing import NoReturn


def exit_now(code: int) -> NoReturn:
    """Flush output, then exit without interpreter teardown.

    deltalake 1.6.6's Rust runtime threads can abort the process during Python
    finalization (SIGABRT, "terminate called without an active exception", exit 134)
    *after* all work is done. Measured in the Airflow container: 19 of 20 short scripts
    that read a Delta table aborted on exit; 0 of 20 with os._exit. Use it only at the
    very end of a CLI, after every write is committed and every sink flushed.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)
