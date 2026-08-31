"""`python -m litelink.migrate` — move a log to the per-stream layout.

Public because it is a command someone runs on their own data; the
implementation is in `_migrate`, like `_preflight` behind `python -m litelink`.
"""

from __future__ import annotations

import sys

from litelink._migrate import build_plan, is_legacy, main, migrate

__all__ = ["build_plan", "is_legacy", "main", "migrate"]

if __name__ == "__main__":
    sys.exit(main())
