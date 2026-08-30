"""`python -m litelink` — check this machine can run a log. See `_preflight`."""

from __future__ import annotations

import sys

from litelink._preflight import main

if __name__ == "__main__":
    sys.exit(main())
