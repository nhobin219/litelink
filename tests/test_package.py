"""Smoke coverage for the packaging itself.

Deliberately not vacuous: pytest exits 5 on an empty suite, so a repo with no
tests at all makes the CI test job a no-op that still reports green.
"""

from pathlib import Path

import litelink


def test_version_is_a_string() -> None:
    assert isinstance(litelink.__version__, str)
    assert litelink.__version__


def test_py_typed_ships_with_the_package() -> None:
    """Without this marker the package's annotations are invisible downstream."""
    marker = Path(litelink.__file__).parent / "py.typed"

    assert marker.is_file()
