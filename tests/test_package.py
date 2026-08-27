"""Smoke coverage for the packaging itself.

Deliberately not vacuous: pytest exits 5 on an empty suite, so a repo with no
tests at all makes the CI test job a no-op that still reports green.
"""

import importlib.util
from pathlib import Path

import litelink
from litelink import Log, LogConfig


def test_version_is_a_string() -> None:
    assert isinstance(litelink.__version__, str)
    assert litelink.__version__


def test_py_typed_ships_with_the_package() -> None:
    """Without this marker the package's annotations are invisible downstream."""
    marker = Path(litelink.__file__).parent / "py.typed"

    assert marker.is_file()


def test_the_websocket_example_builds_a_readable_log(tmp_path: Path) -> None:
    """The example's own loop, against a recorded frame rather than the network.

    `websocket.py` connects to a live public exchange, which is the point of it
    — no producer to start, no credentials — and is exactly why the test does
    not run the script. §14 requires the suite to pass with no network at all,
    and a test that skips when the internet is down covers nothing on the day
    it matters.

    So this imports the two pieces the script actually owns — its schema and
    its frame decoder — and drives the same append/seal loop over a frame
    captured from the real feed. What is left untested is `websockets.connect`,
    which is not ours.
    """
    example = Path(__file__).resolve().parent.parent / "examples" / "websocket.py"
    spec = importlib.util.spec_from_file_location("ws_example", example)

    assert spec is not None and spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    frame = {
        "id": 624438572,
        "timestamp": "1787772776",
        "amount": 0.0076347,
        "price": 78501.62,
        "type": 0,
        "microtimestamp": "1787772776240000",
        "buy_order_id": 2043649235279894,
        "sell_order_id": 2043649227448330,
    }
    config = LogConfig(target_seal_size=4096, compact_min_files=2)
    with Log.new(tmp_path, "trades", schema=module.SCHEMA, config=config) as log:
        for index in range(400):
            log.append(module.row({**frame, "id": frame["id"] + index}))
            log.seal_due()

        while log.seal() is not None:
            pass

        # Reached Parquet rather than only SQLite, which is what calling
        # `seal_due` in the loop is for — and the closing `seal()` is what gets
        # the OPEN group there, which `seal_due` alone never does.
        assert log.table_files() > 0
        assert log.buffered_rows() == 0
        assert log.scan().read_all().num_rows == 400


def test_every_type_a_caller_must_name_is_exported() -> None:
    """A type in a public parameter has to be importable from the package root.

    `S3Options` was not. It appears in the signatures of `new`, `open`,
    `restore` and `replication_config_for`, and reaching it meant
    `from litelink.log import S3Options` — an import that happens to work
    because `log.py` imports it, rather than an interface. A caller pointing at
    a non-AWS endpoint has to construct one, and a caller annotating against
    those signatures has to spell it.

    Written against the whole surface rather than that one name, because the
    defect is structural: anything `Log` asks a caller to pass can grow the
    same hole. Parameters only — a returned object is used, not named, and
    `recovery()` deliberately hands back a private record.

    Annotations are compared as SOURCE TEXT. `log.py` uses
    `from __future__ import annotations` with its typing imports under
    `TYPE_CHECKING`, so resolving them at runtime fails on names that were
    never imported — and a test that has to import a module's private
    machinery to check its public surface is testing the wrong thing.
    """
    import inspect
    import pkgutil
    import re

    public_classes = set()
    for info in pkgutil.iter_modules(litelink.__path__):
        module = importlib.import_module(f"litelink.{info.name}")
        for name, member in vars(module).items():
            if (
                inspect.isclass(member)
                and not name.startswith("_")
                and getattr(member, "__module__", "").startswith("litelink")
            ):
                public_classes.add(name)

    assert "S3Options" in public_classes, "the fixture below would prove nothing"

    leaked: dict[str, set[str]] = {}
    for name in dir(Log):
        if name.startswith("_"):
            continue

        attribute = inspect.getattr_static(Log, name)
        target = attribute.__func__ if isinstance(attribute, classmethod) else attribute
        if not inspect.isfunction(target):
            continue

        for parameter in inspect.signature(target).parameters.values():
            if parameter.annotation is inspect.Parameter.empty:
                continue

            named = set(re.findall(r"\w+", str(parameter.annotation)))
            unreachable = named & public_classes - set(litelink.__all__)
            if unreachable:
                leaked.setdefault(name, set()).update(unreachable)

    assert not leaked, (
        f"public parameters name types that are not importable from litelink: "
        f"{ {call: sorted(types) for call, types in leaked.items()} }"
    )
