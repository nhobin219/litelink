"""Smoke coverage for the packaging itself.

Deliberately not vacuous: pytest exits 5 on an empty suite, so a repo with no
tests at all makes the CI test job a no-op that still reports green.
"""

import importlib.util
from pathlib import Path

import litelink
from litelink import LogConfig, WriteHandle


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
    with litelink.new(tmp_path, "trades", schema=module.SCHEMA, config=config) as log:
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

    Two names failed this. `S3Options` appears in the signatures of `new`,
    `open`, `restore` and `replication_config_for`, and reaching it meant
    `from litelink.log import S3Options` — an import that works because
    `log.py` happens to import the name, rather than an interface. `Row` was
    worse: an alias under `if TYPE_CHECKING`, named by `append` and `extend`
    and importable from nowhere at all, because it existed to a type checker
    and to nothing else.

    Written against the whole surface rather than those two names, because the
    defect is structural: anything `WriteHandle` asks a caller to pass can grow the
    same hole.

    Collected from the SOURCE, which is what an earlier version of this test
    got wrong. It read runtime namespaces, and `inspect.isclass` over an
    imported module cannot see a `TYPE_CHECKING` alias at all — so the check
    passed over `Row` while claiming to cover exactly it. Parsing also lets the
    annotations stay source text, which they have to: `log.py` keeps its typing
    imports under `TYPE_CHECKING`, so resolving them at runtime fails on names
    that were never imported.

    Parameters only. A returned object is used rather than named, and
    `recovery()` deliberately hands back a private record.
    """
    import ast
    import inspect
    import pkgutil
    import re

    def declared(body: list[ast.stmt]) -> set[str]:
        """Public names a block binds that could appear in an annotation.

        Descends into `if`, so a `TYPE_CHECKING` block counts. Not into
        functions or classes: a name bound in there is not importable from the
        module, so it cannot be what a caller is told to name.
        """
        names: set[str] = set()
        for node in body:
            if isinstance(node, ast.ClassDef):
                names.add(node.name)
            elif isinstance(node, ast.If):
                names |= declared(node.body) | declared(node.orelse)
            elif isinstance(node, ast.Assign | ast.AnnAssign):
                # An alias is a subscript or a dotted name. A constant is
                # neither, which is what keeps `CONFIG_KEY = "config"` out.
                if not isinstance(node.value, ast.Subscript | ast.Attribute):
                    continue

                targets = (
                    [node.target] if isinstance(node, ast.AnnAssign) else node.targets
                )
                names |= {t.id for t in targets if isinstance(t, ast.Name)}

        return {name for name in names if not name.startswith("_")}

    package = Path(litelink.__file__).parent
    nameable: set[str] = set()
    for info in pkgutil.iter_modules([str(package)]):
        source = (package / f"{info.name}.py").read_text()
        nameable |= declared(ast.parse(source).body)

    # Fixture rot: if the collector stops seeing the two names this test was
    # written for, everything below passes and proves nothing.
    assert {"S3Options", "Row"} <= nameable, f"collector missed a fixture: {nameable}"

    leaked: dict[str, set[str]] = {}
    for name in dir(WriteHandle):
        if name.startswith("_"):
            continue

        attribute = inspect.getattr_static(WriteHandle, name)
        target = attribute.__func__ if isinstance(attribute, classmethod) else attribute
        if not inspect.isfunction(target):
            continue

        for parameter in inspect.signature(target).parameters.values():
            if parameter.annotation is inspect.Parameter.empty:
                continue

            named = set(re.findall(r"\w+", str(parameter.annotation)))
            unreachable = named & nameable - set(litelink.__all__)
            if unreachable:
                leaked.setdefault(name, set()).update(unreachable)

    assert not leaked, (
        f"public parameters name types that are not importable from litelink: "
        f"{ {call: sorted(types) for call, types in leaked.items()} }"
    )
