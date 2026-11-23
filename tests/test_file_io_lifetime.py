"""The shared FileIO bound also bounds clients awaiting cyclic collection."""

from __future__ import annotations

import gc
import weakref
from collections import OrderedDict
from pathlib import Path

import pytest
from pyarrow.fs import FileSystem
from pyiceberg.io.pyarrow import PyArrowFileIO

from litelink import _table


@pytest.mark.parametrize("scheme", ["file", "s3"])
def test_evicted_clients_are_released_without_cyclic_gc(
    monkeypatch: pytest.MonkeyPatch, scheme: str
) -> None:
    monkeypatch.setattr(_table, "_FILE_IOS", OrderedDict())
    monkeypatch.setattr(_table, "_FILE_IOS_LIMIT", 3)
    owners: list[weakref.ReferenceType[PyArrowFileIO]] = []
    clients: list[weakref.ReferenceType[FileSystem]] = []
    enabled = gc.isenabled()
    gc.disable()
    try:
        for cycle in range(12):
            properties = {
                "s3.access-key-id": "synthetic",
                "s3.secret-access-key": "synthetic",
                "s3.region": "us-east-1",
                "warehouse": str(cycle),
            }
            io = _table.shared_file_io(properties, f"{scheme}://example/data")
            assert isinstance(io, PyArrowFileIO)
            client = io.fs_by_scheme(scheme, "example")
            owners.append(weakref.ref(io))
            clients.append(weakref.ref(client))
            for _ in range(5):
                assert (
                    _table.shared_file_io(properties, f"{scheme}://example/other") is io
                )
                assert io.fs_by_scheme(scheme, "example") is client

            del io, client
            assert sum(owner() is not None for owner in owners) <= 3
            assert sum(client() is not None for client in clients) <= 3

        _table._FILE_IOS.clear()
        assert all(owner() is None for owner in owners)
        assert all(client() is None for client in clients)
    finally:
        if enabled:
            gc.enable()


def test_eviction_preserves_an_in_use_client_and_its_open_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_table, "_FILE_IOS", OrderedDict())
    monkeypatch.setattr(_table, "_FILE_IOS_LIMIT", 1)
    path = tmp_path / "data"
    path.write_bytes(b"still readable")
    io = _table.shared_file_io({}, str(path))
    assert isinstance(io, PyArrowFileIO)
    client = weakref.ref(io.fs_by_scheme("file", ""))
    source = io.new_input(str(path))
    owner = weakref.ref(io)
    with source.open() as stream:
        _table.shared_file_io({"warehouse": "other"}, str(path))
        assert io.fs_by_scheme("file", "") is client()
        assert stream.read() == b"still readable"

    del io
    assert owner() is None
    # A returned file owns its filesystem independently of the evicted FileIO.
    with source.open() as stream:
        assert stream.read() == b"still readable"

    del source
    assert client() is None


@pytest.mark.parametrize(
    ("property_name", "other"),
    [
        ("s3.access-key-id", "other-key"),
        ("s3.secret-access-key", "other-secret"),
        ("s3.session-token", "other-token"),
        ("s3.endpoint", "http://other.invalid"),
    ],
)
def test_filesystem_reuse_is_scoped_by_credentials_and_endpoint(
    monkeypatch: pytest.MonkeyPatch, property_name: str, other: str
) -> None:
    monkeypatch.setattr(_table, "_FILE_IOS", OrderedDict())
    properties = {
        "s3.access-key-id": "synthetic",
        "s3.secret-access-key": "synthetic",
        "s3.session-token": "synthetic",
        "s3.region": "us-east-1",
        "s3.endpoint": "http://example.invalid",
    }
    first = _table.shared_file_io(properties, "s3://example/data")
    second = _table.shared_file_io(
        {**properties, property_name: other}, "s3://example/data"
    )
    assert isinstance(first, PyArrowFileIO)
    assert isinstance(second, PyArrowFileIO)
    assert first is not second
    assert first.fs_by_scheme("s3", "example") is not second.fs_by_scheme(
        "s3", "example"
    )
