"""The same ownership/interleaving contract on local files and real local S3."""

from __future__ import annotations

import json
import multiprocessing
import os
import stat
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

import litelink
from litelink import _lease
from litelink._lease_store import Conflict, FileStore, S3Store


@pytest.fixture(params=["file", pytest.param("s3", marks=pytest.mark.s3)])
def location(request, tmp_path):
    if request.param == "s3":
        return (
            f"s3://{request.getfixturevalue('bucket')}/published",
            request.getfixturevalue("s3"),
        )

    return tmp_path.as_uri(), None


@pytest.fixture
def clock(monkeypatch):
    clock = SimpleNamespace(now=time.time())
    monkeypatch.setattr(_lease, "time", SimpleNamespace(time=lambda: clock.now))

    return clock


def make(location, owner="a", **kwargs):
    published, options = location

    return litelink.lease(
        published, "events", owner=owner, s3_options=options, **kwargs
    )


def test_published_writer_claim(location):
    a, b = make(location), make(location, "b")
    assert a.state() is None
    held = a.acquire()
    assert held.owner == "a" and held.token and held.expires_at > time.time()
    assert b.state() == held
    with pytest.raises(litelink.Leased) as error:
        b.acquire()

    assert error.value.holder == held
    assert error.value.expires_at == held.expires_at
    with pytest.raises(RuntimeError, match="already acquired"):
        a.acquire()


def test_renew_rotates_version_even_at_same_clock_tick(location, clock):
    a = make(location)
    held = a.acquire()
    renewed = a.renew()
    assert renewed == replace(held, token=renewed.token)
    assert renewed.token != held.token
    assert a.state() == renewed


def test_expiry_and_skew_are_conservative(location, clock):
    a, b = make(location, ttl_s=10, skew_s=2), make(location, "b", skew_s=2)
    held = a.acquire()
    clock.now = held.expires_at - 2
    with pytest.raises(litelink.LeaseLost, match="expired"):
        a.renew()

    clock.now = held.expires_at
    assert b.state() == held  # Observation does not conceal an expired record.
    with pytest.raises(litelink.Leased):
        b.acquire()

    clock.now += 2
    assert b.acquire().owner == "b"
    with pytest.raises(litelink.LeaseLost):
        a.release()


def test_release_tombstone_and_reacquire(location):
    a, b = make(location), make(location, "b")
    held = a.acquire()
    a.release()
    assert a.state() is None
    raw, _ = a._store.read()
    tombstone = json.loads(raw)
    assert tombstone["owner"] == "" and tombstone["expires_at"] == 0
    assert tombstone["token"] != held.token
    with pytest.raises(litelink.LeaseLost):
        a.release()

    assert b.acquire().owner == "b"
    with pytest.raises(litelink.LeaseLost):
        a.renew()

    b.release()
    assert a.acquire().owner == "a"


@pytest.mark.parametrize("operation", ["renew", "release"])
def test_takeover_cannot_be_overwritten_by_stale_operation(
    location, clock, monkeypatch, operation
):
    a, b = make(location, ttl_s=10), make(location, "b")
    first = a.acquire()
    write = a._store.write
    successor = []

    def take_over_before_cas(data, expected):
        # The old operation passed its deadline check; now it stalls while
        # its lease expires and another writer commits. No sleeps required.
        clock.now = first.expires_at
        successor.append(b.acquire())
        assert a.state() == successor[0]  # state must not adopt b's ETag.

        return write(data, expected)

    monkeypatch.setattr(a._store, "write", take_over_before_cas)
    with pytest.raises(litelink.LeaseLost, match="changed"):
        getattr(a, operation)()

    assert b.state() == successor[0]


@pytest.mark.parametrize("expired", [False, True])
def test_two_acquirers_read_the_same_version_exactly_one_wins(
    location, clock, monkeypatch, expired
):
    a, b = make(location), make(location, "b")
    if expired:
        old = make(location, "old").acquire()
        clock.now = old.expires_at

    barrier = Barrier(2)
    for handle in (a, b):
        read = handle._store.read
        first = [True]

        def synchronized_read(read=read, first=first):
            record = read()
            if first[0]:
                first[0] = False
                barrier.wait(timeout=10)

            return record

        monkeypatch.setattr(handle._store, "read", synchronized_read)

    def acquire(handle):
        try:
            return handle.acquire()
        except litelink.Leased as exc:
            return exc

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(acquire, (a, b)))

    winners = [r for r in results if isinstance(r, litelink.Held)]
    losers = [r for r in results if isinstance(r, litelink.Leased)]
    assert len(winners) == len(losers) == 1
    assert losers[0].holder == winners[0]
    assert make(location).state() == winners[0]


def _process_acquire(published, barrier, queue):
    handle = litelink.lease(
        published, "events", owner=str(multiprocessing.current_process().pid)
    )
    read = handle._store.read
    first = True

    def synchronized_read():
        nonlocal first
        record = read()
        if first:
            first = False
            barrier.wait(timeout=10)

        return record

    patch = pytest.MonkeyPatch()
    patch.setattr(handle._store, "read", synchronized_read)
    try:
        handle.acquire()
        queue.put("won")
    except litelink.Leased:
        queue.put("lost")
    finally:
        patch.undo()


def test_file_claim_arbitrates_processes(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    barrier, queue = ctx.Barrier(2), ctx.Queue()
    children = [
        ctx.Process(target=_process_acquire, args=(tmp_path.as_uri(), barrier, queue))
        for _ in range(2)
    ]
    try:
        for child in children:
            child.start()

        assert sorted(queue.get(timeout=20) for _ in children) == ["lost", "won"]
        for child in children:
            child.join(timeout=10)
            assert child.exitcode == 0

    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
                child.join(timeout=10)

        queue.close()


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"{",
        b"{}",
        b'{"owner":"a","token":"t","expires_at":NaN}',
        b'{"owner":"","token":"t","expires_at":7}',
    ],
)
def test_corrupt_record_is_an_error_never_absence(location, payload):
    a = make(location)
    a._store.write(payload, None)
    with pytest.raises(ValueError):
        a.state()

    with pytest.raises(ValueError):
        a.acquire()


def test_names_and_published_prefixes_isolate_leases(location):
    published, options = location
    a = make(location)
    a.acquire()
    assert (
        litelink.lease(published, "other", owner="b", s3_options=options)
        .acquire()
        .owner
        == "b"
    )
    assert make((published + "/other", options)).acquire().owner == "a"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"owner": ""},
        {"ttl_s": 0},
        {"ttl_s": -1},
        {"ttl_s": float("nan")},
        {"ttl_s": float("inf")},
        {"skew_s": -1},
        {"skew_s": 60},
    ],
)
def test_invalid_options_refused_before_io(tmp_path, kwargs):
    with pytest.raises(ValueError):
        litelink.lease(tmp_path.as_uri(), "events", **({"owner": "a"} | kwargs))

    assert list(tmp_path.iterdir()) == []


def test_unknown_backend_refused():
    with pytest.raises(ValueError, match="s3://"):
        litelink.lease("memory://bucket", "events", owner="a")


@pytest.mark.s3
@pytest.mark.parametrize("ignored", ["IfMatch", "IfNoneMatch"])
def test_s3_silently_ignored_condition_refused(s3, bucket, monkeypatch, ignored):
    original = S3Store.write

    def ignores_one_condition(self, data, expected):
        if (expected is None) == (ignored == "IfNoneMatch"):
            return self.client.put_object(Bucket=self.bucket, Key=self.key, Body=data)[
                "ETag"
            ]

        return original(self, data, expected)

    monkeypatch.setattr(S3Store, "write", ignores_one_condition)
    with pytest.raises(litelink.ConditionalWritesUnsupported, match="ignored"):
        litelink.lease(f"s3://{bucket}", "events", owner="a", s3_options=s3)


@pytest.mark.parametrize(
    "code", ["NotImplemented", "InvalidRequest", "InvalidArgument"]
)
def test_s3_explicitly_unsupported_conditions_refused(code):
    def unsupported(**kwargs):
        raise ClientError({"Error": {"Code": code}}, "PutObject")

    store = S3Store(SimpleNamespace(put_object=unsupported), "bucket", "key")
    with pytest.raises(
        litelink.ConditionalWritesUnsupported, match="If-None-Match and If-Match"
    ):
        store.write(b"data", None)


def test_s3_read_errors_are_not_absence():
    def denied(**kwargs):
        raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")

    with pytest.raises(ClientError, match="AccessDenied"):
        S3Store(SimpleNamespace(get_object=denied), "bucket", "key").read()


def test_file_replace_uses_full_record_version(tmp_path):
    store = FileStore(tmp_path / "_lease.json")
    first = store.write(b"first", None)
    second = store.write(b"second", first)
    with pytest.raises(Conflict):
        store.write(b"stale", first)

    assert store.read() == (b"second", second)


def test_file_replace_fsyncs_before_rename_and_directory_after(tmp_path, monkeypatch):
    from litelink import _lease_store

    store = FileStore(tmp_path / "_lease.json")
    initial = store.write(b"first", None)
    events = []
    sync, rename = _lease_store.os.fsync, _lease_store.os.replace

    def fsync(fd):
        events.append("file-sync" if stat.S_ISREG(os.fstat(fd).st_mode) else "dir-sync")
        sync(fd)

    def replace(source, destination):
        events.append("rename")
        rename(source, destination)

    monkeypatch.setattr(_lease_store.os, "fsync", fsync)
    monkeypatch.setattr(_lease_store.os, "replace", replace)
    store.write(b"second", initial)
    assert events[-3:] == ["file-sync", "rename", "dir-sync"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["_lease.json", "_lease.lock"]


def test_file_incomplete_initial_create_fails_closed(tmp_path, monkeypatch):
    from litelink import _lease_store

    a = litelink.lease(tmp_path.as_uri(), "events", owner="a")
    original = _lease_store.os.fsync

    def crash(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("injected fsync failure")

        original(fd)

    monkeypatch.setattr(_lease_store.os, "fsync", crash)
    with pytest.raises(OSError, match="injected"):
        a.acquire()

    monkeypatch.setattr(_lease_store.os, "fsync", original)
    # No authority was returned; a complete surviving record still excludes
    # a second owner. A partial record instead raises as tested above.
    b = litelink.lease(tmp_path.as_uri(), "events", owner="b")
    with pytest.raises(litelink.Leased):
        b.acquire()


def test_file_create_is_exclusive_and_persists_ancestor_entries(tmp_path, monkeypatch):
    from litelink import _lease_store

    path = tmp_path / "new-prefix" / "events" / "_lease.json"
    opened, synced = [], set()
    descriptors = {}
    original_open, original_sync = _lease_store.os.open, _lease_store.os.fsync

    def open_file(path, flags, *args):
        opened.append((Path(path), flags))

        fd = original_open(path, flags, *args)
        descriptors[fd] = Path(path)

        return fd

    def sync_file(fd):
        synced.add(descriptors[fd])
        original_sync(fd)

    monkeypatch.setattr(_lease_store.os, "open", open_file)
    monkeypatch.setattr(_lease_store.os, "fsync", sync_file)
    FileStore(path).write(b"record", None)
    flags = [flags for target, flags in opened if target == path][0]
    assert flags & os.O_EXCL and flags & os.O_CREAT
    assert {path, *path.parents} <= synced


@pytest.mark.parametrize("reject_creation", [False, True])
def test_s3_rejecting_valid_conditions_is_refused(reject_creation):
    calls = []
    deleted = []

    def put(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1 and not reject_creation:
            return {"ETag": '"initial"'}

        raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")

    backend = S3Store(
        SimpleNamespace(put_object=put, delete_object=lambda **kw: deleted.append(kw)),
        "bucket",
        "lease",
    )
    with pytest.raises(
        litelink.ConditionalWritesUnsupported, match="valid conditional"
    ):
        backend.verify()

    assert len(calls) == (1 if reject_creation else 4)
    assert len(deleted) == (0 if reject_creation else 1)


@pytest.mark.parametrize("endpoint", [None, "https://explicit.invalid"])
def test_lease_uses_the_published_tiers_resolved_endpoint(monkeypatch, endpoint):
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://unexpected.invalid")
    monkeypatch.setattr(S3Store, "verify", lambda self: None)
    options = litelink.S3Options(
        endpoint=endpoint, access_key="test", secret_key="test", region="us-east-1"
    )
    lease = litelink.lease(
        "s3://bucket/prefix", "events", owner="a", s3_options=options
    )
    store = lease._store
    assert isinstance(store, S3Store)
    try:
        resolved = store.client.meta.endpoint_url
        if endpoint is None:
            assert resolved.endswith(".amazonaws.com")
        else:
            assert resolved == endpoint

    finally:
        store.client.close()
