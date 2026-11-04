"""One isolated source variant; the controller sends serial trials on stdin."""

from __future__ import annotations

import asyncio
import gc
import importlib
import importlib.metadata
import json
import math
import shutil
import socket
import sqlite3
import sys
import time
import traceback
from pathlib import Path
from typing import Any

# The controller selects these source trees before this worker starts. Dynamic
# imports keep the optional broker stack out of the library's lint/type environment.
pa = importlib.import_module("pyarrow")
streamcast = importlib.import_module("streamcast")
uvicorn = importlib.import_module("uvicorn")
litelink = importlib.import_module("litelink")
_buffer = importlib.import_module("litelink._buffer")

VARIANT = sys.argv[1] if __name__ == "__main__" else "BASE"
SCHEMA = pa.schema(
    [
        pa.field("event_ts", pa.int64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("value", pa.float64(), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
    ]
)
CAPTURE_SCHEMA = {
    "type": "object",
    "properties": {
        "event_ts": {"type": "integer"},
        "capture_ts": {"type": "integer"},
        "endpoint": {"type": "string"},
        "status": {"type": "integer"},
        "body": {"type": "string"},
    },
    "required": ["event_ts", "capture_ts", "endpoint", "status", "body"],
}


def cpu() -> dict[str, Any]:
    ticks = [int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    # guest/guest_nice are already included in user/nice by Linux.
    return {
        "ticks": ticks,
        "total": sum(ticks[:8]),
        "steal": ticks[7],
        "at_ns": time.perf_counter_ns(),
    }


def finish(
    latencies: list[int], rows: int, start: int, end: int, before: dict, after: dict
) -> dict:
    delta = after["total"] - before["total"]
    steal = after["steal"] - before["steal"]
    ordered = sorted(latencies)
    return {
        "rows": rows,
        "calls": len(latencies),
        "elapsed_ns": end - start,
        "rows_s": rows / ((end - start) / 1e9),
        "p50_us": ordered[math.ceil(len(ordered) * 0.50) - 1] / 1000,
        "p99_us": ordered[math.ceil(len(ordered) * 0.99) - 1] / 1000,
        "latencies_ns": latencies,
        "cpu_before": before,
        "cpu_after": after,
        "steal_ticks": steal,
        "total_ticks": delta,
        "steal_percent": 100 * steal / delta if delta else None,
    }


def verify(log: Any, root: Path, expected: int, row_bytes: int) -> dict:
    paths = list(root.rglob("buffer.db"))
    assert len(paths) == 1, paths
    with sqlite3.connect(paths[0]) as con:
        actual = con.execute(
            "SELECT count(*), min(litelink_offset), max(litelink_offset) FROM buffer"
        ).fetchone()
        assert actual == (expected, 1, expected), (actual, expected)
        assert con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        # synchronous is connection-local: inspect the actual writer backend.
        backend = log._buffer._backend
        with backend.read() as cursor:
            synchronous = cursor.execute("PRAGMA synchronous").fetchone()[0]

        assert synchronous == 2, synchronous
        size = expected * row_bytes
        if VARIANT == "ON":
            usage = log.buffer_usage()
            assert (usage.rows, usage.bytes) == (expected, size), (
                usage,
                expected,
                size,
            )

    return {
        "verified_rows": expected,
        "verified_bytes": size,
        "row_bytes": row_bytes,
        "synchronous": synchronous,
        "journal_mode": "wal",
    }


def direct(request: dict) -> dict:
    cell = request["cell"]
    root = Path(request["root"])
    row = {
        "event_ts": 1790000000000000,
        "source": "sensor-00001",
        "value": 1.25,
        "payload": "x" * cell["payload"],
    }
    batch = [dict(row) for _ in range(cell["batch"])]
    jobs = min(8, math.ceil(10000 / len(batch))) if cell["group"] else 1
    queued = [batch] * jobs
    physical = len(batch) * jobs
    log = litelink.new(
        root,
        "bench",
        schema=SCHEMA,
        sort_by=("event_ts",),
        config=litelink.LogConfig(target_seal_size=8 << 30),
    )
    try:
        written = 0
        for _ in range(request["warmup"]):
            offsets = log.extend([row for job in queued for row in job])
            assert offsets == list(range(written + 1, written + physical + 1))
            written += physical

        latencies = []
        gc.collect()
        before = cpu()
        start = time.perf_counter_ns()
        deadline = start + int(request["seconds"] * 1e9)
        while len(latencies) < request["max_calls"]:
            began = time.perf_counter_ns()
            offsets = log.extend([row for job in queued for row in job])
            ended = time.perf_counter_ns()
            assert len(offsets) == physical
            latencies.append(ended - began)
            written += physical
            if ended >= deadline:
                break

        end = time.perf_counter_ns()
        after = cpu()
        result = finish(latencies, len(latencies) * physical, start, end, before, after)
        result.update(verify(log, root, written, 45 + cell["payload"]))
        result.update(window_complete=(end - start) >= request["seconds"] * 1e9)
        result.update(
            physical_rows=physical,
            hit_call_bound=len(latencies) == request["max_calls"],
        )
        return result
    finally:
        log.close()


def capture() -> tuple[dict, int]:
    # Synthetic shape only: the published description did not include the body.
    channels = [
        {"channel": i, "temperature": 20.5 + i / 10, "humidity": 45.0, "quality": "ok"}
        for i in range(22)
    ]
    body = json.dumps(
        {"station": "synthetic-01", "channels": channels}, separators=(",", ":")
    )
    assert len(body.encode()) <= 1627
    body += " " * (1627 - len(body.encode()))
    row = {
        "event_ts": 1790000000000000,
        "capture_ts": 1790000000000100,
        "endpoint": "https://example.invalid/observations",
        "status": 200,
        "body": body,
    }
    return row, len(body.encode())


async def publish_window(
    producer: Any, batch: list[dict], seconds: float, max_calls: int
) -> dict:
    latencies = []
    submitted = 0
    pending: set[asyncio.Task] = set()

    async def acknowledged(future: asyncio.Future, began: int) -> None:
        offsets = await future
        ended = time.perf_counter_ns()
        assert len(offsets) == len(batch)
        assert all(offset is not None for offset in offsets)
        latencies.append(ended - began)

    before = cpu()
    start = time.perf_counter_ns()
    deadline = start + int(seconds * 1e9)
    while submitted < max_calls and (
        submitted == 0 or time.perf_counter_ns() < deadline
    ):
        if len(pending) >= 8:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()

            if time.perf_counter_ns() >= deadline:
                break

        began = time.perf_counter_ns()
        future = await producer.submit_many(batch)
        pending.add(asyncio.create_task(acknowledged(future, began)))
        submitted += 1

    await asyncio.gather(*pending)
    end = time.perf_counter_ns()
    after = cpu()
    return finish(latencies, submitted * len(batch), start, end, before, after)


async def e2e(request: dict) -> dict:
    root = Path(request["root"])
    row, body_bytes = capture()
    batch = [dict(row) for _ in range(256)]
    stream = streamcast.StreamHandle.open(
        streamcast.Stream(
            spec=streamcast.StreamSpec(
                name="bench",
                schema=CAPTURE_SCHEMA,
                group_commit=request["cell"]["group"],
                max_inbound=65536,
            ),
            storage=streamcast.Storage(
                root=root, config=litelink.LogConfig(target_seal_size=8 << 30)
            ),
        ),
        initialization="create_if_absent",
    )
    app = streamcast.create_app(
        streams=stream, config=streamcast.BrokerConfig(maintain=False, stats=False)
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            lifespan="off",
            log_level="critical",
            ws_per_message_deflate=False,
            ws="websockets-sansio",
        )
    )
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        sock.setblocking(False)
        async with app.router.lifespan_context(app):
            serving = asyncio.create_task(server.serve(sockets=[sock]))
            try:
                async with asyncio.timeout(15):
                    while not server.started:
                        if serving.done():
                            await serving
                            raise RuntimeError("broker startup failed")

                        await asyncio.sleep(0)

                uri = f"ws://127.0.0.1:{sock.getsockname()[1]}/bench"
                async with streamcast.publish(
                    uri, max_in_flight=8, compression=None
                ) as producer:
                    # Warmup uses the same eight-request pipeline, excluded from timing.
                    warmup = None
                    if request["warmup"]:
                        warmup = await publish_window(
                            producer, batch, 1e9, request["warmup"]
                        )

                    gc.collect()
                    result = await publish_window(
                        producer, batch, request["seconds"], request["max_calls"]
                    )
                    expected = result["rows"] + (warmup["rows"] if warmup else 0)
                    assert stream.log is not None
                    shape = stream.log._buffer.shape()
                    stamped = row | {"streamcast_ts": 0}
                    row_bytes = shape.measure(tuple(stamped[c] for c in shape.columns))
                    result.update(verify(stream.log, root, expected, row_bytes))
                    result.update(
                        body_bytes=body_bytes,
                        window_complete=result["elapsed_ns"]
                        >= request["seconds"] * 1e9,
                        hit_call_bound=result["calls"] == request["max_calls"],
                    )
                    return result
            finally:
                server.should_exit = True
                await serving
                await stream.aclose()


def main() -> None:
    if VARIANT in ("OFF", "ON"):
        if not hasattr(_buffer, "_BUFFER_USAGE_ENABLED"):
            raise RuntimeError("OFF/ON source lacks PR #222 counter switch")

        _buffer._BUFFER_USAGE_ENABLED = VARIANT == "ON"

    packages = {
        d.metadata["Name"]: d.version for d in importlib.metadata.distributions()
    }
    for line in sys.stdin:
        request = json.loads(line)
        root = Path(request["root"])
        try:
            if root.exists():
                raise RuntimeError("trial directory already exists")

            result = (
                direct(request)
                if request["cell"]["kind"] == "direct"
                else asyncio.run(e2e(request))
            )
            result.update(
                packages=packages,
                sqlite=sqlite3.sqlite_version,
                litelink_source=litelink.__file__,
                consumer_source=streamcast.__file__,
            )
        except Exception:
            result = {"error": traceback.format_exc()}
        finally:
            shutil.rmtree(root, ignore_errors=True)

        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
