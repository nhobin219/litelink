# Append-cap matrix

Run [`../append_cap.py`](../append_cap.py) for the append-cap comparison. This is a
**rebuild from thread 447's published description, not the original scripts**.
The source method and prior results are in
[the published matrix](https://github.com/nhobin219/litelink/pull/222#issuecomment-6624345473).
Do not pool these results with that earlier experiment or call them an exact
reproduction. The original payload constructor and dependency lock were not
published. This harness defines its own synthetic 22-channel station response,
padded with JSON whitespace to the described 1,627 body bytes, and a new pinned
runtime manifest. Its endpoint, field names and resulting logical row size can
differ; the worker records body and retained row bytes. It makes no performance
claim until the complete matrix has run on the chosen machine.

## Setup and invocation

Linux `/proc/stat`, GNU `du`/`df`, git, uv and CPython 3.11 are required. Run from the repository
root. Use scratch on the same local filesystem as the logs you intend to measure;
setup resolves dependencies over the network, measurement uses loopback only.
The consumer checkout is the published source revision, overriding its declared
`litelink<0.15` dependency for this experiment. No installed library or consumer
pin changes. Neither source tree is edited.

```bash
BENCH_SCRATCH="$PWD/../append-cap-run"
mkdir -p "$BENCH_SCRATCH"
export UV_CACHE_DIR="$BENCH_SCRATCH/uv-cache"
export UV_PYTHON_INSTALL_DIR="$BENCH_SCRATCH/uv-python"
export UV_PYTHON_BIN_DIR="$BENCH_SCRATCH/uv-bin"
uv venv "$BENCH_SCRATCH/venv" --python 3.11
uv pip install --python "$BENCH_SCRATCH/venv/bin/python" \
  -r benchmarks/append_cap_requirements.txt
git clone https://github.com/nhobin219/streamcast "$BENCH_SCRATCH/streamcast"
git fetch origin pull/222/head:refs/remotes/origin/pr222
"$BENCH_SCRATCH/venv/bin/python" benchmarks/append_cap.py \
  --repo . --base b1b4e3506e157507f1fc3a72e586451a954fcaed \
  --off origin/pr222 --on origin/pr222 \
  --consumer-repo "$BENCH_SCRATCH/streamcast" \
  --output "$BENCH_SCRATCH/results" --repetitions 15 --seconds 30
```

This command runs the full matrix; it is intentionally **not a smoke command**.
`--base`, `--off`, `--on` and `--consumer` accept git refs in their respective
repositories or paths to clean git checkouts. BASE defaults to the published
merge base. OFF and ON must have identical Python source digests; OFF disables
`_buffer._BUFFER_USAGE_ENABLED` before creating any disposable log, ON enables it.
BASE does not touch that switch. The consumer ref defaults to
`84e86d533be43945673aa8e374c7cb5cf8931ff0`.
Source commits and digests are recorded; refs are exported into the new output
directory so a moving branch does not change an experiment mid-run.

A brief plumbing check (three cells, two ABBA repetitions, one call per run):

```bash
"$BENCH_SCRATCH/venv/bin/python" benchmarks/append_cap.py \
  --off origin/pr222 --on origin/pr222 \
  --consumer-repo "$BENCH_SCRATCH/streamcast" \
  --output "$BENCH_SCRATCH/smoke" --smoke --repetitions 2 --seconds .001 \
  --warmup 0 --max-calls 1 --bootstrap 50 \
  --cells direct/0/16/1 e2e/0/256/0 e2e/0/256/1
```

Smoke mode reports `SMOKE`, never an overall gate PASS. Non-smoke runs require at
least 15 repetitions and 10,000 bootstrap draws. `--seconds` is the minimum timed
submission window; each in-flight request is drained afterward. `--max-calls`
bounds retained latency samples (default 100,000/run). Hitting that bound before
the window finishes prevents a cell PASS. `--warmup` configures untimed calls or
requests (default 16). `--batches` and `--cells` permit selected experiments;
an incomplete matrix never receives an overall PASS. Use a fresh output path
for each run; the harness refuses to overwrite or append to an earlier result.
Exit zero means execution completed, including a FAIL decision; read
`summary.json` for the statistical verdict. Worker/setup failures exit nonzero.

### Disk reserve and supervision

Reserve **3 GiB plus the largest per-window transient**, as well as space for the
environment, source snapshots and evidence. The large direct cells can temporarily
use roughly **4.5 GB per window** at 60-second windows; this is an estimate, not a
quota, and faster storage or longer windows can require more. Allow additional
headroom on a shared filesystem. Only one trial directory exists at a time; it is
removed after each window. Retained logical row bytes are not on-disk allocation.

Use the portable supervisor for a long run, inside an externally capped process
scope (CPU, memory, tasks and runtime). Supply the same controller arguments after
`--`, with `--output` supplied only to the supervisor:

```bash
"$BENCH_SCRATCH/venv/bin/python" benchmarks/append_cap_supervise.py \
  --output "$BENCH_SCRATCH/results" --status "$BENCH_SCRATCH/status.json" -- \
  --repo . --base b1b4e3506e157507f1fc3a72e586451a954fcaed \
  --off origin/pr222 --on origin/pr222 \
  --consumer-repo "$BENCH_SCRATCH/streamcast" --repetitions 15 --seconds 60
```

The output and status paths must be fresh, their parents must exist, and status
must be outside output. The supervisor records `df` free bytes and percent used at
launch, every 30 seconds (configurable), and completion. Its status file is
atomically replaced, retaining launch and latest samples. An abort additionally
retains the triggering sample in `disk_at_abort`, before terminating the controller
and workers. The internal deadline defaults to 46 hours. Archive this status file
with the results; no upload or machine resource caps are installed by the wrapper.

The guard compares **free bytes + live trial allocated bytes** with the 3 GiB
reserve: subtracting the reclaimable trial from used space distinguishes external
pressure from the run's own transient. Actual free space and live trial size remain
visible separately. `du` is sampled before `df`; these are non-atomic diagnostic
samples, so a concurrent trial cleanup can over-credit one sample. The guard does
not guarantee space for an unbounded window; provision the reserve and transient
above. A failed live `du` aborts with an explicit accounting-error reason, retaining
`df` and the error, rather than claiming external pressure. Worker `trial_du_bytes`
records allocated bytes with GNU `du -s -B1`, after
verification/close and before removal, outside timing; this is a final size, not a
peak sampler. Periodic live samples can include open WAL files.

The aborted partial 60-second run recorded **112 windows, repetition 0 only** before
its old supervisor's disk guard stopped it. Those results **must not be pooled**
with a fresh run. They do not form complete paired repetitions or a gate result;
retain them only as diagnostic evidence. Restart with fresh output and status paths.

## Matrix and measurement

The 18 default cells are direct batches 1/16/256/4096 × empty/2,000-ASCII-byte
payload × modeled group off/on, plus actual end-to-end publish with broker group
off/on. Direct rows are non-null event_ts:int64, source:string (`sensor-00001`),
value:float64, payload:string: logical row sizes 45 and 2,045 bytes. Group-on
flattens at most eight queued application batches, stopping after the job that
reaches 10,000 rows: physical appends 8/128/2048/12288. Flattening is timed.
Direct p99 is physical append service latency, excluding queue waiting.

End-to-end runs the real consumer ASGI application and WebSocket publisher on
loopback port 0, one worker and one durable writer, eight requests in flight,
256 capture rows/request, max_inbound=65536, actual broker group_commit off/on.
Latency starts before submit/encoding and ends when the ACK task resumes.
Throughput includes draining every ACK. Each ACK count and final dense stored
offsets are checked. This is durable broker publish acknowledgement, not Iceberg
object-store publication. There are no subscribers, publisher deduplication,
compression, maintainer, eviction, remote store, replication or stats sampler.

Each trial creates a fresh log in WAL at synchronous=FULL, verified on the writer
connection. The seal target and default byte cap are 8 GiB; no seal/maintenance
runs. A run that reaches a cap aborts rather than relaxing it. Input generation,
log/server setup, warmup, explicit GC, post-run verification and teardown are
outside timing. Normal Python GC and SQLite checkpoints remain enabled. Stored
rows must match all warmup and measured ACKs; ON counters must match the exact
logical row-byte totals. Logs are removed after each trial, including on failure.

Every repetition and cell runs OFF–ON–ON–OFF and BASE–ON–ON–BASE. Odd repetitions
reverse to ON–OFF–OFF–ON and ON–BASE–BASE–ON and reverse reference order. Cells
rotate and reverse between repetitions. This gives 30 BASE, 30 OFF and 60 ON
trials per cell at the 15-repetition minimum, including 15 ABBA blocks
per comparison. Only one worker measures at a time. Workers stay alive across
trials so dependency imports are outside measurement. No observation is dropped,
retried, ranked or selected as a best run. ABBA balances linear time drift; it
does not eliminate nonlinear scheduling/storage noise.

## Statistics, gate and artifacts

Per-call quantiles use nearest rank. For each variant, per-repetition medians of
its run throughput and p99 values feed a median estimate and a percentile
bootstrap 95% CI. Ratios use only the ON and reference trials in the corresponding
ABBA block: median the two observations per variant per repetition, then compute
ratio of medians across repetitions. Resample whole repetition indices together
for numerator and denominator, with replacement, 10,000 draws, seed 447221 by
default. The resampling unit is the repetition, not individual correlated ACKs.
These are separate marginal intervals, not simultaneous familywise guarantees;
ABBA repetitions may still share machine-state effects. Longer windows are
needed for precise per-run p99; small sample counts remain visible in raw data.

For **each** direct batch >=16 and **both** end-to-end cells, ON must clear both
OFF and BASE independently: throughput-ratio CI lower bound >=0.98 and p99-ratio
CI upper bound <=1.05. Crossing either limit is FAIL, including unresolved noise.
Batch 1 is report-only. There is no cross-cell average. Every cell and reference
prints its verdict, absolute throughput/p99 intervals and ratio intervals; the
full matrix passes only if every required cell passes.

- `environment.json`: source commits/digests, harness hashes, interpreter, platform,
  CPU affinity and invocation settings.
- `runs.jsonl`: all ordered trials, repetition/reference/ABBA position, raw
  latencies, rates/p50/p99, row/byte verification, package versions and SQLite
  version; **before/after aggregate `/proc/stat` tick vectors and steal counts**
  immediately surround each timed window. Total ticks exclude guest fields
  already counted in user/nice. Steal delta/total and percentage are system-wide,
  not process CPU time; a window with no tick delta records a null percentage.
  `trial_du_bytes` records the trial directory's allocated disk size before deletion.
- `summary.json`: per-cell estimates/CIs, paired ratio CIs, window completion,
  cell and overall decisions. No summary is produced for an aborted experiment.
- Variant stderr files and exported source snapshots remain for diagnosis. Remove
  the output directory when its evidence has been archived. Data copies and sample
  retention are bounded by selected cells, repetitions, calls and the 8 GiB cap.

Only tiny smoke settings are used to validate this rebuild. The full matrix and
its gate are a separate measurement task. Statistical unit checks can run with
`python -m pytest benchmarks/test_append_cap.py -q`; they require only the
repository's existing pytest development dependency.
