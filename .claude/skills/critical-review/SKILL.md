---
name: critical-review
description: This skill should be used when the user asks to "run the reviewer loop", "review this to convergence", "critically review", "run the critical reviewer", or when a change to litelink is complete and needs adversarial review before merging. Governs how to drive the `critical-reviewer` subagent, how many to run, and when the loop is finished.
version: 0.1.0
---

# Critical review, to convergence

litelink is the durable record of a stream. A defect is not a wrong answer; it is rows
acknowledged and then gone, present twice, or unreadable. Review is therefore adversarial and
empirical, and it runs until a reviewer says it is clean — not until a round completes.

## Use the `critical-reviewer` subagent

`.claude/agents/critical-reviewer.md`. It is `model: fable`, tools `Bash, Read, Grep, Glob` —
**no Write or Edit**, which is the guard against a reviewer clobbering in-flight work.

```
Agent(subagent_type="critical-reviewer", prompt=...)
```

**If the harness reports the type is not registered**, fall back to `general-purpose` with
that file's rubric pasted verbatim into the prompt, and say in the reply that the fallback was
used. Do not silently substitute a general-purpose reviewer with a prompt of your own devising
— its bar will be wrong and it will report style notes as findings.

## One at a time

The agent's own description says **"Expensive. Use for a specific doubt about a change — not
as a routine second pass."**

- **One reviewer per round.** Not three in parallel.
- Parallel reviewers on one box each build logs and may each start `rustfs`. Check `free -h`
  first; this machine has ~7.7 GB total.
- If a change genuinely has two unrelated risk surfaces, run them **sequentially**, and only
  when the first comes back.

## Give it 2–3 specific questions

Not a five-area sweep. Name the questions that could kill the design, phrased as *find the
state where X breaks*, and map each to an invariant:

- **I9** — offset reuse. "Find a state where an offset is issued twice."
- **I4 / I6** — a file that breaks `sync`, `evict`, `archived_prefix`, or `_union`.
- **I2** — ordering against a crash. "State what a crash between these two durable writes
  leaves, and whether anything reconciles it."

## Tell it what is already verified

The rubric says **"Do not re-derive what you were told is verified."** Re-verifying covered
ground is most of what makes a round expensive. List, as bullets, the measurements you already
have — with the numbers, so it can tell whether its own run agrees.

## The loop

Repeat until a round returns **NO CRITICAL FINDINGS**:

1. **Fix** each finding. Do not argue with a finding that has a repro.
2. **Falsify each fix.** Break the code deliberately; confirm the test fails; restore; confirm
   it passes. A fix without a falsified test is not done.
3. **Send it back** with `SendMessage` to the same agent — it keeps its context and knows what
   it already checked. Say what changed, what you falsified, and what you deliberately did
   *not* change and why.
4. **Expect new findings from a fix.** This is normal and is why the loop exists: in one arc a
   fix for a regex that fabricated breaking changes introduced two ways of silently losing
   them. Round two is not a formality.

The loop is finished when a reviewer that has seen the current code says it is clean. It is
**not** finished because a round produced no fixes you felt like making.

## Safety while a reviewer is running

A reviewer running `Bash` can still write files even without Edit.

- **Do not edit files the reviewer is falsifying.** It snapshots to a scratchpad and restores
  from that copy; if you edit in between, your edit is silently reverted. This has happened —
  four completed fixes were lost and only a re-run of the suite caught it.
- **Every scripted patch asserts its anchor.** `assert s.count(old) == 1` before `replace`. A
  `replace` whose anchor does not match no-ops and leaves no trace; that is how a re-apply
  landed incomplete after a clobber.
- **Restore from a snapshot taken immediately before**, and verify with `diff`. Never
  `git checkout` — it discards uncommitted work.
- **After any bulk removal, diff test names against `HEAD`**, not the pass count. A suite that
  silently lost three tests still reports success.

## What is not this skill's job

The bar is I2/I4/I6/I9 — data loss, silent duplication, unreadable data, offset reuse, or a
wedged log. Completeness, staging, API shape, docs debt and prose are **not** critical
findings. Review those yourself, or with a general-purpose agent under their own name. A
report that grades everything the same makes the reader escalate the wrong thing.

## Reporting back

Relay findings, not process. State plainly when a reviewer found something you had already
declared safe, and when a fix of yours caused the next round's finding — that history is the
argument for running the loop at all.
