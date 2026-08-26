# Security policy

litelink is pre-1.0 and maintained by one person. This document says what to report, how,
and what to expect back — including where the honest limits are.

## Supported versions

`main`, and the most recent tag. There are no backports: a fix lands on `main` and the next
tag carries it.

## Reporting a vulnerability

**Use GitHub's private vulnerability reporting** — the Security tab of this repository,
"Report a vulnerability". It opens a private advisory visible only to the maintainers.

Please do not open a public issue for anything that could put someone's data at risk. If
private reporting is unavailable to you, open an issue that says only that you have a
security report and asks for a private channel — no details, no reproduction.

Include what you would want if you were fixing it: the version or commit, what you did, what
happened, and what you expected. A failing test against this repository is the ideal form.

## What is in scope

This library's promise is that a row is durable when `append()` returns and that what comes
back out is what went in. Anything that breaks that is a security issue here, not merely a
bug:

- **Committed data lost or corrupted.** A row acknowledged by `append()` that does not
  survive a crash, a power loss, or a restore.
- **Reads that lie.** A scan that drops rows, double-counts them, or returns rows belonging
  to another log — including across the buffer / local table / archive boundary.
- **Offsets reissued.** `litelink_offset` is monotonic and never reused; a failover or
  `Log.restore` that hands out an offset the dead machine already served breaks every
  guarantee built on it.
- **Credentials escaping into data.** The library reads credentials from the environment at
  the point of use and deliberately never writes them into a log directory or a generated
  config. A path that persists them anywhere on disk is a vulnerability, because log
  directories get copied, backed up, and attached elsewhere.
- **Writes outside the log root.** Any input — an archive path, a restored file name, a
  catalog row — that causes a read or write outside the directory the caller named.
- **Denial of service through ordinary input**, such as a row shape that makes the write path
  allocate without bound.

## What is out of scope

- **Unbounded local growth with no retention and no archive.** Documented in the README and
  in `docs/SPEC.md` §13.7: a local-only log that keeps everything degrades as the table
  grows. It is a known design limit with a stated workaround, not a vulnerability.
- **The demo credentials.** `Justfile` carries `litelink` / `litelink-secret` for a
  throwaway rustfs container bound to localhost. They are fixtures, not secrets.
- **The replication sidecar.** litelink generates a litestream config; it does not run,
  supervise, or ship litestream. Report litestream issues upstream.
- **Anything requiring write access to the log directory already.** A caller who can edit
  `buffer.db` can do anything the library can.

## What to expect

Best effort from one maintainer, not a staffed response. There is no bounty. You will get an
acknowledgement that a human read it, an assessment of whether it reproduces, and credit in
the advisory and the commit unless you would rather not be named.
