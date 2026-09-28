# Contributing to lightflow

Thanks for your interest! lightflow is small on purpose (~700 lines of core
code) — the bar is **correctness over features**.

## The one rule

> A completed step must never execute again.

Every PR is judged against this. If your change makes any side effect
re-runnable after a crash + replay, it won't be merged, no matter how clean.

## Setup

```bash
git clone https://github.com/agentuser/lightflow
cd lightflow
npm install

# local Postgres 14+ (Docker is fine)
docker run -d --name lightflow-pg -p 5432:5432 \
  -e POSTGRES_USER=lightflow -e POSTGRES_PASSWORD=lightflow \
  -e POSTGRES_DB=lightflow_test postgres:16

export LIGHTFLOW_PG_URL=postgres://lightflow:lightflow@localhost:5432/lightflow_test
```

## Workflow

1. `npm run build` — must pass with zero errors
2. `npm test` — includes crash-recovery and concurrency suites
3. If you touch the engine or store, **add a kill-test**: SIGKILL the process
   mid-run and assert exactly-once behavior on resume. PRs that change
   durability semantics without a kill-test will be asked for one.

## What we look for

- **Exactly-once side effects** — memoization keyed by call position
- **BIGINT epoch-millis everywhere** — no `timestamptz`, no timezone skew
- **No new dependencies** — the only runtime dep is `pg`
- **Plain SQL over abstractions** — the store is two tables + a few queries

## What we don't take (yet)

- New storage backends (the Postgres store is the product; a generic interface
  exists but is not stable)
- Dashboard/UI (planned, not designed)
- Windows support for the test harness (kill-tests use SIGKILL)

## License

By contributing you agree your contributions are licensed under the MIT
License.
