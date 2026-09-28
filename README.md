# lightflow

**A tiny durable workflow engine for Node.js and Postgres.**

Steps that survive crashes. Timers that survive restarts. Streams that replay.
~700 lines of TypeScript, two Postgres tables, one dependency (`pg`).

```bash
npm install lightflow-engine
```

## Why

If you've ever needed a workflow where *the server can die at any moment* and
the work must continue correctly — background jobs, agent loops, billing
pipelines, sandbox lifecycle management — you've had to choose between:

- **Temporal / Cadence**: extremely powerful, extremely heavy to operate
- **Vercel Workflow / Inngest**: excellent — Vercel Workflow is open source
  (Apache-2.0) and self-hostable via `@workflow/world-postgres`; lightflow
  trades its adapter/SDK surface for a single engine you can read end-to-end
- **BullMQ / plain queues**: a queue is not durable execution — one crash and
  you re-run side effects (and re-charge your LLM provider)

**lightflow** is the middle path: a real durable-execution engine you can run
yourself in an afternoon. It gives you the primitives that matter:

| Primitive | What it means |
|---|---|
| `step()` | Side effects run **at most once** — memoized in Postgres |
| `sleep(Date \| ms)` | Timers survive process death; a worker wakes them |
| `getWritable()` | Ordered streams with **exactly-once** emission and replay |
| `start()` / `getRun()` | Launch runs, resume by ID, await results |
| `FatalError` | Errors that must never be retried |
| `cancel()` | Idempotent cancellation that propagates to the run |
| Hooks | Durable external callbacks a workflow can await |
| Reaper | Stuck `running` runs are automatically recovered |

## The one rule

> **A completed step must never execute again.**

That's the correctness contract everything else serves. If a step charged a
credit card or committed to GitHub, replaying it after a crash is a bug.
lightflow keys every step by its deterministic call position and memoizes the
result in Postgres — verified under `SIGKILL`-mid-flight stress tests.

## Quickstart

```bash
# 1. A Postgres database (any instance, any version)
export LIGHTFLOW_PG_URL=postgres://user:pass@localhost:5432/lightflow

# 2. Install
npm install lightflow-engine
```

```ts
import { Engine, step, sleep, getWritable, FatalError } from "lightflow-engine";
import { createPostgresStore } from "lightflow-engine/pg";

// 1. Define a workflow — plain async functions
async function onboardingWorkflow(user: { id: string; email: string }) {
  const writable = getWritable<string>();

  await writable.write("stage:start");

  // Side effects are durable: retried on transient failure (3 attempts),
  // never re-executed once completed.
  const token = await step(async () => sendWelcomeEmail(user.email));

  // Durable timer — the process can die here; a worker wakes the run later.
  await sleep(new Date(Date.now() + 24 * 60 * 60 * 1000));

  await writable.write("stage:followup-sent");
  await writable.close();

  return { userId: user.id, token, done: true };
}

// 2. Register + run
const store = await createPostgresStore(process.env.LIGHTFLOW_PG_URL!);
const engine = new Engine(store, { pollMs: 300, staleRunMs: 60_000 });

engine.register("onboarding", onboardingWorkflow);
void engine.startWorker(); // background loop: timers + stale-run recovery

const { runId } = await engine.start("onboarding", [
  { id: "u1", email: "ada@example.com" },
]);

// 3. Stream results (replayable — new clients can reconnect mid-run)
const run = await engine.getRun(runId);
for await (const chunk of run.getReadable()) {
  console.log(chunk); // "stage:start" ...
}
console.log(await run.returnValue);
```

That's the whole API.

## Retry classification

```ts
import { step, FatalError } from "lightflow-engine";

// Transient errors: retried with exponential backoff (default 3 attempts).
await step(async () => callFlakyApi());

// Permanent errors: never retried, run fails immediately.
await step(async () => {
  if (badInput) throw new FatalError("cannot process this input");
  return doWork();
});
```

## Cancellation

```ts
const run = await engine.getRun(runId);
await run.cancel(); // idempotent; safe to call twice
```

Cancellation marks the run terminal and is checked at every suspend/resume
boundary, so in-flight sleeps and steps wind down cleanly.

## Hooks (wait for the outside world)

```ts
// Inside a workflow:
const { token } = await defineHook().create();
const approval = await hookResult<boolean>(token); // suspends the run

// Outside (HTTP handler, CLI, another service):
await store.resolveHook(token, { approved: true }); // run wakes up
```

## Crash-recovery, tested

```text
30 concurrent workflows × 20 durable timers each
SIGKILL the entire process mid-flight
resume in a fresh process

→ 30/30 completed, 0 stuck, 0 duplicate side effects, 0 duplicate chunks
```

Kill-and-resume is a first-class path, not an edge case. The engine includes a
**reaper** that resumes runs wedged in `running` — a failure mode we hit in
other engines and designed out here.

## Benchmarks

Throughput measured with [`test/bench.ts`](test/bench.ts) — plain `node`
against a local Postgres 16, no network hop. Run it yourself:

```bash
npm run build
LIGHTFLOW_PG_URL=postgres://... node dist/test/bench.js
```

### v0.1.0 → v0.1.1 (before / after)

Same machine, same workload (200 runs × 5 steps, 20 in flight):

| | v0.1.0 | v0.1.1 | |
|---|---|---|---|
| Runs / sec | 184 | 456–556 | **~2.7× faster** |
| Steps / sec | 922 | ~2,300–2,800 | **~2.7× faster** |
| SQL statements per event | 5 (BEGIN, advisory lock, max(seq), INSERT, COMMIT) | 1 (single INSERT … ON CONFLICT) | −80% |
| Advisory lock in hot path | every event, 7.8ms avg | removed | — |
| Racing resumes double-executing a step | possible (latent) | impossible (run lease) | fixed |

Sustained load, v0.1.1 (500 runs × 10 steps, 100 in flight):
**445 runs/sec, 4,450 steps/sec.**

What changed in v0.1.1, and why it matters:

- **Single-statement event append.** The old path took an advisory lock and
  scanned `max(seq)` per event; profiling showed the lock averaging 7.8ms —
  40× the cost of any other query. The new path relies on the `(run_id, seq)`
  unique constraint and a single upsert.
- **Run lease.** Concurrent `resume()` calls for the same run are arbitrated
  by an atomic `claimed_until` column, so two replays can never execute the
  same step at once (verified by `test/race.ts`: 20 racing resumes × 10 runs,
  zero double-executions).
- **No `step_started` writes** (replay never reads them), stream writes use
  the in-memory replay log instead of re-fetching the event log, `createRun`
  is one round trip, and `getRun().returnValue` polls adaptively (5ms → 250ms).

## Design decisions

- **All timestamps are BIGINT epoch milliseconds.** No `timestamptz`, no
  timezone skew. (A naive-UTC bug in another engine cost us 8 hours of silent
  workflow delay; this schema makes that class of bug impossible.)
- **Plain JSON state only.** Workflows receive and return serializable data.
  No closures across the boundary — reconstruct clients inside steps.
- **Two tables.** `lightflow_runs` + `lightflow_events`. Event-sourced replay:
  the workflow function is re-executed and step results are restored from the
  event log, so the engine can be debugged with plain SQL.

## Comparison

| | lightflow | Temporal | Vercel Workflow | BullMQ |
|---|---|---|---|---|
| Durable steps | ✅ | ✅ | ✅ | ❌ |
| Durable timers | ✅ | ✅ | ✅ | delayed jobs |
| Replayable streams | ✅ | ❌ | ✅ | ❌ |
| Dependencies | 1 (`pg`) | many | multi-package | redis |
| Lines of core code | ~700 | 100k+ | large monorepo | ~10k |

Vercel Workflow is itself open source (Apache-2.0) and self-hostable via its
Postgres world (`@workflow/world-postgres`) — lightflow is the alternative for
when you want a single small engine you can read end-to-end, not an SDK with
an adapter system.

## Status

**Pre-1.0 / experimental.** The core is stress-tested (crash recovery,
concurrency, exactly-once steps and streams). Not yet battle-tested in
production. Known gaps:

- No bundler plugin — `step()` is an explicit call, not a `"use step"` directive
- Single-writer per run (multi-worker is safe but one worker wins; no
  leader election yet)
- No Web dashboard (query Postgres directly for now)

## Contributing

PRs welcome. Run tests with a local Postgres:

```bash
export LIGHTFLOW_PG_URL=postgres://localhost/lightflow_test
npm test   # covers crash recovery + concurrency
```

## License

MIT
