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

## Vercel Workflow drop-in compat

lightflow ships a compatibility layer mirroring the exact Vercel Workflow
API surface (`start(fn, args)`, sync `getRun()`, `run.status` Promise,
`run.getReadable({ startIndex })` + `getTailIndex()`, `run.cancel()`,
`run.returnValue`, `getWritable()` as a web `WritableStream`,
`getWorkflowMetadata()`, `sleep(Date)`, `FatalError`, `withWorkflow`).
See [COMPAT.md](./COMPAT.md) for the 3-step swap guide and honest
differences list.

### Self-hosting the worker (Pxxl, VPS, Fly, Railway)

Serverless hosts freeze between requests, so the polling worker must live on
an always-on runtime. See [docs/brimble-worker.md](./docs/brimble-worker.md)
for a step-by-step Brimble deployment (first-class Worker service type,
process-liveness health, managed Postgres, env vars) or
[docs/pxxl-worker.md](./docs/pxxl-worker.md) for Pxxl (worker service,
Multi-Service `pxxl.toml`, secrets, health check). The same pattern works on
any VPS or long-running container host:
`LIGHTFLOW_PG_URL=... node dist/worker.js`.

```ts
import { createPostgresStore } from "lightflow-engine/pg";
import { Engine } from "lightflow-engine";
import { initWorkflowApi } from "lightflow-engine/compat/api";

const store = await createPostgresStore(process.env.LIGHTFLOW_PG_URL!);
const engine = new Engine(store, { pollMs: 50 });
await engine.startWorker();
initWorkflowApi(store, engine);
```

## Benchmarks

Full before/after numbers for every version live in [BENCHMARKS.md](BENCHMARKS.md).

Headline: **184 → ~500 runs/sec (~2.7×)** on the standard workload, with
single-statement event appends, O(1) replay lookups, a run lease that makes
racing replays safe, and LISTEN/NOTIFY-assisted timer wake-up.

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
