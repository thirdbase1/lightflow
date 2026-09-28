# lightflow — Benchmarks

All numbers measured locally on the dev machine: Node 22, Postgres 16,
localhost socket (no network hop). Reproduce with:

```bash
npm run build
export LIGHTFLOW_PG_URL=postgres://user:***@localhost:5432/lightflow
node dist/test/bench.js          # short-workflow throughput
node dist/test/bench-long.mjs    # long-workflow throughput (RN=<steps>)
node --test dist/test/race.js    # concurrency correctness
```

Benchmarks live in [`test/bench.ts`](test/bench.ts) and
[`test/bench-long.mjs`](test/bench-long.mjs). SQL-statement counts come from
`pg_stat_statements`.

---

## v0.1.0 — baseline

The starting point: advisory-locked, statement-per-concern persistence.

| Metric | Value |
|---|---|
| Runs / sec (200 runs × 5 steps, conc 20) | **184** |
| Steps / sec | **922** |
| SQL statements per event append | 5 (BEGIN, advisory lock, max(seq), INSERT, COMMIT) |
| Advisory lock avg time in hot path | **7.8 ms** (40× every other query) |
| `writable.write` | re-fetches full event log per write |
| Racing resumes | can double-execute a memoized step (latent) |
| `wakeAt` timer index | present but **silently unused** (text/bigint cast mismatch) |
| Worker timer wake | ~4 queries + full event-log fetch per due timer |
| Replay memo lookup | O(n) linear scan → O(n²) replay |

---

## v0.1.1 — event-append round

**Focus: per-event write cost.** Profiled with `pg_stat_statements`, then:

- `appendEvent` fast path: single `INSERT … ON CONFLICT DO NOTHING` (caller
  supplies seq; the `(run_id, seq)` unique constraint arbitrates races).
  Advisory lock only on rare collisions.
- **Run lease** (`claimed_until`): atomic claim/release around each replay —
  concurrent `resume()` calls can no longer re-execute a memoized step.
- Dropped `step_started` events (replay never reads them).
- `writable.write` uses the in-memory replay log, not a re-fetch.
- `createRun`: single CTE round trip. `waitFor`: adaptive poll 5→250 ms.

| Metric | v0.1.0 | v0.1.1 | Δ |
|---|---|---|---|
| Runs / sec (200×5, conc 20) | 184 | **456–556** | **~2.7×** |
| Steps / sec | 922 | **~2,300–2,800** | **~2.7×** |
| SQL per event append | 5 | 1 | −80% |
| Advisory lock in hot path | every event | removed | — |
| Double-execution under 20 racing resumes | possible | **0 in 200 races** | fixed |

Sustained (500 runs × 10 steps, conc 100): **445 runs/s, 4,450 steps/s**.

---

## v0.1.2 — replay & timer round

**Focus: replay cost and timer dispatch.** Researched Temporal's "replay
debt", PgQue's notify pattern, and Absurd's minimal-query design, then:

- **O(1) replay memo**: event log indexed once into a `Map` at replay start.
- **Fixed the timer index**: expression index on `((payload->>'wakeAt')::bigint)`.
- **`dueTimers` anti-join** against `sleep_completed` (returns key too) —
  worker completes a timer in 2 queries instead of ~4 + full-log fetch.
- **Adaptive worker poll** (50 ms → configured, backs off when idle) plus an
  optional LISTEN/NOTIFY wakeup nudge (lossy; polling remains authoritative).

| Metric | v0.1.1 | v0.1.2 | Δ |
|---|---|---|---|
| Steps / sec, 400-step workflows (50 runs) | 8,230 | **9,070** | +10% |
| Runs / sec, 100-step workflows (50 runs) | — | 65 (6,470 steps/s) | new |
| Queries per timer wake | ~4 + full log fetch | 2 | −60%+ |
| Replay memo lookup | O(n) | O(1) | — |
| Worker idle poll floor | 500 ms fixed | 50 ms adaptive + notify | — |

---

## v0.1.3 — compaction & client round

**Focus: replay-from-scratch cost, client overhead, polling waste.**

- **Snapshot compaction** (the Temporal `ContinueAsNew` problem, designed out):
  every 200 completed steps, memoized state folds into a single `snapshot`
  event; replay resumes from the latest snapshot instead of seq 0. A 700-step
  run mid-way through sleeps and resumes correctly through snapshots.
- **pg pipelining** (`pg >= 8.23`, one option): queries batch per connection —
  biggest wins under concurrency and on networked Postgres.
- **Zero-poll `returnValue`**: in-process callers await a deferred promise;
  the DB is only polled by cross-process callers (adaptive 5→250 ms).

| Metric | v0.1.2 | v0.1.3 |
|---|---|---|
| Runs / sec, 500×10 @ conc 100 | — | **497** |
| Steps / sec, 400-step workflows | 9,070 | 8,956–10,658* |
| Steps / sec, 600-step workflows | — | 10,055 |
| `returnValue` DB polls (same process) | every 5–250 ms | **0** |
| Replay cost for an old run | O(total events) | O(events since snapshot) |

\* range across runs; the long-workflow number varies with how many
snapshots land before each resume.

---

## v0.1.4 — round-trip & fsync round

**Focus: query-count per run and the commit durability dial.**

- **Merged control round trips**: `claimRun` returns the cancelled flag with
  the lease; new `finishRun` writes terminal status and releases the lease in
  one UPDATE. Control queries per run: 6 → 4.
- **Pipelined step appends**: consecutive `step_completed` events share one
  flush (buffered in `ctx.inflight`, awaited at suspension/terminal/snapshot
  boundaries). Verified: 700-step sleep-resume path stays correct.
- **Opt-in async commit** (`sessionOptions: { synchronous_commit: 'off' }`):
  documented tradeoff — the last few transactions may be lost on a server
  crash (never corruption). Off by default; durability remains the default.

| Metric | v0.1.3 | v0.1.4 | Δ |
|---|---|---|---|
| Runs / sec (200×5, conc 20) | 437–480 | **502–512** | +8% |
| Runs / sec (500×10, conc 100) | 497 | **503–512** | +2% |
| Steps / sec (400-step) | 8,956 | **9,017–9,939** | +5% |
| Steps / sec (400-step, async commit opt-in) | — | **11,004** | +23% vs durable |
| Control queries per run | 6 | 4 | −33% |

---

## Reproducing

`test/bench.ts` honours `BENCH_N` (runs), `BENCH_M` (steps per run),
`BENCH_CONC` (in-flight). `bench-long.mjs` honours `RN` (steps) and runs 50
sequential-ish workflows through `start()` + terminal-poll.

Every version's table above was produced on the same machine and database,
so cross-version rows are directly comparable.

## v0.2.x — compat layer (all versions re-benchmarked)

Each published version was benchmarked from its published tarball against the
same Postgres (16, local) and the same harness (200 runs x 5 steps @ conc 20;
50 runs x 400 steps). Steps/wf = 5 for short, 400 for long.

| Version | Short runs/s | Short steps/s | Long steps/s |
|---|---|---|---|
| 0.2.0 | 462.7 | 2,313 | 8,493 |
| 0.2.1 | 466.0 | 2,330 | 9,062 |
| 0.2.2 | 466.9 | 2,335 | 8,348 |
| 0.2.3 | 461.6 | 2,308 | 8,384 |
| 0.2.4 | 456.1 | 2,280 | 8,280 |
| 0.2.5 | 459.1 | 2,295 | 9,068 |
| 0.2.6 | 494.9–525.1 | 2,474–2,626 | 8,379–9,737 |
| 0.2.7 | 416.0 | 2,080 | 9,180 |
| 0.2.8 | 455.2 | 2,276 | 8,695 |
| 0.2.9 | 474.1 | 2,371 | 9,008 |
| 0.2.10 | 455.3 | 2,277 | 9,368 |

### What changed per version (all perf-neutral by design)

- **0.2.0** — Vercel Workflow drop-in compat (`compat/workflow`,
  `compat/api`, `compat/next`). Two data-loss bugs fixed en route: chunk
  memo keys now include the durable-timer position (`w:<sleepCalls>:<writes>`)
  so post-resume writes can't be skipped against pre-timer chunks; live chunk
  appends now fold into snapshot compaction (chunks previously vanished from
  `getReadable` after ~200 steps).
- **0.2.1** — `run.status` is a live getter (fresh promise per access).
  entry-agents' startStopMonitor re-awaits it in a 150ms poll loop; a single
  memoized promise left the monitor blocked until terminal.
- **0.2.2** — `makeStep(fn, {retries})`: durable wrapper for arg-taking
  step functions (entry's "use step" style).
- **0.2.3–0.2.4** — packaging: explicit `types` in subpath exports,
  declaration files emitted. No runtime change.
- **0.2.5** — `rootDir: "src"` so the published `dist/` layout actually
  matches the subpath export map (0.2.0–0.2.4 shipped `dist/src/...` while
  exports claimed `dist/...` — subpath imports still worked because Node
  resolved the package root, but the types were broken).
- **0.2.6** — `export { workflowFetch as fetch }` (entry imports both
  names), `WorkflowFn` accepts typed args, `run.exists: true`.
- **0.2.7** — lazy `returnValue` on run handles: no unhandled rejection
  when a status-only handle outlives a failed run. Added
  `test/compat-runtime.ts` (makeStep args+retries, FatalError, ghost runs).
- **0.2.8–0.2.10** — cross-bundle runtime state for Next.js: engine/store,
  workflow/step registries, and the execution context moved to globalThis
  because instrumentation and route handlers load as separate module
  instances. Found by a live end-to-end Entry chat run; zero measurable
  cost (globalThis lookup vs module global).

### Spread notes

Short-run spread across versions is ~2% (456–467 runs/s) — within run-to-run
noise; the compat layer added no measurable overhead. 0.2.6 shows the best
short-run numbers (494–525 runs/s across repeated runs) and the long-run
8.3k–9.7k steps/s band matches the v0.1.4-era 9.9k within noise.

## Reproducing

Each version's tarball can be re-benchmarked independently:

```bash
npm pack lightflow-engine@<version>
tar xzf lightflow-engine-<version>.tgz -C <dir> --strip-components=1
ln -s <path-to>/lightflow/node_modules <dir>/node_modules
PKG=<dir> LIGHTFLOW_PG_URL=postgres://... BENCH_N=200 RN=5 npx tsx bench-pkg.mts
PKG=<dir> LIGHTFLOW_PG_URL=postgres://... BENCH_N=50 RN=400 npx tsx bench-pkg.mts
```
