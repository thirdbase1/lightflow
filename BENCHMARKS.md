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

## Reproducing

`test/bench.ts` honours `BENCH_N` (runs), `BENCH_M` (steps per run),
`BENCH_CONC` (in-flight). `bench-long.mjs` honours `RN` (steps) and runs 50
sequential-ish workflows through `start()` + terminal-poll.

Every version's table above was produced on the same machine and database,
so cross-version rows are directly comparable.
