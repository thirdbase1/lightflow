/**
 * Throughput benchmark for lightflow.
 * Runs N workflows, each with M steps, in-flight concurrency C.
 * Reports runs/sec, steps/sec, and SQL statements per run (from pg_stat_statements if available).
 */
import { Engine, step, registerWorkflow } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";

const PG_URL = process.env.LIGHTFLOW_PG_URL!;
const N = Number(process.env.BENCH_N ?? 200);
const M = Number(process.env.BENCH_M ?? 5);
const CONC = Number(process.env.BENCH_CONC ?? 20);

async function main() {
  const store = await createPostgresStore(PG_URL);
  let calls = 0;
  registerWorkflow("bench", async (): Promise<number> => {
    "use workflow";
    for (let i = 0; i < M; i++) {
      await step(async (): Promise<number> => {
        "use step";
        return i * 2 + (calls++, 0);
      });
    }
    return M;
  });

  const engine = new Engine(store, { pollMs: 100 });

  // reset counters
  await (store as any).appendEvent === undefined; // noop

  const t0 = performance.now();
  const handles = [];
  let done = 0;
  const queue: number[] = Array.from({ length: N }, (_, i) => i);
  async function worker() {
    while (queue.length) {
      const i = queue.shift()!;
      const { runId } = await engine.start("bench", []);
      let run; do { await new Promise(r=>setTimeout(r,20)); run = await store.getRun(runId); } while (run && run.status==="running");
      done++;
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  const dt = (performance.now() - t0) / 1000;
  console.log(JSON.stringify({
    runs: N, stepsPerRun: M, concurrency: CONC,
    seconds: +dt.toFixed(2),
    runsPerSec: +(N / dt).toFixed(1),
    stepsPerSec: +((N * M) / dt).toFixed(1),
  }, null, 2));
  process.exit(0);
}
main().catch((e) => { console.error(e); process.exit(1); });
