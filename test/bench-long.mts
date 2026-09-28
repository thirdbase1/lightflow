
// Long-workflow bench: RN env = steps per workflow (default 400), 50 runs.
import { Engine, step, registerWorkflow } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";

const url = process.env.LIGHTFLOW_PG_URL;
const RN = Number(process.env.RN ?? 400);
const RUNS = 50;
const store = await createPostgresStore(url);
const engine = new Engine(store, { pollMs: 25 });
const worker = engine.startWorker();
registerWorkflow("benchlong", async () => {
  for (let i = 0; i < RN; i++) {
    await step(async () => i);
  }
  return "ok";
});
const ids = [];
const t0 = performance.now();
for (let i = 0; i < RUNS; i++) {
  ids.push((await engine.start("benchlong", [])).runId);
}
for (const id of ids) {
  const run = await engine.getRun(id);
  await run.returnValue;
}
const secs = (performance.now() - t0) / 1000;
engine.stopWorker();
await worker;
console.log(JSON.stringify({ stepsPerWorkflow: RN, runs: RUNS, seconds: +secs.toFixed(2), runsPerSec: +(RUNS / secs).toFixed(1), stepsPerSec: +((RUNS * RN) / secs).toFixed(0) }));
process.exit(0);
