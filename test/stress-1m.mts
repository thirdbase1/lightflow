
import { Engine } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";

const url = process.env.LIGHTFLOW_PG_URL!;
const store = await createPostgresStore(url);
const engine = new Engine(store);

const N = Number(process.env.STEPS || 100000);
const t0 = Date.now();
const { registerWorkflow, step } = await import("../src/index.js");
registerWorkflow("stress100k", async function (n: number) {
    let acc = 0;
    for (let i = 1; i <= n; i += 1) {
      acc = await step(async () => i) as unknown as number;
      if (i % 50000 === 0) console.log("progress", i, Date.now() - t0, "ms");
    }
    return acc;
  });
const h = await engine.start("stress100k", [N]);
// wait for terminal
for (;;) {
  const row = await store.getRun(h.runId);
  if (row && row.status !== "running") {
    console.log(JSON.stringify({ runId: h.runId, status: row.status, steps: N, ms: Date.now() - t0 }));
    break;
  }
  await new Promise((r) => setTimeout(r, 1000));
}
process.exit(0);
