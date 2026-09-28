
import { test } from "node:test";
import assert from "node:assert/strict";
import { Engine, step, sleep, FatalError } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
import { initWorkflowApi, start, getRun } from "../src/compat/api.js";
import { getWritable, getWorkflowMetadata } from "../src/compat/workflow.js";
import pg from "pg";

const url = process.env.LIGHTFLOW_PG_URL!;

test("cancel is terminal: racing completion cannot resurrect a cancelled run", async () => {
  const pool = new pg.Pool({ connectionString: url });
  const store = await createPostgresStore(url);
  const engine = new Engine(store);
  initWorkflowApi(store, engine);

  const slow: any = await start(async function victim(id: string) {
    const w = getWritable<string>().getWriter();
    await w.write("started");
    await sleep(new Date(Date.now() + 60_000)); // long timer
    await w.write("after-timer");
    return "done";
  }, ["a"]);

  // give the run time to suspend on its timer
  await new Promise((r) => setTimeout(r, 400));
  const h = slow.runId ? await getRun(slow.runId) : slow;
  await h.cancel();
  assert.equal(await h.status, "cancelled");

  // force a resume attempt (as the reaper/worker would)
  await engine.resume(slow.runId).catch(() => {});
  // status must STILL be cancelled
  assert.equal(await (await getRun(slow.runId)).status, "cancelled");
  const row = await pool.query("SELECT status, cancelled FROM lightflow_runs WHERE run_id=$1", [slow.runId]);
  assert.equal(row.rows[0].cancelled, true);
  assert.equal(row.rows[0].status, "cancelled");

  await pool.end();
  engine.stopWorker();
});
