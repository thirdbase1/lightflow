import { test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import { Engine, step } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
import { initWorkflowApi, start } from "../src/compat/api.js";

const PG_URL = process.env.LIGHTFLOW_PG_URL!;

test("snapshot compaction prunes folded events without breaking replay", async () => {
  const store = await createPostgresStore(PG_URL!);
  const engine = new Engine(store, { pollMs: 25 });
  initWorkflowApi(store, engine);
  const pool = new pg.Pool({ connectionString: PG_URL });

  const h = await start(async function pruner() {
    let acc = 0;
    for (let i = 1; i <= 450; i += 1) {
      acc = await step(async () => i) as unknown as number;
    }
    return acc;
  }, []);

  // wait for completion (no sleeps; fast)
  for (let i = 0; i < 600; i += 1) {
    const row = await pool.query("SELECT status FROM lightflow_runs WHERE run_id=$1", [h.runId]);
    if (row.rows[0]?.status !== "running") break;
    await new Promise((r) => setTimeout(r, 50));
  }
  const row = await pool.query("SELECT status FROM lightflow_runs WHERE run_id=$1", [h.runId]);
  assert.equal(row.rows[0].status, "completed");

  // snapshots exist; folded pre-snapshot events pruned
  const types = await pool.query(
    "SELECT type, count(*)::int AS n FROM lightflow_events WHERE run_id=$1 GROUP BY type", [h.runId]);
  const byType = Object.fromEntries(types.rows.map((r) => [r.type, r.n]));
  assert.ok(byType.snapshot >= 1, "expected at least one snapshot");
  // step_completed events BELOW the first snapshot seq are pruned
  const firstSnapSeq = await pool.query(
    "SELECT min(seq)::int AS s FROM lightflow_events WHERE run_id=$1 AND type='snapshot'", [h.runId]);
  const oldSteps = await pool.query(
    "SELECT count(*)::int AS n FROM lightflow_events WHERE run_id=$1 AND type='step_completed' AND seq < $2",
    [h.runId, firstSnapSeq.rows[0].s]);
  assert.equal(oldSteps.rows[0].n, 0, "pre-snapshot step_completed events should be pruned");
  // chunks preserved (none here) and replay correctness: run completed with correct value
  const out = await pool.query("SELECT output FROM lightflow_runs WHERE run_id=$1", [h.runId]);
  assert.equal((out.rows[0].output as { returnValue?: number }).returnValue ?? out.rows[0].output, 450);

  await pool.end();
});
