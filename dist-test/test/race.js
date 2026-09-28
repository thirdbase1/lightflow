/**
 * Concurrency stress: each run is resumed by 20 concurrent resume() calls.
 * Contract: exactly one step_completed, zero step_failed per run — racing
 * replays must never re-execute a memoized step (run-lease arbitration).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import { Engine, step, registerWorkflow } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
const PG_URL = process.env.LIGHTFLOW_PG_URL;
if (!PG_URL)
    throw new Error("LIGHTFLOW_PG_URL is required");
test("racing resumes never re-execute a memoized step", async () => {
    const store = await createPostgresStore(PG_URL);
    const pool = new pg.Pool({ connectionString: PG_URL });
    registerWorkflow("race-stress", async () => {
        "use workflow";
        return await step(async () => {
            "use step";
            await new Promise((r) => setTimeout(r, 5));
            return "x";
        });
    });
    const engine = new Engine(store);
    const RUNS = 10, RACERS = 20;
    for (let i = 0; i < RUNS; i++) {
        const { runId } = await engine.start("race-stress", []);
        await Promise.all(Array.from({ length: RACERS }, () => engine.resume(runId).catch(() => { })));
        // wait for terminal state
        for (let w = 0; w < 200; w++) {
            const row = await store.getRun(runId);
            if (row && row.status !== "running")
                break;
            await new Promise((r) => setTimeout(r, 25));
        }
        const sc = Number((await pool.query("SELECT count(*) c FROM lightflow_events WHERE run_id=$1 AND type='step_completed'", [runId])).rows[0].c);
        const sf = Number((await pool.query("SELECT count(*) c FROM lightflow_events WHERE run_id=$1 AND type='step_failed'", [runId])).rows[0].c);
        assert.equal(sc, 1, `run ${runId}: expected 1 step_completed, got ${sc}`);
        assert.equal(sf, 0, `run ${runId}: expected 0 step_failed, got ${sf}`);
    }
    await pool.end();
});
