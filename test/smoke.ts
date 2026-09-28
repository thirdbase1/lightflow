/**
 * CI smoke suite — node:test, runs against a real Postgres (see ci.yml).
 * Uses the real engine API: plain async fns marked "use step" / "use workflow",
 * driven through Engine + createPostgresStore (same pattern as test/entry-parity.ts).
 *
 * The kill-recovery harnesses (simulate/drain/resume/resume-all) stay as
 * manual scripts — SIGKILL harnesses can't run inside node:test.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import { Engine, step, sleep, getWritable, FatalError, getWorkflowMetadata, registerWorkflow } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";

const PG_URL = process.env.LIGHTFLOW_PG_URL;
if (!PG_URL) throw new Error("LIGHTFLOW_PG_URL is required");

type Store = Awaited<ReturnType<typeof createPostgresStore>>;

async function freshStore(): Promise<Store> {
  return createPostgresStore(PG_URL!);
}

async function resetTables() {
  // Ensure schema exists first (fresh CI database), then clear it.
  await createPostgresStore(PG_URL!);
  const pool = new pg.Pool({ connectionString: PG_URL });
  await pool.query("TRUNCATE lightflow_events, lightflow_runs");
  await pool.end();
}

/** Poll until the run leaves "running" (or timeout). */
async function awaitTerminal(store: Store, runId: string, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  let run = await store.getRun(runId);
  while (run?.status === "running" && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 100));
    run = await store.getRun(runId);
  }
  return run;
}

test("step memoizes — the side effect executes exactly once", async () => {
  await resetTables();
  const store = await freshStore();

  let calls = 0;
  registerWorkflow("smoke-memo", async (): Promise<{ calls: number }> => {
    "use workflow";
    const memoized = async (): Promise<string> => {
      "use step";
      calls += 1;
      return `call-${calls}`;
    };
    await step(memoized);
    return { calls };
  });

  const engine = new Engine(store, { pollMs: 100 });
  const workerPromise = engine.startWorker();
  try {
    const { runId } = await engine.start("smoke-memo", []);
    const run = await awaitTerminal(store, runId);
    assert.equal(run?.status, "completed");
    assert.equal((run?.output as { calls: number })?.calls, 1);
  } finally {
    engine.stopWorker();
    await workerPromise;
  }
});

test("FatalError is caught as fatal inside the workflow", async () => {
  await resetTables();
  const store = await freshStore();

  registerWorkflow("smoke-fatal", async (): Promise<{ fatalCaught: boolean }> => {
    "use workflow";
    const bomb = async (): Promise<never> => {
      "use step";
      throw new FatalError("hard stop");
    };
    try {
      await step(bomb);
      return { fatalCaught: false };
    } catch (e) {
      return { fatalCaught: e instanceof FatalError };
    }
  });

  const engine = new Engine(store, { pollMs: 100 });
  const workerPromise = engine.startWorker();
  try {
    const { runId } = await engine.start("smoke-fatal", []);
    const run = await awaitTerminal(store, runId);
    assert.equal(run?.status, "completed");
    assert.equal((run?.output as { fatalCaught: boolean })?.fatalCaught, true);
  } finally {
    engine.stopWorker();
    await workerPromise;
  }
});

test("durable sleep resumes and completes via the worker loop", async () => {
  await resetTables();
  const store = await freshStore();

  registerWorkflow("smoke-sleep", async (): Promise<string> => {
    "use workflow";
    await sleep(100);
    return "after-sleep";
  });

  const engine = new Engine(store, { pollMs: 50 });
  const workerPromise = engine.startWorker();
  try {
    const { runId } = await engine.start("smoke-sleep", []);
    const run = await awaitTerminal(store, runId);
    assert.equal(run?.status, "completed");
    assert.equal(run?.output, "after-sleep");
  } finally {
    engine.stopWorker();
    await workerPromise;
  }
});

test("streaming chunks are ordered and replayable", async () => {
  await resetTables();
  const store = await freshStore();

  registerWorkflow("smoke-stream", async (): Promise<number> => {
    "use workflow";
    const writable = getWritable<string>();
    for (let i = 0; i < 3; i += 1) {
      await writable.write(`chunk-${i}`);
      await sleep(50);
    }
    await writable.close();
    return 3;
  });

  const engine = new Engine(store, { pollMs: 50 });
  const workerPromise = engine.startWorker();
  try {
    const { runId } = await engine.start("smoke-stream", []);
    const run = await awaitTerminal(store, runId);
    assert.equal(run?.status, "completed");

    const pool = new pg.Pool({ connectionString: PG_URL });
    const rows = await pool.query<{ val: string | null }>(
      `SELECT payload->>'value' AS val FROM lightflow_events
       WHERE run_id=$1 AND type='chunk' AND (payload->>'done') IS NULL
       ORDER BY (payload->>'index')::int`,
      [runId],
    );
    await pool.end();
    assert.deepEqual(rows.rows.map((r) => r.val), ["chunk-0", "chunk-1", "chunk-2"]);
  } finally {
    engine.stopWorker();
    await workerPromise;
  }
});

test("runId is unique per start", async () => {
  await resetTables();
  const store = await freshStore();

  registerWorkflow("smoke-ids", async (): Promise<string> => {
    "use workflow";
    return getWorkflowMetadata().runId;
  });

  const engine = new Engine(store, { pollMs: 100 });
  const a = await engine.start("smoke-ids", []);
  const b = await engine.start("smoke-ids", []);
  assert.notEqual(a.runId, b.runId);
});
