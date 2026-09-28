/**
 * Drop-in verification: the exact Vercel Workflow surface entry-agents uses,
 * exercised against lightflow's compat layer:
 *   start(fn, args) / getRun(id) / run.status (Promise) / run.runId /
 *   run.returnValue / run.cancel() / run.getReadable({startIndex}) /
 *   getTailIndex() / getWritable() as WritableStream / getWorkflowMetadata()
 *   -> { workflowRunId } / sleep(Date) / FatalError.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import { Engine, step, sleep, FatalError } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
import { initWorkflowApi, start, getRun } from "../src/compat/api.js";
import { getWritable, getWorkflowMetadata } from "../src/compat/workflow.js";

const PG_URL = process.env.LIGHTFLOW_PG_URL;
if (!PG_URL) throw new Error("LIGHTFLOW_PG_URL is required");

test("compat: vercel-style agent chat workflow end-to-end", async () => {
  const store = await createPostgresStore(PG_URL!);
  const engine = new Engine(store, { pollMs: 25 });
  initWorkflowApi(store, engine);
  const workerPromise = engine.startWorker();
  setTimeout(() => engine.stopWorker(), 60_000);
  const pool = new pg.Pool({ connectionString: PG_URL });

  // A workflow shaped like entry-agents' runAgentWorkflow:
  // - "use step"-style direct calls via step()
  // - getWritable() as a real WritableStream, written via getWriter()
  // - getWorkflowMetadata() -> { workflowRunId }
  // - sleep(new Date(...)) for durable timers
  // - FatalError for non-retryable failures
  const runAgentWorkflow = async (opts: { messages: string[]; chatId: string }) => {
    "use workflow";
    const { workflowRunId } = getWorkflowMetadata();
    const writable = getWritable<string>();
    const writer = writable.getWriter();

    await step(async () => {
      await writer.write(`start:${opts.chatId}`);
    });

    const answer = await step(async () => `reply-to:${opts.messages.join(",")}`);

    // mid-run durable timer like sandbox-lifecycle
    await sleep(new Date(Date.now() + 50));

    await step(async () => { await writer.write(`chunk:${answer}`); });
    await step(async () => { await writer.write("done-marker"); });
    await writer.close();

    if (opts.messages.includes("boom")) throw new FatalError("nope");
    return { workflowRunId, answer };
  };

  // Vercel-style start(fn, args) — function, not id
  const run = await start(runAgentWorkflow, [
    { messages: ["hello", "world"], chatId: "c1" },
  ]);
  assert.ok(run.runId.startsWith("lrun_"));

  // status is a live getter; await terminal state via returnValue
  await run.returnValue;
  const status = await run.status;
  assert.equal(status, "completed");
  const ret = (await run.returnValue) as { workflowRunId: string; answer: string };
  assert.equal(ret.answer, "reply-to:hello,world");
  assert.equal(ret.workflowRunId, run.runId);

  // getReadable() with startIndex + getTailIndex
  const readable = run.getReadable<string>({ startIndex: 0 });
  const tail = await readable.getTailIndex();
  const parts: string[] = [];
  const reader = readable.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(value);
  }
  assert.deepEqual(parts, ["start:c1", "chunk:reply-to:hello,world", "done-marker"]);
  assert.ok(tail >= 1);

  // resume from startIndex: skipping the first chunk
  const resumed = run.getReadable<string>({ startIndex: 1 });
  const rreader = resumed.getReader();
  const r1 = await rreader.read();
  assert.equal(r1.value, "chunk:reply-to:hello,world");

  // cancel semantics: getRun + status + cancel()
  const dup = await start(runAgentWorkflow, [{ messages: ["x"], chatId: "c2" }]);
  await dup.cancel();
  assert.equal(await dup.status, "cancelled");  // live getter reflects cancel


  await pool.end();
  engine.stopWorker();
  await workerPromise;
});
