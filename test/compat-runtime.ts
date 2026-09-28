
import { Engine, step, sleep, FatalError, registerWorkflow } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
import { makeStep, getWritable } from "../src/compat/workflow.js";
import { initWorkflowApi, start, getRun } from "../src/compat/api.js";

import assert from "node:assert/strict";

const url = process.env.LIGHTFLOW_PG_URL!;
const store = await createPostgresStore(url);
const engine = new Engine(store, { pollMs: 20 });
initWorkflowApi(store, engine);
const wp = engine.startWorker();
setTimeout(() => engine.stopWorker(), 30000);

let callCount = 0;
// arg-taking step with retries (fails twice, succeeds third)
const flakyStep = makeStep(async (a: string, b: number) => {
  callCount++;
  if (callCount <= 2) throw new Error("transient " + a);
  return `${a}-${b}`;
}, { retries: 4 });

// step that throws FatalError -> no retry
let fatalCalls = 0;
const fatalStep = makeStep(async () => { fatalCalls++; throw new FatalError("permanent"); });

// workflow: loop with sleep + makeStep + writable
const wf = async (opts: { n: number }) => {
  const writer = getWritable<string>().getWriter();
  for (let i = 0; i < opts.n; i++) {
    await sleep(new Date(Date.now() + 20));
    const v = await flakyStep("x", i);
    await step(async () => { await writer.write(v); });
  }
  return "done:" + opts.n;
};

// failed-run returnValue rejection
const failWf = async () => { await fatalStep(); return "never"; };

// run 1: loop+sleep
const r1 = await start(wf, [{ n: 3 }]);
async function untilTerminal(r: any): Promise<string> {
  for (;;) { const st = await r.status; if (!["pending","running"].includes(st)) return st; await new Promise(res => setTimeout(res, 25)); }
}
assert.equal(await untilTerminal(r1), "completed");
const ret = await r1.returnValue;
assert.equal(ret, "done:3");
// chunks all persisted (3)
const reader = r1.getReadable<string>({ startIndex: 0 }).getReader();
const parts: string[] = [];
for (;;) { const { done, value } = await reader.read(); if (done) break; parts.push(value); }
assert.deepEqual(parts, ["x-0","x-1","x-2"]);
console.log("loop+sleep+makeStep OK", parts);

// run 2: fatal failure -> returnValue rejects with the error
const r2 = await start(failWf, []);
await assert.rejects(() => r2.returnValue, /permanent/);
assert.equal(await getRun(r2.runId).status, "failed");
assert.equal(fatalCalls, 1, "FatalError must not retry");
console.log("failed returnValue rejects OK, fatalCalls:", fatalCalls);

// run 3: getRun on unknown id -> pending
const ghost = getRun("lrun_ghost");
assert.equal(await ghost.status, "pending");
console.log("unknown run -> pending OK");

process.exit(0);
