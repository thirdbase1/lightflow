/**
 * Coverage suite: every primitive Entry's workflows use, plus cancel,
 * hooks, retry-then-FatalError, and durable timers — all crash-tested.
 */
import {
  Engine, step, sleep, getWritable, FatalError, getWorkflowMetadata,
  defineHook, hookResult, workflowFetch, CancelledError,
} from "../src/index.js";

const URL = process.env.LIGHTFLOW_PG_URL!;

export async function coverageWorkflow(opts: {
  ticks: number;
  shouldFailFatally: boolean;
  useHook: boolean;
}) {
  "use workflow";
  const { runId } = getWorkflowMetadata();
  const writable = getWritable<string>();
  const observed: Record<string, unknown> = { runId };

  await writable.write("stage:start;");

  // 1. step retries a transient error, then succeeds
  let attempts = 0;
  const resilient = await step(async () => {
    attempts += 1;
    if (attempts < 3) throw new Error("transient");
    return "ok-after-2-retries";
  });
  observed.resilient = resilient;

  // 2. FatalError is NOT retried and aborts the run
  let fatalMessage: string | null = null;
  try {
    await step(async () => { throw new FatalError("hard stop"); });
  } catch (e) {
    fatalMessage = e instanceof FatalError ? e.message : "unknown";
  }
  observed.fatalMessage = fatalMessage;

  // 3. durable sleep (Date)
  for (let i = 0; i < opts.ticks; i += 1) {
    await sleep(new Date(Date.now() + 700));
    await writable.write(`stage:tick:${i + 1};`);
  }
  observed.ticks = opts.ticks;

  // 4. durable sleep (ms overload)
  await sleep(300);
  observed.sleptMsOverload = true;

  // 5. hooks
  if (opts.useHook) {
    const { token } = await defineHook<{ ok: boolean }>().create();
    observed.hookToken = token;
  }

  // 6. durable fetch (memoized)
  try {
    const r = await workflowFetch("http://127.0.0.1:9/nope");
    observed.fetchStatus = (r as unknown as { status: number }).status;
  } catch (e) {
    observed.fetchError = e instanceof Error ? e.message : String(e);
  }

  await writable.write("stage:done;");
  await writable.close();
  return observed;
}

export async function register(): Promise<void> {
  const { registerWorkflow } = await import("../src/index.js");
  registerWorkflow("coverage", coverageWorkflow as never);
}

/* ---------------- runner ---------------- */

const { createPostgresStore } = await import("../src/pg-store.js");
const mode = process.argv[2] ?? "run";

if (mode === "register-only") process.exit(0);

const store = await createPostgresStore(URL);
const engine = new Engine(store, { pollMs: 200, staleRunMs: 15_000 });
await register();
void engine.startWorker(() => {});

if (mode === "run") {
  const { runId } = await engine.start("coverage", [
    { ticks: Number(process.argv[3] ?? 3), shouldFailFatally: false, useHook: true },
  ]);
  console.log("STARTED", runId);
  const run = await engine.getRun(runId);
  const rv = await run.returnValue;
  console.log("RESULT", JSON.stringify(rv));
  process.exit(0);
}

if (mode === "cancel") {
  const { runId } = await engine.start("coverage", [
    { ticks: 12, shouldFailFatally: false, useHook: false },
  ]);
  await new Promise((r) => setTimeout(r, 1200));
  const run = await engine.getRun(runId);
  await run.cancel();
  const after = await store.getRun(runId);
  console.log("CANCELLED", JSON.stringify(after));
  process.exit(0);
}

if (mode === "resume") {
  const runId = process.argv[3]!;
  await engine.resume(runId);
  const row = await store.getRun(runId);
  console.log("RESUMED", row?.status, JSON.stringify(row?.output));
  process.exit(0);
}

if (mode === "hook") {
  const runId = process.argv[3]!;
  const token = process.argv[4]!;
  const target = await store.resolveHook!(token, { ok: true });
  console.log("HOOK_RESOLVED", target);
  await engine.resume(runId);
  const row = await store.getRun(runId);
  console.log("AFTER_HOOK", row?.status, JSON.stringify(row?.output));
  process.exit(0);
}
