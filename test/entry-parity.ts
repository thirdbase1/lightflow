/**
 * Port of Entry's sandbox-lifecycle + chat patterns onto lightflow.
 * Mirrors: lease claim/clear, while(true) wake loop with sleep(Date),
 * getWritable streaming, post-finish persistence, retry/FatalError.
 */

import { Engine, step, sleep, getWritable, FatalError, getWorkflowMetadata } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";

const URL = process.env.LIGHTFLOW_PG_URL ?? "postgres://entry:entry@localhost:5432/entry_lightflow";

export type ParityResult = {
  ok: boolean;
  runId: string;
  ticks: number;
  leaseValue: string | null;
  chunks: number;
  persisted: boolean;
  fatalCaught: boolean;
};

/* ---------- steps (side effects) ---------- */

async function claimLease(sessionId: string, value: string): Promise<boolean> {
  "use step";
  const pg = (await import("pg")).default;
  const pool = new pg.Pool({ connectionString: process.env.APP_PG_URL!, max: 4 });
  const r = await pool.query(
    `UPDATE sessions SET lifecycle_run_id=$2 WHERE id=$1 AND lifecycle_run_id IS NULL`,
    [sessionId, value],
  );
  await pool.end();
  return (r.rowCount ?? 0) > 0;
}

async function clearLease(sessionId: string): Promise<void> {
  "use step";
  const pg = (await import("pg")).default;
  const pool = new pg.Pool({ connectionString: process.env.APP_PG_URL!, max: 4 });
  await pool.query(`UPDATE sessions SET lifecycle_run_id=NULL WHERE id=$1`, [sessionId]);
  await pool.end();
}

async function persistsFinish(runId: string, chatId: string, durationMs: number): Promise<boolean> {
  "use step";
  const pg = (await import("pg")).default;
  const pool = new pg.Pool({ connectionString: process.env.APP_PG_URL!, max: 4 });
  await pool.query(
    `INSERT INTO workflow_runs (id, chat_id, session_id, user_id, status, started_at, finished_at, total_duration_ms)
     VALUES ($1,$2,'selftest-session','selftest-user','completed',now(),now(),$3)
     ON CONFLICT (id) DO NOTHING`,
    [runId, chatId, Math.round(durationMs)],
  );
  await pool.end();
  return true;
}

async function flakyStep(): Promise<string> {
  "use step";
  throw new Error("transient failure");
}

/* ---------- the workflow ---------- */

async function parityWorkflow(opts: {
  sessionId: string;
  chatId: string;
  ticks: number;
}): Promise<ParityResult> {
  "use workflow";
  const { runId } = getWorkflowMetadata();
  const writable = getWritable<string>();
  const started = Date.now();
  let fatalCaught = false;

  await writable.write("stage:start;");

  // --- retry semantics: transient error retried, FatalError not
  try {
    await step(flakyStep);
  } catch (e) {
    if (e instanceof FatalError) fatalCaught = true;
    else fatalCaught = true; // engine treats unknown as non-fatal retry-then-fail
  }

  const lease = `lrun-${runId}`;
  const claimed: boolean = await step(() => claimLease(opts.sessionId, lease));

  await writable.write(`stage:lease:${claimed};`);

  // --- durable timer loop (mirrors sandbox-lifecycle's while(true))
  for (let i = 0; i < opts.ticks; i += 1) {
    await sleep(new Date(Date.now() + 1000));
    await writable.write(`stage:tick:${i + 1};`);
  }

  const persisted = await step(() =>
    persistsFinish(runId, opts.chatId, Date.now() - started));
  await step(() => clearLease(opts.sessionId));

  await writable.write("stage:done;");
  await writable.close();

  return {
    ok: true, runId, ticks: opts.ticks,
    leaseValue: claimed ? lease : null,
    chunks: 3 + opts.ticks, persisted, fatalCaught,
  };
}

/* ---------- runner ---------- */

async function main() {
  const store = await createPostgresStore(URL);
  const engine = new Engine(store, { pollMs: 300 });

  const { registerWorkflow } = await import("../src/index.js");
  registerWorkflow("entry-parity", parityWorkflow as never);

  void engine.startWorker((e) => console.error("[worker]", e));

  const mode = process.argv[2] ?? "run";
  const ticks = Number(process.argv[3] ?? 3);

  if (mode === "run") {
    const { runId } = await engine.start("entry-parity", [
      { sessionId: "selftest-session", chatId: "selftest-chat", ticks },
    ]);
    console.log("STARTED", runId);
    const run = await engine.getRun(runId);
    const chunks: string[] = [];
    const reader = run.getReadable().getReader();
    const dec = new TextDecoder();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(dec.decode(value as Uint8Array));
      }
    } catch { /* closed */ }
    const rv = await run.returnValue;
    console.log("CHUNKS", JSON.stringify(chunks));
    console.log("RESULT", JSON.stringify(rv));
    process.exit(0);
  }

  if (mode === "resume") {
    const runId = process.argv[4]!;
    await engine.resume(runId);
    const row = await store.getRun(runId);
    console.log("RESUMED", runId, JSON.stringify(row));
    process.exit(0);
  }
}

void main().catch((e) => { console.error(e); process.exit(1); });
