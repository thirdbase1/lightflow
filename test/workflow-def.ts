import { registerWorkflow } from "../src/index.js";

export async function parityWorkflow(opts: {
  sessionId: string;
  chatId: string;
  ticks: number;
}): Promise<unknown> {
  "use workflow";
  const { step, sleep, getWritable, FatalError, getWorkflowMetadata } =
    await import("../src/index.js");

  const { runId } = getWorkflowMetadata();
  const writable = getWritable<string>();
  const started = Date.now();
  let fatalCaught = false;

  await writable.write("stage:start;");

  try {
    await step(async () => { throw new Error("transient"); });
  } catch {
    fatalCaught = true;
  }

  const lease = `lrun-${runId}`;
  const claimed = await step(async () => {
    const pg = (await import("pg")).default;
    const pool = new pg.Pool({ connectionString: process.env.APP_PG_URL!, max: 4 });
    const r = await pool.query(
      `UPDATE sessions SET lifecycle_run_id=$2 WHERE id=$1 AND lifecycle_run_id IS NULL`,
      [opts.sessionId, lease],
    );
    await pool.end();
    return (r.rowCount ?? 0) > 0;
  }) as boolean;

  await writable.write(`stage:lease:${claimed};`);

  for (let i = 0; i < opts.ticks; i += 1) {
    await sleep(new Date(Date.now() + 1000));
    await writable.write(`stage:tick:${i + 1};`);
  }

  const persisted = await step(async () => {
    const pg = (await import("pg")).default;
    const pool = new pg.Pool({ connectionString: process.env.APP_PG_URL!, max: 4 });
    await pool.query(
      `INSERT INTO workflow_runs (id, chat_id, session_id, user_id, status, started_at, finished_at, total_duration_ms)
       VALUES ($1,$2,'selftest-session','selftest-user','completed',now(),now(),$3)
       ON CONFLICT (id) DO NOTHING`,
      [runId, opts.chatId, Math.round(Date.now() - started)],
    );
    await pool.end();
    return true;
  });

  await step(async () => {
    const pg = (await import("pg")).default;
    const pool = new pg.Pool({ connectionString: process.env.APP_PG_URL!, max: 4 });
    await pool.query(`UPDATE sessions SET lifecycle_run_id=NULL WHERE id=$1`, [opts.sessionId]);
    await pool.end();
  });

  await writable.write("stage:done;");
  await writable.close();

  return {
    ok: true, runId, ticks: opts.ticks,
    leaseValue: claimed ? lease : null,
    persisted, fatalCaught,
  };
}

export function registerAll(): void {
  registerWorkflow("entry-parity", parityWorkflow as never);
}
