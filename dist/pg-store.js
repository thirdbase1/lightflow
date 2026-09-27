/**
 * Postgres-backed store for lightflow.
 *
 * Two tables only. All times are BIGINT epoch milliseconds — no timestamp
 * columns, so no timezone ambiguity (the bug that cost hours in world-postgres).
 */
import pg from "pg";
export async function createPostgresStore(url) {
    const pool = new pg.Pool({ connectionString: url, max: 20 });
    await pool.query(`
    CREATE TABLE IF NOT EXISTS lightflow_runs (
      run_id     TEXT PRIMARY KEY,
      name       TEXT NOT NULL,
      status     TEXT NOT NULL DEFAULT 'running',
      input      JSONB NOT NULL,
      output     JSONB,
      created_at BIGINT NOT NULL,
      updated_at BIGINT NOT NULL
    );
  `);
    await pool.query(`
    CREATE TABLE IF NOT EXISTS lightflow_events (
      id          BIGSERIAL PRIMARY KEY,
      run_id      TEXT NOT NULL REFERENCES lightflow_runs(run_id) ON DELETE CASCADE,
      seq         INTEGER NOT NULL,
      type        TEXT NOT NULL,
      payload     JSONB NOT NULL,
      created_at  BIGINT NOT NULL,
      UNIQUE (run_id, seq)
    );
  `);
    await pool.query(`
    CREATE INDEX IF NOT EXISTS lightflow_events_run_idx
      ON lightflow_events (run_id, seq);
  `);
    await pool.query(`
    CREATE INDEX IF NOT EXISTS lightflow_events_timer_idx
      ON lightflow_events ((payload->>'wakeAt'))
      WHERE type = 'sleep_created';
  `);
    return {
        async createRun(runId, name, input) {
            const now = Date.now();
            await pool.query(`INSERT INTO lightflow_runs (run_id, name, status, input, created_at, updated_at)
         VALUES ($1,$2,'running',$3,$4,$4) ON CONFLICT (run_id) DO NOTHING`, [runId, name, JSON.stringify(input), now]);
            await pool.query(`INSERT INTO lightflow_events (run_id, seq, type, payload, created_at)
         VALUES ($1,0,'run_completed',$2,$3) ON CONFLICT DO NOTHING`, [runId, JSON.stringify({ name, args: input }), now]);
        },
        async getRun(runId) {
            const r = await pool.query(`SELECT status, output FROM lightflow_runs WHERE run_id=$1`, [runId]);
            if (r.rowCount === 0)
                return null;
            return { status: r.rows[0].status, output: r.rows[0].output };
        },
        async appendEvent(e) {
            await pool.query(`INSERT INTO lightflow_events (run_id, seq, type, payload, created_at)
         VALUES ($1,$2,$3,$4,$5) ON CONFLICT (run_id, seq) DO NOTHING`, [e.runId, e.seq, e.type, JSON.stringify(e.payload), e.createdAt]);
        },
        async getEvents(runId) {
            const r = await pool.query(`SELECT run_id, seq, type, payload, created_at FROM lightflow_events
         WHERE run_id=$1 ORDER BY seq ASC`, [runId]);
            return r.rows.map((x) => ({
                runId: x.run_id, seq: x.seq, type: x.type,
                payload: x.payload, createdAt: Number(x.created_at),
            }));
        },
        async dueTimers(now) {
            const r = await pool.query(`SELECT run_id, seq FROM lightflow_events
         WHERE type='sleep_created' AND (payload->>'wakeAt')::bigint <= $1`, [now]);
            return r.rows.map((x) => ({ runId: x.run_id, seq: x.seq }));
        },
        async claimDue() { return false; },
        async setStatus(runId, status, output) {
            await pool.query(`UPDATE lightflow_runs SET status=$2, output=$3, updated_at=$4 WHERE run_id=$1`, [runId, status, output === undefined ? null : JSON.stringify(output), Date.now()]);
        },
    };
}
export async function applyTimezoneGuard(url) {
    // Belt and braces: even though we store BIGINT, ensure the session TZ is UTC.
    const pool = new pg.Pool({ connectionString: url, max: 2 });
    await pool.query(`SET timezone = 'UTC'`);
    await pool.end();
}
