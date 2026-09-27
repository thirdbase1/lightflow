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
    // Cancellation flag (Entry cancels duplicate streams).
    await pool.query(`
    ALTER TABLE lightflow_runs ADD COLUMN IF NOT EXISTS cancelled BOOLEAN NOT NULL DEFAULT false;
  `);
    // Durable hooks: an external caller resolves a token, the workflow wakes.
    await pool.query(`
    CREATE TABLE IF NOT EXISTS lightflow_hooks (
      token     TEXT PRIMARY KEY,
      run_id    TEXT NOT NULL,
      key       TEXT NOT NULL,
      payload   JSONB,
      resolved  BOOLEAN NOT NULL DEFAULT false,
      created_at BIGINT NOT NULL
    );
  `);
    await pool.query(`
    CREATE INDEX IF NOT EXISTS lightflow_events_timer_idx
      ON lightflow_events ((payload->>'wakeAt'))
      WHERE type = 'sleep_created';
  `);
    // Exactly-once chunks: one row per (run, write index). The appendEvent for
    // chunks passes index in payload; this turns a write-race duplicate into a
    // harmless no-op.
    await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS lightflow_events_chunk_unique
      ON lightflow_events (run_id, (payload->>'index'))
      WHERE type = 'chunk';
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
            if (e.type === "chunk") {
                // Exactly-once by write index; seq is allocated atomically to avoid
                // (run_id, seq) collisions between concurrent replays.
                const c = await pool.connect();
                try {
                    await c.query("BEGIN");
                    await c.query("SELECT pg_advisory_xact_lock(hashtext($1))", [e.runId]);
                    const r = await c.query(`SELECT COALESCE(max(seq),-1)+1 AS seq FROM lightflow_events WHERE run_id=$1`, [e.runId]);
                    await c.query(`INSERT INTO lightflow_events (run_id, seq, type, payload, created_at)
             VALUES ($1,$2,$3,$4,$5)
             ON CONFLICT (run_id, (payload->>'index')) WHERE type='chunk' DO NOTHING`, [e.runId, r.rows[0].seq, e.type, JSON.stringify(e.payload), e.createdAt]);
                    await c.query("COMMIT");
                }
                catch (err) {
                    await c.query("ROLLBACK").catch(() => { });
                    const msg = err instanceof Error ? err.message : String(err);
                    if (!/duplicate key value.*chunk_unique/.test(msg))
                        throw err;
                    // duplicate chunk from a replayed tick: expected, ignore
                }
                finally {
                    c.release();
                }
                return;
            }
            const c = await pool.connect();
            try {
                await c.query("BEGIN");
                await c.query("SELECT pg_advisory_xact_lock(hashtext($1))", [e.runId]);
                const r = await c.query(`SELECT COALESCE(max(seq),-1)+1 AS seq FROM lightflow_events WHERE run_id=$1`, [e.runId]);
                await c.query(`INSERT INTO lightflow_events (run_id, seq, type, payload, created_at)
           VALUES ($1,$2,$3,$4,$5) ON CONFLICT (run_id, seq) DO NOTHING`, [e.runId, r.rows[0].seq, e.type, JSON.stringify(e.payload), e.createdAt]);
                await c.query("COMMIT");
            }
            catch (err) {
                await c.query("ROLLBACK").catch(() => { });
                throw err;
            }
            finally {
                c.release();
            }
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
        async nextChunkIndex(runId) {
            const r = await pool.query(`SELECT COALESCE(max((payload->>'index')::int),-1)+1 AS n
         FROM lightflow_events WHERE run_id=$1 AND type='chunk'`, [runId]);
            return Number(r.rows[0].n);
        },
        async staleRuns(cutoff) {
            const r = await pool.query(`SELECT run_id FROM lightflow_runs
         WHERE status='running' AND updated_at < $1`, [cutoff]);
            return r.rows.map((x) => x.run_id);
        },
        async cancel(runId) {
            await pool.query(`UPDATE lightflow_runs SET cancelled=true, updated_at=$2 WHERE run_id=$1`, [runId, Date.now()]);
        },
        async isCancelled(runId) {
            const r = await pool.query(`SELECT cancelled FROM lightflow_runs WHERE run_id=$1`, [runId]);
            return r.rowCount ? Boolean(r.rows[0].cancelled) : false;
        },
        async createHook(runId, token, key) {
            await pool.query(`INSERT INTO lightflow_hooks (token, run_id, key, resolved, created_at)
         VALUES ($1,$2,$3,false,$4) ON CONFLICT (token) DO NOTHING`, [token, runId, key, Date.now()]);
        },
        async resolveHook(token, payload) {
            const r = await pool.query(`UPDATE lightflow_hooks SET resolved=true, payload=$2
         WHERE token=$1 RETURNING run_id`, [token, JSON.stringify(payload)]);
            return r.rowCount ? r.rows[0].run_id : null;
        },
        async getHook(token) {
            const r = await pool.query(`SELECT run_id, payload, resolved FROM lightflow_hooks WHERE token=$1`, [token]);
            if (!r.rowCount)
                return null;
            return {
                runId: r.rows[0].run_id,
                payload: { ...(r.rows[0].payload ?? {}), resolved: Boolean(r.rows[0].resolved) },
            };
        },
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
