/**
 * Postgres-backed store for lightflow.
 *
 * Two tables only. All times are BIGINT epoch milliseconds — no timestamp
 * columns, so no timezone ambiguity (the bug that cost hours in world-postgres).
 */

import pg from "pg";

export type Store = import("../src/index.js").Store;
type StepEvent = import("../src/index.js").StepEvent;
type RunStatus = import("../src/index.js").RunStatus;

export async function createPostgresStore(
  url: string,
  opts: { /** Session-level SET options, e.g. { synchronous_commit: 'off' } */
    sessionOptions?: Record<string, string> } = {},
): Promise<Store> {
  // Pipelining (pg >= 8.23): batch queries on one connection instead of one
  // round trip per query — 1.5-2.4x on multi-query workloads.
  const pool = new pg.Pool({ connectionString: url, max: 20, pipeline: true });
  if (opts.sessionOptions) {
    const sets = Object.entries(opts.sessionOptions)
      .map(([k, v]) => `SET ${k} = ${v === "off" || v === "on" ? v : `'${v}'`}`)
      .join("; ");
    // Apply per connection as it opens (pg fires 'connect' per client).
    pool.on("connect", (client) => {
      void client.query(sets).catch(() => {});
    });
  }

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
  // Run lease: prevents racing replays (two workers resuming the same run)
  // from re-executing steps concurrently. Claimed = someone is executing.
  await pool.query(`
    ALTER TABLE lightflow_runs ADD COLUMN IF NOT EXISTS claimed_until BIGINT NOT NULL DEFAULT 0;
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
  // Stream chunk counter: O(1) nextChunkIndex without scanning all chunks.
  await pool.query(`
    ALTER TABLE lightflow_runs ADD COLUMN IF NOT EXISTS chunk_count INTEGER NOT NULL DEFAULT 0;
  `);
  // Durable timers: expression index matching the dueTimers query exactly
  // (bigint cast on the payload value, partial on pending timers).
  await pool.query(`
    CREATE INDEX IF NOT EXISTS lightflow_events_timer_idx
      ON lightflow_events (((payload->>'wakeAt')::bigint))
      WHERE type = 'sleep_created';
  `);
  // Exactly-once chunks: one row per (run, write index) AND one per (run,
  // call-position key). The key index is what makes a replayed write a
  // harmless no-op even when two replays race.
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS lightflow_events_chunk_unique
      ON lightflow_events (run_id, (payload->>'index'))
      WHERE type = 'chunk';
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS lightflow_events_chunk_key_unique
      ON lightflow_events (run_id, (payload->>'key'))
      WHERE type = 'chunk' AND payload->>'key' IS NOT NULL;
  `);


  return {
    async createRun(runId, name, input) {
      const now = Date.now();
      // One round trip: run row + start event, atomically.
      await pool.query(
        `WITH ins AS (
           INSERT INTO lightflow_runs (run_id, name, status, input, created_at, updated_at)
           VALUES ($1,$2,'running',$3,$4,$4) ON CONFLICT (run_id) DO NOTHING
         )
         INSERT INTO lightflow_events (run_id, seq, type, payload, created_at)
         VALUES ($1,0,'run_completed',$5,$4) ON CONFLICT DO NOTHING`,
        [runId, name, JSON.stringify(input), now, JSON.stringify({ name, args: input })],
      );
    },

    async getRun(runId) {
      const r = await pool.query(
        `SELECT status, output FROM lightflow_runs WHERE run_id=$1`, [runId]);
      if (r.rowCount === 0) return null;
      return { status: r.rows[0].status as RunStatus, output: r.rows[0].output };
    },

    async appendEvent(e: StepEvent) {
      const payload = JSON.stringify(e.payload);
      // FAST PATH: one statement, caller-supplied seq. The (run_id, seq) unique
      // constraint makes this atomic; the chunk-key unique index makes a
      // replayed write a harmless no-op. No advisory lock, no max(seq) scan,
      // no explicit transaction (single statements are already atomic).
      try {
        const r = await pool.query(
          `INSERT INTO lightflow_events (run_id, seq, type, payload, created_at)
           VALUES ($1,$2,$3,$4,$5)
           ON CONFLICT DO NOTHING`,
          [e.runId, e.seq, e.type, payload, e.createdAt],
        );
        if (r.rowCount) return;
        // Conflict: either a racing replay appended the SAME event (same
        // type+key) -> drop, or a DIFFERENT event took this seq -> slow path.
        const key = (e.payload as { key?: string }).key;
        if (key) {
          const dup = await pool.query(
            `SELECT 1 FROM lightflow_events
             WHERE run_id=$1 AND type=$2 AND payload->>'key'=$3 LIMIT 1`,
            [e.runId, e.type, key]);
          if (dup.rowCount) return; // racing replay lost: memoized event exists
        }
      } catch (err: unknown) {
        const code = (err as { code?: string }).code;
        if (code === "23505" && (e.type as string) === "chunk") return;
        if (code !== "23505") throw err;
      }
      // SLOW PATH (rare): allocate seq under the advisory lock.
      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        await c.query("SELECT pg_advisory_xact_lock(hashtext($1))", [e.runId]);
        const key = (e.payload as { key?: string }).key;
        if (key) {
          // Re-check dedupe under the lock: a racing replay's insert may have
          // committed after our unlocked check ran.
          const dup = await c.query(
            `SELECT 1 FROM lightflow_events
             WHERE run_id=$1 AND type=$2 AND payload->>'key'=$3 LIMIT 1`,
            [e.runId, e.type, key]);
          if (dup.rowCount) { await c.query("COMMIT"); return; }
        }
        if (e.type === "chunk" && key) {
          const dup = await c.query(
            `SELECT 1 FROM lightflow_events
             WHERE run_id=$1 AND type='chunk' AND payload->>'key'=$2 LIMIT 1`,
            [e.runId, key]);
          if (dup.rowCount) { await c.query("COMMIT"); return; }
        }
        const r = await c.query(
          `SELECT COALESCE(max(seq),-1)+1 AS seq FROM lightflow_events WHERE run_id=$1`,
          [e.runId]);
        await c.query(
          `INSERT INTO lightflow_events (run_id, seq, type, payload, created_at)
           VALUES ($1,$2,$3,$4,$5)
           ON CONFLICT (run_id, seq) DO NOTHING`,
          [e.runId, r.rows[0].seq, e.type, payload, e.createdAt]);
        await c.query("COMMIT");
      } catch (err) {
        await c.query("ROLLBACK").catch(() => {});
        const msg = err instanceof Error ? err.message : String(err);
        if (!/duplicate key value/.test(msg)) throw err;
      } finally { c.release(); }
    },

    async getEvents(runId) {
      // Snapshot compaction: replay starts from the latest snapshot, not seq 0.
      const snap = await pool.query(
        `SELECT seq, payload FROM lightflow_events
         WHERE run_id=$1 AND type='snapshot' ORDER BY seq DESC LIMIT 1`, [runId]);
      if (snap.rowCount) {
        const s = snap.rows[0];
        const r = await pool.query(
          `SELECT run_id, seq, type, payload, created_at FROM lightflow_events
           WHERE run_id=$1 AND seq > $2 ORDER BY seq ASC`, [runId, s.seq]);
        // Prepend the run_completed event so resume() can find name/args.
        const start = await pool.query(
          `SELECT payload FROM lightflow_events WHERE run_id=$1 AND seq=0`, [runId]);
        const events = [{
          runId, seq: 0, type: "run_completed",
          payload: start.rows[0]?.payload ?? {}, createdAt: 0,
        } as StepEvent, {
          runId, seq: s.seq, type: "snapshot",
          payload: s.payload, createdAt: 0,
        } as StepEvent].concat(r.rows.map((x) => ({
          runId: x.run_id, seq: x.seq, type: x.type,
          payload: x.payload, createdAt: Number(x.created_at),
        })) as StepEvent[]);
        return events;
      }
      const r = await pool.query(
        `SELECT run_id, seq, type, payload, created_at FROM lightflow_events
         WHERE run_id=$1 ORDER BY seq ASC`, [runId]);
      return r.rows.map((x) => ({
        runId: x.run_id, seq: x.seq, type: x.type,
        payload: x.payload, createdAt: Number(x.created_at),
      })) as StepEvent[];
    },

    async dueTimers(now) {
      // Only timers not yet completed (anti-join against sleep_completed by
      // payload key). Paired with claimDue() so racing workers don't wake the
      // same timer twice.
      const r = await pool.query(
        `SELECT e.run_id, e.seq, e.payload->>'key' AS key FROM lightflow_events e
         WHERE e.type='sleep_created'
           AND (e.payload->>'wakeAt')::bigint <= $1
           AND NOT EXISTS (
             SELECT 1 FROM lightflow_events c
             WHERE c.run_id = e.run_id AND c.type='sleep_completed'
               AND c.payload->>'key' = e.payload->>'key'
           )
         LIMIT 100`, [now]);
      return r.rows.map((x) => ({ runId: x.run_id, seq: x.seq, key: x.key }));
    },

    async claimDue() { return false; },

    async claimRun(runId, leaseMs = 30_000) {
      const now = Date.now();
      // One round trip: claim the lease AND read the cancelled flag.
      const r = await pool.query(
        `UPDATE lightflow_runs SET claimed_until=$2, updated_at=$3
         WHERE run_id=$1 AND claimed_until < $3
         RETURNING cancelled`,
        [runId, now + leaseMs, now],
      );
      return { ok: (r.rowCount ?? 0) > 0, cancelled: r.rows[0]?.cancelled === true };
    },

    async releaseRun(runId) {
      await pool.query(
        `UPDATE lightflow_runs SET claimed_until=0 WHERE run_id=$1 AND claimed_until > 0`,
        [runId],
      );
    },

    /** Terminal status + lease release in one round trip. */
    async finishRun(runId, status, output) {
      // A cancelled run is terminal: the cancel path already wrote the final
      // status, and a still-running replay must not resurrect it.
      await pool.query(
        `UPDATE lightflow_runs
         SET status=$2, output=$3, claimed_until=0, updated_at=$4
         WHERE run_id=$1 AND cancelled=false`,
        [runId, status, output === undefined ? null : JSON.stringify(output), Date.now()],
      );
    },

    /** LISTEN/NOTIFY wakeup support: re-poll immediately on notify. */
    async notifyWake() {
      await pool.query(`NOTIFY lightflow_wake`);
    },

    getListenClient() {
      const client = new pg.Client({ connectionString: url });
      const ready = client.connect().then(() => {
        // A listening client must not keep the Node process alive on its own.
        (client as unknown as { stream?: { unref?(): void } }).stream?.unref?.();
        return client;
      });
      return {
        async query(sql: string) { return (await ready).query(sql); },
        on(_event: "notification", cb: () => void) {
          void ready.then((c) => c.on("notification", cb));
        },
        release() { void ready.then((c) => c.end()).catch(() => {}); },
      };
    },

    async nextChunkIndex(runId) {
      // Atomic fetch-and-increment on the run row: O(1), race-free across
      // replays (the chunk-key unique index still guards duplicates).
      const r = await pool.query(
        `UPDATE lightflow_runs SET chunk_count=chunk_count+1, updated_at=$2
         WHERE run_id=$1 RETURNING chunk_count-1 AS n`, [runId, Date.now()]);
      return Number(r.rows[0].n);
    },

    async staleRuns(cutoff) {
      // Only runs whose lease has EXPIRED are orphaned. A run inside a long
      // (>staleMs) step still holds a live lease and must not be double-run.
      const r = await pool.query(
        `SELECT run_id FROM lightflow_runs
         WHERE status='running' AND updated_at < $1 AND claimed_until < $2`,
        [cutoff, Date.now()]);
      return r.rows.map((x) => x.run_id as string);
    },

    async cancel(runId) {
      // Terminal in one statement: flag + final status together, so a racing
      // replay can never observe cancel-flag-without-terminal-status.
      await pool.query(
        `UPDATE lightflow_runs
         SET cancelled=true, status='cancelled', output='{"error":"cancelled"}', claimed_until=0, updated_at=$2
         WHERE run_id=$1`,
        [runId, Date.now()],
      );
    },

    async isCancelled(runId) {
      const r = await pool.query(
        `SELECT cancelled FROM lightflow_runs WHERE run_id=$1`, [runId]);
      return r.rowCount ? Boolean(r.rows[0].cancelled) : false;
    },

    async createHook(runId, token, key) {
      await pool.query(
        `INSERT INTO lightflow_hooks (token, run_id, key, resolved, created_at)
         VALUES ($1,$2,$3,false,$4) ON CONFLICT (token) DO NOTHING`,
        [token, runId, key, Date.now()],
      );
    },

    async resolveHook(token, payload) {
      const r = await pool.query(
        `UPDATE lightflow_hooks SET resolved=true, payload=$2
         WHERE token=$1 RETURNING run_id`, [token, JSON.stringify(payload)]);
      return r.rowCount ? (r.rows[0].run_id as string) : null;
    },

    async getHook(token) {
      const r = await pool.query(
        `SELECT run_id, payload, resolved FROM lightflow_hooks WHERE token=$1`, [token]);
      if (!r.rowCount) return null;
      return {
        runId: r.rows[0].run_id as string,
        payload: { ...(r.rows[0].payload ?? {}), resolved: Boolean(r.rows[0].resolved) },
      };
    },

    async pruneEvents(runId, beforeSeq) {
      // Remove events fully folded into the latest snapshot. Chunk events are
      // KEPT: getReadable/getTailIndex stream chunks from the event log, and
      // the snapshot memo is not a chunk source. run_completed (seq 0) is
      // protected by beforeSeq > 0.
      await pool.query(
        `DELETE FROM lightflow_events
         WHERE run_id=$1 AND seq < $2
           AND type IN ('step_completed','step_failed','sleep_created','sleep_completed')`,
        [runId, beforeSeq],
      );
    },

    async setStatus(runId, status, output) {
      await pool.query(
        `UPDATE lightflow_runs SET status=$2, output=$3, updated_at=$4 WHERE run_id=$1`,
        [runId, status, output === undefined ? null : JSON.stringify(output), Date.now()],
      );
    },
  };
}

export async function applyTimezoneGuard(url: string): Promise<void> {
  // Belt and braces: even though we store BIGINT, ensure the session TZ is UTC.
  const pool = new pg.Pool({ connectionString: url, max: 2 });
  await pool.query(`SET timezone = 'UTC'`);
  await pool.end();
}
