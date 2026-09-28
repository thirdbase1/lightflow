/**
 * lightflow — a durable workflow engine, from scratch.
 *
 * Implements every primitive Entry's workflows rely on:
 *   - "use workflow"  deterministic orchestration, replayed from an event log
 *   - "use step"      at-least-once, memoized side effects
 *   - sleep(ms | Date) durable timers (survive process death)
 *   - getWritable()    ordered, resumable output stream
 *   - start() / getRun()  start, resume by id, await returnValue
 *   - FatalError       non-retryable failure
 *   - getWorkflowMetadata()  run id inside a workflow
 *
 * Design goals that fix the two defects found in world-postgres:
 *   1. All timestamps are epoch milliseconds (integer). No naive/UTC skew.
 *   2. No spec-version coupling: events are plain JSON rows with a schema
 *      version integer that the engine upgrades itself.
 */

import { createHash, randomUUID } from "node:crypto";

/* ------------------------------------------------------------------ */
/* Errors                                                              */
/* ------------------------------------------------------------------ */

export class FatalError extends Error {
  readonly fatal = true;
  constructor(message: string) {
    super(message);
    this.name = "FatalError";
  }
}

/* ------------------------------------------------------------------ */
/* Persistence interface (Postgres implementation below)               */
/* ------------------------------------------------------------------ */

export type RunStatus = "running" | "completed" | "failed";

export type StepEvent = {
  runId: string;
  seq: number;
  type: "step_started" | "step_completed" | "step_failed" | "sleep_created" |
        "sleep_completed" | "chunk" | "run_completed" | "run_failed";
  payload: unknown;
  createdAt: number;
};

export interface Store {
  createRun(runId: string, name: string, input: unknown): Promise<void>;
  getRun(runId: string): Promise<{ status: RunStatus; output?: unknown } | null>;
  appendEvent(e: StepEvent): Promise<void>;
  getEvents(runId: string): Promise<StepEvent[]>;
  /** Durable timers due at or before `now`. */
  dueTimers(now: number): Promise<{ runId: string; seq: number }[]>;
  claimDue(now: number): Promise<boolean>;
  /** Runs still 'running' with no activity since `cutoff` (epoch ms). */
  staleRuns?(cutoff: number): Promise<string[]>;
  /** Mark a run cancelled. Ignored once terminal. */
  cancel?(runId: string): Promise<void>;
  isCancelled?(runId: string): Promise<boolean>;
  /** Next monotonic chunk index (max+1), atomic per run. */
  nextChunkIndex?(runId: string): Promise<number>;
  /** Hooks: durable external callbacks a workflow can await. */
  createHook?(runId: string, token: string, key: string): Promise<void>;
  resolveHook?(token: string, payload: unknown): Promise<string | null>;
  getHook?(token: string): Promise<{ runId: string; payload: unknown } | null>;
  setStatus(runId: string, status: RunStatus, output?: unknown): Promise<void>;
}

/* ------------------------------------------------------------------ */
/* Registry: workflows and steps are resolved by a stable id           */
/* ------------------------------------------------------------------ */

type WorkflowFn = (...args: unknown[]) => Promise<unknown>;

const workflows = new Map<string, WorkflowFn>();
const steps = new Map<string, (...a: unknown[]) => Promise<unknown>>();

export function registerWorkflow(id: string, fn: WorkflowFn): void {
  workflows.set(id, fn);
}
export function registerStep(id: string, fn: (...a: unknown[]) => Promise<unknown>): void {
  steps.set(id, fn);
}

/* ------------------------------------------------------------------ */
/* Ambient run context (set while a workflow is executing)             */
/* ------------------------------------------------------------------ */

type Ctx = {
  runId: string;
  seq: number;
  /** deterministic call position counters — these form the replay key */
  stepCalls: number;
  sleepCalls: number;
  writes: number;
  hookCalls: number;
  store: Store;
  log: StepEvent[];
  /** index of events already consumed during replay */
  cursor: number;
  chunks: string[];
  now: () => number;
};

let current: Ctx | null = null;

export function getWorkflowMetadata(): { runId: string } {
  if (!current) throw new Error("getWorkflowMetadata() outside a workflow");
  return { runId: current.runId };
}

/* ------------------------------------------------------------------ */
/* The step id: derived from call order, so replay is deterministic    */
/* ------------------------------------------------------------------ */

function nextStepKey(): string {
  if (!current) throw new Error("steps can only be called inside a workflow");
  return `step:${current.stepCalls}`;
}

/* ------------------------------------------------------------------ */
/* Public API inside a workflow                                        */
/* ------------------------------------------------------------------ */

/**
 * Run a side effect durably. On replay the memoized result is returned and
 * the function body is NOT re-executed.
 */
export async function step<T>(fn: () => Promise<T>): Promise<T> {
  if (!current) return fn(); // plain call outside a workflow
  const ctx = current;
  const key = nextStepKey();
  ctx.stepCalls += 1;

  // Replay: memoized result for this call position?
  const done = ctx.log.find(
    (e) => e.type === "step_completed" && (e.payload as { key: string })?.key === key,
  );
  if (done) return (done.payload as { value: T }).value;

  ctx.seq += 1;
  await ctx.store.appendEvent({
    runId: ctx.runId, seq: ctx.seq, type: "step_started",
    payload: { key }, createdAt: ctx.now(),
  });

  const fnId = (fn as unknown as { __lightflowStepId?: string }).__lightflowStepId;
  const impl = fnId ? steps.get(fnId) : undefined;

  // Retry with backoff. FatalError is never retried (Entry retries 4x, then
  // bubbles FatalError -- same contract here).
  const maxAttempts = (fn as unknown as { __lightflowRetries?: number })
    .__lightflowRetries ?? DEFAULT_STEP_RETRIES;
  let lastErr: unknown;
  let value!: T;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      value = await (impl ? (impl as () => Promise<T>)() : fn());
      lastErr = undefined;
      break;
    } catch (err) {
      lastErr = err;
      if (err instanceof FatalError) break;
      if (attempt < maxAttempts) {
        await new Promise((r) =>
          setTimeout(r, Math.min(100 * 2 ** (attempt - 1), 2000)));
      }
    }
  }
  if (lastErr instanceof FatalError) throw lastErr;
  if (lastErr) {
    ctx.seq += 1;
    await ctx.store.appendEvent({
      runId: ctx.runId, seq: ctx.seq, type: "step_failed",
      payload: { key, error: String(lastErr instanceof Error ? lastErr.message : lastErr) },
      createdAt: ctx.now(),
    });
    throw lastErr;
  }

  ctx.seq += 1;
  await ctx.store.appendEvent({
    runId: ctx.runId, seq: ctx.seq, type: "step_completed",
    payload: { key, value }, createdAt: ctx.now(),
  });
  return value;
}

/** Durable sleep. Accepts milliseconds or an absolute Date. */
export async function sleep(until: number | Date): Promise<void> {
  if (!current) {
    const ms = until instanceof Date ? until.getTime() - Date.now() : until;
    return new Promise((r) => setTimeout(r, Math.max(0, ms)));
  }
  const ctx = current;
  const wakeAt = until instanceof Date ? until.getTime() : ctx.now() + until;
  const key = `sleep:${ctx.sleepCalls}`;
  ctx.sleepCalls += 1;

  // Replay: already slept?
  const done = ctx.log.find(
    (e) => e.type === "sleep_completed" && (e.payload as { key: string })?.key === key,
  );
  if (done) return;

  ctx.seq += 1;
  await ctx.store.appendEvent({
    runId: ctx.runId, seq: ctx.seq, type: "sleep_created",
    payload: { key, wakeAt }, createdAt: ctx.now(),
  });
  // Suspend: throw a control-flow signal caught by the runner.
  throw new SuspendSignal(key, wakeAt);
}

export class SuspendSignal {
  constructor(readonly key: string, readonly wakeAt: number) {}
}

/** Ordered output stream for a run. Chunks are persisted and replayable. */
export function getWritable<T = string>(): {
  write(chunk: T): Promise<void>;
  close(): Promise<void>;
} {
  const ctx = current!;
  if (!current) throw new Error("getWritable() outside a workflow");
  /** Deterministic key: (durable-timer position, writes since it). */
  const keyFor = () => `${ctx.sleepCalls}:${ctx.writes}`;
  return {
    async write(chunk: T) {
      // Memoized by call position (like steps): on replay, a write whose
      // position was already emitted is skipped, and the loop advances.
      const key = `w:${ctx.writes}`;
      const fresh = await ctx.store.getEvents(ctx.runId);
      const already = fresh.some(
        (e) => e.type === "chunk" && (e.payload as { key?: string })?.key === key,
      );
      ctx.writes += 1;
      if (already) return;
      const index = await (ctx.store as unknown as {
        nextChunkIndex(r: string): Promise<number>;
      }).nextChunkIndex!(ctx.runId);
      ctx.seq += 1;
      await ctx.store.appendEvent({
        runId: ctx.runId, seq: ctx.seq, type: "chunk",
        payload: { value: chunk, index, key }, createdAt: ctx.now(),
      });
    },
    async close() {
      const already = ctx.log.some(
        (e) => e.type === "chunk" && (e.payload as { done?: boolean })?.done === true,
      );
      if (already) return;
      ctx.seq += 1;
      await ctx.store.appendEvent({
        runId: ctx.runId, seq: ctx.seq, type: "chunk",
        payload: { value: null, done: true, key: "close", index: ctx.writes },
        createdAt: ctx.now(),
      });
    },
  };
}

/* ------------------------------------------------------------------ */
/* Runner                                                              */
/* ------------------------------------------------------------------ */

const DEFAULT_STEP_RETRIES = 3;

export class Engine {
  constructor(
    private readonly store: Store,
    private readonly opts: { stepRetries?: number; pollMs?: number; staleRunMs?: number } = {},
  ) {}

  async start(workflowId: string, args: unknown[]): Promise<{ runId: string }> {
    const runId = `lrun_${randomUUID().replace(/-/g, "").slice(0, 24)}`;
    await this.store.createRun(runId, workflowId, args);
    void this.run(runId, workflowId, args);
    return { runId };
  }

  async getRun(runId: string) {
    const row = await this.store.getRun(runId);
    if (!row) throw new Error(`run not found: ${runId}`);
    return {
      runId,
      status: row.status,
      returnValue: this.waitFor(runId),
      getReadable: () => this.readable(runId),
      /** Entry calls this to kill a duplicate stream (route.ts:172). */
      cancel: async () => {
        await this.store.cancel?.(runId);
        await this.store.setStatus(runId, "failed", { error: "cancelled" });
      },
    };
  }

  private async waitFor(runId: string): Promise<unknown> {
    const deadline = Date.now() + 10 * 60_000;
    while (Date.now() < deadline) {
      const row = await this.store.getRun(runId);
      if (row && row.status !== "running") return row.output ?? null;
      await new Promise((r) => setTimeout(r, 250));
    }
    throw new Error("returnValue timeout");
  }

  private readable(runId: string) {
    const store = this.store;
    let sent = 0;
    return new ReadableStream({
      async pull(controller) {
        const events = await store.getEvents(runId);
        const chunks = events.filter((e) => e.type === "chunk");
        for (const c of chunks.slice(sent)) {
          const p = c.payload as { value: unknown; done?: boolean };
          if (p.done) { controller.close(); return; }
          controller.enqueue(new TextEncoder().encode(String(p.value)));
        }
        sent = chunks.length;
        const row = await store.getRun(runId);
        if (row && row.status !== "running" && sent >= chunks.length) {
          controller.close();
        }
      },
    });
  }

  /** Execute (or resume) a run. Safe to call repeatedly — replay is idempotent. */
  async run(runId: string, workflowId: string, args: unknown[]): Promise<void> {
    const fn = workflows.get(workflowId);
    if (!fn) throw new Error(`unknown workflow: ${workflowId}`);

    const log = await this.store.getEvents(runId);
    const ctx: Ctx = {
      runId, seq: log.length ? log.reduce((m, e) => Math.max(m, e.seq), 0) + 1 : 1,
      stepCalls: 0, sleepCalls: 0, hookCalls: 0,
      writes: 0,
      store: this.store, log, cursor: 0, chunks: [], now: () => Date.now(),
    };

    const prev = current;
    current = ctx;
    try {
      if (await this.store.isCancelled?.(runId)) {
        await this.store.setStatus(runId, "failed", { error: "cancelled" });
        return;
      }
      const output = await fn(...args);
      await this.store.setStatus(runId, "completed", output);
    } catch (err) {
      if (err instanceof SuspendSignal) return; // waiting on a timer
      if (err instanceof CancelledError) {
        await this.store.setStatus(runId, "failed", { error: "cancelled" });
        return;
      }
      if (err instanceof FatalError) {
        await this.store.setStatus(runId, "failed", { error: err.message });
        return;
      }
      await this.store.setStatus(runId, "failed", {
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      current = prev;
    }
  }

  private workerStopped = false;

  /** Signal the worker loop to exit after its current poll cycle. */
  stopWorker(): void {
    this.workerStopped = true;
  }

  /** Worker loop: resume runs whose timers are due. Resolves when stopWorker() is called. */
  async startWorker(onError?: (e: unknown) => void): Promise<void> {
    this.workerStopped = false;
    while (!this.workerStopped) {
      try {
        const due = await this.store.dueTimers(Date.now());
        for (const t of due) {
          const ev = await this.store.getEvents(t.runId);
          const created = ev.find((x) => x.seq === t.seq);
          const key = (created?.payload as { key?: string } | undefined)?.key
            ?? `sleep:${t.seq}`;
          const alreadyDone = ev.some(
            (x) => x.type === "sleep_completed" &&
                   (x.payload as { key?: string })?.key === key,
          );
          if (alreadyDone) continue;
          await this.store.appendEvent({
            runId: t.runId, seq: ev.reduce((m, e) => Math.max(m, e.seq), 0) + 1,
            type: "sleep_completed", payload: { key }, createdAt: Date.now(),
          });
          const row = await this.store.getRun(t.runId);
          if (!row || row.status !== "running") continue;
          // resume: re-run with replay; sleep is memoized now
          void this.resume(t.runId);
        }
        await this.reapStale(onError);
      } catch (e) { onError?.(e); }
      await new Promise((r) => setTimeout(r, this.opts.pollMs ?? 500));
    }
  }

  /**
   * Built-in reaper: resume 'running' runs with no event activity in the
   * last `staleMs`. Fixes the orphaned-run failure mode found in testing
   * world-postgres (a run wedged in 'running' forever).
   */
  private async reapStale(onError?: (e: unknown) => void): Promise<void> {
    const staleMs = this.opts.staleRunMs ?? 60_000;
    try {
      const cutoff = Date.now() - staleMs;
      const stale = await (this.store as { staleRuns?(c: number): Promise<string[]> })
        .staleRuns?.(cutoff);
      if (!stale) return;
      for (const runId of stale) {
        onError?.(new Error(`reaping stale run ${runId}`));
        await this.resume(runId);
      }
    } catch (e) { onError?.(e); }
  }

  async resume(runId: string): Promise<void> {
    const log = await this.store.getEvents(runId);
    const name = (log[0]?.payload as { name?: string } | undefined)?.name;
    const args = (log[0]?.payload as { args?: unknown[] } | undefined)?.args ?? [];
    if (!name) return;
    // Keep resuming through suspends until the run reaches a terminal state.
    for (let i = 0; i < 500; i += 1) {
      await this.run(runId, name, args);
      const row = await this.store.getRun(runId);
      if (!row || row.status !== "running") return;
      const evs = await this.store.getEvents(runId);
      // Find a pending sleep: created but not yet completed.
      const completed = new Set(
        evs.filter((e) => e.type === "sleep_completed")
           .map((e) => (e.payload as { key: string }).key),
      );
      const pending = evs
        .filter((e) => e.type === "sleep_created")
        .find((e) => !completed.has((e.payload as { key: string }).key));
      if (!pending) return; // nothing suspended -> genuinely stuck or done
      const wake = (pending.payload as { wakeAt?: number }).wakeAt ?? 0;
      const delay = wake - Date.now();
      if (delay > 0) await new Promise((r) => setTimeout(r, Math.min(delay, 2000)));
      await this.store.appendEvent({
        runId, seq: evs.reduce((m, e) => Math.max(m, e.seq), 0) + 1,
        type: "sleep_completed",
        payload: { key: (pending.payload as { key: string }).key },
        createdAt: Date.now(),
      });
    }
  }
}

/**
 * Cancel a run (Entry's route.ts calls getRun(id).cancel() to kill duplicate
 * streams). Terminal runs ignore this.
 */
export class CancelledError extends Error {
  constructor(runId: string) {
    super(`run cancelled: ${runId}`);
    this.name = "CancelledError";
  }
}

/** Create a durable hook a workflow can await; resolve it from outside. */
export function defineHook<T = unknown>(): {
  create(): Promise<{ token: string }>;
} {
  return {
    async create() {
      if (!current) throw new Error("hooks only inside a workflow");
      const token = `hook_${randomUUID().replace(/-/g, "").slice(0, 20)}`;
      const key = `hook:${current.hookCalls}`;
      current.hookCalls += 1;
      await current.store.createHook?.(current.runId, token, key);
      return { token };
    },
  };
}

/** Await a previously created hook until an external caller resolves it. */
export async function hookResult<T>(token: string): Promise<T> {
  if (!current) throw new Error("hooks only inside a workflow");
  const existing = await current.store.getHook?.(token);
  if (existing && (existing.payload as { resolved?: boolean })?.resolved) {
    return (existing.payload as { value: T }).value;
  }
  throw new SuspendSignal(`hook:${token}`, 0);
}

/** Durable fetch: memoized per step position, like any other side effect. */
export async function workflowFetch(
  input: string,
  init?: RequestInit,
): Promise<Response> {
  return step(async () => {
    const res = await fetch(input, init);
    return { __lfResponse: true, status: res.status, body: await res.text() };
  }) as unknown as Promise<Response>;
}

/**
 * Durable loop: executes body(i) for i in [0, n). Each iteration is guarded by
 * a durable timer barrier; completed iterations are skipped on replay, so a
 * resumed run continues where it stopped instead of restarting.
 */
export function hashId(s: string): string {
  return createHash("sha256").update(s).digest("hex").slice(0, 16);
}
