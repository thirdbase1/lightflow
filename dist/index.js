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
    fatal = true;
    constructor(message) {
        super(message);
        this.name = "FatalError";
    }
}
const workflows = new Map();
const steps = new Map();
export function registerWorkflow(id, fn) {
    workflows.set(id, fn);
}
export function registerStep(id, fn) {
    steps.set(id, fn);
}
let current = null;
export function getWorkflowMetadata() {
    if (!current)
        throw new Error("getWorkflowMetadata() outside a workflow");
    return { runId: current.runId };
}
/* ------------------------------------------------------------------ */
/* The step id: derived from call order, so replay is deterministic    */
/* ------------------------------------------------------------------ */
function nextStepKey() {
    if (!current)
        throw new Error("steps can only be called inside a workflow");
    return `step:${current.stepCalls}`;
}
/* ------------------------------------------------------------------ */
/* Public API inside a workflow                                        */
/* ------------------------------------------------------------------ */
/**
 * Run a side effect durably. On replay the memoized result is returned and
 * the function body is NOT re-executed.
 */
export async function step(fn) {
    if (!current)
        return fn(); // plain call outside a workflow
    const ctx = current;
    const key = nextStepKey();
    ctx.stepCalls += 1;
    // Replay: memoized result for this call position?
    const done = ctx.log.find((e) => e.type === "step_completed" && e.payload?.key === key);
    if (done)
        return done.payload.value;
    ctx.seq += 1;
    await ctx.store.appendEvent({
        runId: ctx.runId, seq: ctx.seq, type: "step_started",
        payload: { key }, createdAt: ctx.now(),
    });
    const fnId = fn.__lightflowStepId;
    const impl = fnId ? steps.get(fnId) : undefined;
    // Retry with backoff. FatalError is never retried (Entry retries 4x, then
    // bubbles FatalError -- same contract here).
    const maxAttempts = fn
        .__lightflowRetries ?? DEFAULT_STEP_RETRIES;
    let lastErr;
    let value;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        try {
            value = await (impl ? impl() : fn());
            lastErr = undefined;
            break;
        }
        catch (err) {
            lastErr = err;
            if (err instanceof FatalError)
                break;
            if (attempt < maxAttempts) {
                await new Promise((r) => setTimeout(r, Math.min(100 * 2 ** (attempt - 1), 2000)));
            }
        }
    }
    if (lastErr instanceof FatalError)
        throw lastErr;
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
export async function sleep(until) {
    if (!current) {
        const ms = until instanceof Date ? until.getTime() - Date.now() : until;
        return new Promise((r) => setTimeout(r, Math.max(0, ms)));
    }
    const ctx = current;
    const wakeAt = until instanceof Date ? until.getTime() : ctx.now() + until;
    const key = `sleep:${ctx.sleepCalls}`;
    ctx.sleepCalls += 1;
    // Replay: already slept?
    const done = ctx.log.find((e) => e.type === "sleep_completed" && e.payload?.key === key);
    if (done)
        return;
    ctx.seq += 1;
    await ctx.store.appendEvent({
        runId: ctx.runId, seq: ctx.seq, type: "sleep_created",
        payload: { key, wakeAt }, createdAt: ctx.now(),
    });
    // Suspend: throw a control-flow signal caught by the runner.
    throw new SuspendSignal(key, wakeAt);
}
export class SuspendSignal {
    key;
    wakeAt;
    constructor(key, wakeAt) {
        this.key = key;
        this.wakeAt = wakeAt;
    }
}
/** Ordered output stream for a run. Chunks are persisted and replayable. */
export function getWritable() {
    const ctx = current;
    if (!ctx)
        throw new Error("getWritable() outside a workflow");
    return {
        async write(chunk) {
            const index = ctx.writes;
            ctx.writes += 1;
            const already = ctx.log.some((e) => e.type === "chunk" &&
                e.payload?.index === index);
            if (already)
                return;
            ctx.seq += 1;
            await ctx.store.appendEvent({
                runId: ctx.runId, seq: ctx.seq, type: "chunk",
                payload: { value: chunk, index }, createdAt: ctx.now(),
            });
        },
        async close() {
            const already = ctx.log.some((e) => e.type === "chunk" && e.payload?.done === true);
            if (already)
                return;
            ctx.seq += 1;
            await ctx.store.appendEvent({
                runId: ctx.runId, seq: ctx.seq, type: "chunk",
                payload: { value: null, done: true, index: ctx.writes },
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
    store;
    opts;
    constructor(store, opts = {}) {
        this.store = store;
        this.opts = opts;
    }
    async start(workflowId, args) {
        const runId = `lrun_${randomUUID().replace(/-/g, "").slice(0, 24)}`;
        await this.store.createRun(runId, workflowId, args);
        void this.run(runId, workflowId, args);
        return { runId };
    }
    async getRun(runId) {
        const row = await this.store.getRun(runId);
        if (!row)
            throw new Error(`run not found: ${runId}`);
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
    async waitFor(runId) {
        const deadline = Date.now() + 10 * 60_000;
        while (Date.now() < deadline) {
            const row = await this.store.getRun(runId);
            if (row && row.status !== "running")
                return row.output ?? null;
            await new Promise((r) => setTimeout(r, 250));
        }
        throw new Error("returnValue timeout");
    }
    readable(runId) {
        const store = this.store;
        let sent = 0;
        return new ReadableStream({
            async pull(controller) {
                const events = await store.getEvents(runId);
                const chunks = events.filter((e) => e.type === "chunk");
                for (const c of chunks.slice(sent)) {
                    const p = c.payload;
                    if (p.done) {
                        controller.close();
                        return;
                    }
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
    async run(runId, workflowId, args) {
        const fn = workflows.get(workflowId);
        if (!fn)
            throw new Error(`unknown workflow: ${workflowId}`);
        const log = await this.store.getEvents(runId);
        const ctx = {
            runId, seq: log.length ? log.reduce((m, e) => Math.max(m, e.seq), 0) + 1 : 1,
            stepCalls: 0, sleepCalls: 0, writes: 0, hookCalls: 0,
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
        }
        catch (err) {
            if (err instanceof SuspendSignal)
                return; // waiting on a timer
            if (err instanceof FatalError) {
                await this.store.setStatus(runId, "failed", { error: err.message });
                return;
            }
            await this.store.setStatus(runId, "failed", {
                error: err instanceof Error ? err.message : String(err),
            });
        }
        finally {
            current = prev;
        }
    }
    /** Worker loop: resume runs whose timers are due. */
    async startWorker(onError) {
        for (;;) {
            try {
                const due = await this.store.dueTimers(Date.now());
                for (const t of due) {
                    const ev = await this.store.getEvents(t.runId);
                    const created = ev.find((x) => x.seq === t.seq);
                    const key = created?.payload?.key
                        ?? `sleep:${t.seq}`;
                    const alreadyDone = ev.some((x) => x.type === "sleep_completed" &&
                        x.payload?.key === key);
                    if (alreadyDone)
                        continue;
                    await this.store.appendEvent({
                        runId: t.runId, seq: ev.reduce((m, e) => Math.max(m, e.seq), 0) + 1,
                        type: "sleep_completed", payload: { key }, createdAt: Date.now(),
                    });
                    const row = await this.store.getRun(t.runId);
                    if (!row || row.status !== "running")
                        continue;
                    // resume: re-run with replay; sleep is memoized now
                    void this.resume(t.runId);
                }
                await this.reapStale(onError);
            }
            catch (e) {
                onError?.(e);
            }
            await new Promise((r) => setTimeout(r, this.opts.pollMs ?? 500));
        }
    }
    /**
     * Built-in reaper: resume 'running' runs with no event activity in the
     * last `staleMs`. Fixes the orphaned-run failure mode found in testing
     * world-postgres (a run wedged in 'running' forever).
     */
    async reapStale(onError) {
        const staleMs = this.opts.staleRunMs ?? 60_000;
        try {
            const cutoff = Date.now() - staleMs;
            const stale = await this.store
                .staleRuns?.(cutoff);
            if (!stale)
                return;
            for (const runId of stale) {
                onError?.(new Error(`reaping stale run ${runId}`));
                await this.resume(runId);
            }
        }
        catch (e) {
            onError?.(e);
        }
    }
    async resume(runId) {
        const log = await this.store.getEvents(runId);
        const name = log[0]?.payload?.name;
        const args = log[0]?.payload?.args ?? [];
        if (!name)
            return;
        // Keep resuming through suspends until the run reaches a terminal state.
        for (let i = 0; i < 500; i += 1) {
            await this.run(runId, name, args);
            const row = await this.store.getRun(runId);
            if (!row || row.status !== "running")
                return;
            const evs = await this.store.getEvents(runId);
            // Find a pending sleep: created but not yet completed.
            const completed = new Set(evs.filter((e) => e.type === "sleep_completed")
                .map((e) => e.payload.key));
            const pending = evs
                .filter((e) => e.type === "sleep_created")
                .find((e) => !completed.has(e.payload.key));
            if (!pending)
                return; // nothing suspended -> genuinely stuck or done
            const wake = pending.payload.wakeAt ?? 0;
            const delay = wake - Date.now();
            if (delay > 0)
                await new Promise((r) => setTimeout(r, Math.min(delay, 2000)));
            await this.store.appendEvent({
                runId, seq: evs.reduce((m, e) => Math.max(m, e.seq), 0) + 1,
                type: "sleep_completed",
                payload: { key: pending.payload.key },
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
    constructor(runId) {
        super(`run cancelled: ${runId}`);
        this.name = "CancelledError";
    }
}
/** Create a durable hook a workflow can await; resolve it from outside. */
export function defineHook() {
    return {
        async create() {
            if (!current)
                throw new Error("hooks only inside a workflow");
            const token = `hook_${randomUUID().replace(/-/g, "").slice(0, 20)}`;
            const key = `hook:${current.hookCalls}`;
            current.hookCalls += 1;
            await current.store.createHook?.(current.runId, token, key);
            return { token };
        },
    };
}
/** Await a previously created hook until an external caller resolves it. */
export async function hookResult(token) {
    if (!current)
        throw new Error("hooks only inside a workflow");
    const existing = await current.store.getHook?.(token);
    if (existing && existing.payload?.resolved) {
        return existing.payload.value;
    }
    throw new SuspendSignal(`hook:${token}`, 0);
}
/** Durable fetch: memoized per step position, like any other side effect. */
export async function workflowFetch(input, init) {
    return step(async () => {
        const res = await fetch(input, init);
        return { __lfResponse: true, status: res.status, body: await res.text() };
    });
}
export function hashId(s) {
    return createHash("sha256").update(s).digest("hex").slice(0, 16);
}
