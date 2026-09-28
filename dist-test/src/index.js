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
export let current = null;
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
    // Replay: memoized result for this call position? (O(1) map lookup)
    const done = ctx.memo.get(`step_completed:${key}`);
    if (done)
        return done.payload.value;
    // Skip the step_started write entirely on the normal path: replay only
    // consults step_completed / step_failed, so starting is implicit.
    ctx.seq += 1;
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
    ctx.freshResults.set(key, value);
    ctx.append({
        runId: ctx.runId, seq: ctx.seq, type: "step_completed",
        payload: { key, value }, createdAt: ctx.now(),
    });
    await maybeSnapshot(ctx);
    return value;
}
const SNAPSHOT_INTERVAL = 200;
const SNAPSHOT_MIN_EVENTS = 100;
/**
 * History compaction: every SNAPSHOT_INTERVAL completed steps, fold the
 * memoized step results (and completed sleeps) into a single 'snapshot'
 * event. getEvents() replays from the latest snapshot, so old events stop
 * costing replay time and can be pruned later without breaking resumes.
 */
async function maybeSnapshot(ctx) {
    await Promise.all(ctx.inflight);
    ctx.completedNow += 1;
    const done = ctx.completedNow +
        [...ctx.memo.values()].filter((e) => e.type === "step_completed").length;
    if (done < SNAPSHOT_MIN_EVENTS || done % SNAPSHOT_INTERVAL !== 0)
        return;
    const memo = {};
    for (const e of ctx.memo.values()) {
        const key = e.payload.key;
        if (key === undefined)
            continue;
        if (e.type === "step_completed")
            memo[`step_completed:${key}`] = e.payload.value;
        else if (e.type === "sleep_completed")
            memo[`sleep_completed:${key}`] = true;
        else if (e.type === "chunk")
            memo[`chunk:${key}`] = e.payload;
    }
    for (const [key, value] of ctx.freshResults)
        memo[`step_completed:${key}`] = value;
    // include the step we just completed (not yet in ctx.memo)
    ctx.seq += 1;
    await ctx.store.appendEvent({
        runId: ctx.runId, seq: ctx.seq, type: "snapshot",
        payload: { memo }, createdAt: ctx.now(),
    });
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
    // Replay: already slept? (O(1) map lookup)
    if (ctx.memo.has(`sleep_completed:${key}`))
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
    if (!current)
        throw new Error("getWritable() outside a workflow");
    return {
        async write(chunk) {
            // Memoized by deterministic call position: (sleep position, writes
            // since it). sleepCalls survives a resume so a post-timer write can
            // never collide with (and be skipped against) a pre-timer chunk.
            const key = `w:${ctx.sleepCalls}:${ctx.writes}`;
            // Use the replay memo — no per-write re-fetch of the event log.
            const already = ctx.memo.has(`chunk:${key}`);
            ctx.writes += 1;
            if (already)
                return;
            const index = await ctx.store.nextChunkIndex(ctx.runId);
            ctx.seq += 1;
            const payload = { value: chunk, index, key };
            await ctx.store.appendEvent({
                runId: ctx.runId, seq: ctx.seq, type: "chunk",
                payload, createdAt: ctx.now(),
            });
            // Record in the live memo so snapshot compaction folds it.
            ctx.memo.set(`chunk:${key}`, { runId: ctx.runId, seq: ctx.seq,
                type: "chunk", payload, createdAt: ctx.now() });
        },
        async close() {
            const already = [...ctx.memo.values()].some((e) => e.type === "chunk" && e.payload?.done === true);
            if (already)
                return;
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
    store;
    opts;
    /** In-process run completions: runId -> deferred. Avoids polling entirely. */
    local = new Map();
    defer(runId) {
        let resolve;
        let reject;
        const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
        const d = { promise, resolve, reject };
        this.local.set(runId, d);
        return d;
    }
    constructor(store, opts = {}) {
        this.store = store;
        this.opts = opts;
    }
    async start(workflowId, args) {
        const runId = `lrun_${randomUUID().replace(/-/g, "").slice(0, 24)}`;
        await this.store.createRun(runId, workflowId, args);
        this.defer(runId);
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
        // In-process fast path: if this engine instance started the run, await
        // the deferred promise directly — no database polling at all.
        const d = this.local.get(runId);
        if (d)
            return d.promise;
        // Cross-process fallback: adaptive poll (5ms -> 250ms).
        const deadline = Date.now() + 10 * 60_000;
        let delay = 5;
        while (Date.now() < deadline) {
            const row = await this.store.getRun(runId);
            if (row && row.status !== "running")
                return row.output ?? null;
            await new Promise((r) => setTimeout(r, delay));
            delay = Math.min(delay * 1.6, 250);
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
    /** Execute (or resume) a run. Racing resumes are arbitrated by a lease. */
    async run(runId, workflowId, args) {
        const fn = workflows.get(workflowId);
        if (!fn)
            throw new Error(`unknown workflow: ${workflowId}`);
        // Run lease: only one replay executes at a time. A racing resume that
        // loses exits immediately — its side effects are already being written
        // by the winner, and any newer events it would have seen are picked up
        // by the next resume.
        const claimer = this.store;
        const claim = await claimer.claimRun?.(runId);
        if (claim && !claim.ok)
            return;
        if (claim?.cancelled) {
            await this.store.setStatus(runId, "failed", { error: "cancelled" });
            return;
        }
        const log = await this.store.getEvents(runId);
        // Memo index: one pass over the log, O(1) lookups during replay.
        // A leading 'snapshot' event pre-fills completed step/sleep/chunk state.
        const memo = new Map();
        for (const e of log) {
            if (e.type === "snapshot") {
                const m = e.payload.memo ?? {};
                for (const [k, v] of Object.entries(m)) {
                    if (k.startsWith("step_completed:")) {
                        memo.set(k, { runId, seq: 0, type: "step_completed",
                            payload: { key: k.slice("step_completed:".length), value: v }, createdAt: 0 });
                    }
                    else if (k.startsWith("sleep_completed:")) {
                        memo.set(k, { runId, seq: 0, type: "sleep_completed",
                            payload: { key: k.slice("sleep_completed:".length) }, createdAt: 0 });
                    }
                    else if (k.startsWith("chunk:")) {
                        memo.set(k, { runId, seq: 0, type: "chunk",
                            payload: v, createdAt: 0 });
                    }
                }
                continue;
            }
            const key = e.payload.key;
            if (key !== undefined && key !== null)
                memo.set(`${e.type}:${key}`, e);
        }
        const ctx = {
            runId, seq: log.length ? log.reduce((m, e) => Math.max(m, e.seq), 0) + 1 : 1,
            stepCalls: 0, sleepCalls: 0, hookCalls: 0,
            writes: 0,
            store: this.store, log, memo, completedNow: 0, freshResults: new Map(),
            inflight: [], cursor: 0, chunks: [], now: () => Date.now(),
            append: (e) => {
                // Pipelined append: capture the promise; callers only await at
                // suspension/end, so several step completions share one flush.
                const p = ctx.store.appendEvent(e).then(() => { ctx.inflight = ctx.inflight.filter(x => x !== p); });
                ctx.inflight.push(p);
                return p;
            },
        };
        const prev = current;
        current = ctx;
        try {
            const output = await fn(...args);
            await Promise.all(ctx.inflight);
            await (this.store.finishRun?.(runId, "completed", output)
                ?? this.store.setStatus(runId, "completed", output));
            this.local.get(runId)?.resolve(output);
        }
        catch (err) {
            if (err instanceof SuspendSignal) {
                await Promise.all(ctx.inflight);
                return; // waiting on a timer
            }
            const fail = async (error) => {
                await (this.store.finishRun?.(runId, "failed", {
                    error: error instanceof Error ? error.message : String(error),
                }) ?? this.store.setStatus(runId, "failed", {
                    error: error instanceof Error ? error.message : String(error),
                }));
                this.local.get(runId)?.reject(err instanceof Error ? err : new Error(String(err)));
            };
            if (err instanceof CancelledError)
                return fail("cancelled");
            if (err instanceof FatalError)
                return fail(err.message);
            return fail(err instanceof Error ? err.message : String(err));
        }
        finally {
            current = prev;
            await claimer.releaseRun?.(runId);
        }
    }
    workerStopped = false;
    listenClient;
    /** Signal the worker loop to exit after its current poll cycle. */
    stopWorker() {
        this.workerStopped = true;
        this.listenClient?.release?.();
    }
    /** Worker loop: resume runs whose timers are due. Resolves when stopWorker() is called. */
    async startWorker(onError) {
        this.workerStopped = false;
        // Adaptive poll: snap to work when there is any, back off when idle.
        let delay = this.opts.pollMs ?? 500;
        const minDelay = 50;
        // Optional LISTEN/NOTIFY: a dedicated client turns notifications into an
        // immediate poll. Lossy by design — polling remains the correctness path.
        let wake = () => { delay = minDelay; };
        const listener = this.store.getListenClient?.();
        if (listener) {
            try {
                await listener.query("LISTEN lightflow_wake");
                listener.on("notification", () => wake());
                this.listenClient = listener;
            }
            catch {
                listener.release?.();
            }
        }
        while (!this.workerStopped) {
            try {
                const due = await this.store.dueTimers(Date.now());
                if (due.length)
                    delay = minDelay;
                for (const t of due) {
                    // dueTimers() already excludes completed timers (anti-join), so no
                    // per-timer getEvents round trip: complete directly and resume.
                    await this.store.appendEvent({
                        runId: t.runId, seq: 0, // seq=0 forces slow path (collision) -> lock-allocated max+1
                        type: "sleep_completed", payload: { key: t.key }, createdAt: Date.now(),
                    });
                    const row = await this.store.getRun(t.runId);
                    if (!row || row.status !== "running")
                        continue;
                    void this.resume(t.runId);
                }
                await this.reapStale(onError);
            }
            catch (e) {
                onError?.(e);
            }
            await new Promise((r) => setTimeout(r, delay));
            delay = Math.min(Math.round(delay * 1.5), this.opts.pollMs ?? 500);
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
/**
 * Durable loop: executes body(i) for i in [0, n). Each iteration is guarded by
 * a durable timer barrier; completed iterations are skipped on replay, so a
 * resumed run continues where it stopped instead of restarting.
 */
export function hashId(s) {
    return createHash("sha256").update(s).digest("hex").slice(0, 16);
}
