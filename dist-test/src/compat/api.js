/**
 * Vercel Workflow compat — `workflow/api` module surface.
 *
 * Differences from our core Engine API:
 *  - start(fn, args): takes the workflow FUNCTION (not an id); returns the
 *    run handle directly (not a promise).
 *  - getRun(runId): returns the handle synchronously; `status` is a Promise;
 *    status includes "pending" and "cancelled".
 *  - getReadable({ startIndex }): chunk-indexed resumable stream with
 *    getTailIndex().
 *  - run.cancel(), run.returnValue.
 */
import { Engine, registerWorkflow } from "../index.js";
import { workflowIdFor } from "./workflow.js";
let sharedEngine = null;
let sharedStore = null;
/** Configure the compat layer with a store/engine (call once at boot). */
export function initWorkflowApi(store, engine) {
    sharedStore = store;
    sharedEngine = engine ?? new Engine(store);
}
export function getEngine() {
    if (!sharedEngine) {
        throw new Error("workflow/api not initialized — call initWorkflowApi(store) at startup");
    }
    return sharedEngine;
}
/**
 * Register a workflow function under a stable id and start it. Mirrors
 * Vercel's start(workflowFn, args).
 */
function makeRunHandle(runId) {
    const engine = getEngine();
    const store = sharedStore;
    /** One DB read -> Vercel status vocabulary. */
    const readStatus = async () => {
        const row = await store.getRun(runId);
        if (!row)
            return "pending";
        if (row.status === "completed")
            return "completed";
        if (row.status === "failed") {
            return row.output?.error === "cancelled"
                ? "cancelled"
                : "failed";
        }
        return "running";
    };
    /** Promise resolving when the run reaches a terminal state. */
    const terminal = (cache) => {
        const deadline = Date.now() + 10 * 60_000;
        let delay = 25;
        const loop = async () => {
            while (Date.now() < deadline) {
                const st = await readStatus();
                if (st === "completed" || st === "failed" || st === "cancelled") {
                    cache.status = st;
                    return st;
                }
                await new Promise((r) => setTimeout(r, delay));
                delay = Math.min(delay * 1.4, 250);
            }
            throw new Error(`run status timeout: ${runId}`);
        };
        return loop();
    };
    const cache = {};
    const handle = {
        runId,
        get status() {
            // Live snapshot: resolve immediately with the current state; the
            // startStopMonitor poll loop relies on this re-reading each tick.
            return readStatus().then((st) => {
                cache.status = st;
                return st;
            });
        },
        returnValue: (async () => {
            await terminal(cache);
            return (await engine.getRun(runId)).returnValue;
        })(),
        getReadable(opts) {
            const startIndex = opts?.startIndex ?? 0;
            let sent = startIndex;
            let closed = false;
            const collectChunks = async () => {
                // Chunks can live in two places: the latest snapshot memo
                // (compaction folds pre-snapshot chunks) and as raw events after
                // it. Merge, dedupe by key, and order by chunk index.
                const events = await store.getEvents(runId);
                const byKey = new Map();
                for (const e of events) {
                    if (e.type === "snapshot") {
                        const memo = e.payload.memo ?? {};
                        for (const [k, v] of Object.entries(memo)) {
                            if (!k.startsWith("chunk:"))
                                continue;
                            const p = v;
                            byKey.set(p.key, p);
                        }
                    }
                    else if (e.type === "chunk") {
                        const p = e.payload;
                        byKey.set(p.key, p);
                    }
                }
                return [...byKey.values()].sort((a, b) => a.index - b.index);
            };
            const stream = new ReadableStream({
                async pull(controller) {
                    if (closed) {
                        controller.close();
                        return;
                    }
                    const chunks = await collectChunks();
                    const hasDone = chunks.some((c) => c.done);
                    for (const c of chunks) {
                        if (c.done) {
                            closed = true;
                            controller.close();
                            return;
                        }
                        if (c.index < startIndex)
                            continue;
                        if (c.index >= sent) {
                            controller.enqueue(c.value);
                            sent = c.index + 1;
                        }
                    }
                    if (hasDone) {
                        closed = true;
                        controller.close();
                        return;
                    }
                    const row = await store.getRun(runId);
                    if (row && row.status !== "running") {
                        closed = true;
                        controller.close();
                    }
                },
            });
            stream.getTailIndex = async () => {
                const chunks = await collectChunks();
                let max = -1;
                for (const c of chunks) {
                    if (c.done)
                        return max;
                    if (typeof c.index === "number" && c.index > max)
                        max = c.index;
                }
                return max;
            };
            return stream;
        },
        async cancel() {
            await (await engine.getRun(runId)).cancel();
        },
    };
    return handle;
}
export async function start(fn, args) {
    const engine = getEngine();
    const id = workflowIdFor(fn);
    registerWorkflow(id, fn);
    const { runId } = await engine.start(id, args);
    return makeRunHandle(runId);
}
export function getRun(runId) {
    return makeRunHandle(runId);
}
