/**
 * Vercel Workflow compatibility layer — `workflow` module surface.
 *
 * Drop-in for entry-agents-style code: same named exports, same shapes.
 * Directive notes:
 *  - "use workflow" / "use step" are inert strings at runtime. This layer
 *    makes directly-called "use step" functions durable via the companion
 *    webpack loader (workflow/next) or explicit step() wrappers.
 */
import { createHash } from "node:crypto";
import { step, registerStep, } from "../index.js";
import { current } from "../index.js";
export { FatalError, CancelledError, sleep } from "../index.js";
export { getWorkflowMetadata } from "../compat/metadata.js";
export { workflowFetch } from "../compat/fetch.js";
/**
 * Vercel's getWritable returns a web-standard WritableStream whose writes
 * are durable chunk events. Ours maps 1:1: every writer.write(chunk) is a
 * memoized chunk event keyed by call position; writer.close() emits the
 * terminal done marker.
 */
export function getWritable() {
    const ctx = current;
    if (!ctx)
        throw new Error("getWritable() outside a workflow");
    const underlying = {
        async write(chunk) {
            // Mirror of engine getWritable().write but through ctx.append.
            // Key includes the durable-timer position so a post-resume write
            // can never collide with (and be skipped against) a pre-timer chunk.
            const key = `w:${ctx.sleepCalls}:${ctx.writes}`;
            const already = ctx.memo.has(`chunk:${key}`);
            ctx.writes += 1;
            if (already)
                return;
            const index = await ctx.store.nextChunkIndex(ctx.runId);
            ctx.seq += 1;
            const payload = { value: chunk, index, key };
            ctx.append({
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
            ctx.append({
                runId: ctx.runId, seq: ctx.seq, type: "chunk",
                payload: { value: null, done: true, key: "close", index: ctx.writes },
                createdAt: ctx.now(),
            });
        },
    };
    return new WritableStream({
        async write(chunk) { await underlying.write(chunk); },
        async close() { await underlying.close(); },
        async abort() { },
    });
}
/** Auto-registration id for a workflow function (stable per code site). */
export function workflowIdFor(fn) {
    const named = fn.name;
    if (named && named !== "anonymous")
        return named;
    return "wf_" + createHash("sha256")
        .update(String(fn).slice(0, 512)).digest("hex").slice(0, 16);
}
/**
 * makeStep — turns a plain async function into a durable step callable.
 *
 * Entry's "use step" functions take arguments; lightflow's step() takes a
 * thunk. makeStep wraps both: the returned function is a drop-in for the
 * original (same signature), but each call becomes a memoized, retrying,
 * replayable step keyed by call position. Outside a workflow it degrades
 * to a plain call, so tests and non-durable paths keep working.
 */
export function makeStep(fn, opts) {
    const wrapped = async (...args) => step(() => fn(...args));
    Object.defineProperty(wrapped, "__lightflowStepId", {
        value: `step_${workflowIdFor(fn)}`,
        enumerable: false,
    });
    if (opts?.retries !== undefined) {
        Object.defineProperty(wrapped, "__lightflowRetries", {
            value: opts.retries,
            enumerable: false,
        });
    }
    registerStep(wrapped.__lightflowStepId, fn);
    return wrapped;
}
