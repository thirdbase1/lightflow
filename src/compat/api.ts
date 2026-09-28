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

import { Engine, registerWorkflow, type Store } from "../index.js";
import { workflowIdFor } from "./workflow.js";

export type VercelRunStatus =
  | "pending" | "running" | "completed" | "failed" | "cancelled";

export type VercelRun = {
  runId: string;
  /** Vercel's run handle exposes exists; ours always resolves from the DB. */
  readonly exists: true;
  /** Live getter: each access returns a FRESH promise snapshotting the
   *  current status ("pending"/"running" until terminal). Vercel's is a
   *  getter too (chat.test.ts mocks `get status()`), and entry-agents'
   *  startStopMonitor re-awaits it in a 150ms poll loop. */
  readonly status: Promise<VercelRunStatus>;
  returnValue: Promise<unknown>;
  getReadable<T = unknown>(opts?: { startIndex?: number }): ReadableStream<T> & {
    getTailIndex(): Promise<number>;
  };
  cancel(): Promise<void>;
};

// Cross-bundle singleton: Next.js loads instrumentation and route handlers as
// separate module instances, so module-level state is NOT shared. Persisting
// on globalThis makes the engine/store visible to every bundle in-process.
type CompatGlobal = typeof globalThis & {
  __lightflowCompat?: { engine: Engine | null; store: Store | null };
};
const g = globalThis as CompatGlobal;
if (!g.__lightflowCompat) g.__lightflowCompat = { engine: null, store: null };

/** Configure the compat layer with a store/engine (call once at boot). */
export function initWorkflowApi(store: Store, engine?: Engine): void {
  g.__lightflowCompat!.store = store;
  g.__lightflowCompat!.engine = engine ?? new Engine(store);
}

export function getEngine(): Engine {
  if (!g.__lightflowCompat!.engine) {
    throw new Error(
      "workflow/api not initialized — call initWorkflowApi(store) at startup",
    );
  }
  return g.__lightflowCompat!.engine;
}

/**
 * Register a workflow function under a stable id and start it. Mirrors
 * Vercel's start(workflowFn, args).
 */
function makeRunHandle(runId: string): VercelRun {
  const engine = getEngine();
  const store = g.__lightflowCompat!.store!;

  /** One DB read -> Vercel status vocabulary. */
  const readStatus = async (): Promise<VercelRunStatus> => {
    const row = await store.getRun(runId);
    if (!row) return "pending";
    if (row.status === "completed") return "completed";
    if (row.status === "failed") {
      return (row.output as { error?: string } | null)?.error === "cancelled"
        ? "cancelled"
        : "failed";
    }
    return "running";
  };

  /** Promise resolving when the run reaches a terminal state. */
  const terminal = (cache: { status?: VercelRunStatus }): Promise<VercelRunStatus> => {
    const deadline = Date.now() + 10 * 60_000;
    let delay = 25;
    const loop = async (): Promise<VercelRunStatus> => {
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

  const cache: { status?: VercelRunStatus } = {};
  let lazyReturnValue: Promise<unknown> | undefined;
  let pendingRejection: unknown;
  const handle: VercelRun = {
    runId,
    exists: true,
    get status() {
      // Live snapshot: resolve immediately with the current state; the
      // startStopMonitor poll loop relies on this re-reading each tick.
      return readStatus().then((st) => {
        cache.status = st;
        return st;
      });
    },
    // Lazy: created on first access so a handle whose returnValue is never
    // awaited (e.g. status-only getRun polls) can't produce an unhandled
    // rejection when the run fails.
    get returnValue() {
      if (!lazyReturnValue) {
        lazyReturnValue = (async () => {
          try {
            await terminal(cache);
            return await (await engine.getRun(runId)).returnValue;
          } catch (e) {
            pendingRejection = e;
            throw e;
          }
        })();
        if (pendingRejection) throw pendingRejection; // unreachable
      }
      return lazyReturnValue;
    },
    getReadable<T>(opts?: { startIndex?: number }) {
      const startIndex = opts?.startIndex ?? 0;
      let sent = startIndex;
      let closed = false;
      const collectChunks = async (): Promise<
        { index: number; value: unknown; done?: boolean }[]
      > => {
        // Chunks can live in two places: the latest snapshot memo
        // (compaction folds pre-snapshot chunks) and as raw events after
        // it. Merge, dedupe by key, and order by chunk index.
        const events = await store.getEvents(runId);
        const byKey = new Map<string, { index: number; value: unknown; done?: boolean }>();
        for (const e of events) {
          if ((e.type as string) === "snapshot") {
            const memo = (e.payload as { memo?: Record<string, unknown> }).memo ?? {};
            for (const [k, v] of Object.entries(memo)) {
              if (!k.startsWith("chunk:")) continue;
              const p = v as { value: unknown; done?: boolean; key: string; index: number };
              byKey.set(p.key, p);
            }
          } else if ((e.type as string) === "chunk") {
            const p = e.payload as { value: unknown; done?: boolean; key: string; index: number };
            byKey.set(p.key, p);
          }
        }
        return [...byKey.values()].sort((a, b) => a.index - b.index);
      };
      const stream = new ReadableStream<T>({
        async pull(controller) {
          if (closed) { controller.close(); return; }
          const chunks = await collectChunks();
          const hasDone = chunks.some((c) => c.done);
          for (const c of chunks) {
            if (c.done) { closed = true; controller.close(); return; }
            if (c.index < startIndex) continue;
            if (c.index >= sent) {
              controller.enqueue(c.value as T);
              sent = c.index + 1;
            }
          }
          if (hasDone) { closed = true; controller.close(); return; }
          const row = await store.getRun(runId);
          if (row && row.status !== "running") {
            closed = true; controller.close();
          }
        },
      }) as ReadableStream<T> & { getTailIndex(): Promise<number> };
      stream.getTailIndex = async () => {
        const chunks = await collectChunks();
        let max = -1;
        for (const c of chunks) {
          if (c.done) return max;
          if (typeof c.index === "number" && c.index > max) max = c.index;
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

export async function start(
  fn: (...args: never[]) => Promise<unknown>,
  args: unknown[],
): Promise<VercelRun> {
  const engine = getEngine();
  const id = workflowIdFor(fn as (...a: unknown[]) => Promise<unknown>);
  registerWorkflow(id, fn as (...a: unknown[]) => Promise<unknown>);
  const { runId } = await engine.start(id, args);
  return makeRunHandle(runId);
}

export function getRun(runId: string): VercelRun {
  return makeRunHandle(runId);
}

