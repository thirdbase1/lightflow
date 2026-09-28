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
  readonly status: Promise<VercelRunStatus>;
  returnValue: Promise<unknown>;
  getReadable<T = unknown>(opts?: { startIndex?: number }): ReadableStream<T> & {
    getTailIndex(): Promise<number>;
  };
  cancel(): Promise<void>;
};

let sharedEngine: Engine | null = null;
let sharedStore: Store | null = null;

/** Configure the compat layer with a store/engine (call once at boot). */
export function initWorkflowApi(store: Store, engine?: Engine): void {
  sharedStore = store;
  sharedEngine = engine ?? new Engine(store);
}

export function getEngine(): Engine {
  if (!sharedEngine) {
    throw new Error(
      "workflow/api not initialized — call initWorkflowApi(store) at startup",
    );
  }
  return sharedEngine;
}

/**
 * Register a workflow function under a stable id and start it. Mirrors
 * Vercel's start(workflowFn, args).
 */
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

function makeRunHandle(runId: string): VercelRun {
  const engine = getEngine();
  const store = sharedStore!;
  let cachedStatus: VercelRunStatus | null = null;

  const statusPromise: Promise<VercelRunStatus> = (async () => {
    if (cachedStatus) return cachedStatus;
    const deadline = Date.now() + 10 * 60_000;
    let delay = 5;
    let seenRow = false;
    while (Date.now() < deadline) {
      const row = await store.getRun(runId);
      if (row) {
        seenRow = true;
        if (row.status === "completed") { cachedStatus = "completed"; return cachedStatus; }
        if (row.status === "failed") {
          // cancelled runs are 'failed' with error 'cancelled'
          cachedStatus =
            (row.output as { error?: string } | null)?.error === "cancelled"
              ? "cancelled" : "failed";
          return cachedStatus;
        }
      }
      await new Promise((r) => setTimeout(r, delay));
      delay = seenRow ? Math.min(delay * 1.6, 250) : 5;
    }
    throw new Error(`run status timeout: ${runId}`);
  })();

  const handle: VercelRun = {
    runId,
    status: statusPromise,
    returnValue: (async () => (await engine.getRun(runId)).returnValue)(),
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
          // chunks carry a monotonic `index`; entries after startIndex
          for (const c of chunks) {
            const p = c as { value: unknown; done?: boolean; index: number };
            if (p.done) { closed = true; controller.close(); return; }
            if (p.index < startIndex) continue;
            if (p.index >= sent) {
              controller.enqueue(p.value as T);
              sent = p.index + 1;
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
