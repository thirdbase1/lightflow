# Swapping Vercel Workflow → lightflow in entry-agents

lightflow ships a compatibility layer that mirrors the exact Vercel Workflow
API surface entry-agents consumes. The swap is a dependency + import-path
change — workflow code, routes, and stream semantics stay untouched.

## What entry-agents actually uses (audited)

| Import | Names used | Where |
|---|---|---|
| `workflow` | `sleep`, `FatalError`, `getWorkflowMetadata`, `getWritable`, `fetch as workflowFetch` | workflows, lib |
| `workflow/api` | `start(fn, args)`, `getRun(runId)` (sync), `run.status` (Promise), `run.runId`, `run.returnValue`, `run.getReadable({startIndex})` + `getTailIndex()`, `run.cancel()` | API routes, kick libs |
| `workflow/next` | `withWorkflow(config)` | next.config.ts |
| `@workflow/ai` | `WorkflowChatTransport` (client-side; talks to **your** routes, not the engine) | lib/abortable-chat-transport.ts |

Not used: hooks, queues, child workflows, webhooks, per-step retry options.

## The 3-step swap

### 1. Dependencies

```bash
npm uninstall workflow @workflow/ai
npm install lightflow-engine
```

### 2. Import rewrites (17 lines across the repo)

| Old | New |
|---|---|
| `from "workflow"` | `from "lightflow-engine/compat/workflow"` |
| `from "workflow/api"` | `from "lightflow-engine/compat/api"` |
| `from "workflow/next"` | `from "lightflow-engine/compat/next"` |

A single sed does it:

```bash
grep -rl 'from "workflow' apps/web --include='*.ts' --include='*.tsx' |
  xargs sed -i \
    -e 's|from "workflow/api"|from "lightflow-engine/compat/api"|g' \
    -e 's|from "workflow/next"|from "lightflow-engine/compat/next"|g' \
    -e 's|from "workflow"|from "lightflow-engine/compat/workflow"|g'
```

`@workflow/ai` needs **no replacement**: `WorkflowChatTransport` is a client
fetch wrapper over your own `/api/chat` routes — those routes keep the same
request/response contract (`createUIMessageStreamResponse`, the
`x-workflow-stream-tail-index` header, `startIndex` query param). Keep the
package, or copy `lib/abortable-chat-transport.ts` to extend plain
`@ai-sdk/react`'s `DefaultChatTransport` with the same reconnect logic.

### 3. Boot wiring (one file)

In your server entry (e.g. `instrumentation.ts` or the module the routes
share), point the compat layer at Postgres:

```ts
import { createPostgresStore } from "lightflow-engine/pg";
import { Engine } from "lightflow-engine";
import { initWorkflowApi } from "lightflow-engine/compat/api";

const store = await createPostgresStore(process.env.LIGHTFLOW_PG_URL!);
const engine = new Engine(store, { pollMs: 50 });
await engine.startWorker();          // resumes suspended runs, fires timers
initWorkflowApi(store, engine);
```

### 4. next.config.ts

```diff
-import { withWorkflow } from "workflow/next";
-export default withWorkflow(withBotId(nextConfig));
+export default withBotId(nextConfig);
```

The `"use workflow"` / `"use step"` directives stay in the source (they are
inert strings); wrap step bodies with `step()` from
`lightflow-engine/compat/workflow` where durability is wanted — or use
`lightflow-engine/compat/next`'s loader, which rewrites `"use step"`-marked
functions automatically.

## Semantics preserved

- `start(fn, args)` — takes the function directly, returns `{ runId }`
  (`lrun_`-prefixed instead of `wrun_` — stored opaquely by entry-agents).
- `getRun(runId)` is **synchronous**; `.status` is a Promise resolving to
  `"pending" | "running" | "completed" | "failed" | "cancelled"`.
- `run.getReadable({ startIndex })` — chunk-indexed resumable stream with
  `getTailIndex()`; reconnects replay exactly the chunks after `startIndex`
  (this is what powers `AbortableChatTransport` reconnects).
- `run.cancel()` — takes effect at the next suspension point; status
  becomes `"cancelled"`.
- `sleep(new Date(atMs))` — durable timers, survive process restarts.
- `FatalError` — skips retries, fails the run.
- `getWorkflowMetadata()` → `{ workflowRunId }`.
- `getWritable<T>()` → web-standard `WritableStream` (`.getWriter()`),
  writes persisted as durable chunk events and replayed on reconnect.
- `workflowFetch` — plain `fetch` today (no step-offloading); safe swap.

## Differences (honest list)

- Chunk `startIndex` resume replays from a compacted snapshot after ~200
  steps — identical observable behavior, cheaper replay.
- Retries: lightflow defaults to 3 attempts per step (matching Vercel's
  default); per-step retry *options* are not configurable yet.
- No Vercel dashboard — inspect runs via SQL (`lightflow_runs`,
  `lightflow_events`) or `getRun()`.
- Self-hosted: your Postgres is the world. No vendor lock, no Vercel-only
  infra.

## Verified

`test/compat.ts` exercises the exact entry-agents surface end-to-end:
start → status → returnValue → getReadable(startIndex resume) →
getTailIndex → cancel → getWorkflowMetadata → getWritable WritableStream →
sleep across a durable timer. 7/7 tests pass, perf unchanged (≈504 runs/s,
≈9,000 steps/s durable).
