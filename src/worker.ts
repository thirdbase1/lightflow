#!/usr/bin/env node
/**
 * lightflow standalone worker
 *
 * A long-running process that polls Postgres and executes durable workflow
 * runs — deployable to any always-on host (Pxxl worker service, VPS, Fly.io,
 * Railway). Your web app (e.g. Vercel serverless) only handles HTTP: it calls
 * lightflow's start()/getRun() against the SAME Postgres and this worker
 * executes the steps.
 *
 * Usage:
 *   LIGHTFLOW_PG_URL=postgres://... node dist/worker.js
 *
 * Optional env:
 *   LIGHTFLOW_POLL_MS   poll interval (default 50)
 *   LIGHTFLOW_WORKER_ID unique worker name for leases (default random)
 *   LIGHTFLOW_HEALTH_PORT  if set, serves GET /health on that port
 *   PORT                used for /health when LIGHTFLOW_HEALTH_PORT is unset
 */
import { createPostgresStore } from "./pg-store.js";
import { Engine } from "./index.js";
import { createServer } from "node:http";

const url = process.env.LIGHTFLOW_PG_URL ?? process.env.POSTGRES_URL;
if (!url) {
  console.error("[lightflow-worker] LIGHTFLOW_PG_URL (or POSTGRES_URL) is required");
  process.exit(1);
}

const pollMs = Number(process.env.LIGHTFLOW_POLL_MS ?? 50);
const workerId =
  process.env.LIGHTFLOW_WORKER_ID ?? `worker-${process.pid}-${Date.now().toString(36)}`;

const store = await createPostgresStore(url);
const engine = new Engine(store, { pollMs });
// Registers the step/workflow API on globalThis so `lightflow-engine/compat`
// callers in OTHER processes (web server) and this worker share one registry
// shape. Workflow functions themselves must be registered in the worker:
// import the module that defines them here, or use registerWorkflow().
const { initWorkflowApi } = await import("./compat/api.js");
initWorkflowApi(store, engine);

const worker = engine.startWorker();
console.log(`[lightflow-worker] started ${workerId} (pollMs=${pollMs})`);

// Optional health endpoint so always-on platforms (Pxxl web-type service,
// Fly, Kubernetes) can probe liveness. A pure worker with no route doesn't
// need it, but it costs nothing.
const healthPort = Number(process.env.LIGHTFLOW_HEALTH_PORT ?? process.env.PORT ?? 0);
if (healthPort) {
  createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, workerId, pollMs }));
    } else {
      res.writeHead(404);
      res.end();
    }
  }).listen(healthPort, "0.0.0.0", () =>
    console.log(`[lightflow-worker] health on :${healthPort}`),
  );
}

const shutdown = async (sig: string) => {
  console.log(`[lightflow-worker] ${sig} — stopping worker`);
  engine.stopWorker();
  await worker;
  process.exit(0);
};
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
