# Self-Hosting the lightflow Worker on Pxxl

Vercel (and every serverless platform) freezes your process between requests.
lightflow's worker is a **polling loop that must stay alive** to pick up runs,
fire durable sleeps, and resume crashed steps. The fix: run the worker as a
long-lived service on Pxxl, pointed at the same Postgres your web app uses.

Architecture:

```
┌──────────────────┐         ┌─────────────────┐
│ Vercel / any web │  HTTP   │  same Postgres  │
│ app (entry web)  │──────▶  │  lightflow_runs │
│ start()/stream() │         │  lightflow_events│
└──────────────────┘         └────────┬────────┘
                                      │ poll + execute
                             ┌────────▼────────┐
                             │ Pxxl worker     │
                             │ node dist/worker│
                             └─────────────────┘
```

Your web app's code does not change. It calls `start()`, `getRun()`, and reads
stream chunks from `LIGHTFLOW_PG_URL`. The worker executes the steps.

---

## What Pxxl expects (from their docs)

- Projects deploy from a Git repo with **install / build / start** commands.
- A **Worker-type service** runs a long-lived process, gets **no public route**,
  and must **not exit** — Pxxl restarts it if it does.
- Workers should **not bind a public port**; if you deploy the worker as a Web
  Service (or want a health check), expose `GET /health` on `$PORT`.
- Secrets live in the project's **Secrets** tab; runtime logs in **Live Logs**.
- Two supported layouts: a **separate Pxxl project** on the same repo with a
  different start command, or **Multiple Services** in one project
  (`Deploy > Build Configuration > Multiple Services`, or a `pxxl.toml`).

## Option A — separate Pxxl project (simplest)

1. Push this repo (or your fork) to GitHub.
2. In Pxxl: **Deploy > link the repo** and pick the `lightflow` project.
3. Build configuration:

   | Setting | Value |
   |---|---|
   | Runtime | Node.js |
   | Install command | `npm install` |
   | Build command | `npm run build` |
   | Start command | `node dist/worker.js` |
   | Service type | Worker (no public route) |
   | Branch | `main` |

4. Secrets (project → Secrets):

   | Key | Value |
   |---|---|
   | `LIGHTFLOW_PG_URL` | your Postgres URL — **required** |
   | `LIGHTFLOW_POLL_MS` | optional, default `50` |
   | `LIGHTFLOW_WORKER_ID` | optional, defaults to pid+timestamp |
   | `LIGHTFLOW_HEALTH_PORT` | optional; set only if you want `/health` |
   | `PORT` | set only if you enabled the health port |

   Your Postgres must be reachable from Pxxl's runtime (public host or Pxxl
   managed database — Pxxl can provision Postgres and sync the URL for you).
   If your URL uses `sslmode=require`, append `&uselibpqcompat=true` to silence
   the pg SSL-mode warning.

5. Deploy. Verify in **Live Logs**:

   ```
   [lightflow-worker] started worker-... (pollMs=50)
   ```

## Option B — Multiple Services (web + worker in one project)

If your web app and the worker share one repo/monorepo, enable **Multiple
Services** in `Deploy > Build Configuration` and add a `pxxl.toml`:

```toml
[services.web]
baseDirectory = "apps/web"        # your Next.js app
packageManager = "pnpm"
buildCommand = "cd ../.. && pnpm --filter web build"
startCommand = "pnpm run start"
port = 3000

[services.worker]
baseDirectory = "."               # repo root with lightflow
packageManager = "npm"
installCommand = "npm install"
buildCommand = "npm run build"
startCommand = "node dist/worker.js"
# no port, no route — worker services stay private
```

The `web` service gets the public route; `worker` runs un-routed next to it.
Both read the same `LIGHTFLOW_PG_URL` secret, so the worker executes what the
web app starts.

## Registering your workflow functions

The worker executes steps, so the workflow functions must be loadable there.
Two ways:

1. **Module side effects** — if your workflow modules use `"use workflow"` /
   `"use step"` via the compat transform, import them from a small entry file:

   ```js
   // worker-entry.mjs
   await import("lightflow-engine/worker");
   await import("./your-workflows.js"); // registers on import
   ```

   then start with `node worker-entry.mjs`.

2. **Explicit registration**:

   ```js
   import "lightflow-engine/worker";
   import { registerWorkflow, registerStep } from "lightflow-engine";
   registerWorkflow("runAgentWorkflow", runAgentWorkflow);
   ```

For entry-agents, the compat transform auto-registers imports on the shared
`globalThis` registry — importing the same modules the web app imports is
enough.

## Operations

- **Scaling**: one worker replica is enough — runs are claimed via
  `claimed_until` leases, so extra replicas are safe but rarely needed.
- **Logs**: Pxxl → project → Live Logs. The worker logs step failures and
  run transitions.
- **Redeploys**: Pxxl restarts the process on deploy; in-flight runs resume
  automatically from the event log on boot (that's the whole point).
- **Health**: if you exposed `LIGHTFLOW_HEALTH_PORT`, `GET /health` returns
  `{ok, workerId, pollMs}` — useful for Pxxl web-service health checks.
- **Migrations**: lightflow creates its tables automatically on first connect
  (`lightflow_runs`, `lightflow_events`, `lightflow_hooks`).

## Why not just run it on Vercel?

Serverless functions freeze between requests and cap execution time
(10–300s). Steps that sleep, wait on humans, or outlive a request would never
be picked back up. The worker is the piece Vercel can't host — everything else
(HTTP, streaming, auth) stays wherever it is.
