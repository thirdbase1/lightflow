# Self-Hosting the lightflow Worker on Brimble

Brimble has a first-class **Worker** service type — a long-running process with
no HTTP port, exactly what lightflow's polling worker is. This is the simplest
deployment target for the worker; your web app (Vercel or anything else) stays
as-is and talks to the same Postgres.

```
┌──────────────────┐         ┌──────────────────┐
│ Vercel / web app │  HTTP   │  same Postgres   │
│ start()/stream() │──────▶  │  lightflow_runs  │
└──────────────────┘         │  lightflow_events│
                             └────────┬─────────┘
                                      │ poll + execute
                             ┌────────▼─────────┐
                             │ Brimble Worker   │
                             │ node dist/worker │
                             └──────────────────┘
```

## Why Brimble fits well

From Brimble's docs (`paper.brimble.io`):

- **Worker service type**: "a long-running process with no HTTP port … for
  queue consumers, schedulers, message handlers, or any background process
  that does its own work." No `PORT`, no public URL, no HTTP health probe —
  health is **process liveness**.
- **Auto-restart**: if the process exits (clean or crash), Brimble restarts
  it. After 5 rapid restarts in 60s the deployment is marked **Failed**
  (crash-loop protection). lightflow's worker exits only on a missing
  `LIGHTFLOW_PG_URL` — set the secret and it runs forever.
- **Crash-safety note**: if the host dies, Brimble does **not** start a
  duplicate worker elsewhere — it waits for the original host, rescheduling
  only if the host is confirmed dead. That avoids double-executing runs, and
  lightflow's `claimed_until` leases are a second safety layer regardless.
- **Managed Postgres**: Brimble can provision the database in the same
  workspace; use the **private connection string** for in-region traffic.

## Step 1 — Create the worker project

1. Dashboard → **New project**.
2. Connect the `lightflow` repo (or your fork) and pick the branch.
3. **Service type: Worker**.

## Step 2 — Build configuration

| Setting | Value |
|---|---|
| Install command | `npm install` (auto-detected) |
| Build command | `npm run build` |
| Start command | `node dist/worker.js` |
| Region | Same region as your Postgres |
| Compute size | Small is fine — the worker is I/O-bound, ~50MB RSS |

No `PORT`, no route, no health-check path. Workers must run forever —
lightflow's worker blocks on its poll loop and never exits on its own.

## Step 3 — Environment variables

Project → **Environment** → pick the environment → **Add variable** (or
**Bulk import** a `.env`-style block):

| Key | Value |
|---|---|
| `LIGHTFLOW_PG_URL` | **required** — your Postgres connection string |
| `LIGHTFLOW_POLL_MS` | optional, default `50` |
| `LIGHTFLOW_WORKER_ID` | optional, defaults to pid+timestamp |
| `LIGHTFLOW_HEALTH_PORT` | not needed on Brimble (no port probing) |

Tips from Brimble's docs:

- Variables apply on the **next deploy** — click **Redeploy** after changing
  them, or the container keeps old values.
- You can reference another project's database with
  `LIGHTFLOW_PG_URL={{@my-pg-prod.CONNECTION_STRING}}` instead of pasting the
  URL.
- If your Postgres URL uses `sslmode=require`, append
  `&uselibpqcompat=true` to silence the pg SSL-mode warning.

## Step 4 — Deploy and verify

Click **Deploy**. Verify:

1. Project status **Active** (process alive).
2. **Logs** show: `[lightflow-worker] started worker-... (pollMs=50)`.
3. Optional heartbeat — lightflow logs run transitions (running → completed /
   failed) and step failures to stdout, so a chat turn on your web app should
   show run activity in the logs within seconds.

## Registering your workflow functions

The worker executes the steps, so the workflow functions must be loadable in
its process:

```js
// worker-entry.mjs
await import("lightflow-engine/worker");
await import("./your-workflows.js"); // modules that define "use workflow" fns
```

then use `node worker-entry.mjs` as the start command. For entry-agents, the
compat transform auto-registers on the shared `globalThis` registry —
importing the same modules the web app imports is enough.

## Multiple workers / scaling

Runs are claimed via `claimed_until` leases, so multiple replicas are safe —
but one worker is enough for most workloads (it sustained ~9,900 durable
steps/s in benchmarks on a small box). If you scale, keep `LIGHTFLOW_WORKER_ID`
unset so each container gets a unique id.

## Restart behavior summary

| Event | What happens |
|---|---|
| Process exits (clean/crash) | Brimble restarts it; in-flight runs resume from the event log on boot |
| 5 restarts in 60s | Deployment marked **Failed** — check logs (almost always a missing env var) |
| Host dies | Brimble waits for the original host (no duplicate worker), reschedules if host is confirmed dead |

## Troubleshooting quick hits

- **Starts and immediately exits** → `LIGHTFLOW_PG_URL` missing; the worker
  exits with `LIGHTFLOW_PG_URL (or POSTGRES_URL) is required`.
- **Can't connect to the database** → region mismatch with the database; use
  the private connection string for in-region traffic.
- **Auth failed against Postgres** → the URL's user/password; test the exact
  string locally with `psql "$LIGHTFLOW_PG_URL" -c 'select 1'`.

Same pattern works on Pxxl (see [pxxl-worker.md](./pxxl-worker.md)), any VPS,
Fly.io, or Railway — the worker is a plain Node process.
