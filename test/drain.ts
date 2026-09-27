import { Engine } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
import { registerAll } from "./workflow-def.js";
import pg from "pg";
registerAll();
const store = await createPostgresStore(process.env.LIGHTFLOW_PG_URL!);
const engine = new Engine(store, { pollMs: 300, staleRunMs: 10_000 });
const p = new pg.Pool({ connectionString: process.env.LIGHTFLOW_PG_URL! });
// persistent driver: keeps resuming until nothing is running
void engine.startWorker(() => {});
const deadline = Date.now() + 9 * 60_000;
while (Date.now() < deadline) {
  const rows = await p.query("SELECT run_id FROM lightflow_runs WHERE status='running'");
  if (rows.rows.length === 0) break;
  await Promise.all(rows.rows.slice(0, 12).map((r: { run_id: string }) =>
    engine.resume(r.run_id).catch(() => {})));
  await new Promise((r) => setTimeout(r, 1200));
}
const done = await p.query("SELECT status,count(*) FROM lightflow_runs GROUP BY status");
console.log("FINAL", JSON.stringify(done.rows));
process.exit(0);
