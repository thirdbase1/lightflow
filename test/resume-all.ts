import { Engine } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
import { registerAll } from "./workflow-def.js";
import pg from "pg";
registerAll();
const store = await createPostgresStore(process.env.LIGHTFLOW_PG_URL!);
const engine = new Engine(store, { pollMs: 200, staleRunMs: 15_000 });
const p = new pg.Pool({ connectionString: process.env.LIGHTFLOW_PG_URL! });
const rows = await p.query("SELECT run_id FROM lightflow_runs WHERE status='running'");
console.log("RESUMING", rows.rows.length);
await Promise.all(rows.rows.map((r: { run_id: string }) =>
  engine.resume(r.run_id).catch((e: Error) => console.error("ERR", e.message))));
const done = await p.query("SELECT status,count(*) FROM lightflow_runs GROUP BY status");
console.log("FINAL", JSON.stringify(done.rows));
process.exit(0);
