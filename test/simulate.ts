import { Engine } from "../src/index.js";
import { createPostgresStore } from "../src/pg-store.js";
import { registerAll } from "./workflow-def.js";

registerAll();

const users = Number(process.argv[2] ?? 10);
const chats = Number(process.argv[3] ?? 3);
const ticks = Number(process.argv[4] ?? 6);

const store = await createPostgresStore(process.env.LIGHTFLOW_PG_URL!);
const engine = new Engine(store, { pollMs: 200, staleRunMs: 20_000 });
void engine.startWorker(() => {});

const ids: string[] = [];
const t0 = Date.now();
for (let u = 0; u < users; u += 1) {
  for (let c = 0; c < chats; c += 1) {
    const { runId } = await engine.start("entry-parity", [
      { sessionId: "selftest-session", chatId: `sim-${u}-chat-${c}`, ticks },
    ]);
    ids.push(runId);
  }
}
console.log("STARTED", ids.length, "in", Date.now() - t0, "ms");

// poll to terminal state
const deadline = Date.now() + 20 * 60_000;
let done = 0;
while (Date.now() < deadline) {
  let completed = 0, running = 0, failed = 0;
  for (const id of ids) {
    const r = await store.getRun(id);
    if (!r) continue;
    if (r.status === "completed") completed += 1;
    else if (r.status === "running") running += 1;
    else failed += 1;
  }
  done = completed;
  console.log(`t+${Math.round((Date.now() - t0) / 1000)}s completed=${completed} running=${running} failed=${failed}`);
  if (completed + failed === ids.length) break;
  await new Promise((r) => setTimeout(r, 10_000));
  void running;
}
console.log("DONE", done, "of", ids.length);
process.exit(0);
