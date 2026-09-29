/**
 * Durable fetch: memoized per step position, like any other side effect.
 * Mirrors Vercel's workflow fetch(): Response-shaped result.
 */
import { step } from "../index.js";

export async function workflowFetch(
  input: string,
  init?: RequestInit,
): Promise<Response> {
  const res = await step(async () => {
    const r = await fetch(input, init);
    // Persist the durable side effect as a plain serializable record (replay
    // must never re-hit the network). Reconstruct a real Response from it so
    // callers can use .ok/.status/.text()/.json() like a normal Response.
    return {
      __lfResponse: true,
      status: r.status,
      statusText: r.statusText,
      headers: Object.fromEntries(r.headers.entries()),
      body: await r.text(),
    };
  });
  const record = res as {
    status: number;
    statusText?: string;
    headers: Record<string, string>;
    body: string;
  };
  const headers = new Headers(record.headers);
  if (!headers.has("content-type")) headers.set("content-type", "application/json");
  return new Response(record.body, {
    status: record.status,
    statusText: record.statusText ?? "",
    headers,
  });
}
