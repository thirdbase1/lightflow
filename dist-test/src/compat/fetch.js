/**
 * Durable fetch: memoized per step position, like any other side effect.
 * Mirrors Vercel's workflow fetch(): Response-shaped result.
 */
import { step } from "../index.js";
export async function workflowFetch(input, init) {
    return step(async () => {
        const res = await fetch(input, init);
        return {
            __lfResponse: true,
            status: res.status,
            headers: Object.fromEntries(res.headers.entries()),
            body: await res.text(),
        };
    });
}
