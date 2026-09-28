/**
 * Vercel Workflow compat — `workflow/next` surface.
 *
 * Vercel's withWorkflow wraps the Next.js config to enable the directive
 * compiler ("use workflow" / "use step"). lightflow's runtime does not need
 * a bundler transform: directives are inert strings, workflow functions are
 * registered explicitly by start(), and "use step" functions can be wrapped
 * with step() from compat/workflow for durability.
 *
 * This drop-in is an identity wrapper so next.config.ts keeps working
 * unchanged:
 *   import { withWorkflow } from "workflow/next";
 * + import { withWorkflow } from "lightflow-engine/compat/next";
 */
export function withWorkflow(config) {
    // Directives are inert at runtime; nothing to compile. Accepts and
    // returns the config untouched so existing call sites compose the same.
    return config;
}
