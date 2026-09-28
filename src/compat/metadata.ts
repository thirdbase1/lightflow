/**
 * Vercel Workflow compat: getWorkflowMetadata() returns { workflowRunId }.
 */
import { getCurrent } from "../index.js";

export function getWorkflowMetadata(): { workflowRunId: string } {
  const ctx = getCurrent();
  if (!ctx) throw new Error("getWorkflowMetadata() outside a workflow");
  return { workflowRunId: ctx.runId };
}
