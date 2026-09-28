/**
 * Vercel Workflow compat: getWorkflowMetadata() returns { workflowRunId }.
 */
import { current } from "../index.js";
export function getWorkflowMetadata() {
    if (!current)
        throw new Error("getWorkflowMetadata() outside a workflow");
    return { workflowRunId: current.runId };
}
