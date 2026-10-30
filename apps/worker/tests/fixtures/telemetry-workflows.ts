import { continueAsNew, executeChild, proxyActivities, sleep } from "@temporalio/workflow";
import { beginMissionTrace } from "../../src/workflow-trace-state.js";
const { syntheticActivity } = proxyActivities<{ syntheticActivity(input?: unknown): Promise<string> }>({
  startToCloseTimeout: "5 seconds", retry: { maximumAttempts: 2, initialInterval: "1 millisecond" }
});
/** Executes a synthetic child workflow activity for telemetry tests. */
export async function telemetryChild(): Promise<string> { return syntheticActivity(); }
/** Executes a failing synthetic activity for telemetry error outcome tests. */
export async function telemetryFailure(): Promise<string> { return syntheticActivity(); }
/** Sleeps indefinitely to exercise workflow cancellation telemetry. */
export async function telemetryCancellation(): Promise<void> { await sleep("1 day"); }
/** Executes a parent workflow with an activity, child workflow, and optional continue-as-new. */
export async function telemetryParent(input: { again?: boolean }): Promise<string> {
  await syntheticActivity();
  await executeChild(telemetryChild);
  if (input.again) return continueAsNew<typeof telemetryParent>({ again: false });
  return "complete";
}
/** Simulates sequential supervisor missions, including optional paused re-queueing and background continuations. */
export async function telemetryTwoMissions(turns: Array<{ traceId: string; traceparent: string; agentRunId: string;
  pausedOnce?: boolean; roomAgentContinuation?: { sourceTraceparent?: string } }>): Promise<void> {
  for (const turn of turns) {
    if (turn.pausedOnce) {
      const finishPaused = beginMissionTrace(turn);
      finishPaused(null);
    }
    const finish = beginMissionTrace(turn);
    try { await syntheticActivity(); } finally { finish(); }
  }
}
