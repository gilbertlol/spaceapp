import { continueAsNew, executeChild, proxyActivities, sleep } from "@temporalio/workflow";
import { beginMissionTrace } from "../../src/workflow-trace-state.js";
const { syntheticActivity } = proxyActivities<{ syntheticActivity(input?: unknown): Promise<string> }>({
  startToCloseTimeout: "5 seconds", retry: { maximumAttempts: 2, initialInterval: "1 millisecond" }
});
export async function telemetryChild(): Promise<string> { return syntheticActivity(); }
export async function telemetryFailure(): Promise<string> { return syntheticActivity(); }
export async function telemetryCancellation(): Promise<void> { await sleep("1 day"); }
export async function telemetryParent(input: { again?: boolean }): Promise<string> {
  await syntheticActivity();
  await executeChild(telemetryChild);
  if (input.again) return continueAsNew<typeof telemetryParent>({ again: false });
  return "complete";
}
export async function telemetryTwoMissions(turns: Array<{ traceId: string; traceparent: string; agentRunId: string;
  roomAgentContinuation?: { sourceTraceparent?: string } }>): Promise<void> {
  for (const turn of turns) {
    const finish = beginMissionTrace(turn);
    try { await syntheticActivity(); } finally { finish(); }
  }
}
