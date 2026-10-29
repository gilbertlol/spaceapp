import { expect, it } from "vitest";
import type { ServerSpan } from "@space/runtime";
import { childTraceparent, observeActivityExecution } from "../src/activity-observability.js";

it("propagates the recorded activity span to an outbound call", async () => {
  const recorded: ServerSpan[] = [];
  const parent = "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01";
  const spans = { record: (span: ServerSpan) => { recorded.push(span); },
    flush: async () => {}, close: async () => {},
    stats: () => ({ exported: recorded.length, dropped: 0, failed: 0 }) };
  let outbound: string | null = null;
  await observeActivityExecution({
    activityId: "1", activityType: "syntheticOutbound", attempt: 1,
    currentAttemptScheduledTimestampMs: Date.now(), workflowType: "synthetic",
    workflowExecution: { workflowId: "workflow:synthetic", runId: "run:synthetic" }
  }, [{ traceparent: parent }], async () => { outbound = childTraceparent(parent); }, { spans });
  expect(recorded).toHaveLength(1);
  expect(outbound).toBe("00-" + recorded[0]!.traceId + "-" + recorded[0]!.spanId + "-01");
});
