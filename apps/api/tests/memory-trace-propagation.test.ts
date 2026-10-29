import { expect, it } from "vitest";
import { scheduleMemoryConsolidation } from "../src/memory-consolidation-coordinator.js";
import { scheduleMemoryMutation } from "../src/memory-mutation-coordinator.js";
import { createRequestTraceContext, requestTraceContexts } from "../src/trace-context.js";

it("passes the recorded API SERVER parent into consolidation and mutation starts", async () => {
  const traceId = "req:memory-parent-regression";
  const context = createRequestTraceContext(undefined);
  requestTraceContexts.remember(traceId, context);
  const memos: Array<Record<string, string> | undefined> = [];
  await scheduleMemoryConsolidation({ runId: "memory_consolidation:synthetic", traceId },
    "space-memory-consolidation:" + "a".repeat(64), async (_type, options) => {
      memos.push(options.memo);
      return { workflowId: options.workflowId };
    });
  await scheduleMemoryMutation({ changeSetId: "memory_change:synthetic", traceId },
    async (_type, options) => {
      memos.push(options.memo);
      return { workflowId: options.workflowId };
    });
  expect(memos).toHaveLength(2);
  for (const memo of memos) {
    expect(memo?.traceparent).toBe(context.traceparent);
    expect(memo?.spaceTraceId).toBe(traceId);
  }
});
