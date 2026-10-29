import { expect, it } from "vitest";
import type { WorkflowInfo } from "@temporalio/workflow";
import type { ServerSpan } from "@space/runtime";
import { createWorkflowTelemetrySinks } from "../src/workflow-telemetry.js";
import type { WorkflowSpan } from "../src/workflow-telemetry-types.js";

it("exports only approved workflow identifiers and status, never arbitrary input fields", () => {
  const spans: ServerSpan[] = [];
  const exporter = {
    record: (span: ServerSpan) => { spans.push(span); },
    flush: async () => {}, close: async () => {},
    stats: () => ({ exported: spans.length, dropped: 0, failed: 0 })
  };
  const record = createWorkflowTelemetrySinks(exporter).telemetry.record.fn;
  const value: WorkflowSpan = {
    traceId: "1".repeat(32), spanId: "2".repeat(16), parentSpanId: null,
    name: "workflow synthetic", startMs: 1000, endMs: 1002, outcome: "FAILED",
    attributes: { "space.workflow_id": "workflow:synthetic",
      prompt: "synthetic-user-text", "space.run_id": "/private/synthetic/path",
      message: "synthetic-error-message", url: "https://example.invalid/private" },
    links: []
  };
  record({} as WorkflowInfo, value);
  expect(spans).toHaveLength(1);
  expect(spans[0]!.attributes).toEqual({
    "space.workflow_id": "workflow:synthetic", "space.outcome": "FAILED"
  });
  expect(JSON.stringify(spans, (_key, v) => typeof v === "bigint" ? v.toString() : v))
    .not.toMatch(/synthetic-user-text|synthetic-error-message|example.invalid|private/);
  expect(spans[0]!.error).toBe(true);
});
