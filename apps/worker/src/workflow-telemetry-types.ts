import type { Sinks } from "@temporalio/workflow";

export interface WorkflowSpan {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  name: string;
  startMs: number;
  endMs: number;
  attributes: Record<string, string | number>;
  outcome: "COMPLETED" | "FAILED" | "CANCELLED" | "CONTINUED";
  links: Array<{ traceId: string; spanId: string }>;
}
export interface WorkflowTelemetry extends Sinks {
  telemetry: { record(span: WorkflowSpan): void };
}
export const TRACE_HEADER = "space-otlp-v1";
export interface TraceEnvelope {
  traceparent: string;
  predecessor?: string;
}
