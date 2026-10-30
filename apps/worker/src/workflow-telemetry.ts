import type { InjectedSinks } from "@temporalio/worker";
import type { SpanExporter } from "@space/runtime";
import type { WorkflowTelemetry } from "./workflow-telemetry-types.js";

const id = /^[0-9a-f]+$/;
const keys = new Set(["space.workflow_id", "space.run_id", "space.workflow_type", "space.mission_id"]);
/** Creates replay-suppressed Temporal worker sinks that forward allowlisted workflow spans to the OTLP exporter. */
export function createWorkflowTelemetrySinks(exporter: SpanExporter | null): InjectedSinks<WorkflowTelemetry> {
  return {
    telemetry: {
      record: {
        callDuringReplay: false,
        fn(_info, span) {
          if (!exporter || !id.test(span.traceId) || span.traceId.length !== 32 ||
              !id.test(span.spanId) || span.spanId.length !== 16 ||
              !Number.isSafeInteger(span.startMs) || !Number.isSafeInteger(span.endMs)) return;
          const attributes: Record<string, string | number> = {};
          for (const [key, value] of Object.entries(span.attributes)) {
            if (keys.has(key) && typeof value === "string" && /^[A-Za-z0-9:._-]{1,200}$/.test(value))
              attributes[key] = value;
          }
          attributes["space.outcome"] = span.outcome;
          try {
            exporter.record({
              traceId: span.traceId, spanId: span.spanId, parentSpanId: span.parentSpanId,
              name: span.name, startTimeUnixNano: BigInt(span.startMs) * 1_000_000n,
              endTimeUnixNano: BigInt(Math.max(span.startMs, span.endMs)) * 1_000_000n,
              attributes, kind: 1, error: span.outcome === "FAILED",
              links: span.links.slice(0, 4)
            });
          } catch { /* Trace transport must never change a workflow outcome. */ }
        }
      }
    }
  };
}
