import { defaultPayloadConverter } from "@temporalio/common";
import { parseTraceparent } from "@space/contracts";
import {
  ContinueAsNew, isCancellation, patched, workflowInfo,
  type Headers, type WorkflowInterceptorsFactory
} from "@temporalio/workflow";
import { TRACE_HEADER, type TraceEnvelope, type WorkflowSpan } from "./workflow-telemetry-types.js";
import { currentTrace, finishTrace, initializeTrace, newTrace, traceparentOf } from "./workflow-trace-state.js";

/** Extracts and validates a propagated trace envelope from Temporal workflow headers. */
function readHeader(headers: Headers): TraceEnvelope | null {
  try {
    const payload = headers[TRACE_HEADER];
    if (!payload) return null;
    const value = defaultPayloadConverter.fromPayload(payload) as Partial<TraceEnvelope>;
    if (!value || !parseTraceparent(value.traceparent)) return null;
    return { traceparent: value.traceparent!,
             ...(parseTraceparent(value.predecessor) ? { predecessor: value.predecessor } : {}) };
  } catch { return null; }
}
/** Resolves an initial parent traceparent from workflow input arguments or Temporal memo. */
function inputParent(args: unknown[]): unknown {
  const first = args[0] as { traceparent?: unknown; turn?: { traceparent?: unknown } } | undefined;
  return first?.traceparent ?? first?.turn?.traceparent ?? workflowInfo().memo?.traceparent;
}
/** Attaches the current workflow trace envelope to outbound Temporal headers. */
function decorate(headers: Headers, continuation = false): Headers {
  const state = currentTrace();
  if (!state) return headers;
  const envelope: TraceEnvelope = continuation
    ? { traceparent: state.parent ?? traceparentOf(state), predecessor: traceparentOf(state) }
    : { traceparent: traceparentOf(state) };
  return { ...headers, [TRACE_HEADER]: defaultPayloadConverter.toPayload(envelope)! };
}
/** Replay-safe Temporal workflow interceptor factory for OTLP trace propagation and span recording. */
export const interceptors: WorkflowInterceptorsFactory = () => {
  let enabled = false;
  return {
    inbound: [{
      async execute(input, next) {
        // Install this interceptor even with export disabled. Old histories do
        // not acquire header-changing commands; new histories retain the marker.
        enabled = patched("space-otlp-workflow-v1");
        if (!enabled) return next(input);
        const inherited = readHeader(input.headers);
        const state = newTrace(inherited?.traceparent ?? inputParent(input.args), "execution");
        const predecessor = parseTraceparent(inherited?.predecessor);
        if (predecessor) state.span.links.push({ traceId: predecessor.traceId, spanId: predecessor.parentId });
        initializeTrace(state);
        let outcome: WorkflowSpan["outcome"] = "COMPLETED";
        try { return await next(input); }
        catch (error) {
          outcome = error instanceof ContinueAsNew ? "CONTINUED" : isCancellation(error) ? "CANCELLED" : "FAILED";
          throw error;
        } finally { finishTrace(outcome); }
      }
    }],
    outbound: [{
      scheduleActivity: (input, next) => next(enabled ? { ...input, headers: decorate(input.headers) } : input),
      scheduleLocalActivity: (input, next) => next(enabled ? { ...input, headers: decorate(input.headers) } : input),
      startChildWorkflowExecution: (input, next) => next(enabled ? { ...input, headers: decorate(input.headers) } : input),
      continueAsNew: (input, next) => next(enabled ? { ...input, headers: decorate(input.headers, true) } : input)
    }]
  };
};
