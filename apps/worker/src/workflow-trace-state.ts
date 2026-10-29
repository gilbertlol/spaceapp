import { formatTraceparent, parseTraceparent } from "@space/contracts";
import { proxySinks, workflowInfo } from "@temporalio/workflow";
import type { WorkflowSpan, WorkflowTelemetry } from "./workflow-telemetry-types.js";

export interface TraceState {
  span: WorkflowSpan;
  parent: string | null;
  flags: string;
}
let root: TraceState | null = null;
let active: TraceState | null = null;
let missionSequence = 0;

/** Pure identifier derivation: never consumes the workflow's deterministic PRNG. */
export function stableSpanId(value: string): string {
  let hash = 0xcbf29ce484222325n;
  for (let i = 0; i < value.length; i++) {
    hash ^= BigInt(value.charCodeAt(i));
    hash = BigInt.asUintN(64, hash * 0x100000001b3n);
  }
  return (hash || 1n).toString(16).padStart(16, "0");
}
export function currentTrace(): TraceState | null { return active; }
export function traceparentOf(state: TraceState): string {
  return formatTraceparent(state.span.traceId, state.span.spanId, state.flags);
}
export function newTrace(parent: unknown, suffix: string): TraceState {
  const info = workflowInfo();
  const parsed = parseTraceparent(parent);
  return {
    flags: parsed?.flags ?? "01",
    parent: parsed ? formatTraceparent(parsed.traceId, parsed.parentId, parsed.flags) : null,
    span: {
      traceId: parsed?.traceId ?? stableSpanId(info.runId + ":a") + stableSpanId(info.runId + ":b"),
      spanId: stableSpanId(info.runId + ":" + suffix),
      parentSpanId: parsed?.parentId ?? null,
      name: "workflow " + info.workflowType,
      startMs: Date.now(), endMs: Date.now(), outcome: "COMPLETED", links: [],
      attributes: { "space.workflow_id": info.workflowId, "space.run_id": info.runId,
                    "space.workflow_type": info.workflowType }
    }
  };
}
export function emitTrace(state: TraceState, outcome: WorkflowSpan["outcome"]): void {
  state.span.endMs = Date.now();
  state.span.outcome = outcome;
  proxySinks<WorkflowTelemetry>().telemetry.record(state.span);
}
export function initializeTrace(state: TraceState): void { root = active = state; missionSequence = 0; }
export function finishTrace(outcome: WorkflowSpan["outcome"]): void {
  if (active && active !== root) emitTrace(active, outcome);
  if (root) emitTrace(root, outcome);
  root = active = null;
}

/**
 * The supervisor processes one queued turn at a time. Scope all of that turn's
 * activities, including its completion record, to the API request for that turn.
 * Signal handlers only enqueue/cancel; they never replace this active scope.
 */
export function beginMissionTrace(turn: {
  traceparent?: string; traceId: string; agentRunId?: string; roomAgentMissionId?: string;
  roomAgentContinuation?: { sourceTraceparent?: string };
}): (outcome?: WorkflowSpan["outcome"]) => void {
  if (!root) return () => {};
  const previous = active;
  const state = newTrace(turn.traceparent ?? (previous ? traceparentOf(previous) : undefined),
                         "mission:" + (turn.agentRunId ?? turn.traceId) + ":" + missionSequence++);
  state.span.name += " mission";
  if (turn.roomAgentMissionId) state.span.attributes["space.mission_id"] = turn.roomAgentMissionId;
  const predecessor = parseTraceparent(turn.roomAgentContinuation?.sourceTraceparent);
  if (turn.roomAgentContinuation) {
    state.span.parentSpanId = null;
    state.parent = null;
    if (predecessor) state.span.links.push({ traceId: predecessor.traceId, spanId: predecessor.parentId });
  }
  active = state;
  let finished = false;
  return (outcome = "COMPLETED") => {
    if (finished) return;
    finished = true;
    emitTrace(state, outcome);
    active = previous;
  };
}
