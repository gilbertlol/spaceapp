import { describe, expect, it } from "vitest";
import { createRequestTraceContext, TraceContextRegistry, traceparentForTraceId } from "../src/trace-context.js";
describe("exported API parent", () => {
  it("propagates the exported SERVER span rather than an unrecorded synthetic span", () => {
    const registry = new TraceContextRegistry();
    const request = createRequestTraceContext(undefined, bytes => bytes === 16 ? "1".repeat(32) : "2".repeat(16));
    registry.remember("req:pilot", request);
    expect(traceparentForTraceId("req:pilot", registry)).toBe(request.traceparent);
  });
  it("does not invent a parent when the request context is absent", () => {
    expect(traceparentForTraceId("req:absent", new TraceContextRegistry())).toBeNull();
  });
});
