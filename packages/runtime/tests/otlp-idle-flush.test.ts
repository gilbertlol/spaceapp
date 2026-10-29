import { expect, it } from "vitest";
import { createOtlpSpanExporter, type ServerSpan } from "../src/otlp-span-exporter.js";
const span: ServerSpan = {
  traceId: "1".repeat(32), spanId: "2".repeat(16), parentSpanId: null,
  name: "synthetic", startTimeUnixNano: 1n, endTimeUnixNano: 2n,
  attributes: {}, error: false
};
it("can flush recorded work after an initial idle flush", async () => {
  const bodies: string[] = [];
  const exporter = createOtlpSpanExporter({
    endpoint: "http://127.0.0.1:4318",
    serviceName: "pilot",
    flushIntervalMs: 60_000,
    fetch: async (_url, init) => { bodies.push(init.body); return { ok: true }; }
  });
  try {
    await exporter.flush();
    exporter.record(span);
    await exporter.flush();
    expect(bodies).toHaveLength(1);
    expect(exporter.stats()).toEqual({ exported: 1, dropped: 0, failed: 0 });
  } finally { await exporter.close(); }
});
