import { afterEach, describe, expect, it } from "vitest";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import Fastify from "fastify";
import { createHttpObservability } from "../../api/src/observability.js";
import { requestTraceContexts, traceContextForRawRequest, traceparentForTraceId, workflowCorrelationMemo } from "../../api/src/trace-context.js";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { Context } from "@temporalio/activity";
import { createOtlpSpanExporter, type ServerSpan } from "@space/runtime";
import { createWorkflowTelemetrySinks } from "../src/workflow-telemetry.js";
import { createActivityObservabilityInterceptors } from "../src/activity-observability.js";

const parent = "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01";
const interceptor = fileURLToPath(new URL("../src/workflow-telemetry-interceptors.ts", import.meta.url));
const fixture = fileURLToPath(new URL("./fixtures/telemetry-workflows.ts", import.meta.url));
const serverPath = process.env.SPACE_TEST_SERVER_PATH;
const environments: TestWorkflowEnvironment[] = [];
afterEach(async () => {
  for (const env of environments.splice(0)) await env.teardown();
});
async function environment() {
  const env = await TestWorkflowEnvironment.createTimeSkipping(serverPath
    ? { server: { executable: { type: "existing-path", path: serverPath } } }
    : undefined);
  environments.push(env);
  return env;
}
async function worker(env: TestWorkflowEnvironment, exporter: ReturnType<typeof createOtlpSpanExporter> | null,
    activities: Record<string, (...args: any[]) => any> = { syntheticActivity: async () => "ok" },
    workflowsPath = fixture, tracing = true) {
  const queue = "pilot-" + randomUUID();
  const instance = await Worker.create({
    connection: env.nativeConnection, taskQueue: queue, workflowsPath, activities,
    sinks: createWorkflowTelemetrySinks(exporter),
    interceptors: { workflowModules: tracing ? [interceptor] : [], activity: [createActivityObservabilityInterceptors({ spans: exporter })] },
    maxCachedWorkflows: 10, maxConcurrentWorkflowTaskExecutions: 2,
    maxConcurrentActivityTaskExecutions: 2
  });
  return { queue, instance };
}
function capture() {
  const spans: ServerSpan[] = [];
  return { spans, exporter: {
    record: (span: ServerSpan) => { spans.push(span); },
    flush: async () => {}, close: async () => {},
    stats: () => ({ exported: spans.length, dropped: 0, failed: 0 })
  } };
}
describe("real Temporal trace boundaries", () => {
  it("replays a history created before workflow tracing was installed", async () => {
    const env = await environment();
    const captured = capture();
    const { queue, instance } = await worker(env, captured.exporter, undefined, fixture, false);
    let history: Awaited<ReturnType<ReturnType<typeof env.client.workflow.getHandle>["fetchHistory"]>>;
    await instance.runUntil(async () => {
      const handle = await env.client.workflow.start("telemetryParent", {
        workflowId: "legacy-" + randomUUID(), taskQueue: queue, args: [{ again: false }]
      });
      expect(await handle.result()).toBe("complete");
      history = await handle.fetchHistory();
    });
    const count = captured.spans.length;
    await Worker.runReplayHistory({ workflowsPath: fixture,
      interceptors: { workflowModules: [interceptor] },
      sinks: createWorkflowTelemetrySinks(captured.exporter)
    }, history!);
    expect(captured.spans).toHaveLength(count);
  }, 60_000);

  it("propagates supplied API context through workflow, child and retry attempts, then suppresses replay export", async () => {
    const env = await environment();
    const captured = capture();
    const { queue, instance } = await worker(env, captured.exporter, {
      syntheticActivity: async () => {
        if (Context.current().info.attempt === 1) throw new Error("synthetic retry");
        return "ok";
      }
    });
    let history: Awaited<ReturnType<ReturnType<typeof env.client.workflow.getHandle>["fetchHistory"]>>;
    await instance.runUntil(async () => {
      const handle = await env.client.workflow.start("telemetryParent", {
        workflowId: "parent-" + randomUUID(), taskQueue: queue, args: [{ again: false }],
        memo: { traceparent: parent }
      });
      expect(await handle.result()).toBe("complete");
      history = await handle.fetchHistory();
    });
    const root = captured.spans.find(s => s.name === "workflow telemetryParent")!;
    expect(root.traceId).toBe("1".repeat(32));
    expect(root.parentSpanId).toBe("2".repeat(16));
    const child = captured.spans.find(s => s.name === "workflow telemetryChild")!;
    expect(child.parentSpanId).toBe(root.spanId);
    const activities = captured.spans.filter(s => s.name === "activity syntheticActivity");
    expect(activities).toHaveLength(4);
    expect(activities.map(s => s.attributes["space.attempt"]).sort()).toEqual([1,1,2,2]);
    expect(activities.every(s => s.parentSpanId === root.spanId || s.parentSpanId === child.spanId)).toBe(true);
    expect(captured.spans.every(s => s.traceId === root.traceId)).toBe(true);
    const count = captured.spans.length;
    await Worker.runReplayHistory({
      workflowsPath: fixture,
      interceptors: { workflowModules: [interceptor] },
      sinks: createWorkflowTelemetrySinks(captured.exporter)
    }, history!);
    expect(captured.spans).toHaveLength(count);
    await Worker.runReplayHistory({
      workflowsPath: fixture, interceptors: { workflowModules: [interceptor] },
      sinks: createWorkflowTelemetrySinks(null)
    }, history!);
    await expect(Worker.runReplayHistory({
      workflowsPath: fileURLToPath(new URL("./fixtures/changed-telemetry-workflows.ts", import.meta.url)),
      interceptors: { workflowModules: [interceptor] },
      sinks: createWorkflowTelemetrySinks(null)
    }, history!)).rejects.toThrow(/determin|nondetermin/i);
  }, 60_000);

  it("links continue-as-new executions without changing the trace", async () => {
    const env = await environment();
    const captured = capture();
    const { queue, instance } = await worker(env, captured.exporter);
    await instance.runUntil(() => env.client.workflow.execute("telemetryParent", {
      workflowId: "continued-" + randomUUID(), taskQueue: queue, args: [{ again: true }],
      memo: { traceparent: parent }
    }));
    const runs = captured.spans.filter(s => s.name === "workflow telemetryParent");
    expect(runs).toHaveLength(2);
    const continued = runs.find(s => s.links?.length)!;
    const previous = runs.find(s => s.spanId !== continued.spanId)!;
    expect(continued.traceId).toBe(previous.traceId);
    expect(continued.links).toContainEqual({ traceId: previous.traceId, spanId: previous.spanId });
  }, 60_000);

  it("keeps separate API requests separate within a long-lived supervisor turn loop", async () => {
    const env = await environment();
    const captured = capture();
    const { queue, instance } = await worker(env, captured.exporter);
    await instance.runUntil(() => env.client.workflow.execute("telemetryTwoMissions", {
      workflowId: "missions-" + randomUUID(), taskQueue: queue, memo: { traceparent: parent },
      args: [[{ traceId: "req:one", agentRunId: "agent_run:one", traceparent: parent },
        { traceId: "req:two", agentRunId: "agent_run:two",
          traceparent: "00-" + "3".repeat(32) + "-" + "4".repeat(16) + "-01" }]]
    }));
    const activities = captured.spans.filter(s => s.name === "activity syntheticActivity");
    expect(activities.map(s => s.traceId)).toEqual(["1".repeat(32), "3".repeat(32)]);
    for (const span of activities) {
      const mission = captured.spans.find(s => s.spanId === span.parentSpanId)!;
      expect(mission.name).toContain("mission");
      expect(mission.traceId).toBe(span.traceId);
    }
  }, 60_000);

  it("links a background mission's new trace to its persisted source context", async () => {
    const env = await environment();
    const captured = capture();
    const { queue, instance } = await worker(env, captured.exporter);
    const background = "00-" + "3".repeat(32) + "-" + "4".repeat(16) + "-01";
    await instance.runUntil(() => env.client.workflow.execute("telemetryTwoMissions", {
      workflowId: "background-" + randomUUID(), taskQueue: queue, memo: { traceparent: parent },
      args: [[{ traceId: "trace:bg-synthetic", agentRunId: "agent_run:background", traceparent: background,
        roomAgentContinuation: { sourceTraceparent: parent } }]]
    }));
    const mission = captured.spans.find(s => s.name.endsWith(" mission"))!;
    expect(mission.traceId).toBe("3".repeat(32));
    expect(mission.parentSpanId).toBeNull();
    expect(mission.links).toContainEqual({ traceId: "1".repeat(32), spanId: "2".repeat(16) });
    expect(captured.spans.find(s => s.name === "activity syntheticActivity")?.parentSpanId).toBe(mission.spanId);
  }, 60_000);

  it("runs the production memory mutation workflow against a real local HTTP collector", async () => {
    const received: any[] = [];
    const collector = createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => { received.push(JSON.parse(body)); response.writeHead(200).end("{}"); });
    });
    await new Promise<void>(resolve => collector.listen(0, "127.0.0.1", resolve));
    const address = collector.address() as { port: number };
    const external = process.env.SPACE_TEST_JAEGER_ENDPOINT;
    // An explicit loopback collector enables the documented manual Jaeger proof.
    if (external && !/^http:\/\/127\.0\.0\.1:\d+$/.test(external)) throw new Error("Test collector must be loopback");
    const endpoint = external ?? "http://127.0.0.1:" + address.port;
    const send: Parameters<typeof createOtlpSpanExporter>[0]["fetch"] = external ? async (url, init) => {
      const response = await fetch(url, init);
      received.push(JSON.parse(init.body));
      return response;
    } : undefined;
    const exporter = createOtlpSpanExporter({ endpoint, serviceName: "space-worker", fetch: send });
    const apiExporter = createOtlpSpanExporter({ endpoint, serviceName: "space-api", fetch: send });
    const app = Fastify({ genReqId: () => "req:" + randomUUID() });
    const http = createHttpObservability({ serviceName: "space-api", spanExporter: apiExporter });
    app.addHook("onRequest", async request => {
      http.onRequest(request);
      requestTraceContexts.remember(String(request.id), traceContextForRawRequest(request.raw));
    });
    app.addHook("onResponse", async (request, reply) => http.onResponse(request, reply));
    try {
      const env = await environment();
      const { queue, instance } = await worker(env, exporter, {
        executePersistedMemoryChangeSet: async () => ({
          status: "APPLIED", changeSetId: "change:synthetic",
          resultingSourceHash: "a".repeat(64), reason: null
        })
      }, fileURLToPath(new URL("../src/memory-mutation-workflows.ts", import.meta.url)));
      app.get("/pilot", request => env.client.workflow.execute("memoryMutationWorkflow", {
        workflowId: "memory-" + randomUUID(), taskQueue: queue,
        args: [{ changeSetId: "change:synthetic", traceId: String(request.id) }],
        memo: workflowCorrelationMemo({
          traceId: String(request.id), traceparent: traceparentForTraceId(String(request.id))
        })
      }));
      await app.listen({ port: 0, host: "127.0.0.1" });
      const apiAddress = app.server.address() as { port: number };
      await instance.runUntil(async () => {
        const response = await fetch("http://127.0.0.1:" + apiAddress.port + "/pilot");
        expect(response.status).toBe(200);
        await response.text();
      });
      await exporter.flush();
      await apiExporter.flush();
      expect(exporter.stats().failed + apiExporter.stats().failed).toBe(0);
      expect(exporter.stats().exported + apiExporter.stats().exported).toBe(3);
      const spans = received.flatMap(r => r.resourceSpans.flatMap((rs: any) => rs.scopeSpans.flatMap((ss: any) => ss.spans)));
      expect(spans).toHaveLength(3);
      const api = spans.find(s => s.kind === 2)!;
      const workflow = spans.find(s => s.name === "workflow memoryMutationWorkflow")!;
      expect(workflow.parentSpanId).toBe(api.spanId);
      expect(spans.every(s => s.traceId === api.traceId)).toBe(true);
      expect(spans.find(s => s.name.startsWith("activity ")).parentSpanId).toBe(workflow.spanId);
      if (external) console.log("SPACE_TRACE:" + api.traceId);
    } finally {
      await app.close();
      http.close();
      await apiExporter.close();
      await exporter.close();
      await new Promise<void>(resolve => collector.close(() => resolve()));
    }
  }, 60_000);

  it("preserves execution when the exporter throws or is disabled", async () => {
    const env = await environment();
    for (const exporter of [null, {
      record: () => { throw new Error("synthetic exporter failure"); },
      close: async () => {}, flush: async () => {},
      stats: () => ({ exported: 0, dropped: 0, failed: 0 })
    }]) {
      const { queue, instance } = await worker(env, exporter);
      await instance.runUntil(async () => {
        expect(await env.client.workflow.execute("telemetryParent", {
          workflowId: "disabled-" + randomUUID(), taskQueue: queue, args: [{ again: false }],
          memo: { traceparent: parent }
        })).toBe("complete");
      });
    }
  }, 60_000);

  it("records safe failure and cancellation outcomes without error text", async () => {
    const env = await environment();
    const captured = capture();
    const { queue, instance } = await worker(env, captured.exporter, {
      syntheticActivity: async () => { throw new Error("private synthetic error text"); }
    });
    await instance.runUntil(async () => {
      await expect(env.client.workflow.execute("telemetryFailure", {
        workflowId: "failure-" + randomUUID(), taskQueue: queue, memo: { traceparent: parent }
      })).rejects.toThrow();
      const handle = await env.client.workflow.start("telemetryCancellation", {
        workflowId: "cancel-" + randomUUID(), taskQueue: queue, memo: { traceparent: parent }
      });
      await handle.cancel();
      await expect(handle.result()).rejects.toThrow();
    });
    expect(captured.spans.find(s => s.name === "workflow telemetryFailure")?.attributes["space.outcome"]).toBe("FAILED");
    expect(captured.spans.find(s => s.name === "workflow telemetryCancellation")?.attributes["space.outcome"]).toBe("CANCELLED");
    expect(JSON.stringify(captured.spans, (_key, value) => typeof value === "bigint" ? value.toString() : value))
      .not.toContain("private synthetic error text");
  }, 60_000);
});
