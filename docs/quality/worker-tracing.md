# Worker trace export

Worker export uses the same bounded OTLP/HTTP JSON exporter as the API. Set
`SPACE_OTEL_EXPORTER_OTLP_ENDPOINT` to a collector base URL for each process.
It is disabled when the setting is absent. No tracing SDK or collector is needed
for normal operation.

The API propagates its actual exported SERVER span. A replay-safe workflow
interceptor makes a workflow span, propagates its context in Temporal headers
to child workflows and activities, and sends completed spans through a sink.
The sink is registered with `callDuringReplay: false`. Each activity attempt
gets its own span; an outbound HTTP call from that activity propagates that
exported span rather than an unrecorded intermediate parent.

A Room Agent supervisor can process requests from different API traces. Its
per-turn mission scope parents the turn's activities and child workflow to that
turn's originating request. Queued signals do not replace an executing turn's
scope. Continue-as-new records link the previous run. Background continuations
start their own trace and link their persisted source context when available;
older persisted turns without a source context cannot supply that link.

## Compatibility and failure handling

Keep the workflow interceptor and sink registration installed even if export is
disabled. The `space-otlp-workflow-v1` version marker preserves the command
shape of histories that predate instrumentation. Do not remove it without the
usual workflow versioning and history replay review.

Workflow code performs no tracing I/O and does not consume the workflow random
number generator to make trace identifiers. The worker process exports spans.
Replay suppression is not a claim of exactly-once delivery: retried workflow
tasks may invoke sinks again. Workflow span identifiers remain stable within a
run. Collector failures and rejected exports do not change activity results.

The exporter queue, timeout, drops and failure counters retain the API's existing
behavior. Completed spans are best effort. A running or abruptly terminated
workflow may not export its completed execution span; per-turn mission spans can
finish while the supervisor remains active.

## Data permitted in traces

Workflow attributes are restricted to workflow/run/type/mission identifiers and
outcome. Activity attributes are restricted to activity type, workflow/run
identifiers, attempt, outcome, stable reason code and category. Raw prompts,
model output, error messages, URLs, filesystem paths and arbitrary input fields
are not copied into worker spans. Source contexts in continuation metadata carry
trace identifiers only.

## Verification

Run the normal repository checks and affected suites:

```sh
npm run check
npm test -w @space/worker -- workflow-telemetry activity-observability
npm test -w @space/api -- trace-export-parent otlp
npm test -w @space/runtime -- otlp-idle-flush
```

The integration suite starts an actual Temporal time-skipping server and actual
workers, with synthetic activities. It covers child spans, retries,
continue-as-new links, separate mission traces, replay suppression, collector
delivery, and disabled/throwing export. It also executes the production memory
mutation workflow against a real loopback HTTP collector. This does not execute
a provider, change user memory or certify production histories.

For offline execution, set `SPACE_TEST_SERVER_PATH` to a previously verified
Temporal test-server executable. Otherwise the SDK uses its normal download
mechanism. A failed download is a failed setup, not a skipped passing test.

Manually inspect the same synthetic trace in Jaeger or another OTLP collector.
A passing fake-collector test alone is not the Jaeger screenshot required by
issue #61. Record the actual commands, environment, remaining failures and
personal review in the contribution.

With a local Jaeger OTLP/HTTP listener on port 4318, the same production-workflow
test can send the actual API and worker exporter payloads directly to Jaeger:

```sh
SPACE_TEST_JAEGER_ENDPOINT=http://127.0.0.1:4318 npm test -w @space/worker -- workflow-telemetry.integration --testNamePattern 'production memory mutation'
```

The test prints `SPACE_TRACE:<W3C trace id>` after checking successful export and
the three API/workflow/activity parent relationships. Open `/trace/<id>` in the
Jaeger UI (normally port 16686). The explicit test override accepts loopback only.
These are synthetic activities executing the real production memory workflow;
this demonstration does not change user memory or invoke an AI provider.
