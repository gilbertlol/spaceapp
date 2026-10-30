import { sleep } from "@temporalio/workflow";
import { telemetryParent as original } from "./telemetry-workflows.js";
export { telemetryChild } from "./telemetry-workflows.js";
/** Mutated workflow fixture that inserts an extra sleep command to verify non-determinism detection on replay. */
export async function telemetryParent(input: { again?: boolean }): Promise<string> {
  await sleep("1 second");
  return original(input);
}
