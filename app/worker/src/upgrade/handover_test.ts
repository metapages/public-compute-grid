import type { WorkerStatusResponse } from "@metapages/compute-queues-shared";
import { assertEquals } from "std/assert";

import { gpuIndicesFromDeviceRequests } from "../queue/utils.ts";
import {
  isSuccessorRegistered,
  predecessorStateFrom,
  RETRY_BASE,
  retryDelay,
  successorContainerName,
  takeoverDecision,
  workersFromQueueStatus,
} from "./handover.ts";

const ID = "worker-1";

const worker = (overrides: Partial<WorkerStatusResponse>): WorkerStatusResponse => ({
  time: 0,
  id: ID,
  cpus: 1,
  gpus: 0,
  queue: {},
  ...overrides,
});

Deno.test("predecessorStateFrom: only a 404 means gone, any other docker error is unknown", () => {
  assertEquals(predecessorStateFrom({ ok: false, error: { statusCode: 404 } }), "gone");
  assertEquals(predecessorStateFrom({ ok: false, error: { statusCode: 500 } }), "unknown");
  assertEquals(
    predecessorStateFrom({ ok: false, error: new Error("connect ECONNREFUSED /var/run/docker.sock") }),
    "unknown",
  );
  assertEquals(predecessorStateFrom({ ok: true, info: { State: { Running: true } } }), "running");
  assertEquals(predecessorStateFrom({ ok: true, info: { State: { Running: false } } }), "stopped");
  assertEquals(predecessorStateFrom({ ok: true, info: {} }), "stopped");
});

Deno.test("takeoverDecision: never on a docker hiccup", () => {
  const retiring = [worker({ handover: "retiring" })];
  // even with the predecessor saying it is retiring: without docker we cannot stop it
  assertEquals(takeoverDecision("unknown", retiring, ID), "wait");
  assertEquals(takeoverDecision("unknown", null, ID), "wait");
});

Deno.test("takeoverDecision: a predecessor that is gone or stopped is taken over at once", () => {
  assertEquals(takeoverDecision("gone", null, ID), "take-over");
  assertEquals(takeoverDecision("stopped", [], ID), "take-over");
});

Deno.test("takeoverDecision: a running predecessor only once it is retiring", () => {
  assertEquals(takeoverDecision("running", null, ID), "wait"); // the status could not be read
  assertEquals(takeoverDecision("running", [], ID), "wait");
  assertEquals(takeoverDecision("running", [worker({})], ID), "wait");
  assertEquals(takeoverDecision("running", [worker({ handover: "draining" })], ID), "wait");
  assertEquals(takeoverDecision("running", [worker({ handover: "standby", version: "0.54.88" })], ID), "wait");
  // another worker id retiring is not our predecessor
  assertEquals(takeoverDecision("running", [worker({ id: "someone-else", handover: "retiring" })], ID), "wait");
  assertEquals(
    takeoverDecision("running", [worker({ handover: "standby" }), worker({ handover: "retiring" })], ID),
    "take-over",
  );
});

Deno.test("isSuccessorRegistered: same id, the new version, in standby", () => {
  assertEquals(isSuccessorRegistered([worker({ version: "0.54.88", handover: "standby" })], ID, "0.54.88"), true);
  assertEquals(isSuccessorRegistered([worker({ version: "0.54.88" })], ID, "0.54.88"), false); // not in standby
  assertEquals(isSuccessorRegistered([worker({ version: "0.54.87", handover: "standby" })], ID, "0.54.88"), false);
  assertEquals(
    isSuccessorRegistered([worker({ id: "other", version: "0.54.88", handover: "standby" })], ID, "0.54.88"),
    false,
  );
  assertEquals(isSuccessorRegistered([], ID, "0.54.88"), false);
});

Deno.test("workersFromQueueStatus: workers on every server instance, not just the one that answered", () => {
  const predecessor = worker({ version: "0.54.87", handover: "draining" });
  const successor = worker({ version: "0.54.88", handover: "standby" });
  const workers = workersFromQueueStatus({
    localWorkers: { [ID]: predecessor },
    otherServers: { "server-2": { workers: { [ID]: successor } }, "server-3": {} },
  });
  assertEquals(workers, [predecessor, successor]);
  assertEquals(isSuccessorRegistered(workers, ID, "0.54.88"), true);
  assertEquals(workersFromQueueStatus({}), []);
  assertEquals(workersFromQueueStatus(null), []);
});

Deno.test("retryDelay: starts short, doubles, never later than the regular check", () => {
  const hour = 60 * 60_000;
  assertEquals([1, 2, 3, 4, 5, 6].map((n) => retryDelay(hour, n) / 60_000), [5, 10, 20, 40, 60, 60]);
  // frequent checks: retry at the check interval, backing off to the base
  assertEquals([1, 2, 3, 9].map((n) => retryDelay(15_000, n)), [15_000, 30_000, 60_000, RETRY_BASE]);
});

Deno.test("successorContainerName: a version on the end is replaced, not stacked", () => {
  assertEquals(successorContainerName("/worker", "0.54.88"), "worker-0.54.88");
  assertEquals(successorContainerName("/worker-0.54.88", "0.54.89"), "worker-0.54.89");
  assertEquals(successorContainerName("/worker-0.54.88-0.54.89", "0.54.90"), "worker-0.54.90");
  assertEquals(successorContainerName("/gpu-box-2", "0.54.88"), "gpu-box-2-0.54.88");
});

Deno.test("gpuIndicesFromDeviceRequests: the device a running container was given", () => {
  assertEquals(gpuIndicesFromDeviceRequests([{ DeviceIDs: ["3"] }]), [3]);
  assertEquals(gpuIndicesFromDeviceRequests([{ DeviceIDs: ["1", "3"] }]), [1, 3]);
  assertEquals(gpuIndicesFromDeviceRequests([{ DeviceIDs: ["GPU-6f2c-uuid"] }]), undefined);
  assertEquals(gpuIndicesFromDeviceRequests([{ DeviceIDs: null }]), undefined);
  assertEquals(gpuIndicesFromDeviceRequests(undefined), undefined);
});
