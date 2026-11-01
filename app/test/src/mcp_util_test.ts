import { assertEquals, assertStringIncludes } from "std/assert";

import { describeUnfinishedJob, type JobWaitObservation } from "./mcp_util.ts";

const observed = (overrides: Partial<JobWaitObservation>): JobWaitObservation => ({
  queue: "q1",
  polls: 10,
  unreachablePolls: 0,
  lastPollUnreachable: false,
  foundPolls: 0,
  absentPollsAtEnd: 0,
  queueAtTimeout: {},
  ...overrides,
});

Deno.test("timeout message: a job stuck behind others names the queue's contents", () => {
  const queueAtTimeout = Object.fromEntries([
    ...Array.from({ length: 15 }, (_, i) => [`queued${i}`, { state: "Queued" }]),
    ["running", { state: "Running" }],
  ]);
  assertEquals(
    describeUnfinishedJob(observed({ lastState: "Queued", lastFound: true, foundPolls: 10, queueAtTimeout })),
    "still Queued; queue q1 at timeout: 15 Queued, 1 Running",
  );
});

Deno.test("timeout message: a job that disappeared names who holds its namespace", () => {
  const message = describeUnfinishedJob(observed({
    foundPolls: 2,
    lastSeenState: "Queued",
    absentPollsAtEnd: 8,
    namespace: "ns",
    queueAtTimeout: { abcdef123456: { state: "Running", namespaces: ["ns"] } },
  }));
  assertEquals(
    message,
    "last seen Queued, then neither on queue q1 nor stored as a result for the last 8/10 polls; " +
      "queue q1 at timeout: 1 Running; namespace ns is held by abcdef (Running)",
  );
});

Deno.test("timeout message: says so when nothing holds the namespace, and offers no reasons", () => {
  const message = describeUnfinishedJob(observed({ absentPollsAtEnd: 10, namespace: "ns" }));
  assertEquals(
    message,
    "neither on queue q1 nor stored as a result on any of 10 polls; queue q1 at timeout: empty; " +
      "no job on the queue holds namespace ns",
  );
});

Deno.test("timeout message: an unreachable API is unknown, not absent", () => {
  const message = describeUnfinishedJob(observed({
    unreachablePolls: 3,
    lastPollUnreachable: true,
    lastFailure: "boom",
    queueAtTimeout: { unreadable: "503" },
  }));
  assertStringIncludes(message, "state unknown");
  assertStringIncludes(message, "3/10 polls");
  assertStringIncludes(message, "boom");
  assertStringIncludes(message, "could not read queue q1: 503");
});

Deno.test("timeout message: one failed poll does not hide that the job was absent", () => {
  const message = describeUnfinishedJob(observed({ unreachablePolls: 1, absentPollsAtEnd: 9 }));
  assertStringIncludes(message, "neither on queue q1");
});
