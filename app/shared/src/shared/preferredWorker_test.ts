import { assert, assertEquals } from "std/assert";

import { DockerJobState, type InMemoryDockerJob } from "@shared/types.ts";
import { getWorkerColorizedString, isJobClaimedByPreferredOtherWorker, resolvePreferredWorker } from "@shared/util.ts";

const workerIds = [
  "test-worker1",
  "test-worker2",
  "Test-worker1",
  "prd-330-double-claimed-job-timing",
  "a",
  "A",
  "",
  "é", // precomposed
  "é", // decomposed: localeCompare calls these two equal
  "worker-10",
  "worker-9",
];

Deno.test("resolvePreferredWorker: total, deterministic, and both sides agree", () => {
  for (const a of workerIds) {
    for (const b of workerIds) {
      const fromA = resolvePreferredWorker(a, b);
      const fromB = resolvePreferredWorker(b, a);
      assert(fromA === a || fromA === b, `${a} vs ${b} picked an outsider`);
      assertEquals(fromA, fromB, `${JSON.stringify(a)} vs ${JSON.stringify(b)}: workers disagree`);
      assertEquals(resolvePreferredWorker(a, b), fromA, "not deterministic");
    }
  }
});

Deno.test("resolvePreferredWorker: lexicographically smaller id wins", () => {
  assertEquals(resolvePreferredWorker("test-worker2", "test-worker1"), "test-worker1");
  assertEquals(resolvePreferredWorker("test-worker1", "test-worker2"), "test-worker1");
});

const runningOn = (worker: string): InMemoryDockerJob => ({
  queuedTime: 1,
  state: DockerJobState.Running,
  time: 2,
  worker,
});

Deno.test("isJobClaimedByPreferredOtherWorker: loser yields to the preferred worker", () => {
  assert(isJobClaimedByPreferredOtherWorker(runningOn("test-worker1"), "test-worker2"));
});

Deno.test("isJobClaimedByPreferredOtherWorker: winner keeps its copy", () => {
  assertEquals(isJobClaimedByPreferredOtherWorker(runningOn("test-worker2"), "test-worker1"), false);
});

Deno.test("isJobClaimedByPreferredOtherWorker: a job running on this worker is never 'elsewhere'", () => {
  for (const id of ["test-worker1", "test-worker2", "z-worker"]) {
    assertEquals(isJobClaimedByPreferredOtherWorker(runningOn(id), id), false);
  }
  // The old check compared the server's raw id to the colorized display
  // string, which never matches, so every Running job looked foreign (#330).
  assert(getWorkerColorizedString("test-worker2") !== "test-worker2");
  assertEquals(isJobClaimedByPreferredOtherWorker(runningOn("test-worker2"), "test-worker2"), false);
});

Deno.test("isJobClaimedByPreferredOtherWorker: only Running jobs with a worker count", () => {
  assertEquals(isJobClaimedByPreferredOtherWorker(undefined, "test-worker2"), false);
  assertEquals(
    isJobClaimedByPreferredOtherWorker({ ...runningOn("test-worker1"), state: DockerJobState.Queued }, "test-worker2"),
    false,
  );
  assertEquals(
    isJobClaimedByPreferredOtherWorker(
      { ...runningOn("test-worker1"), state: DockerJobState.Finished },
      "test-worker2",
    ),
    false,
  );
  assertEquals(isJobClaimedByPreferredOtherWorker(runningOn(""), "test-worker2"), false);
});
