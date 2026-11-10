import { assert, assertEquals } from "std/assert";
import { join } from "std/path";

import {
  type DockerJobDefinitionInputRefs,
  DockerJobFinishedReason,
  DockerJobState,
  type DockerRunResultWithOutputs,
  type InMemoryDockerJob,
  type WebsocketMessageWorkerToServer,
} from "@metapages/compute-queues-shared";
import type { KvStore } from "../../../shared/src/shared/kv.ts";
import { DockerJobQueue } from "/@/queue/DockerJobQueue.ts";
import { FinishedJobsStore, fitForKv } from "/@/queue/FinishedJobsStore.ts";
import type { JobDefinitionCache } from "/@/queue/JobDefinitionCache.ts";

/**
 * #358: what a worker remembers about the jobs it finished must survive the
 * process. Each DockerJobQueue here opens its own handle on one KV file, the
 * way a restarted worker, or an --auto-update successor next to its
 * predecessor, opens the same data directory.
 */

const workerId = "test-worker-358";
const jobId = "job-358";
const definition: DockerJobDefinitionInputRefs = { image: "alpine:3", command: "echo once" };

const apiJob = (state: DockerJobState, worker = workerId): InMemoryDockerJob => ({
  queuedTime: 1,
  time: 2,
  state,
  worker,
});

const openKv = async (path: string) => (await Deno.openKv(path)) as unknown as KvStore;

/** A worker process: its own KV handle, no server and no docker (see DockerJobQueue_test.ts) */
const createWorker = async (kvPath: string, opts?: { handover?: "standby" }) => {
  const kv = await openKv(kvPath);
  const sent: WebsocketMessageWorkerToServer[] = [];
  const started: string[] = [];
  const jobDefinitions = {
    prefetch: () => {},
    get: () => Promise.resolve(definition),
  } as unknown as JobDefinitionCache;
  const queue = new DockerJobQueue({
    sender: (message: WebsocketMessageWorkerToServer) => sent.push(message),
    cpus: 2,
    gpus: 0,
    id: workerId,
    version: "test",
    time: Date.now(),
    maxJobDuration: "1h",
    queue: "test-queue-358",
    jobDefinitions,
    finishedJobs: new FinishedJobsStore(kv, workerId),
    handover: opts?.handover,
  });
  queue.stopPeriodicRegistration();
  queue.stopRunningJobHealthInterval();
  queue._startJob = (id: string) => {
    started.push(id);
    queue._forgetFinished(id, { persisted: true });
  };
  const broadcast = (state: DockerJobState, worker?: string) =>
    queue.onUpdateUpdateASubsetOfJobs({ state: { jobs: { [jobId]: apiJob(state, worker) } } });
  return { kv, queue, sent, started, broadcast };
};

const result: DockerRunResultWithOutputs = {
  StatusCode: 0,
  logs: [["the answer is 42", 1]],
  outputs: {},
  isTimedOut: false,
};

const finishLocally = async (queue: DockerJobQueue) => {
  delete queue.queue[jobId];
  await queue._reportFinished(jobId, {
    type: DockerJobState.Finished,
    reason: DockerJobFinishedReason.Success,
    worker: workerId,
    time: Date.now(),
    result,
  });
};

const finishedSent = (sent: WebsocketMessageWorkerToServer[]) =>
  sent.filter((m) => (m.payload as { state?: DockerJobState })?.state === DockerJobState.Finished);

/** Let fire-and-forget KV deletes settle before the handles close */
const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

const withKvFile = async (fn: (path: string) => Promise<void>) => {
  const dir = await Deno.makeTempDir({ prefix: "worker-358-" });
  try {
    await fn(join(dir, "kv"));
  } finally {
    await Deno.remove(dir, { recursive: true });
  }
};

Deno.test("a restarted worker does not rerun a job it finished before the restart (#358)", async () => {
  await withKvFile(async (path) => {
    const before = await createWorker(path);
    await before.broadcast(DockerJobState.Running);
    assertEquals(before.started, [jobId]);
    await finishLocally(before.queue);
    // The process dies before the server saw Finished (it was still queued in the socket)
    before.kv.close();

    const after = await createWorker(path);
    await after.broadcast(DockerJobState.Running);
    assertEquals(after.started, [], "a job finished before the restart must not run a second time");
    const resent = finishedSent(after.sent);
    assertEquals(resent.length, 1, "the persisted Finished is sent again instead");
    assertEquals(
      (resent[0].payload as { value: { result: DockerRunResultWithOutputs } }).value.result.logs,
      result.logs,
      "with the original result, so it is not lost with the old process",
    );

    // The server catches up: the record has done its job and is removed from disk
    await after.broadcast(DockerJobState.Finished);
    await settle();
    assertEquals(await new FinishedJobsStore(after.kv, workerId).get(jobId), null);

    // The same definition submitted again is a new run and is claimed
    await after.broadcast(DockerJobState.Queued, "");
    assertEquals(after.started, [jobId]);
    await settle();
    after.kv.close();
  });
});

Deno.test("an --auto-update successor does not rerun a job its predecessor just finished (#358)", async () => {
  await withKvFile(async (path) => {
    // Both processes are up at once and share the worker id and data directory
    const predecessor = await createWorker(path);
    const successor = await createWorker(path, { handover: "standby" });

    await predecessor.broadcast(DockerJobState.Running);
    await successor.broadcast(DockerJobState.Running);
    assertEquals(successor.started, [], "a successor in standby claims nothing");

    // The predecessor drains: it finishes the job and retires before the server reflects it
    await finishLocally(predecessor.queue);
    predecessor.kv.close();

    // The successor takes over while the server still says Running on this worker id
    await successor.queue.exitHandover();
    assertEquals(predecessor.started, [jobId]);
    assertEquals(successor.started, [], "the successor must not run the predecessor's finished job");
    assertEquals(finishedSent(successor.sent).length, 1, "it resends the predecessor's Finished");
    await settle();
    successor.kv.close();
  });
});

Deno.test("a stale record on disk does not stop a new run from being picked up after a restart", async () => {
  await withKvFile(async (path) => {
    const first = await createWorker(path);
    await first.broadcast(DockerJobState.Running);
    await finishLocally(first.queue);
    first.kv.close();

    // Restarted after the server saw Finished: the record is on disk, unknown in memory
    const second = await createWorker(path);
    await second.broadcast(DockerJobState.Finished);
    // A resubmission is claimed and run, which clears the record on disk
    await second.broadcast(DockerJobState.Queued, "");
    assertEquals(second.started, [jobId]);
    await settle();
    second.kv.close();

    // Restarted mid-run: the server says Running on us, and this time it is true
    const third = await createWorker(path);
    await third.broadcast(DockerJobState.Running);
    assertEquals(third.started, [jobId], "a job lost mid-run in a restart is picked up again");
    assertEquals(finishedSent(third.sent).length, 0);
    await settle();
    third.kv.close();
  });
});

Deno.test("a Finished too big for a KV value keeps the tail of its logs", () => {
  const logs: [string, number][] = Array.from({ length: 5000 }, (_, i) => [`line ${i} ${"x".repeat(40)}`, i]);
  const message: WebsocketMessageWorkerToServer = {
    type: "StateChange",
    payload: { job: jobId, tag: workerId, state: DockerJobState.Finished, value: { result: { ...result, logs } } },
  } as unknown as WebsocketMessageWorkerToServer;
  const fitted = fitForKv(message);
  const fittedLogs = (fitted.payload as { value: { result: DockerRunResultWithOutputs } }).value.result.logs!;
  assert(new TextEncoder().encode(JSON.stringify(fitted)).length <= 60 * 1024);
  assert(fittedLogs.length > 0 && fittedLogs.length < logs.length);
  assertEquals(fittedLogs.at(-1), logs.at(-1), "the last line, where an error shows, is kept");
  assertEquals(
    (message.payload as { value: { result: DockerRunResultWithOutputs } }).value.result.logs!.length,
    logs.length,
    "the message itself is not modified",
  );
});
