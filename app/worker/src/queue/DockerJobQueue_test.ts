import { assertEquals } from "std/assert";

import {
  type DockerJobDefinitionInputRefs,
  DockerJobFinishedReason,
  DockerJobState,
  type InMemoryDockerJob,
  type WebsocketMessageWorkerToServer,
} from "@metapages/compute-queues-shared";
import { DockerJobQueue } from "/@/queue/DockerJobQueue.ts";
import type { JobDefinitionCache } from "/@/queue/JobDefinitionCache.ts";

const workerId = "test-worker-346";
const jobId = "job-346";
const definition: DockerJobDefinitionInputRefs = { image: "alpine:3", command: "echo once" };

const apiJob = (state: DockerJobState, worker = workerId): InMemoryDockerJob => ({
  queuedTime: 1,
  time: 2,
  state,
  worker,
});

/**
 * A DockerJobQueue with no server and no docker: what it sends is recorded, and
 * starting a job only records the id, so the claim logic can be driven by hand.
 */
const createQueue = () => {
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
    queue: "test-queue-346",
    jobDefinitions,
  });
  queue.stopPeriodicRegistration();
  queue.stopRunningJobHealthInterval();
  queue._startJob = (id: string) => {
    started.push(id);
  };
  const broadcast = (state: DockerJobState, worker?: string) =>
    queue.onUpdateUpdateASubsetOfJobs({ state: { jobs: { [jobId]: apiJob(state, worker) } } });
  return { queue, sent, started, broadcast };
};

const finishLocally = (queue: DockerJobQueue) => {
  // What the end of a run does: drop the local entry, then report Finished
  delete queue.queue[jobId];
  queue._reportFinished(jobId, {
    type: DockerJobState.Finished,
    reason: DockerJobFinishedReason.Success,
    worker: workerId,
    time: Date.now(),
    result: { StatusCode: 0, logs: [], outputs: {}, isTimedOut: false },
  });
};

const finishedSends = (sent: WebsocketMessageWorkerToServer[]) =>
  sent.filter((m) => (m.payload as { state?: DockerJobState })?.state === DockerJobState.Finished).length;

Deno.test("a stale Running broadcast does not restart a job this worker already finished (#346)", async () => {
  const { queue, sent, started, broadcast } = createQueue();

  // The server saw us claim it, and we ran it
  await broadcast(DockerJobState.Running);
  assertEquals(started, [jobId], "a job the server says is running on us, that we do not have, is (re)started");
  finishLocally(queue);
  assertEquals(finishedSends(sent), 1);

  // The server has not processed our Finished yet, and says Running on us
  await broadcast(DockerJobState.Running);
  await broadcast(DockerJobState.Running);
  assertEquals(started, [jobId], "the finished job must not run a second time");
  assertEquals(finishedSends(sent), 3, "each stale Running is answered with our Finished again");

  // The server catches up
  await broadcast(DockerJobState.Finished);
  assertEquals(started, [jobId]);

  // The same definition submitted again (same id): it is a new run and must be claimed
  await broadcast(DockerJobState.Queued, "");
  assertEquals(started, [jobId, jobId], "a resubmission of a finished id is claimed");
});

Deno.test("a resubmission seen as Queued right after finishing is claimed", async () => {
  const { queue, started, broadcast } = createQueue();

  await broadcast(DockerJobState.Running);
  finishLocally(queue);

  // No Finished broadcast in between: any non-Running observation ends the suppression
  await broadcast(DockerJobState.Queued, "");
  assertEquals(started, [jobId, jobId]);
});

Deno.test("a job lost in a worker restart is still restarted", async () => {
  // A fresh queue has finished nothing, so the server's Running on us means we lost it
  const { started, broadcast } = createQueue();
  await broadcast(DockerJobState.Running);
  assertEquals(started, [jobId]);
});
