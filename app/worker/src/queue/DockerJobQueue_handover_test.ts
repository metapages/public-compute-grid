/**
 * The worker's half of an --auto-update handover (#337), without docker: a
 * stub sender, stub job definitions, a hand-built server state, and
 * _startJob replaced by a recorder. These guards are what stop a standby
 * successor from killing or double-claiming its predecessor's jobs, and a
 * retiring predecessor from being stopped with work in flight.
 */
import {
  type BroadcastJobStates,
  type DockerJobDefinitionInputRefs,
  DockerJobState,
  type InMemoryDockerJob,
  WebsocketMessageTypeWorkerToServer,
  type WebsocketMessageWorkerToServer,
  type WorkerHandoverState,
} from "@metapages/compute-queues-shared";
import { assertEquals } from "std/assert";

import { DockerJobQueue } from "./DockerJobQueue.ts";
import type { JobDefinitionCache } from "./JobDefinitionCache.ts";
import { DockerRunPhase } from "./types.ts";

const ID = "worker-handover-test";

const CPU_JOB: DockerJobDefinitionInputRefs = { image: "alpine", command: "true" };
const GPU_JOB: DockerJobDefinitionInputRefs = { image: "alpine", command: "nvidia-smi", requirements: { gpus: 1 } };

const job = (state: DockerJobState, worker = "", queuedTime = 1): InMemoryDockerJob => ({
  queuedTime,
  state,
  time: queuedTime,
  worker,
});

const allJobs = (jobs: Record<string, InMemoryDockerJob>): BroadcastJobStates => ({ state: { jobs } });

const setup = (args: {
  handover?: WorkerHandoverState;
  definitions: Record<string, DockerJobDefinitionInputRefs>;
  gpus?: number;
  /** GPU devices of a job's already running container */
  runningContainerGpus?: Record<string, number[]>;
}) => {
  const sent: WebsocketMessageWorkerToServer[] = [];
  const jobDefinitions = {
    get: (jobId: string) => Promise.resolve(args.definitions[jobId] ?? null),
    prefetch: () => {},
    onWebsocketMessage: () => {},
  } as unknown as JobDefinitionCache;
  const queue = new DockerJobQueue({
    sender: (message) => sent.push(message),
    cpus: 4,
    gpus: args.gpus ?? 0,
    id: ID,
    version: "0.54.88",
    time: Date.now(),
    maxJobDuration: "5m",
    queue: "handover-test",
    jobDefinitions,
    handover: args.handover,
  });
  // no timers left behind by the test
  queue.stopPeriodicRegistration();
  queue.stopRunningJobHealthInterval();

  const started: { jobId: string; gpuIndices?: number[] }[] = [];
  // Everything _startJob does before it goes async, without docker
  queue._startJob = (jobId, definition, opts) => {
    queue.queue[jobId] = {
      phase: DockerRunPhase.Running,
      time: Date.now(),
      execution: null,
      definition,
      runningMessageToServer: {} as WebsocketMessageWorkerToServer,
    };
    queue._allocateGpus(jobId, definition, opts?.gpuIndices);
    started.push({ jobId, gpuIndices: queue.queue[jobId].gpuIndices });
  };
  queue.findRunningJobGpuIndices = (jobId) => Promise.resolve(args.runningContainerGpus?.[jobId]);

  const containerChecks = { killOrphanedJobs: 0, checkForLocallyRunningJobs: 0 };
  const internals = queue as unknown as Record<string, unknown>;
  internals.killOrphanedJobs = () => {
    containerChecks.killOrphanedJobs++;
    return Promise.resolve();
  };
  internals.checkForLocallyRunningJobs = () => {
    containerChecks.checkForLocallyRunningJobs++;
    return Promise.resolve();
  };

  const registrations = () =>
    sent.filter((m) => m.type === WebsocketMessageTypeWorkerToServer.WorkerRegistration).length;

  return { queue, sent, started, containerChecks, registrations };
};

/** A job this worker is running, as the queue holds it */
const runLocally = (queue: DockerJobQueue, jobId: string, phase = DockerRunPhase.Running) => {
  queue.queue[jobId] = {
    phase,
    time: Date.now(),
    execution: null,
    definition: CPU_JOB,
    runningMessageToServer: {} as WebsocketMessageWorkerToServer,
  };
};

Deno.test("standby successor: claims nothing, adopts nothing, kills nothing", async () => {
  const { queue, started, containerChecks } = setup({
    handover: "standby",
    definitions: { queued: CPU_JOB, predecessors: CPU_JOB },
  });
  await queue.onUpdateSetAllJobStates(allJobs({
    queued: job(DockerJobState.Queued),
    // the predecessor's job: same worker id, so it looks like ours
    predecessors: job(DockerJobState.Running, ID),
  }));
  assertEquals(started, []);
  // the orphan sweeps would see the predecessor's containers as ours-but-unknown
  assertEquals(containerChecks, { killOrphanedJobs: 0, checkForLocallyRunningJobs: 0 });
  assertEquals(queue.status().handover, "standby");
});

Deno.test("standby successor taking over: claims queued jobs, sweeps once, re-registers", async () => {
  const { queue, started, containerChecks, registrations } = setup({
    handover: "standby",
    definitions: { queued: CPU_JOB },
  });
  await queue.onUpdateSetAllJobStates(allJobs({ queued: job(DockerJobState.Queued) }));
  assertEquals(started, []);

  await queue.exitHandover();
  assertEquals(queue.handover, undefined);
  assertEquals(started.map((s) => s.jobId), ["queued"]);
  assertEquals(containerChecks, { killOrphanedJobs: 1, checkForLocallyRunningJobs: 1 });
  assertEquals(registrations(), 1);
  assertEquals(queue.status().handover, undefined);

  // the sweep is once per process, not once per state broadcast
  await queue.onUpdateSetAllJobStates(allJobs({ queued: job(DockerJobState.Running, ID) }));
  assertEquals(containerChecks, { killOrphanedJobs: 1, checkForLocallyRunningJobs: 1 });
});

Deno.test("taking over from a predecessor that died with jobs: they are picked up again", async () => {
  const { queue, started } = setup({ handover: "standby", definitions: { orphaned: CPU_JOB } });
  await queue.onUpdateSetAllJobStates(allJobs({ orphaned: job(DockerJobState.Running, ID) }));
  assertEquals(started, []);
  await queue.exitHandover();
  assertEquals(started.map((s) => s.jobId), ["orphaned"]);
});

Deno.test("draining predecessor: keeps its running jobs, claims no new ones", async () => {
  const { queue, started } = setup({ definitions: { mine: CPU_JOB, queued: CPU_JOB } });
  runLocally(queue, "mine");
  queue.enterHandover("draining");
  await queue.onUpdateUpdateASubsetOfJobs(allJobs({
    mine: job(DockerJobState.Running, ID),
    queued: job(DockerJobState.Queued),
  }));
  assertEquals(started, []);
  assertEquals(Object.keys(queue.queue), ["mine"]);
  assertEquals(queue.status().handover, "draining");
});

Deno.test("retiring is only reported with no jobs left, so the successor never stops a busy worker", () => {
  const { queue } = setup({ definitions: {} });
  runLocally(queue, "uploading", DockerRunPhase.UploadOutputs);
  queue.enterHandover("retiring");
  assertEquals(queue.handover, "retiring");
  assertEquals(queue.status().handover, "draining");
  assertEquals(queue.reportedHandover(), "draining");

  delete queue.queue["uploading"];
  assertEquals(queue.status().handover, "retiring");
});

Deno.test("an abandoned upgrade: the predecessor claims again", async () => {
  const { queue, started, registrations } = setup({ definitions: { queued: CPU_JOB } });
  queue.enterHandover("draining");
  await queue.onUpdateSetAllJobStates(allJobs({ queued: job(DockerJobState.Queued) }));
  assertEquals(started, []);
  await queue.exitHandover();
  assertEquals(started.map((s) => s.jobId), ["queued"]);
  assertEquals(registrations(), 1);
});

Deno.test("exitHandover outside a handover does nothing", async () => {
  const { queue, registrations } = setup({ definitions: {} });
  await queue.exitHandover();
  assertEquals(registrations(), 0);
});

Deno.test("status carries the version and handover state for the predecessor to verify", () => {
  const { queue } = setup({ handover: "standby", definitions: {} });
  const status = queue.status();
  assertEquals(status.id, ID);
  assertEquals(status.version !== undefined, true);
  assertEquals(status.handover, "standby");
});

Deno.test("a GPU job picked up again keeps the device its container has; new GPU jobs get a free one", async () => {
  const { queue, started } = setup({
    handover: "standby",
    gpus: 2,
    definitions: { running: GPU_JOB, queued: GPU_JOB },
    runningContainerGpus: { running: [1] },
  });
  await queue.onUpdateSetAllJobStates(allJobs({
    running: job(DockerJobState.Running, ID),
    queued: job(DockerJobState.Queued),
  }));
  await queue.exitHandover();
  assertEquals(started, [
    { jobId: "running", gpuIndices: [1] },
    // not device 0 by default and then device 1 again: device 1 is booked
    { jobId: "queued", gpuIndices: [0] },
  ]);
  assertEquals(queue.gpuDeviceIndicesUsed(), [0, 1]);
});

Deno.test("a GPU job picked up again without a running container gets a free device", async () => {
  const { queue, started } = setup({ gpus: 1, definitions: { running: GPU_JOB } });
  await queue.onUpdateSetAllJobStates(allJobs({ running: job(DockerJobState.Running, ID) }));
  assertEquals(started, [{ jobId: "running", gpuIndices: [0] }]);
});
