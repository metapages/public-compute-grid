/**
 * Two workers claiming the same job (#330), without docker: the worker that
 * loses the claim must drop its copy as soon as the server says the preferred
 * worker has it, not after a timer that outlasts most jobs. Deno's timer
 * sanitizer also fails these tests if a delayed kill is left scheduled.
 */
import {
  type BroadcastJobStates,
  type DockerJobDefinitionInputRefs,
  DockerJobState,
  type InMemoryDockerJob,
  type WebsocketMessageWorkerToServer,
} from "@metapages/compute-queues-shared";
import { assertEquals } from "std/assert";

import { DockerJobQueue } from "./DockerJobQueue.ts";
import type { JobDefinitionCache } from "./JobDefinitionCache.ts";
import { type DockerJobExecution, DockerRunPhase } from "./types.ts";

const PREFERRED = "test-worker1";
const LOSER = "test-worker2";
const JOB: DockerJobDefinitionInputRefs = { image: "alpine", command: "true" };

const job = (state: DockerJobState, worker = ""): InMemoryDockerJob => ({
  queuedTime: 1,
  state,
  time: 1,
  worker,
});

const subset = (jobs: Record<string, InMemoryDockerJob>): BroadcastJobStates => ({ state: { jobs } });

const setup = (workerId: string, getDefinition?: (jobId: string) => Promise<DockerJobDefinitionInputRefs | null>) => {
  const jobDefinitions = {
    get: getDefinition ?? (() => Promise.resolve(JOB)),
    prefetch: () => {},
    onWebsocketMessage: () => {},
  } as unknown as JobDefinitionCache;
  const queue = new DockerJobQueue({
    sender: () => {},
    cpus: 4,
    gpus: 0,
    id: workerId,
    version: "0.0.0",
    time: Date.now(),
    maxJobDuration: "5m",
    queue: "double-claim-test",
    jobDefinitions,
  });
  queue.stopPeriodicRegistration();
  queue.stopRunningJobHealthInterval();

  const started: string[] = [];
  queue._startJob = (jobId, definition) => {
    started.push(jobId);
    queue.queue[jobId] = {
      phase: DockerRunPhase.CopyInputs,
      time: Date.now(),
      execution: null,
      definition,
      runningMessageToServer: {} as WebsocketMessageWorkerToServer,
    };
  };
  return { queue, started };
};

/** A local copy of the job whose container is (about to be) running */
const runLocally = (
  queue: DockerJobQueue,
  jobId: string,
  phase: DockerRunPhase,
  execution: DockerJobExecution | null,
) => {
  queue.queue[jobId] = {
    phase,
    time: Date.now(),
    execution,
    definition: JOB,
    runningMessageToServer: {} as WebsocketMessageWorkerToServer,
  };
};

const fakeExecution = () => {
  const kills: string[] = [];
  const execution = {
    isKilled: { value: false },
    kill: (reason: string) => kills.push(reason),
    finish: new Promise(() => {}),
  } as unknown as DockerJobExecution;
  return { execution, kills };
};

Deno.test("loser drops a copy whose container has not started yet, immediately", async () => {
  const { queue } = setup(LOSER);
  runLocally(queue, "j", DockerRunPhase.CopyInputs, null);
  await queue.onUpdateUpdateASubsetOfJobs(subset({ j: job(DockerJobState.Running, PREFERRED) }));
  // gone from the queue: _startJob's async steps see that and never start a container
  assertEquals(queue.queue["j"], undefined);
});

Deno.test("loser kills a running container in the same tick the conflict is seen", async () => {
  const { queue } = setup(LOSER);
  const { execution, kills } = fakeExecution();
  runLocally(queue, "j", DockerRunPhase.Running, execution);
  await queue.onUpdateUpdateASubsetOfJobs(subset({ j: job(DockerJobState.Running, PREFERRED) }));
  assertEquals(queue.queue["j"], undefined);
  assertEquals(execution.isKilled.value, true);
  assertEquals(kills.length, 1);
});

Deno.test("preferred worker keeps its copy when the server briefly shows the loser", async () => {
  const { queue } = setup(PREFERRED);
  const { execution, kills } = fakeExecution();
  runLocally(queue, "j", DockerRunPhase.Running, execution);
  await queue.onUpdateUpdateASubsetOfJobs(subset({ j: job(DockerJobState.Running, LOSER) }));
  assertEquals(queue.queue["j"]?.execution, execution);
  assertEquals(kills, []);
});

Deno.test("a job the server says runs on this worker is never treated as running elsewhere", async () => {
  for (const id of [PREFERRED, LOSER]) {
    const { queue } = setup(id);
    const { execution, kills } = fakeExecution();
    runLocally(queue, "j", DockerRunPhase.Running, execution);
    await queue.onUpdateUpdateASubsetOfJobs(subset({ j: job(DockerJobState.Running, id) }));
    assertEquals(queue.queue["j"]?.execution, execution);
    assertEquals(kills, []);
  }
});

Deno.test("a job finished by another worker is killed here immediately", async () => {
  const { queue } = setup(PREFERRED);
  const { execution, kills } = fakeExecution();
  runLocally(queue, "j", DockerRunPhase.Running, execution);
  await queue.onUpdateUpdateASubsetOfJobs(subset({ j: job(DockerJobState.Finished, LOSER) }));
  assertEquals(queue.queue["j"], undefined);
  assertEquals(kills.length, 1);
});

Deno.test("no claim when another worker took the job while its definition was loading", async () => {
  let releaseDefinition: () => void = () => {};
  const definitionLoaded = new Promise<void>((resolve) => releaseDefinition = resolve);
  const { queue, started } = setup(LOSER, async () => {
    await definitionLoaded;
    return JOB;
  });
  const claiming = queue.onUpdateUpdateASubsetOfJobs(subset({ j: job(DockerJobState.Queued) }));
  // the server's broadcast arrives while _claimJobs is awaiting the definition
  queue._updateApiQueue({ ...subset({ j: job(DockerJobState.Running, PREFERRED) }), isSubset: true });
  releaseDefinition();
  await claiming;
  assertEquals(started, []);
  assertEquals(queue.queue["j"], undefined);
});

Deno.test("a job still queued after its definition loads is claimed", async () => {
  const { queue, started } = setup(LOSER);
  await queue.onUpdateUpdateASubsetOfJobs(subset({ j: job(DockerJobState.Queued) }));
  assertEquals(started, ["j"]);
});
