/**
 * A job's container that exited while its worker was down (#361): the restarted
 * worker (same id, same data directory) collects the container's exit code, logs
 * and outputs and reports them, instead of running the job again. Docker is a
 * stub; the job queue, IO and result reporting are the real code.
 */
import {
  type BroadcastJobStates,
  type DockerJobDefinitionInputRefs,
  DockerJobState,
  type InMemoryDockerJob,
  type StateChange,
  type StateChangeValueFinished,
  WebsocketMessageTypeWorkerToServer,
  type WebsocketMessageWorkerToServer,
} from "@metapages/compute-queues-shared";
import { assert, assertEquals } from "std/assert";
import { join } from "std/path";
import type Docker from "dockerode";

import { config } from "/@/config.ts";
import { removeAllDeadContainersFromQueue } from "/@/queue/cleanup.ts";
import { ContainerLabelId } from "/@/queue/constants.ts";
import { getExitedContainerForJob, parseDockerLogs } from "/@/queue/DockerJob.ts";
import { DockerJobQueue } from "/@/queue/DockerJobQueue.ts";
import { docker } from "/@/queue/dockerClient.ts";
import type { JobDefinitionCache } from "/@/queue/JobDefinitionCache.ts";

const WORKER = "worker-exited-test";
const JOB = "job-exited-while-worker-down";
const DEFINITION: DockerJobDefinitionInputRefs = { image: "alpine", command: "sh -c 'echo hi; exit 3'" };

const STARTED = "2026-11-10T10:00:00.000000000Z";
const FINISHED = "2026-11-10T10:00:08.500000000Z";

/** One frame of docker's multiplexed log stream */
const frame = (stream: 1 | 2, text: string): Uint8Array => {
  const payload = new TextEncoder().encode(text);
  const out = new Uint8Array(8 + payload.length);
  out[0] = stream;
  new DataView(out.buffer).setUint32(4, payload.length);
  out.set(payload, 8);
  return out;
};

const concat = (...parts: Uint8Array[]): Uint8Array => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
};

type FakeContainer = { id: string; created: string; status: "exited" | "running"; worker?: string };

/** Replace the docker calls the worker makes with these containers */
const stubDocker = (containers: FakeContainer[], events: string[]) => {
  const original = {
    listContainers: docker.listContainers,
    getContainer: docker.getContainer,
    createContainer: docker.createContainer,
    getNetwork: docker.getNetwork,
  };
  docker.listContainers = ((opts: { filters: string }) => {
    const filters = JSON.parse(opts.filters) as { status?: string[] };
    return Promise.resolve(
      containers
        .filter((c) => !filters.status || filters.status.includes(c.status))
        .map((c) => ({ Id: c.id, Labels: { [ContainerLabelId]: JOB }, Created: Date.parse(c.created) / 1000 })),
    );
  }) as unknown as typeof docker.listContainers;
  docker.getContainer = ((id: string) => {
    const c = containers.find((c) => c.id === id)!;
    return {
      id,
      inspect: () =>
        Promise.resolve({
          Created: c.created,
          State: { Status: c.status, ExitCode: 3, StartedAt: STARTED, FinishedAt: FINISHED },
        }),
      logs: () =>
        Promise.resolve(concat(frame(1, `${STARTED} hello from the first run\n`), frame(2, `${FINISHED} oops\n`))),
      remove: () => {
        events.push(`remove ${id}`);
        return Promise.resolve();
      },
    } as unknown as Docker.Container;
  }) as typeof docker.getContainer;
  docker.createContainer = (() => {
    events.push("createContainer");
    return Promise.reject(new Error("the job must not run again"));
  }) as unknown as typeof docker.createContainer;
  docker.getNetwork = (() => ({ inspect: () => Promise.resolve({}) })) as unknown as typeof docker.getNetwork;
  return () => Object.assign(docker, original);
};

const job = (state: DockerJobState, worker: string, queuedTime: number): InMemoryDockerJob => ({
  queuedTime,
  state,
  time: queuedTime,
  worker,
});

const newQueue = (sent: WebsocketMessageWorkerToServer[], events: string[]) => {
  const queue = new DockerJobQueue({
    sender: (message) => {
      sent.push(message);
      const change = message.payload as StateChange;
      if (message.type === WebsocketMessageTypeWorkerToServer.StateChange) {
        events.push(`send ${change.state}`);
      }
    },
    cpus: 2,
    gpus: 0,
    id: WORKER,
    version: "0.0.0",
    time: Date.now(),
    maxJobDuration: "5m",
    queue: "exited-test",
    jobDefinitions: {
      get: () => Promise.resolve(DEFINITION),
      prefetch: () => {},
      onWebsocketMessage: () => {},
    } as unknown as JobDefinitionCache,
  });
  queue.stopPeriodicRegistration();
  queue.stopRunningJobHealthInterval();
  return queue;
};

const waitFor = async (predicate: () => boolean, timeout = 5000) => {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeout) {
      throw new Error("timed out");
    }
    await new Promise((r) => setTimeout(r, 20));
  }
};

for (
  const [name, serverState] of [
    // reconnected before the server noticed the worker was gone
    ["server still says Running on this worker", DockerJobState.Running],
    // the server lost the worker and requeued the job (WorkerLost): same queuedTime
    ["server requeued the job after losing the worker", DockerJobState.Queued],
  ] as const
) {
  Deno.test(`restart, ${name}: the exited container's result is reported, the job is not rerun`, async () => {
    const dataDirectory = await Deno.makeTempDir();
    const saved = { dataDirectory: config.dataDirectory, mode: config.mode, server: config.server };
    Object.assign(config, { dataDirectory, mode: "local", server: "http://localhost:0" });

    // What the first run wrote before the worker went down
    const outputsDir = join(dataDirectory, "j", JOB, "outputs");
    await Deno.mkdir(outputsDir, { recursive: true });
    await Deno.writeTextFile(join(outputsDir, "result.txt"), "from the first run");

    const queuedTime = Date.parse(STARTED) - 1000;
    const events: string[] = [];
    const restore = stubDocker([{ id: "exited-container", created: STARTED, status: "exited" }], events);
    const sent: WebsocketMessageWorkerToServer[] = [];
    const queue = newQueue(sent, events);
    try {
      await queue.onUpdateSetAllJobStates({
        state: {
          jobs: { [JOB]: job(serverState, serverState === DockerJobState.Running ? WORKER : "", queuedTime) },
        },
      } as BroadcastJobStates);

      await waitFor(() => events.includes("remove exited-container"));

      assert(!events.includes("createContainer"), "the job must not run again");
      // reported first, removed after
      assertEquals(events.filter((e) => e !== "send Running"), ["send Finished", "remove exited-container"]);

      const finished = sent
        .map((m) => m.payload as StateChange)
        .find((c) => c.state === DockerJobState.Finished)!.value as StateChangeValueFinished;
      assertEquals(finished.result?.StatusCode, 3);
      assertEquals(finished.result?.duration, 8500);
      assertEquals(finished.result?.logs?.slice(0, 2), [
        ["hello from the first run\n", Date.parse(STARTED)],
        ["oops\n", Date.parse(FINISHED), true],
      ]);
      assertEquals(Object.keys(finished.result?.outputs ?? {}), ["result.txt"]);
    } finally {
      restore();
      Object.assign(config, saved);
      // dockerJobExecute's max duration check is a 1s timer
      await new Promise((r) => setTimeout(r, 1100));
      await Deno.remove(dataDirectory, { recursive: true });
    }
  });
}

Deno.test("getExitedContainerForJob: only a container of the job's current run, the newest", async () => {
  const queuedTime = Date.parse("2026-11-10T10:00:00.500Z");
  const events: string[] = [];
  const restore = stubDocker([
    // same whole second as queuedTime but before it: listContainers' Created can't tell
    { id: "previous-run", created: "2026-11-10T10:00:00.400Z", status: "exited" },
    { id: "this-run-1", created: "2026-11-10T10:00:00.600Z", status: "exited" },
    { id: "this-run-2", created: "2026-11-10T10:00:05.000Z", status: "exited" },
  ], events);
  try {
    const found = await getExitedContainerForJob({ jobId: JOB, workerId: WORKER, queuedTime });
    assertEquals(found?.id, "this-run-2");
    // a new submission of the same id: every container is from before it
    assertEquals(
      await getExitedContainerForJob({ jobId: JOB, workerId: WORKER, queuedTime: Date.parse("2026-11-10T11:00:00Z") }),
      undefined,
    );
  } finally {
    restore();
  }
});

Deno.test("parseDockerLogs: splits stdout and stderr frames and keeps their timestamps", () => {
  const logs = parseDockerLogs(concat(
    frame(1, "2026-11-10T10:00:00.123456789Z one\n"),
    frame(2, "2026-11-10T10:00:01Z two\n"),
    frame(1, "no timestamp"),
  ));
  assertEquals(logs[0], ["one\n", Date.parse("2026-11-10T10:00:00.123Z")]);
  assertEquals(logs[1], ["two\n", Date.parse("2026-11-10T10:00:01Z"), true]);
  assertEquals(logs[2][0], "no timestamp");
  assertEquals(logs.length, 3);
});

Deno.test("keepsExitedContainer: the cleanup leaves a result nobody has reported", () => {
  const queue = newQueue([], []);
  // nothing known yet: keep everything
  assert(queue.keepsExitedContainer(JOB));

  const jobs = (state: DockerJobState, worker = "") => ({ state: { jobs: { [JOB]: job(state, worker, 1) } } });
  // not onUpdateSetAllJobStates: its orphan check would reach the real docker
  queue._updateApiQueue({ state: { jobs: {} } } as BroadcastJobStates);
  queue.gotFirstCompleteJobState = true;
  assert(!queue.keepsExitedContainer(JOB), "unknown to the server");

  queue._updateApiQueue(jobs(DockerJobState.Running, WORKER) as BroadcastJobStates);
  assert(queue.keepsExitedContainer(JOB), "running on this worker");
  queue._updateApiQueue(jobs(DockerJobState.Queued) as BroadcastJobStates);
  assert(queue.keepsExitedContainer(JOB), "requeued, this worker may collect it");
  queue._updateApiQueue(jobs(DockerJobState.Running, "other-worker") as BroadcastJobStates);
  assert(!queue.keepsExitedContainer(JOB), "running elsewhere");
  queue._updateApiQueue(jobs(DockerJobState.Finished, WORKER) as BroadcastJobStates);
  assert(!queue.keepsExitedContainer(JOB), "reported");
});

Deno.test("removeAllDeadContainersFromQueue skips the containers it is told to keep", async () => {
  const events: string[] = [];
  const restore = stubDocker([{ id: "exited-container", created: STARTED, status: "exited" }], events);
  try {
    await removeAllDeadContainersFromQueue({ queue: "q", workerId: WORKER, keep: (jobId) => jobId === JOB });
    assertEquals(events, []);
    await removeAllDeadContainersFromQueue({ queue: "q", workerId: WORKER, keep: () => false });
    assertEquals(events, ["remove exited-container"]);
  } finally {
    restore();
  }
});
