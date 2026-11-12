/**
 * The queue's log path (#332, #360): lines written to storage as they stream
 * in, merged into one log when the job finishes, merged into it after that.
 * Whatever order the state changes and log batches arrive in, every line must
 * be stored exactly once, and a server restarting mid-job must keep them.
 *
 * Storage is a stand-in whose operations take a random few milliseconds, so
 * writes, merges and the state changes really do overlap.
 */
import { assertEquals } from "std/assert";

import { BaseDockerJobQueue, NoopBroadcastChannel, setBroadcastChannelFactory } from "./jobqueue.ts";
import type { DB } from "./db.ts";
import { MemoryLogStorage } from "./joblogs_memory.ts";
import {
  type ConsoleLogLine,
  DockerJobFinishedReason,
  DockerJobState,
  type InMemoryDockerJob,
  type StateChangeValueFinished,
} from "./types.ts";
import { readStoredLogs } from "./joblogs.ts";
import { resolveMostCorrectJob, setJobStateFinished } from "./util.ts";

setBroadcastChannelFactory((name) => new NoopBroadcastChannel(name) as unknown as BroadcastChannel);

const WORKER = "worker-1";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const jitter = () => sleep(Math.random() * 5);

/** Just the parts of DB the log path and the state changes touch. */
class FakeDb {
  getRunLogs() {
    return Promise.resolve(null);
  }
  getBuildLogs() {
    return Promise.resolve(null);
  }
  queueJobGet() {
    return Promise.resolve(null);
  }
  getFinishedJob() {
    return Promise.resolve(null);
  }
  async setJobRunning() {
    await jitter();
  }
  async setJobFinished(args: { change: StateChangeValueFinished; job: InMemoryDockerJob }) {
    await jitter();
    const finished = setJobStateFinished(args.job, { finished: args.change });
    const updatedInMemoryJob = resolveMostCorrectJob(args.job, finished);
    return { updatedInMemoryJob: updatedInMemoryJob === args.job ? undefined : updatedInMemoryJob };
  }
}

/** A queue with its job Running. Pass the storage of a previous one to restart it. */
const createQueue = (jobId: string, opts?: { storage?: MemoryLogStorage }) => {
  const storage = opts?.storage ?? new MemoryLogStorage();
  const queue = new BaseDockerJobQueue({
    serverId: "test",
    address: `logs-test-${jobId}`,
    periodicWork: false,
    logStorage: storage,
  });
  const db = new FakeDb();
  queue.db = db as unknown as DB;
  const now = Date.now();
  // deno-lint-ignore no-explicit-any
  (queue as any).state.jobs[jobId] = {
    queuedTime: now,
    state: DockerJobState.Running,
    time: now,
    worker: WORKER,
    namespaces: ["_"],
  } as InMemoryDockerJob;
  return { queue, storage };
};

let counter = 0;
const batch = (n: number): ConsoleLogLine[] => Array.from({ length: n }, () => [`line-${counter++}\n`, Date.now()]);

const deliver = (queue: BaseDockerJobQueue, jobId: string, logs: ConsoleLogLine[]) =>
  queue.broadcastAndSendLogsToLocalClients({ jobId, step: DockerJobState.Running, logs });

const deliverBuild = (queue: BaseDockerJobQueue, jobId: string, logs: ConsoleLogLine[]) =>
  queue.broadcastAndSendLogsToLocalClients({ jobId, step: "docker build", logs });

const finish = (queue: BaseDockerJobQueue, jobId: string) =>
  queue.stateChangeJobFinished(jobId, {
    type: DockerJobState.Finished,
    reason: DockerJobFinishedReason.Success,
    time: Date.now(),
  });

const running = (queue: BaseDockerJobQueue, jobId: string, worker = WORKER) =>
  queue.stateChangeJobRunning(jobId, { type: DockerJobState.Running, worker, time: Date.now() });

/** Write out anything not written yet, then return what storage holds. */
const storedLog = async (queue: BaseDockerJobQueue, storage: MemoryLogStorage, jobId: string, kind = "run") => {
  await queue.flushLiveLogs();
  return await readStoredLogs(storage, jobId, kind as "build" | "run");
};

const assertExactly = (stored: ConsoleLogLine[], expected: ConsoleLogLine[]) => {
  const texts = stored.map((l) => l[0]);
  assertEquals(new Set(texts).size, texts.length, "a line was stored more than once");
  assertEquals(texts, expected.map((l) => l[0]));
};

// The timers queued by stateChangeJobFinished (evicting finished jobs a minute
// later) outlive each test; they touch nothing here.
const opts = { sanitizeOps: false, sanitizeResources: false };

Deno.test("logs: lines arriving after the job finished are merged in exactly once", opts, async () => {
  const jobId = "job-after-flush";
  const { queue, storage } = createQueue(jobId);
  const before = [...batch(50), ...batch(50)];
  deliver(queue, jobId, before.slice(0, 50));
  deliver(queue, jobId, before.slice(50));
  await finish(queue, jobId);

  const after = [batch(10), batch(10), batch(10)];
  for (const b of after) {
    deliver(queue, jobId, b);
  }
  assertExactly(await storedLog(queue, storage, jobId), [...before, ...after.flat()]);
});

Deno.test("logs: lines arriving while the merge is being written are kept, once", opts, async () => {
  const jobId = "job-during-flush";
  const { queue, storage } = createQueue(jobId);
  storage.delay = 20;
  const before = batch(100);
  deliver(queue, jobId, before);
  const finishing = finish(queue, jobId);
  // The merge is still being written
  await sleep(10);
  const during = [batch(5), batch(5), batch(5)];
  for (const b of during) {
    deliver(queue, jobId, b);
  }
  await finishing;
  assertExactly(await storedLog(queue, storage, jobId), [...before, ...during.flat()]);
});

Deno.test("logs: a Running after Finished does not duplicate the log", opts, async () => {
  const jobId = "job-late-running";
  const { queue, storage } = createQueue(jobId);
  const before = batch(100);
  deliver(queue, jobId, before);
  await finish(queue, jobId);
  const late1 = batch(20);
  deliver(queue, jobId, late1);

  // A re-broadcast / a worker re-registering the job it just finished
  await running(queue, jobId);
  const late2 = batch(20);
  deliver(queue, jobId, late2);
  await finish(queue, jobId);
  const late3 = batch(20);
  deliver(queue, jobId, late3);

  assertExactly(await storedLog(queue, storage, jobId), [...before, ...late1, ...late2, ...late3]);
});

Deno.test("logs: repeated Finished, stale Running and batches in any order store every line once", opts, async () => {
  for (let round = 0; round < 20; round++) {
    const jobId = `job-shuffle-${round}`;
    const { queue, storage } = createQueue(jobId);
    const expected: ConsoleLogLine[] = [];
    const pending: Promise<void>[] = [];
    for (let step = 0; step < 40; step++) {
      const roll = Math.random();
      if (roll < 0.1) {
        pending.push(finish(queue, jobId));
      } else if (roll < 0.15) {
        pending.push(running(queue, jobId));
      } else {
        const b = batch(1 + Math.floor(Math.random() * 20));
        expected.push(...b);
        deliver(queue, jobId, b);
      }
      if (Math.random() < 0.5) {
        await jitter();
      }
    }
    pending.push(finish(queue, jobId));
    await Promise.all(pending);
    assertExactly(await storedLog(queue, storage, jobId), expected);
  }
});

Deno.test("logs: a job that really runs again starts a fresh log", opts, async () => {
  const jobId = "job-rerun";
  const { queue, storage } = createQueue(jobId);
  deliver(queue, jobId, batch(30));
  // The worker was lost: the job goes back to Queued, then another worker takes it
  await queue.stateChangeJobFinished(jobId, {
    type: DockerJobState.Finished,
    reason: DockerJobFinishedReason.WorkerLost,
    time: Date.now(),
  });
  // deno-lint-ignore no-explicit-any
  const jobs = (queue as any).state.jobs as Record<string, InMemoryDockerJob>;
  jobs[jobId] = { ...jobs[jobId], state: DockerJobState.Queued, worker: "", time: Date.now() };
  await running(queue, jobId, "worker-2");
  const rerun = batch(30);
  deliver(queue, jobId, rerun);
  assertEquals((await queue.getCurrentRunLogs(jobId)).map((l) => l[0]), rerun.map((l) => l[0]));
  await finish(queue, jobId);
  assertExactly(await storedLog(queue, storage, jobId), rerun);
});

// #360: a server restarting mid-job keeps the lines streamed before it

Deno.test("logs: a restart mid-job keeps the build and run logs streamed before it", opts, async () => {
  const jobId = "job-restart";
  const { queue, storage } = createQueue(jobId);
  const build = batch(5);
  const before = batch(20);
  deliverBuild(queue, jobId, build);
  deliver(queue, jobId, before);
  // Written within the flush interval; then the process dies
  await queue.flushLiveLogs();

  const { queue: restarted } = createQueue(jobId, { storage });
  assertEquals((await restarted.getCurrentRunLogs(jobId)).map((l) => l[0]), before.map((l) => l[0]));
  const after = batch(20);
  deliver(restarted, jobId, after);
  await finish(restarted, jobId);

  assertExactly(await storedLog(restarted, storage, jobId), [...before, ...after]);
  assertExactly(await storedLog(restarted, storage, jobId, "build"), build);
  // Finished: one file per kind, no chunks
  assertEquals([...storage.objects.keys()].sort(), [`j/${jobId}/logs/build.json`, `j/${jobId}/logs/run.json`]);
});

Deno.test("logs: a worker starting the run's output again replaces the run log", opts, async () => {
  const jobId = "job-replay";
  const { queue, storage } = createQueue(jobId);
  const build = batch(5);
  const before = batch(20);
  deliverBuild(queue, jobId, build);
  deliver(queue, jobId, before);
  await queue.flushLiveLogs();

  const { queue: restarted } = createQueue(jobId, { storage });
  // The worker reattaches: docker replays the container's output from the start
  restarted.broadcastAndSendLogsToLocalClients({ jobId, step: DockerJobState.Running, logs: [], replay: true });
  deliver(restarted, jobId, before);
  const after = batch(20);
  deliver(restarted, jobId, after);
  // Another restart before the job finishes still has it all, once
  await restarted.flushLiveLogs();
  const { queue: again } = createQueue(jobId, { storage });
  await finish(again, jobId);

  assertExactly(await storedLog(again, storage, jobId), [...before, ...after]);
  assertExactly(await storedLog(again, storage, jobId, "build"), build);
});
