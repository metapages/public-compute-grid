/**
 * The queue's log path (#332): lines buffered while a job runs, flushed when it
 * finishes, appended after that. Whatever order the state changes and log
 * batches arrive in, every line must be stored exactly once.
 *
 * The db is a stand-in whose reads and writes take a random few milliseconds,
 * so a flush and the appends that follow it really do overlap.
 */
import { assertEquals } from "std/assert";

import { BaseDockerJobQueue, NoopBroadcastChannel, setBroadcastChannelFactory } from "./jobqueue.ts";
import type { DB } from "./db.ts";
import {
  type ConsoleLogLine,
  DockerJobFinishedReason,
  DockerJobState,
  type InMemoryDockerJob,
  type StateChangeValueFinished,
} from "./types.ts";
import { resolveMostCorrectJob, setJobStateFinished } from "./util.ts";

setBroadcastChannelFactory((name) => new NoopBroadcastChannel(name) as unknown as BroadcastChannel);

const WORKER = "worker-1";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const jitter = () => sleep(Math.random() * 5);

/** Just the parts of DB the log path and the state changes touch. */
class FakeDb {
  runLogs = new Map<string, ConsoleLogLine[]>();
  buildLogs = new Map<string, ConsoleLogLine[]>();
  /** Make every store take this long instead of a random few ms */
  storeDelay = 0;

  private storing() {
    return this.storeDelay ? sleep(this.storeDelay) : jitter();
  }

  async getRunLogs(jobId: string) {
    await jitter();
    return this.runLogs.get(jobId) ?? null;
  }
  async storeRunLogs(jobId: string, logs: ConsoleLogLine[]) {
    await this.storing();
    this.runLogs.set(jobId, logs);
  }
  async getBuildLogs(jobId: string) {
    await jitter();
    return this.buildLogs.get(jobId) ?? null;
  }
  async storeBuildLogs(jobId: string, logs: ConsoleLogLine[]) {
    await this.storing();
    this.buildLogs.set(jobId, logs);
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

const createQueue = (jobId: string) => {
  const queue = new BaseDockerJobQueue({ serverId: "test", address: `logs-test-${jobId}`, periodicWork: false });
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
  return { queue, db };
};

let counter = 0;
const batch = (n: number): ConsoleLogLine[] => Array.from({ length: n }, () => [`line-${counter++}\n`, Date.now()]);

const deliver = (queue: BaseDockerJobQueue, jobId: string, logs: ConsoleLogLine[]) =>
  queue.broadcastAndSendLogsToLocalClients({ jobId, step: DockerJobState.Running, logs });

const finish = (queue: BaseDockerJobQueue, jobId: string) =>
  queue.stateChangeJobFinished(jobId, {
    type: DockerJobState.Finished,
    reason: DockerJobFinishedReason.Success,
    time: Date.now(),
  });

const running = (queue: BaseDockerJobQueue, jobId: string, worker = WORKER) =>
  queue.stateChangeJobRunning(jobId, { type: DockerJobState.Running, worker, time: Date.now() });

/** Wait for any appends still in flight, then return what the store holds. */
const storedRunLog = async (queue: BaseDockerJobQueue, db: FakeDb, jobId: string) => {
  // deno-lint-ignore no-explicit-any
  await (queue as any).persistedLogs.get(jobId)?.writes;
  return db.runLogs.get(jobId) ?? [];
};

const assertExactly = (stored: ConsoleLogLine[], expected: ConsoleLogLine[]) => {
  const texts = stored.map((l) => l[0]);
  assertEquals(new Set(texts).size, texts.length, "a line was stored more than once");
  assertEquals(texts, expected.map((l) => l[0]));
};

// The timers queued by stateChangeJobFinished (evicting finished jobs a minute
// later) outlive each test; they touch nothing here.
const opts = { sanitizeOps: false, sanitizeResources: false };

Deno.test("logs: lines arriving after the flush are appended exactly once", opts, async () => {
  const jobId = "job-after-flush";
  const { queue, db } = createQueue(jobId);
  const before = [...batch(50), ...batch(50)];
  deliver(queue, jobId, before.slice(0, 50));
  deliver(queue, jobId, before.slice(50));
  await finish(queue, jobId);

  const after = [batch(10), batch(10), batch(10)];
  for (const b of after) {
    deliver(queue, jobId, b);
  }
  assertExactly(await storedRunLog(queue, db, jobId), [...before, ...after.flat()]);
});

Deno.test("logs: lines arriving while the flush is being written are kept, once", opts, async () => {
  const jobId = "job-during-flush";
  const { queue, db } = createQueue(jobId);
  db.storeDelay = 50;
  const before = batch(100);
  deliver(queue, jobId, before);
  const finishing = finish(queue, jobId);
  // The flush has taken the buffer and is still writing it
  await sleep(10);
  const during = [batch(5), batch(5), batch(5)];
  for (const b of during) {
    deliver(queue, jobId, b);
  }
  await finishing;
  assertExactly(await storedRunLog(queue, db, jobId), [...before, ...during.flat()]);
});

Deno.test("logs: a Running after Finished does not duplicate the log", opts, async () => {
  const jobId = "job-late-running";
  const { queue, db } = createQueue(jobId);
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

  assertExactly(await storedRunLog(queue, db, jobId), [...before, ...late1, ...late2, ...late3]);
});

Deno.test("logs: repeated Finished, stale Running and batches in any order store every line once", opts, async () => {
  for (let round = 0; round < 20; round++) {
    const jobId = `job-shuffle-${round}`;
    const { queue, db } = createQueue(jobId);
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
    assertExactly(await storedRunLog(queue, db, jobId), expected);
  }
});

Deno.test("logs: a job that really runs again starts a fresh log", opts, async () => {
  const jobId = "job-rerun";
  const { queue, db } = createQueue(jobId);
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
  assertExactly(await storedRunLog(queue, db, jobId), rerun);
});
