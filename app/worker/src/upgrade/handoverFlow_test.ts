/**
 * The predecessor's side of the handover (#337), on a fake clock: every way
 * an upgrade can go wrong must leave this worker claiming jobs again, with
 * the successor removed and running jobs untouched; and "retiring" is never
 * reached with a job still running.
 */
import type { WorkerHandoverState, WorkerStatusResponse } from "@metapages/compute-queues-shared";
import { assertEquals, assertRejects } from "std/assert";

import { type HandoverDeps, handOverToSuccessor, type Successor, waitForRunningJobs } from "./handoverFlow.ts";

const ID = "worker-1";
const VERSION = "0.54.88";
const POLL = 3_000;

interface Scenario {
  /** running jobs at the start */
  jobs?: number;
  /** when (ms after start) the running jobs finish */
  jobsFinishAt?: number;
  /** when the successor shows up in the queue status in standby; never if undefined */
  registersAt?: number;
  /** when the successor container dies; never if undefined */
  diesAt?: number;
  /** the queue status cannot be read before this time */
  statusUnavailableUntil?: number;
  createFails?: boolean;
}

const run = async (scenario: Scenario) => {
  let now = 0;
  const events: string[] = [];
  let jobs = scenario.jobs ?? 0;
  const states: { state: WorkerHandoverState | undefined; jobs: number }[] = [];

  const queue = {
    handover: undefined as WorkerHandoverState | undefined,
    enterHandover(state: WorkerHandoverState) {
      this.handover = state;
      states.push({ state, jobs });
      events.push(`handover:${state}`);
    },
    exitHandover() {
      this.handover = undefined;
      events.push("handover:none");
      return Promise.resolve();
    },
    runningJobCount: () => jobs,
  };

  const successorWorker: WorkerStatusResponse = {
    time: 0,
    id: ID,
    cpus: 1,
    gpus: 0,
    queue: {},
    version: VERSION,
    handover: "standby",
  };

  const successor: Successor = {
    name: "worker-0.54.88",
    start: () => {
      events.push(`successor:start (handover=${queue.handover})`);
      return Promise.resolve();
    },
    assertRunning: (when: string) =>
      scenario.diesAt !== undefined && now >= scenario.diesAt
        ? Promise.reject(new Error(`the successor exited ${when}`))
        : Promise.resolve(),
    logsTail: () => Promise.resolve("last log line"),
    remove: () => {
      events.push("successor:removed");
      return Promise.resolve();
    },
  };

  const deps: HandoverDeps = {
    queue,
    workerId: ID,
    version: VERSION,
    timing: { poll: POLL, registerDeadline: 60_000, jobsDeadline: 10 * 60_000, takeoverDeadline: 60_000 },
    getWorkers: () => {
      if (scenario.statusUnavailableUntil !== undefined && now < scenario.statusUnavailableUntil) {
        return Promise.reject(new Error("502"));
      }
      const registered = scenario.registersAt !== undefined && now >= scenario.registersAt;
      return Promise.resolve(registered ? [successorWorker] : []);
    },
    sleep: (ms: number) => {
      now += ms;
      if (scenario.jobsFinishAt !== undefined && now >= scenario.jobsFinishAt) {
        jobs = 0;
      }
      return Promise.resolve();
    },
    now: () => now,
    log: () => {},
  };

  let error: Error | undefined;
  try {
    await handOverToSuccessor(deps, () => {
      events.push(`successor:create (handover=${queue.handover})`);
      return scenario.createFails ? Promise.reject(new Error("no such image")) : Promise.resolve(successor);
    });
  } catch (err) {
    error = err as Error;
  }
  return { error, events, states, jobs: () => jobs, queue, now: () => now };
};

Deno.test("handover: new jobs are paused before the successor is even created", async () => {
  const { events } = await run({ registersAt: 0 });
  assertEquals(events.slice(0, 3), [
    "handover:draining",
    "successor:create (handover=draining)",
    "successor:start (handover=draining)",
  ]);
});

Deno.test("handover: retiring only once the successor is registered and every job has finished", async () => {
  const { states, events, error } = await run({ jobs: 2, registersAt: 2 * POLL, jobsFinishAt: 5 * 60_000 });
  assertEquals(states, [{ state: "draining", jobs: 2 }, { state: "retiring", jobs: 0 }]);
  // nobody stopped us here (that is the successor's move), so the wait times out and cleans up
  assertEquals(error?.message.includes("did not take over"), true);
  assertEquals(events.slice(-2), ["successor:removed", "handover:none"]);
});

Deno.test("handover: jobs that outlast the deadline abandon the upgrade, they are never cut off", async () => {
  const { error, states, events, jobs, queue } = await run({ jobs: 1, registersAt: 0 });
  assertEquals(error?.message.includes("not upgrading with running jobs"), true);
  assertEquals(states.map((s) => s.state), ["draining"]); // never retiring
  assertEquals(jobs(), 1); // the job is left alone
  assertEquals(events.slice(-2), ["successor:removed", "handover:none"]);
  assertEquals(queue.handover, undefined); // claiming again
});

Deno.test("handover: a successor that never registers is removed and the worker carries on", async () => {
  const { error, events, queue, now } = await run({});
  assertEquals(error?.message.includes("did not register"), true);
  assertEquals(error?.message.includes("last log line"), true);
  assertEquals(now() >= 60_000, true);
  assertEquals(events.includes("handover:retiring"), false);
  assertEquals(events.slice(-2), ["successor:removed", "handover:none"]);
  assertEquals(queue.handover, undefined);
});

Deno.test("handover: a successor that exits before registering is removed at once", async () => {
  const { error, events, now } = await run({ diesAt: POLL });
  assertEquals(error?.message, "the successor exited before registering");
  assertEquals(now() < 60_000, true);
  assertEquals(events.slice(-2), ["successor:removed", "handover:none"]);
});

Deno.test("handover: a successor that dies while jobs finish is removed; the jobs carry on here", async () => {
  const { error, events, jobs, queue } = await run({
    jobs: 3,
    registersAt: 0,
    diesAt: 4 * POLL,
    jobsFinishAt: 10 * POLL,
  });
  assertEquals(error?.message, "the successor exited while this worker was waiting for its jobs");
  assertEquals(jobs(), 3);
  assertEquals(events.includes("handover:retiring"), false);
  assertEquals(events.slice(-2), ["successor:removed", "handover:none"]);
  assertEquals(queue.handover, undefined);
});

Deno.test("handover: a successor that dies after this worker retired un-retires it", async () => {
  const { error, events, queue } = await run({ registersAt: 0, diesAt: 3 * POLL });
  assertEquals(error?.message, "the successor exited before taking over");
  assertEquals(events.includes("handover:retiring"), true);
  assertEquals(events.slice(-2), ["successor:removed", "handover:none"]);
  assertEquals(queue.handover, undefined);
});

Deno.test("handover: the queue status being unreadable for a while is not fatal", async () => {
  const { states } = await run({ registersAt: 0, statusUnavailableUntil: 5 * POLL });
  assertEquals(states.map((s) => s.state), ["draining", "retiring"]);
});

Deno.test("handover: a successor that cannot be created resumes claiming, nothing to remove", async () => {
  const { error, events, queue } = await run({ createFails: true });
  assertEquals(error?.message, "no such image");
  assertEquals(events, ["handover:draining", "successor:create (handover=draining)", "handover:none"]);
  assertEquals(queue.handover, undefined);
});

Deno.test("waitForRunningJobs: no jobs, no wait", async () => {
  let slept = 0;
  await waitForRunningJobs({
    queue: {
      handover: "draining",
      enterHandover: () => {},
      exitHandover: () => Promise.resolve(),
      runningJobCount: () => 0,
    },
    workerId: ID,
    version: VERSION,
    timing: { poll: POLL, registerDeadline: 0, jobsDeadline: 0, takeoverDeadline: 0 },
    getWorkers: () => Promise.resolve([]),
    sleep: () => {
      slept++;
      return Promise.resolve();
    },
    now: () => 0,
    log: () => {},
  });
  assertEquals(slept, 0);
});

Deno.test("waitForRunningJobs: throws when jobs outlast the deadline", async () => {
  let now = 0;
  await assertRejects(
    () =>
      waitForRunningJobs({
        queue: {
          handover: "draining",
          enterHandover: () => {},
          exitHandover: () => Promise.resolve(),
          runningJobCount: () => 1,
        },
        workerId: ID,
        version: VERSION,
        timing: { poll: POLL, registerDeadline: 0, jobsDeadline: 30_000, takeoverDeadline: 0 },
        getWorkers: () => Promise.resolve([]),
        sleep: (ms) => {
          now += ms;
          return Promise.resolve();
        },
        now: () => now,
        log: () => {},
      }),
    Error,
    "not upgrading with running jobs",
  );
});
