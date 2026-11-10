/**
 * The predecessor's side of an --auto-update handover, with docker, the queue
 * status and the clock passed in, so every way it can go wrong is testable.
 * autoUpdate.ts supplies the real ones.
 */
import type { WorkerHandoverState, WorkerStatusResponse } from "@metapages/compute-queues-shared";
import humanizeDuration from "humanize-duration";

import { isSuccessorRegistered } from "./handover.ts";

/** The parts of DockerJobQueue a handover drives. */
export interface HandoverQueue {
  handover: WorkerHandoverState | undefined;
  enterHandover(state: WorkerHandoverState): void;
  exitHandover(): Promise<void>;
  runningJobCount(): number;
}

/** A successor container, or anything that behaves like one. */
export interface Successor {
  name: string;
  start(): Promise<void>;
  /** Throws, saying why, if the successor is no longer running. */
  assertRunning(when: string): Promise<void>;
  /** The successor's last log lines, for error messages. */
  logsTail(): Promise<string>;
  remove(): Promise<void>;
}

export interface HandoverTiming {
  poll: number;
  /** How long a successor gets to start, connect and register. */
  registerDeadline: number;
  /** How long running jobs get to finish (and upload) once new ones are paused. */
  jobsDeadline: number;
  /** How long a retiring worker waits for its successor to stop it. */
  takeoverDeadline: number;
}

export interface HandoverDeps {
  queue: HandoverQueue;
  workerId: string;
  version: string;
  timing: HandoverTiming;
  getWorkers(): Promise<WorkerStatusResponse[]>;
  sleep(ms: number): Promise<void>;
  now(): number;
  log(message: string): void;
}

/**
 * With new jobs already paused, wait until every running job has finished and
 * uploaded its outputs: only the worker that ran a job knows when that is, so
 * jobs are never handed over. A job outlasting the deadline (its own
 * --max-job-duration plus upload time) means something is stuck, and the
 * upgrade is abandoned rather than cutting it off.
 */
export const waitForRunningJobs = async (deps: HandoverDeps, successor?: Successor) => {
  const { queue, timing } = deps;
  const deadline = deps.now() + timing.jobsDeadline;
  let lastLogged = -Infinity;
  while (queue.runningJobCount() > 0) {
    if (deps.now() > deadline) {
      throw new Error(
        `${queue.runningJobCount()} job(s) still running after ${
          humanizeDuration(timing.jobsDeadline)
        }: not upgrading with running jobs`,
      );
    }
    if (deps.now() - lastLogged > 60_000) {
      lastLogged = deps.now();
      deps.log(`new jobs paused, waiting for ${queue.runningJobCount()} running job(s) to finish`);
    }
    if (successor) {
      await successor.assertRunning("while this worker was waiting for its jobs");
    }
    await deps.sleep(timing.poll);
  }
};

/** Wait until the successor is on the queue: same worker id, the new version, in standby. */
export const verifySuccessor = async (deps: HandoverDeps, successor: Successor) => {
  const deadline = deps.now() + deps.timing.registerDeadline;
  while (deps.now() < deadline) {
    await successor.assertRunning("before registering");
    try {
      if (isSuccessorRegistered(await deps.getWorkers(), deps.workerId, deps.version)) {
        deps.log(`successor ${deps.version} is registered on the queue ✅`);
        return;
      }
    } catch (err) {
      deps.log(`cannot read the queue status yet: ${err}`);
    }
    await deps.sleep(deps.timing.poll);
  }
  throw new Error(
    `the successor did not register within ${humanizeDuration(deps.timing.registerDeadline)}:\n${await successor
      .logsTail()}`,
  );
};

/**
 * Hand this worker's place on the queue to a successor:
 *
 * 1. pause new jobs ("draining"), so the running ones are the last
 * 2. create and start the successor; it comes up in standby
 * 3. verify it registered (same id, new version, standby)
 * 4. wait for the running jobs to finish
 * 5. "retiring": the successor sees that, stops this worker and takes over
 *
 * This only returns by throwing: on success the successor stops this process
 * mid-wait. Any failure removes the successor and resumes claiming jobs.
 */
export const handOverToSuccessor = async (
  deps: HandoverDeps,
  createSuccessor: () => Promise<Successor>,
): Promise<never> => {
  const { queue, timing } = deps;
  queue.enterHandover("draining");
  let successor: Successor | undefined;
  try {
    successor = await createSuccessor();
    deps.log(`created successor ${successor.name}`);
    await successor.start();
    await verifySuccessor(deps, successor);
    await waitForRunningJobs(deps, successor);
    queue.enterHandover("retiring");
    deps.log(`no jobs left, ready to be replaced: waiting for ${successor.name} to stop this worker`);
    const deadline = deps.now() + timing.takeoverDeadline;
    while (deps.now() < deadline) {
      await successor.assertRunning("before taking over");
      await deps.sleep(timing.poll);
    }
    throw new Error(`the successor did not take over within ${humanizeDuration(timing.takeoverDeadline)}`);
  } catch (err) {
    deps.log(`abandoning the upgrade${successor ? `, removing ${successor.name}` : ""}`);
    await successor?.remove().catch((removeErr) => deps.log(`could not remove ${successor?.name}: ${removeErr}`));
    await queue.exitHandover();
    throw err;
  }
};
