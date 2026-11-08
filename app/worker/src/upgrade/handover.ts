/**
 * The decisions of an --auto-update handover, as pure functions: what
 * autoUpdate.ts does with docker and the queue is built from these, and they
 * are what the tests pin down.
 */
import type { WorkerStatusResponse } from "@metapages/compute-queues-shared";

/** What docker says about the predecessor container, as the successor sees it. */
export type PredecessorState =
  /** docker answered 404: the container does not exist */
  | "gone"
  /** it exists and is not running (exited, restarting, dead) */
  | "stopped"
  | "running"
  /** docker did not answer (any error but a 404): nothing is known */
  | "unknown";

export const predecessorStateFrom = (
  result: { ok: true; info: { State?: { Running?: boolean } } } | { ok: false; error: unknown },
): PredecessorState => {
  if (!result.ok) {
    return (result.error as { statusCode?: number })?.statusCode === 404 ? "gone" : "unknown";
  }
  return result.info.State?.Running ? "running" : "stopped";
};

/**
 * Should the successor, in standby, take over now?
 * - the predecessor is gone or stopped: yes, nobody else is claiming.
 * - it is running: only once it reports "retiring" (it has stopped claiming
 *   and has no jobs left).
 * - docker did not answer: wait. A hiccup must never read as "the predecessor
 *   is gone", or two workers with one id would both claim jobs.
 */
export const takeoverDecision = (
  predecessor: PredecessorState,
  workers: WorkerStatusResponse[] | null,
  workerId: string,
): "take-over" | "wait" => {
  switch (predecessor) {
    case "gone":
    case "stopped":
      return "take-over";
    case "unknown":
      return "wait";
    case "running":
      return (workers ?? []).some((w) => w.id === workerId && w.handover === "retiring") ? "take-over" : "wait";
  }
};

/** Is the successor on the queue: this worker id, the new version, in standby? */
export const isSuccessorRegistered = (
  workers: WorkerStatusResponse[],
  workerId: string,
  version: string,
): boolean => workers.some((w) => w.id === workerId && w.version === version && w.handover === "standby");

/**
 * Every worker the queue's status reports: the ones on the instance that
 * answered (`localWorkers`) and the ones on every other instance
 * (`otherServers[*].workers`). On a multi-instance server the predecessor and
 * successor can be connected to different instances, so reading only
 * `localWorkers` would miss one of them.
 */
export const workersFromQueueStatus = (
  status:
    | {
      localWorkers?: Record<string, WorkerStatusResponse>;
      otherServers?: Record<string, { workers?: Record<string, WorkerStatusResponse> }>;
    }
    | null
    | undefined,
): WorkerStatusResponse[] => [
  ...Object.values(status?.localWorkers ?? {}),
  ...Object.values(status?.otherServers ?? {}).flatMap((server) => Object.values(server?.workers ?? {})),
];

/** The first retry after a failed upgrade, and the base it doubles from. */
export const RETRY_BASE = 5 * 60_000;

/**
 * When to try again after `failures` failed upgrades in a row: from a short
 * base, doubling, never later than the regular check would have come anyway
 * (or the base, if checks are more frequent than that). A failed pull while a
 * release's image is still being pushed recovers in minutes, not hours.
 */
export const retryDelay = (intervalMs: number, failures: number): number => {
  const base = Math.min(RETRY_BASE, intervalMs);
  const ceiling = Math.max(intervalMs, RETRY_BASE);
  return Math.min(base * 2 ** Math.max(failures - 1, 0), ceiling);
};

/**
 * The successor's temporary container name: the predecessor's, with the new
 * version appended. A version already on the end (a successor whose rename
 * failed) is replaced, not stacked: `w-0.54.88` → `w-0.54.89`.
 */
export const successorContainerName = (predecessorName: string, version: string): string => {
  const base = predecessorName.replace(/^\//, "").replace(/(-v?\d+\.\d+\.\d+)+$/, "");
  return `${base}-${version}`.replace(/[^a-zA-Z0-9_.-]/g, "-");
};
