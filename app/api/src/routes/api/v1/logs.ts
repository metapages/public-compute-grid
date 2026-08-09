import type { Context } from "hono";

import { db } from "@/db/db.ts";
import { userJobQueues } from "@/docker-jobs/ApiDockerJobQueue.ts";
import type { ConsoleLogLine } from "@metapages/compute-queues-shared";

type LogKind = "build" | "run";

/**
 * Build and run logs for a job, as a cursor-paged slice.
 *
 *   GET /q/:queue/j/:jobId/build-logs.json?since=N
 *   GET /q/:queue/j/:jobId/run-logs.json?since=N
 *
 * Poll with `since=<previous nextCursor>` to follow a running job without
 * re-reading lines you already have; `isFinal: true` means the job is done and
 * no more lines are coming, so a poller can stop. For live following prefer the
 * SSE endpoint (/q/:queue/j/:jobId/stream) — this exists for callers that
 * cannot hold a connection open.
 *
 * Build logs cover `docker build`, image pull/push and repo cloning; run logs
 * are the container's own stdout/stderr. They are kept apart because a build
 * that failed and a program that failed are different problems.
 */
const getLogs = async (
  queueName: string | undefined,
  jobId: string,
  kind: LogKind,
): Promise<ConsoleLogLine[]> => {
  // A live job's logs only exist in the queue's in-memory buffer until it
  // finishes. Read through the queue when we have one (never create it — this
  // is a read-only endpoint), otherwise go straight to the persisted store.
  const jobQueue = queueName ? userJobQueues[queueName] : undefined;
  if (jobQueue) {
    return kind === "build" ? await jobQueue.getCurrentBuildLogs(jobId) : await jobQueue.getCurrentRunLogs(jobId);
  }
  return (kind === "build" ? await db.getBuildLogs(jobId) : await db.getRunLogs(jobId)) ?? [];
};

const makeLogsHandler = (kind: LogKind) => {
  return async (c: Context) => {
    try {
      const jobId: string | undefined = c.req.param("jobId");
      if (!jobId) {
        c.status(404);
        return c.json({ error: "No job provided" });
      }
      const sinceRaw = c.req.query("since");
      const since = sinceRaw ? Number(sinceRaw) : 0;
      if (Number.isNaN(since)) {
        c.status(400);
        return c.json({ error: "`since` must be a number" });
      }

      const lines = await getLogs(c.req.param("queue"), jobId, kind);
      const isFinal = !!(await db.getFinishedJob(jobId));
      const safeSince = Math.max(0, Math.floor(since));

      return c.json({
        data: lines.slice(safeSince),
        sliceStart: safeSince,
        nextCursor: lines.length,
        isFinal,
      });
    } catch (err) {
      console.error(`Error getting ${kind} logs`, err);
      return c.text((err as Error).message, 500);
    }
  };
};

export const getBuildLogsHandler = makeLogsHandler("build");
export const getRunLogsHandler = makeLogsHandler("run");
