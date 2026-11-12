import type { Context } from "hono";

import { db } from "@/db/db.ts";
import { userJobQueues } from "@/docker-jobs/ApiDockerJobQueue.ts";
import {
  type ConsoleLogLine,
  DEFAULT_QUEUE_DATA_DIRECTORY,
  getLogStorage,
  type LogKind,
  type LogStorage,
  readStoredLogs,
  serveJobLogs,
} from "@metapages/compute-queues-shared";

/**
 * Where the queues store job logs: the bucket (on Cloudflare, R2), so any
 * instance or isolate reads them, wherever the queue holding the worker is.
 * Chosen on first use: Workers only populate the environment once the module
 * graph is loaded.
 */
let storage: LogStorage | undefined;
const logStorage = (): LogStorage => (storage ??= getLogStorage(DEFAULT_QUEUE_DATA_DIRECTORY));

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
  // The queue holding the job's worker also has the lines not written yet:
  // read through it when it is here (never create it — this is a read-only
  // endpoint), otherwise straight from storage.
  const jobQueue = queueName ? userJobQueues[queueName] : undefined;
  if (jobQueue) {
    return kind === "build" ? await jobQueue.getCurrentBuildLogs(jobId) : await jobQueue.getCurrentRunLogs(jobId);
  }
  const lines = await readStoredLogs(logStorage(), jobId, kind);
  // Jobs finished before logs were stored in storage have them in the db
  return lines.length ? lines : (kind === "build" ? await db.getBuildLogs(jobId) : await db.getRunLogs(jobId)) ?? [];
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

/**
 * A job's whole log, live or finished, as one JSON array of lines:
 *
 *   GET /j/:jobId/logs.build.json
 *   GET /j/:jobId/logs.run.json
 *
 * Read from storage, where a live job's log is several chunks and a finished
 * one a single file: they are streamed as one array either way.
 */
const makeLogFileHandler = (kind: LogKind) => {
  return async (c: Context) => {
    try {
      const jobId: string | undefined = c.req.param("jobId");
      if (!jobId) {
        c.status(404);
        return c.json({ error: "No job provided" });
      }
      return await serveJobLogs({ storage: logStorage(), db, jobId, kind });
    } catch (err) {
      console.error(`Error getting ${kind} logs`, err);
      return c.text((err as Error).message, 500);
    }
  };
};

export const getBuildLogFileHandler = makeLogFileHandler("build");
export const getRunLogFileHandler = makeLogFileHandler("run");
