import type { Context } from "hono";
import { streamSSE } from "hono/streaming";

import type { BaseDockerJobQueue } from "@shared/jobqueue.ts";
import { type ConsoleLogLine, DockerJobState } from "@shared/types.ts";

/**
 * Server-Sent-Events handler for following a job. Shared by the API server and
 * the local-mode worker (which serves the same API); the two differ only in how
 * a queue is looked up, so the caller injects a resolver.
 *
 * This exists because the websocket protocol is stateful, per-queue, and
 * assumes a long-lived client. A script or agent following one job wants a
 * single request it can read until the job ends — that is this.
 *
 * `GET /q/:queue/j/:jobId/stream` emits:
 *
 *   event: build-log
 *   data: {"lines":[[text, timestamp, isStderr?], ...], "cursor": N}
 *
 *   event: run-log
 *   data: {"lines":[[text, timestamp, isStderr?], ...], "cursor": N}
 *
 *   event: state
 *   data: {"state":"Queued"|"Running"|"Finished", "reason"?: "..."}
 *
 *   event: final
 *   data: {"state":"Finished", "reason":"Success"|"Error"|...}
 *
 * Everything already known is replayed on connect, so opening the stream
 * against an already-finished job yields the full history and `final` in one
 * short read. `cursor` is the running count of lines emitted for that kind.
 *
 * The server emits `final` and closes once the job reaches a terminal state.
 */
export interface StreamHandlerOpts {
  resolveQueue: (c: Context) => Promise<BaseDockerJobQueue> | BaseDockerJobQueue;
  /** How often to re-check for new lines/state. 500ms reads as "live" to a human. */
  pollIntervalMs?: number;
  /** Upper bound on stream lifetime so a stuck job cannot hold a connection forever. */
  maxDurationMs?: number;
}

export function makeStreamHandler(opts: StreamHandlerOpts): (c: Context) => Promise<Response> {
  const pollIntervalMs = opts.pollIntervalMs ?? 500;
  const maxDurationMs = opts.maxDurationMs ?? 30 * 60 * 1000;

  return async (c: Context) => {
    const jobId = c.req.param("jobId");
    if (!jobId) {
      c.status(404);
      return c.json({ error: "No jobId specified" });
    }

    const queue = await opts.resolveQueue(c);

    return streamSSE(c, async (stream) => {
      let buildCursor = 0;
      let runCursor = 0;
      let lastState: string | undefined;
      // Reason for a job whose live state has already aged out of
      // queue.state.jobs, captured so `final` doesn't report "Unknown".
      let resolvedReason: string | undefined;
      const startedAt = Date.now();
      // Hono's StreamingApi exposes onAbort(), not an .aborted flag; track it
      // ourselves so the poll loop exits promptly on client disconnect.
      let aborted = false;
      stream.onAbort(() => {
        aborted = true;
      });

      const sendLogs = async (kind: "build" | "run"): Promise<void> => {
        const all: ConsoleLogLine[] = kind === "build"
          ? await queue.getCurrentBuildLogs(jobId)
          : await queue.getCurrentRunLogs(jobId);
        const cursor = kind === "build" ? buildCursor : runCursor;
        if (all.length <= cursor) {
          return;
        }
        const lines = all.slice(cursor);
        const newCursor = all.length;
        if (kind === "build") {
          buildCursor = newCursor;
        } else {
          runCursor = newCursor;
        }
        await stream.writeSSE({
          event: `${kind}-log`,
          data: JSON.stringify({ lines, cursor: newCursor }),
        });
      };

      const sendStateIfChanged = async (): Promise<boolean> => {
        const job = queue.getCurrentJobState(jobId);
        if (job) {
          const state = job.state;
          if (state === lastState) {
            return state === DockerJobState.Finished;
          }
          lastState = state;
          const reason = state === DockerJobState.Finished ? job.finishedReason : undefined;
          if (reason) {
            resolvedReason = reason;
          }
          await stream.writeSSE({
            event: "state",
            data: JSON.stringify({ state, reason }),
          });
          return state === DockerJobState.Finished;
        }

        // Not in the in-memory map: either it never existed, or it aged out.
        // Cross-check the persisted result so a stream opened long after the
        // fact still terminates rather than polling until maxDuration.
        const persistedFinished = await queue.db.getFinishedJob(jobId);
        if (persistedFinished && persistedFinished.state === DockerJobState.Finished) {
          resolvedReason = persistedFinished.finishedReason;
          if (lastState !== DockerJobState.Finished) {
            lastState = DockerJobState.Finished;
            await stream.writeSSE({
              event: "state",
              data: JSON.stringify({
                state: DockerJobState.Finished,
                reason: persistedFinished.finishedReason,
              }),
            });
          }
          return true;
        }
        return false;
      };

      while (!aborted) {
        await sendLogs("build");
        await sendLogs("run");
        const isFinal = await sendStateIfChanged();

        if (isFinal) {
          // One last drain: logs can land after the state transition, and the
          // buffer flush moves them from memory to the db mid-poll.
          await sendLogs("build");
          await sendLogs("run");
          await stream.writeSSE({
            event: "final",
            data: JSON.stringify({
              state: DockerJobState.Finished,
              reason: resolvedReason ?? "Unknown",
            }),
          });
          return;
        }

        if (Date.now() - startedAt > maxDurationMs) {
          await stream.writeSSE({
            event: "final",
            data: JSON.stringify({ state: "Timeout", reason: "Stream max duration exceeded" }),
          });
          return;
        }

        await stream.sleep(pollIntervalMs);
      }
    });
  };
}
