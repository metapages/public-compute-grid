import type { CallToolRequest, CallToolResult, Tool } from "@modelcontextprotocol/sdk/types.js";

import { followJobStream, type JobStreamEvent, WorkerMetapageClient } from "../client.ts";
import { buildShortViewUrl } from "../util/view-url.ts";

export const followJobTool: Tool = {
  name: "follow_job",
  description: "Follow a running job and stream its build and run logs back live, then return the final result. " +
    "Logs arrive as progress notifications while the job runs, so you see output as it happens rather than " +
    "waiting for the job to end. Use this instead of polling get_job_status in a loop. " +
    "Returns a `viewUrl` that opens the job in the browser — always show that link to the user.",
  inputSchema: {
    type: "object",
    properties: {
      jobId: {
        type: "string",
        description: "The job ID to follow (from submit_job)",
      },
      queue: {
        type: "string",
        description: "The queue the job was submitted to",
      },
      timeoutSeconds: {
        type: "number",
        description: "Give up after this long. The job keeps running; only the following stops.",
        default: 600,
      },
      includeBuildLogs: {
        type: "boolean",
        description: "Stream image build/pull logs as well as the container's own output",
        default: true,
      },
    },
    required: ["jobId", "queue"],
  },
};

/**
 * MCP streams by sending `notifications/progress` messages that carry the
 * client's `progressToken`. A client only receives them if it supplied that
 * token in `_meta` on the call — so when it is absent we still run to
 * completion and return everything at the end, just without the live view.
 *
 * This is the transport-agnostic half of live logs: it works over stdio and
 * over Streamable HTTP, because the SDK routes the notification through
 * whichever transport the session is using.
 */
export async function handleFollowJob(
  request: CallToolRequest,
  client: WorkerMetapageClient,
  // The SDK hands this to request handlers; it is how a handler talks back
  // mid-call. Typed loosely to avoid coupling to an SDK internal.
  extra?: {
    sendNotification?: (notification: unknown) => Promise<void>;
    signal?: AbortSignal;
  },
): Promise<CallToolResult> {
  const args = request.params.arguments as {
    jobId: string;
    queue: string;
    timeoutSeconds?: number;
    includeBuildLogs?: boolean;
  };
  const { jobId, queue, timeoutSeconds = 600, includeBuildLogs = true } = args;

  const progressToken = request.params._meta?.progressToken;
  const canStream = progressToken !== undefined && typeof extra?.sendNotification === "function";

  const collected: string[] = [];
  let lineCount = 0;

  // Stop following after the timeout without killing the job — the caller can
  // re-follow or fetch results later.
  const timeoutController = new AbortController();
  const timer = setTimeout(() => timeoutController.abort(), timeoutSeconds * 1000);
  const signal = extra?.signal ? anySignal([extra.signal, timeoutController.signal]) : timeoutController.signal;

  const onEvent = async (event: JobStreamEvent) => {
    let message: string | undefined;

    if (event.kind === "build-log" || event.kind === "run-log") {
      if (event.kind === "build-log" && !includeBuildLogs) return;
      const prefix = event.kind === "build-log" ? "[build] " : "";
      for (const line of event.lines ?? []) {
        collected.push(`${prefix}${line}`);
      }
      lineCount += event.lines?.length ?? 0;
      message = (event.lines ?? []).map((l) => `${prefix}${l}`).join("\n");
    } else if (event.kind === "state") {
      message = `▸ ${event.state}${event.reason ? ` (${event.reason})` : ""}`;
      collected.push(message);
    }

    if (!message || !canStream) return;
    await extra!.sendNotification!({
      method: "notifications/progress",
      params: {
        progressToken,
        // The job's length is unknowable up front, so `progress` is a
        // monotonically rising line count with no `total` — which the spec
        // allows and clients render as an indeterminate stream.
        progress: lineCount,
        message,
      },
    });
  };

  let outcome: { state: string; reason?: string };
  let timedOut = false;
  try {
    outcome = await followJobStream(client.baseUrl, queue, jobId, onEvent, signal);
  } catch (err) {
    if (timeoutController.signal.aborted) {
      timedOut = true;
      outcome = { state: "Following stopped", reason: `no result within ${timeoutSeconds}s` };
    } else {
      throw err;
    }
  } finally {
    clearTimeout(timer);
  }

  // Two separate verdicts: finishedReason is whether the JOB completed,
  // StatusCode is whether the PROGRAM succeeded. A crashed container still
  // reports finishedReason "Success".
  let result: Record<string, unknown> | undefined;
  if (!timedOut) {
    try {
      result = await client.getJobRunResult(queue, jobId);
    } catch {
      result = undefined;
    }
  }

  const exitCode = result?.StatusCode as number | undefined;

  const viewUrl = buildShortViewUrl(client.baseUrl, jobId, queue);
  const resultUrl = `${client.baseUrl.replace(/\/$/, "")}/q/${encodeURIComponent(queue)}/j/${jobId}/result.json`;

  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(
          {
            jobId,
            queue,
            viewUrl,
            resultUrl,
            shareWithUser: `Show the user this link so they can see the logs and outputs themselves: ${viewUrl}`,
            state: outcome.state,
            finishedReason: outcome.reason,
            exitCode,
            programSucceeded: exitCode === undefined ? undefined : exitCode === 0,
            durationMs: result?.duration,
            outputFiles: Object.keys((result?.outputs as Record<string, unknown>) ?? {}),
            streamedLive: canStream,
            logLineCount: collected.length,
            logs: collected,
            ...(timedOut ? { note: `Stopped following after ${timeoutSeconds}s; the job is still running.` } : {}),
          },
          null,
          2,
        ),
      },
    ],
  };
}

/** Deno has no AbortSignal.any in every supported version; this is equivalent. */
function anySignal(signals: AbortSignal[]): AbortSignal {
  const controller = new AbortController();
  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort();
      break;
    }
    signal.addEventListener("abort", () => controller.abort(), { once: true });
  }
  return controller.signal;
}
