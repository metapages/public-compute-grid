import { CallToolRequest, CallToolResult, Tool } from "@modelcontextprotocol/sdk/types.js";
import { WorkerMetapageClient } from "../client.ts";
import { buildShortViewUrl } from "../util/view-url.ts";

export const getJobStatusTool: Tool = {
  name: "get_job_status",
  description:
    "Get the status and details of a job by its ID. Returns job state, progress, and results if completed, " +
    "plus a `viewUrl` that opens the job in the browser — always show that link to the user.",
  inputSchema: {
    type: "object",
    properties: {
      jobId: {
        type: "string",
        description: "The unique job ID to check status for",
      },
      queue: {
        type: "string",
        description: "The queue the job was submitted to. Only needed to build the browser view URL.",
      },
      includeResult: {
        type: "boolean",
        description: "Whether to include job results if the job is finished",
        default: true,
      },
    },
    required: ["jobId"],
  },
};

export async function handleGetJobStatus(
  request: CallToolRequest,
  client: WorkerMetapageClient,
): Promise<CallToolResult> {
  try {
    const args = request.params.arguments as any;
    const { jobId, queue, includeResult = true } = args;

    // Live state (Queued/Running) only exists in the queue's job map — the
    // job's own endpoints carry a definition and, once finished, a result, but
    // no state. `/j/<id>.json` in particular returns {definition, results},
    // which is why reading `.state` off it always yielded undefined.
    // Both lookups can fail for reasons that say nothing about the job: the API
    // is unreachable, overloaded, or returned an error. Those failures used to
    // be swallowed and reported as a job with no state, which reads exactly
    // like a job that is not there — the two have opposite fixes, so keep them
    // apart and say which happened.
    const lookupFailures: string[] = [];

    let job: Record<string, any> | undefined;
    if (queue) {
      try {
        const jobs = (await client.listJobs(queue))?.data as Record<string, any> | undefined;
        job = jobs?.[jobId];
      } catch (listError: unknown) {
        const message = (listError as Error).message;
        console.warn(`Could not list queue ${queue}:`, message);
        lookupFailures.push(`listing queue ${queue}: ${message}`);
      }
    }

    // Finished jobs are also persisted with their state, so this covers both a
    // job that has aged out of the queue map and the no-queue-argument case.
    let finished: Record<string, any> | undefined;
    if (!job || job.state === "Finished") {
      try {
        finished = (await client.getJobResult(jobId))?.data as Record<string, any> | undefined;
      } catch (resultError: unknown) {
        const message = (resultError as Error).message;
        console.warn(`Could not fetch result for job ${jobId}:`, message);
        lookupFailures.push(`fetching the result of ${jobId}: ${message}`);
      }
    }

    // Nothing found *and* a lookup failed: the state is unknown, not absent.
    if (!job && !finished && lookupFailures.length > 0) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(
              {
                success: false,
                jobId,
                queue,
                error: "The job API could not be reached, so this job's state is unknown",
                hint: "This is not the same as the job being absent — retry, and check the API is up and reachable " +
                  `at ${client.baseUrl}`,
                lookupFailures,
              },
              null,
              2,
            ),
          },
        ],
        isError: true,
      };
    }

    const status = job ?? finished ?? {};
    // Reached the API and it has no record of this job at all
    const found = !!job || !!finished;
    const result = includeResult ? finished?.finished?.result : undefined;

    // The browser client needs a queue to connect to. Without one we can still
    // report status, just not link to it — better than emitting a link that
    // silently watches the wrong queue.
    const viewUrl = queue ? buildShortViewUrl(client.publicUrl, jobId, queue) : undefined;

    const response = {
      success: true,
      jobId,
      found,
      ...(found ? {} : {
        note: queue
          ? `No job ${jobId} on queue ${queue}, and no stored result: it was never submitted there, or it was ` +
            `removed (a later job in the same namespace replaces the one before it) or has expired.`
          : `No stored result for job ${jobId}. Pass the \`queue\` argument to also look for it on a queue.`,
      }),
      viewUrl,
      shareWithUser: viewUrl
        ? `Show the user this link so they can see the job themselves: ${viewUrl}`
        : "No browser link: pass the `queue` argument to get one.",
      status: {
        state: status.state,
        worker: status.worker,
        finishedReason: status.finishedReason,
        time: status.time,
        queuedTime: status.queuedTime,
      },
      result: result
        ? {
          outputs: result.outputs,
          logs: result.logs,
          statusCode: result.StatusCode,
          duration: result.duration,
          isTimedOut: result.isTimedOut,
        }
        : null,
    };

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(response, null, 2),
        },
      ],
    };
  } catch (error: unknown) {
    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(
            {
              success: false,
              error: (error as Error).message,
              message: `Failed to get status for job: ${request.params.arguments?.jobId}`,
            },
            null,
            2,
          ),
        },
      ],
      isError: true,
    };
  }
}
