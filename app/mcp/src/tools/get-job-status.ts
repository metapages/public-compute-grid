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

    const status = await client.getJobStatus(jobId);

    let result = null;
    if (includeResult && status.state === "Finished") {
      try {
        result = await client.getJobResult(jobId);
      } catch (resultError: unknown) {
        // Result might not be available yet, continue without it
        console.warn(`Could not fetch result for job ${jobId}:`, (resultError as Error).message);
      }
    }

    // The browser client needs a queue to connect to. Without one we can still
    // report status, just not link to it — better than emitting a link that
    // silently watches the wrong queue.
    const viewUrl = queue ? buildShortViewUrl(client.baseUrl, jobId, queue) : undefined;

    const response = {
      success: true,
      jobId,
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
