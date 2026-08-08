import { CallToolRequest, CallToolResult, Tool } from "@modelcontextprotocol/sdk/types.js";
import { WorkerMetapageClient } from "../client.ts";

export const listJobsTool: Tool = {
  name: "list_jobs",
  description: "List all jobs in a queue with their current status and basic information.",
  inputSchema: {
    type: "object",
    properties: {
      queue: {
        type: "string",
        description: "The queue name to list jobs from",
        default: "public1",
      },
      limit: {
        type: "number",
        description: "Maximum number of jobs to return",
        default: 50,
        minimum: 1,
        maximum: 200,
      },
      state: {
        type: "string",
        description: "Filter by job state: 'Queued', 'Running', 'Finished'",
        enum: ["Queued", "Running", "Finished"],
      },
    },
    required: [],
  },
};

export async function handleListJobs(
  request: CallToolRequest,
  client: WorkerMetapageClient,
): Promise<CallToolResult> {
  try {
    const args = request.params.arguments as any;
    const { queue = "public1", limit = 50, state } = args;

    const jobsData = await client.listJobs(queue);

    // Extract jobs from the response
    let jobs: any[] = [];
    if (jobsData.jobs) {
      jobs = Object.entries(jobsData.jobs).map(([jobId, job]: [string, any]) => ({
        jobId,
        state: job.state,
        worker: job.worker,
        finishedReason: job.finishedReason,
        time: job.time,
        queuedTime: job.queuedTime,
        namespaces: job.namespaces,
      }));
    }

    // Filter by state if specified
    if (state) {
      jobs = jobs.filter((job: any) => job.state === state);
    }

    // Apply limit
    jobs = jobs.slice(0, limit);

    const response = {
      success: true,
      queue,
      totalJobs: jobs.length,
      filters: { state, limit },
      jobs,
      workers: jobsData.workers || [],
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
              message: `Failed to list jobs for queue: ${request.params.arguments?.queue}`,
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
