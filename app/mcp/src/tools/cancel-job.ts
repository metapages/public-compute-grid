import { CallToolRequest, CallToolResult, Tool } from "@modelcontextprotocol/sdk/types.js";
import { WorkerMetapageClient } from "../client.ts";

export const cancelJobTool: Tool = {
  name: "cancel_job",
  description: "Cancel a running or queued job in a specific queue.",
  inputSchema: {
    type: "object",
    properties: {
      queue: {
        type: "string",
        description: "The queue name where the job is located",
        default: "public1",
      },
      jobId: {
        type: "string",
        description: "The unique job ID to cancel",
      },
      namespace: {
        type: "string",
        description:
          "Cancel only within this namespace. Defaults to '*', which cancels the job in every namespace it belongs to.",
        default: "*",
      },
    },
    required: ["jobId"],
  },
};

export async function handleCancelJob(
  request: CallToolRequest,
  client: WorkerMetapageClient,
): Promise<CallToolResult> {
  try {
    const args = request.params.arguments as any;
    const { queue = "public1", jobId, namespace = "*" } = args;

    await client.cancelJob(queue, jobId, namespace);

    const response = {
      success: true,
      jobId,
      queue,
      message: `Job ${jobId} has been cancelled in queue '${queue}'`,
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
              message:
                `Failed to cancel job ${request.params.arguments?.jobId} in queue ${request.params.arguments?.queue}`,
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
