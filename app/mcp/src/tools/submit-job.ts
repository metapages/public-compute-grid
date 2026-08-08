import type { CallToolRequest, CallToolResult, Tool } from "@modelcontextprotocol/sdk/types.js";
import type { WorkerMetapageClient } from "../client.ts";
import {
  DataRefType,
  type DockerJobDefinitionInputRefs,
  type EnqueueJob,
  type InputsRefs,
  shaDockerJob,
} from "@metapages/compute-queues-shared";
import { nanoid } from "nanoid";
import { buildShortViewUrl, buildViewUrl } from "../util/view-url.ts";

export const submitJobTool: Tool = {
  name: "submit_job",
  description:
    "Submit a containerized job to a queue for execution. Supports Docker images and git repositories with builds. " +
    "Returns the job ID plus a `viewUrl` that opens the job in the browser — always show that link to the user, " +
    "otherwise the job you just ran is invisible to them.",
  inputSchema: {
    type: "object",
    properties: {
      queue: {
        type: "string",
        description: "The queue name to submit the job to (e.g., 'dev', 'public1')",
        default: "public1",
      },
      image: {
        type: "string",
        description: "Docker image to run (e.g., 'python:3.11', 'alpine:latest') - mutually exclusive with gitRepo",
      },
      gitRepo: {
        type: "string",
        description:
          "Git repository URL to clone and build (e.g., 'https://github.com/user/repo') - mutually exclusive with image",
      },
      dockerfile: {
        type: "string",
        description: "Inline Dockerfile content for building (used with gitRepo or standalone)",
      },
      buildContext: {
        type: "string",
        description:
          "Sub-directory within the git repository to build from (default: repository root). Only meaningful with gitRepo.",
      },
      command: {
        type: "string",
        description: "Command to execute in the container",
        default: "echo 'Hello World'",
      },
      inputs: {
        type: "object",
        description: "Input files as key-value pairs where key is filename and value is file content",
        additionalProperties: {
          type: "string",
        },
        default: {},
      },
      env: {
        type: "object",
        description: "Environment variables as key-value pairs",
        additionalProperties: {
          type: "string",
        },
        default: {},
      },
      maxDuration: {
        type: "string",
        description: "Maximum job duration (e.g., '10m', '1h', '30s')",
        default: "30m",
      },
      namespace: {
        type: "string",
        description: "Optional namespace to group related jobs",
        default: "dev",
      },
    },
    required: [],
  },
};

export async function handleSubmitJob(
  request: CallToolRequest,
  client: WorkerMetapageClient,
): Promise<CallToolResult> {
  try {
    const args = request.params.arguments as any;
    const {
      queue = "public1",
      image,
      gitRepo,
      dockerfile,
      buildContext,
      command = "echo 'Hello World'",
      inputs = {},
      env = {},
      maxDuration = "30m",
      namespace = "dev",
    } = args;

    // Validate mutually exclusive options
    if (image && gitRepo) {
      throw new Error("Cannot specify both 'image' and 'gitRepo' - choose one");
    }
    if (!image && !gitRepo && !dockerfile) {
      throw new Error("Must specify either 'image', 'gitRepo', or 'dockerfile'");
    }

    // Convert inputs to DataRef format
    const inputsRefs: InputsRefs | undefined = inputs
      ? Object.fromEntries(
        Object.entries(inputs).map(([key, value]) => [
          key,
          {
            // A DataRef value is a string; stringify anything else so a
            // non-string input cannot silently produce a malformed ref.
            value: typeof value === "string" ? value : JSON.stringify(value),
            type: DataRefType.utf8,
          },
        ]),
      )
      : undefined;

    // Build job definition following DockerJobDefinitionInputRefs structure
    const jobDefinition: DockerJobDefinitionInputRefs = {
      command,
      inputs: inputsRefs,
      env,
      maxDuration,
    };

    // Handle different image sources.
    // `build.context` is a *downloadable* location (git URL / tarball) that the
    // worker fetches, not a local path — sending "." makes the worker fail with
    // "Unsupported download link: .". `build.buildContext` is the sub-directory
    // within that download to build from.
    if (gitRepo) {
      jobDefinition.build = {
        context: gitRepo,
        ...(buildContext && buildContext !== "." ? { buildContext } : {}),
        ...(dockerfile ? { dockerfile } : {}),
      };
    } else if (dockerfile) {
      // Inline Dockerfile with no downloaded context. An `image` alongside it is
      // ignored by the worker: when `build` is present the image name is derived
      // from the build sha.
      jobDefinition.build = { dockerfile };
    } else if (image) {
      // Regular Docker image
      jobDefinition.image = image;
    }

    // Generate job ID
    const jobId = await shaDockerJob(jobDefinition);

    // Create EnqueueJob object
    const enqueuedJob: EnqueueJob = {
      id: jobId,
      definition: jobDefinition,
      control: {
        namespace,
      },
    };

    const result = await client.submitJob(queue, enqueuedJob);

    // Short form for humans; the embedded form is the fallback for anywhere the
    // definition has not been persisted, and stays useful because it is
    // self-contained and survives the job data expiring.
    const viewUrl = buildShortViewUrl(client.baseUrl, result.jobId, queue);
    const selfContainedUrl = buildViewUrl(client.baseUrl, jobDefinition, queue);
    const resultUrl = `${client.baseUrl.replace(/\/$/, "")}/q/${
      encodeURIComponent(queue)
    }/j/${result.jobId}/result.json`;

    return {
      content: [
        {
          type: "text",
          text: JSON.stringify(
            {
              success: true,
              jobId: result.jobId,
              queue,
              viewUrl,
              selfContainedUrl,
              resultUrl,
              definition: {
                image: jobDefinition.image,
                hasDockerfile: !!dockerfile,
                hasBuild: !!jobDefinition.build,
                command: jobDefinition.command,
                filesCount: Object.keys(inputs).length,
                envCount: Object.keys(env).length,
                maxDuration: jobDefinition.maxDuration,
              },
              namespace,
              message: `Job submitted successfully to queue '${queue}' with ID: ${result.jobId}`,
              shareWithUser: `Show the user this link so they can watch the job and inspect its outputs: ${viewUrl}`,
              nextStep: "Use follow_job to stream logs live, or get_job_status to check on it later",
            },
            null,
            2,
          ),
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
              message: "Failed to submit job",
              troubleshooting: {
                commonIssues: [
                  "Check that either 'image', 'gitRepo', or 'dockerfile' is provided",
                  "Ensure queue name is valid",
                  "Verify git repository URL is accessible if using gitRepo",
                ],
              },
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
