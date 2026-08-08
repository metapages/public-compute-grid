import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import type { CallToolRequest, ReadResourceRequest } from "@modelcontextprotocol/sdk/types.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

import { WorkerMetapageClient } from "./client.ts";
import {
  cancelJobTool,
  followJobTool,
  getJobStatusTool,
  handleCancelJob,
  handleFollowJob,
  handleGetJobStatus,
  handleListJobs,
  handleSubmitJob,
  handleUploadFile,
  listJobsTool,
  submitJobTool,
  uploadFileTool,
} from "./tools/index.ts";

/**
 * Build the MCP server. Transport-agnostic on purpose: the same server is
 * served over stdio (src/server.ts) and over Streamable HTTP (mounted by the
 * API and the local-mode worker), so both get exactly the same tools — and,
 * importantly, the same live-log streaming, because progress notifications are
 * routed by whichever transport the session is using.
 */
export const createMcpServer = (opts?: {
  /** API to talk to. Defaults to $WORKER_METAPAGE_URL, then production. */
  baseUrl?: string;
}): Server => {
  const baseUrl = opts?.baseUrl ||
    Deno.env.get("WORKER_METAPAGE_URL") ||
    "https://container.mtfm.io";
  const client = new WorkerMetapageClient(baseUrl);

  const server = new Server(
    { name: "worker-metapage-io", version: "1.0.0" },
    {
      capabilities: {
        tools: {},
        resources: {},
        // Declares that this server emits progress notifications, which is
        // what carries live logs to the client.
        logging: {},
      },
    },
  );

  server.setRequestHandler(ListToolsRequestSchema, () => ({
    tools: [
      submitJobTool,
      followJobTool,
      getJobStatusTool,
      listJobsTool,
      cancelJobTool,
      uploadFileTool,
    ],
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request: CallToolRequest, extra: unknown) => {
    switch (request.params.name) {
      case "submit_job":
        return await handleSubmitJob(request, client);
      case "follow_job":
        // `extra` carries sendNotification — the channel live logs travel on.
        return await handleFollowJob(request, client, extra as Parameters<typeof handleFollowJob>[2]);
      case "get_job_status":
        return await handleGetJobStatus(request, client);
      case "list_jobs":
        return await handleListJobs(request, client);
      case "cancel_job":
        return await handleCancelJob(request, client);
      case "upload_file":
        return await handleUploadFile(request, client);
      default:
        throw new Error(`Unknown tool: ${request.params.name}`);
    }
  });

  server.setRequestHandler(ListResourcesRequestSchema, () => ({
    resources: [
      {
        uri: "queue://public1/jobs",
        name: "Public Queue Jobs",
        description: "List of jobs in the public1 queue",
        mimeType: "application/json",
      },
    ],
  }));

  server.setRequestHandler(ReadResourceRequestSchema, async (request: ReadResourceRequest) => {
    const { uri } = request.params;
    if (uri.startsWith("queue://")) {
      const parts = uri.replace("queue://", "").split("/");
      const queueName = parts[0];
      if (parts[1] === "jobs") {
        const jobs = await client.listJobs(queueName);
        return {
          contents: [
            { uri, mimeType: "application/json", text: JSON.stringify(jobs, null, 2) },
          ],
        };
      }
    }
    throw new Error(`Unknown resource: ${uri}`);
  });

  return server;
};
