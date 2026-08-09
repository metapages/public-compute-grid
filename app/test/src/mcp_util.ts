import { type ConsoleLogLine, fetchRobust } from "@metapages/compute-queues-shared";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";

const fetch = fetchRobust;

export const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
export const API_URL = Deno.env.get("API_URL") ||
  (QUEUE_ID === "local" ? "http://worker:8000" : "http://api1:8081");

export const MCP_URL = `${API_URL}/mcp`;

/**
 * Interface for MCP request/response
 */
export interface MCPRequest {
  jsonrpc: "2.0";
  id: string | number;
  method: string;
  params?: unknown;
}

export interface MCPResponse {
  jsonrpc: "2.0";
  id: string | number;
  result?: unknown;
  error?: {
    code: number;
    message: string;
    data?: unknown;
  };
}

/**
 * What `get_job_status` actually returns, per app/mcp/src/tools/get-job-status.ts:
 * `state` is nested under `status`, and the container's exit code is
 * `result.statusCode` (the raw docker field is `StatusCode`, renamed there).
 */
export interface McpJobStatus {
  success?: boolean;
  jobId?: string;
  viewUrl?: string;
  status?: {
    state?: string;
    worker?: string;
    finishedReason?: string;
    time?: number;
    queuedTime?: number;
  };
  result?: {
    statusCode?: number;
    logs?: ConsoleLogLine[];
    outputs?: Record<string, unknown>;
    duration?: number;
    isTimedOut?: boolean;
  } | null;
  [key: string]: unknown;
}

/**
 * Send an MCP request to the server and get the response
 */
export async function sendMCPRequest(request: MCPRequest): Promise<MCPResponse> {
  const response = await fetch(MCP_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      // Streamable HTTP replies with SSE unless it can negotiate plain JSON;
      // both are handled below, but asking for both is what the spec requires.
      "Accept": "application/json, text/event-stream",
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new Error(`MCP request failed: ${response.statusText}`);
  }

  return parseMCPResponseBody(await response.text());
}

/**
 * The Streamable HTTP transport frames each reply as SSE:
 *
 *   event: message
 *   data: {"result":…,"jsonrpc":"2.0","id":1}
 *
 * so the body is not JSON on its own. Unwrap the last `data:` payload when the
 * framing is present, and fall back to plain JSON for other deployments.
 */
export function parseMCPResponseBody(body: string): MCPResponse {
  const trimmed = body.trim();
  if (!trimmed.startsWith("event:") && !trimmed.startsWith("data:")) {
    return JSON.parse(trimmed);
  }
  const dataLines = trimmed
    .split("\n")
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice("data:".length).trim());
  if (dataLines.length === 0) {
    throw new Error(`No data frame in MCP SSE response: ${trimmed.slice(0, 200)}`);
  }
  return JSON.parse(dataLines[dataLines.length - 1]);
}

/**
 * Call an MCP tool with the given parameters
 */
export async function callMCPTool(
  toolName: string,
  args: Record<string, unknown>,
): Promise<CallToolResult> {
  const request: MCPRequest = {
    jsonrpc: "2.0",
    id: Date.now(),
    method: "tools/call",
    params: {
      name: toolName,
      arguments: args,
    },
  };

  const response = await sendMCPRequest(request);

  if (response.error) {
    throw new Error(`MCP tool call failed: ${response.error.message}`);
  }

  return response.result;
}

/**
 * List all available MCP tools
 */
export async function listMCPTools(): Promise<Array<{ name: string; description?: string }>> {
  const request: MCPRequest = {
    jsonrpc: "2.0",
    id: Date.now(),
    method: "tools/list",
  };

  const response = await sendMCPRequest(request);

  if (response.error) {
    throw new Error(`Failed to list tools: ${response.error.message}`);
  }

  const result = response.result as { tools?: Array<{ name: string; description?: string }> } | undefined;
  return result?.tools || [];
}

/**
 * Submit a job using MCP submit_job tool
 */
export async function mcpSubmitJob(args: {
  queue?: string;
  image?: string;
  gitRepo?: string;
  dockerfile?: string;
  buildContext?: string;
  command?: string;
  inputs?: Record<string, string>;
  env?: Record<string, string>;
  maxDuration?: string;
  namespace?: string;
}): Promise<{
  jobId: string;
  queue: string;
  viewUrl?: string;
  /** Summary of what was submitted, per app/mcp/src/tools/submit-job.ts. */
  definition: {
    image?: string;
    hasDockerfile: boolean;
    hasBuild: boolean;
    command?: string;
    filesCount: number;
    envCount: number;
    maxDuration?: string;
  };
  [key: string]: unknown;
}> {
  const result = await callMCPTool("submit_job", args);

  // Parse the result
  const content = result.content?.[0];
  if (!content || content.type !== "text") {
    throw new Error("Invalid response from submit_job tool");
  }

  const data = JSON.parse(content.text);

  if (!data.success) {
    throw new Error(data.error || "Job submission failed");
  }

  return data;
}

/**
 * Get job status using MCP get_job_status tool
 */
export async function mcpGetJobStatus(args: {
  queue: string;
  jobId: string;
}): Promise<McpJobStatus> {
  const result = await callMCPTool("get_job_status", args);

  const content = result.content?.[0];
  if (!content || content.type !== "text") {
    throw new Error("Invalid response from get_job_status tool");
  }

  return JSON.parse(content.text);
}

/**
 * List jobs using MCP list_jobs tool
 */
export async function mcpListJobs(args: {
  queue: string;
  filter?: string;
}): Promise<Record<string, unknown>> {
  const result = await callMCPTool("list_jobs", args);

  const content = result.content?.[0];
  if (!content || content.type !== "text") {
    throw new Error("Invalid response from list_jobs tool");
  }

  return JSON.parse(content.text);
}

/**
 * Cancel a job using MCP cancel_job tool
 */
export async function mcpCancelJob(args: {
  queue: string;
  jobId: string;
  namespace?: string;
}): Promise<Record<string, unknown>> {
  const result = await callMCPTool("cancel_job", args);

  const content = result.content?.[0];
  if (!content || content.type !== "text") {
    throw new Error("Invalid response from cancel_job tool");
  }

  return JSON.parse(content.text);
}

/**
 * Poll job status until it reaches a terminal state (finished or cancelled)
 * Returns the final job state
 */
export async function waitForJobCompletion(
  queue: string,
  jobId: string,
  timeoutMs: number = 120000, // 2 minutes default
  pollIntervalMs: number = 1000,
): Promise<McpJobStatus> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    const status = await mcpGetJobStatus({ queue, jobId });

    if (status.status?.state === "Finished" || status.status?.state === "Cancelled") {
      return status;
    }

    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  throw new Error(`Job ${jobId} did not complete within ${timeoutMs}ms`);
}

/**
 * Helper to extract result text from MCP tool response
 */
export function extractTextResult(result: CallToolResult): string {
  const content = result.content?.[0];
  if (!content || content.type !== "text") {
    throw new Error("No text content in result");
  }
  return content.text;
}

/**
 * Helper to parse JSON from MCP tool response
 */
export function parseJSONResult(result: CallToolResult): Record<string, unknown> {
  return JSON.parse(extractTextResult(result));
}
