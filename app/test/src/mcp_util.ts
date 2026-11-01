import { type ConsoleLogLine, fetchRobust } from "@metapages/compute-queues-shared";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { queueJobs } from "./util.ts";

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
  /** false when the API answered but has no record of the job */
  found?: boolean;
  /** set when the API itself could not be reached, so the state is unknown */
  error?: string;
  lookupFailures?: string[];
  note?: string;
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

/** So a timeout can report what is in the job's namespace now */
const submittedNamespaces = new Map<string, string>();

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
  // jobId = sha256(definition), and the worker's data directory outlives a test
  // run, so an identical definition resolves to a previous run's job. Once that
  // job's record has been pruned the id is a ghost: not on the queue, and with
  // no stored result — get_job_status then reports nothing and the test hangs
  // or fails. Give every submission a nonce, the same way the functional tests
  // randomise their sleeps.
  const result = await callMCPTool("submit_job", {
    ...args,
    env: { ...(args.env ?? {}), MCP_TEST_NONCE: crypto.randomUUID() },
  });

  // Parse the result
  const content = result.content?.[0];
  if (!content || content.type !== "text") {
    throw new Error("Invalid response from submit_job tool");
  }

  const data = JSON.parse(content.text);

  if (!data.success) {
    throw new Error(data.error || "Job submission failed");
  }

  if (args.namespace) {
    submittedNamespaces.set(data.jobId, args.namespace);
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

/** What a wait for a job saw, up to the moment it gave up */
export interface JobWaitObservation {
  queue: string;
  polls: number;
  unreachablePolls: number;
  lastFailure?: string;
  /** The final poll could not reach the API, so the job's final state is unknown */
  lastPollUnreachable: boolean;
  /** State on the final poll */
  lastState?: string;
  /** The API returned the job on the final poll */
  lastFound?: boolean;
  /** Polls that found the job, and the latest state any of them reported */
  foundPolls: number;
  lastSeenState?: string;
  /** Consecutive polls at the end that reached the API and did not find the job */
  absentPollsAtEnd: number;
  namespace?: string;
  /** The queue when the wait gave up, or why it could not be read */
  queueAtTimeout: Record<string, { state: string; namespaces?: string[] }> | { unreadable: string };
}

const isUnreadable = (
  queueAtTimeout: JobWaitObservation["queueAtTimeout"],
): queueAtTimeout is { unreadable: string } => typeof queueAtTimeout.unreadable === "string";

/**
 * State what was observed and nothing else. This used to offer reasons a job
 * might be gone ("a later job in the same namespace replaces the one before
 * it..."), which read as a diagnosis whether or not any of it had happened. The
 * queue's contents and who holds the namespace tell them apart: a queue full of
 * Queued jobs is saturation, another job in the namespace is a replacement.
 */
export function describeUnfinishedJob(o: JobWaitObservation): string {
  const { queue, polls } = o;
  const job = o.lastState
    ? `still ${o.lastState}`
    : o.lastPollUnreachable
    ? `state unknown: the job API was unreachable on ${o.unreachablePolls}/${polls} polls, including the last` +
      (o.lastFailure ? ` (last failure: ${o.lastFailure})` : "")
    : o.lastFound
    ? "the API returned the job with no state"
    : o.foundPolls > 0
    ? `last seen ${o.lastSeenState ?? "with no state"}, then neither on queue ${queue} nor stored as a result for ` +
      `the last ${o.absentPollsAtEnd}/${polls} polls`
    : `neither on queue ${queue} nor stored as a result on any of ${polls} polls`;

  if (isUnreadable(o.queueAtTimeout)) {
    return `${job}; could not read queue ${queue}: ${o.queueAtTimeout.unreadable}`;
  }

  const jobs = Object.entries(o.queueAtTimeout);
  const counts = new Map<string, number>();
  for (const [, { state }] of jobs) {
    counts.set(state, (counts.get(state) ?? 0) + 1);
  }
  const contents = [...counts].map(([state, count]) => `${count} ${state}`).join(", ") || "empty";

  let namespace = "";
  if (o.namespace) {
    const holders = jobs
      .filter(([, { namespaces }]) => namespaces?.includes(o.namespace!))
      .map(([id, { state }]) => `${id.substring(0, 6)} (${state})`);
    namespace = holders.length > 0
      ? `; namespace ${o.namespace} is held by ${holders.join(", ")}`
      : `; no job on the queue holds namespace ${o.namespace}`;
  }
  return `${job}; queue ${queue} at timeout: ${contents}${namespace}`;
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

  let lastState: string | undefined;
  let lastSeenState: string | undefined;
  let polls = 0;
  let unreachablePolls = 0;
  let lastPollUnreachable = false;
  let lastFailure: string | undefined;
  let foundPolls = 0;
  let absentPollsAtEnd = 0;
  let lastFound: boolean | undefined;
  while (Date.now() - startTime < timeoutMs) {
    const status = await mcpGetJobStatus({ queue, jobId });
    polls++;
    lastState = status.status?.state;
    lastFound = status.found;
    // get_job_status reports an unreachable API separately from an absent job
    lastPollUnreachable = status.success === false;
    if (lastPollUnreachable) {
      unreachablePolls++;
      lastFailure = status.lookupFailures?.join("; ") ?? status.error;
    } else if (status.found) {
      foundPolls++;
      absentPollsAtEnd = 0;
      lastSeenState = lastState ?? lastSeenState;
    } else {
      absentPollsAtEnd++;
    }

    if (lastState === "Finished" || lastState === "Cancelled") {
      return status;
    }

    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }

  let queueAtTimeout: JobWaitObservation["queueAtTimeout"];
  try {
    queueAtTimeout = await queueJobs(queue);
  } catch (err) {
    queueAtTimeout = { unreadable: (err as Error).message };
  }
  const why = describeUnfinishedJob({
    queue,
    polls,
    unreachablePolls,
    lastFailure,
    lastPollUnreachable,
    lastState,
    lastFound,
    foundPolls,
    lastSeenState,
    absentPollsAtEnd,
    namespace: submittedNamespaces.get(jobId),
    queueAtTimeout,
  });
  throw new Error(`Job ${jobId} did not complete within ${timeoutMs}ms (${why})`);
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
