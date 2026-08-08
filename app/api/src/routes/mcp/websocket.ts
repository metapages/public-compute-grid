import { userJobQueues } from "@metapages/compute-queues-shared";
import type { JobStatusPayload } from "@metapages/compute-queues-shared";
import type { Context } from "hono";

import { readResource, resources } from "./resources.ts";
import { handleToolCall, tools } from "./tools.ts";
import type { JobSubscription, MCPNotification, MCPRequest, MCPResponse, Tool } from "./types.ts";
import { onJobLogs, onJobStateChange } from "@metapages/compute-queues-shared";

/**
 * WebSocket MCP Server for real-time job monitoring
 */

interface MCPWebSocketConnection {
  id: string;
  socket: WebSocket;
  subscriptions: Map<string, JobSubscription>;
  lastPing: number;
}

// Global connection registry
const connections = new Map<string, MCPWebSocketConnection>();
const jobSubscriptions = new Map<string, Set<string>>(); // jobId -> set of connectionIds

/**
 * Attach the MCP protocol to an already-upgraded websocket.
 *
 * The upgrade itself happens in handlerWs.ts, which owns every websocket on
 * this server — a Hono route can never see one. Taking the socket rather than
 * the Context keeps this usable from there, and from the local-mode worker,
 * which has its own dispatcher.
 */
export function attachMCPWebSocket(socket: WebSocket): void {
  const connectionId = generateConnectionId();

  const connection: MCPWebSocketConnection = {
    id: connectionId,
    socket,
    subscriptions: new Map(),
    lastPing: Date.now(),
  };

  const announce = () => {
    connections.set(connectionId, connection);
    console.log(`🔌 MCP WebSocket client connected: ${connectionId}`);

    // Send initial capabilities
    sendMessage(socket, {
      jsonrpc: "2.0",
      method: "initialized",
      params: {
        capabilities: {
          tools: {},
          resources: {},
          logging: {},
        },
        serverInfo: {
          name: "worker-metapage-mcp",
          version: "1.0.0",
        },
      },
    });
  };

  // handlerWs.ts hands over a socket that is already OPEN, so `onopen` would
  // never fire. Announce immediately in that case.
  if (socket.readyState === WebSocket.OPEN) {
    announce();
  } else {
    socket.onopen = announce;
  }

  socket.onmessage = async (event) => {
    try {
      const message = JSON.parse(event.data) as MCPRequest;
      const response = await handleMCPMessage(connection, message);

      if (response && message.id !== undefined) {
        sendMessage(socket, response);
      }
    } catch (error) {
      console.error("MCP WebSocket message error:", error);

      const errorResponse: MCPResponse = {
        jsonrpc: "2.0",
        id: null,
        error: {
          code: -32700,
          message: "Parse error",
          data: (error as Error).message,
        },
      };

      sendMessage(socket, errorResponse);
    }
  };

  socket.onclose = () => {
    console.log(`🔌 MCP WebSocket client disconnected: ${connectionId}`);
    cleanup(connectionId);
  };

  socket.onerror = (error) => {
    console.error(`🚨 MCP WebSocket error for ${connectionId}:`, error);
    cleanup(connectionId);
  };

  // Setup periodic ping to keep connection alive
  const pingInterval = setInterval(() => {
    if (socket.readyState === WebSocket.OPEN) {
      sendMessage(socket, {
        jsonrpc: "2.0",
        method: "ping",
        params: { timestamp: Date.now() },
      });
    } else {
      clearInterval(pingInterval);
    }
  }, 30000); // Ping every 30 seconds
}

async function handleMCPMessage(
  connection: MCPWebSocketConnection,
  message: MCPRequest,
): Promise<MCPResponse | null> {
  const { method, params, id } = message;

  try {
    switch (method) {
      case "initialize":
        return {
          jsonrpc: "2.0",
          id,
          result: {
            capabilities: {
              tools: {},
              resources: {},
              logging: {},
            },
            serverInfo: {
              name: "worker-metapage-mcp",
              version: "1.0.0",
            },
          },
        };

      case "tools/list":
        // subscribe_to_job only exists on this transport (it needs a live
        // socket to push to), so it is appended here rather than living in the
        // shared tool list that the HTTP transport also serves.
        return {
          jsonrpc: "2.0",
          id,
          result: { tools: [...tools, SUBSCRIBE_TO_JOB_TOOL] },
        };

      case "tools/call": {
        // Streaming subscriptions are connection-scoped, so they are served
        // here rather than by the transport-agnostic tool dispatcher.
        if (params.name === "subscribe_to_job") {
          const result = await handleJobSubscription(connection, params.arguments);
          return { jsonrpc: "2.0", id, result };
        }

        const toolResult = await handleToolCall({
          name: params.name,
          arguments: params.arguments,
        });

        return {
          jsonrpc: "2.0",
          id,
          result: toolResult,
        };
      }
      case "resources/list":
        return {
          jsonrpc: "2.0",
          id,
          result: { resources },
        };

      case "resources/read": {
        const resourceResult = await readResource(params.uri);
        return {
          jsonrpc: "2.0",
          id,
          result: resourceResult,
        };
      }
      case "pong":
        connection.lastPing = Date.now();
        return null; // No response needed for pong

      default:
        throw new Error(`Unknown method: ${method}`);
    }
  } catch (error) {
    return {
      jsonrpc: "2.0",
      id,
      error: {
        code: -32601,
        message: "Method not found",
        data: (error as Error).message,
      },
    };
  }
}

const SUBSCRIBE_TO_JOB_TOOL: Tool = {
  name: "subscribe_to_job",
  description:
    "Stream a job's logs and state changes to this connection as they happen. Notifications arrive as JSON-RPC messages with method 'notification' until the job finishes. Websocket transport only.",
  inputSchema: {
    type: "object",
    properties: {
      jobId: {
        type: "string",
        description: "The job ID to follow",
      },
      events: {
        type: "array",
        description: "Which events to receive",
        items: { type: "string", enum: ["logs", "status", "completion"] },
        default: ["logs", "status", "completion"],
      },
    },
    required: ["jobId"],
  },
};

async function handleJobSubscription(
  connection: MCPWebSocketConnection,
  args: { jobId: string; events?: JobSubscription["events"] },
): Promise<{ content: { type: string; text: string }[] }> {
  const { jobId, events = ["logs", "status", "completion"] } = args;

  const subscription: JobSubscription = {
    jobId,
    events,
    clientId: connection.id,
  };

  // Add to connection subscriptions
  connection.subscriptions.set(jobId, subscription);

  // Add to global job subscriptions index
  if (!jobSubscriptions.has(jobId)) {
    jobSubscriptions.set(jobId, new Set());
  }
  jobSubscriptions.get(jobId)!.add(connection.id);

  console.log(`📡 Client ${connection.id} subscribed to job ${jobId} for events: ${events.join(", ")}`);

  // Hook into existing WebSocket infrastructure to forward job updates
  await setupJobForwarding(jobId);

  return {
    content: [{
      type: "text",
      text: JSON.stringify(
        {
          success: true,
          jobId,
          events,
          message: `Subscribed. Logs and state for ${jobId} will arrive as 'notification' messages on this connection.`,
        },
        null,
        2,
      ),
    }],
  };
}

async function setupJobForwarding(jobId: string): Promise<void> {
  // Find which queue contains this job
  let targetQueue = null;

  for (const [queueName, queue] of Object.entries(userJobQueues)) {
    if (queue.getCurrentJobState(jobId)) {
      targetQueue = queue;
      break;
    }
  }

  if (!targetQueue) {
    console.warn(`Job ${jobId} not found in any queue for forwarding setup`);
    return;
  }

  // The existing job queue system already handles WebSocket broadcasting
  // We just need to tap into those broadcasts and forward to MCP clients
  // This is done via the broadcastJobLogsToMCPClients function below
}

// Function to be called by existing WebSocket job log broadcasting
export function broadcastJobLogsToMCPClients(logs: JobStatusPayload): void {
  const subscribedConnections = jobSubscriptions.get(logs.jobId);
  if (!subscribedConnections || subscribedConnections.size === 0) {
    return;
  }

  const notification: MCPNotification = {
    jsonrpc: "2.0",
    method: "notification",
    params: {
      type: "job/logs",
      jobId: logs.jobId,
      data: {
        step: logs.step,
        logs: logs.logs,
        timestamp: Date.now(),
      },
    },
  };

  // Send to all subscribed connections
  for (const connectionId of subscribedConnections) {
    const connection = connections.get(connectionId);
    if (connection && connection.socket.readyState === WebSocket.OPEN) {
      const subscription = connection.subscriptions.get(logs.jobId);
      if (subscription && subscription.events.includes("logs")) {
        sendMessage(connection.socket, notification);
      }
    }
  }
}

// Function to be called when job status changes
export function broadcastJobStatusToMCPClients(jobId: string, newState: string, data?: any): void {
  const subscribedConnections = jobSubscriptions.get(jobId);
  if (!subscribedConnections || subscribedConnections.size === 0) {
    return;
  }

  const notification: MCPNotification = {
    jsonrpc: "2.0",
    method: "notification",
    params: {
      type: "job/status_changed",
      jobId,
      data: {
        newState,
        ...data,
        timestamp: Date.now(),
      },
    },
  };

  // Send to all subscribed connections
  for (const connectionId of subscribedConnections) {
    const connection = connections.get(connectionId);
    if (connection && connection.socket.readyState === WebSocket.OPEN) {
      const subscription = connection.subscriptions.get(jobId);
      if (subscription && subscription.events.includes("status")) {
        sendMessage(connection.socket, notification);
      }
    }
  }

  // If job is completed, also send completion notification
  if (newState === "Finished") {
    const completionNotification: MCPNotification = {
      jsonrpc: "2.0",
      method: "notification",
      params: {
        type: "job/completed",
        jobId,
        data: {
          finalState: newState,
          ...data,
          timestamp: Date.now(),
        },
      },
    };

    for (const connectionId of subscribedConnections) {
      const connection = connections.get(connectionId);
      if (connection && connection.socket.readyState === WebSocket.OPEN) {
        const subscription = connection.subscriptions.get(jobId);
        if (subscription && subscription.events.includes("completion")) {
          sendMessage(connection.socket, completionNotification);
        }
      }
    }
  }
}

function sendMessage(socket: WebSocket, message: MCPResponse | MCPNotification): void {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

function cleanup(connectionId: string): void {
  const connection = connections.get(connectionId);
  if (connection) {
    // Remove all job subscriptions for this connection
    for (const [jobId, subscription] of connection.subscriptions) {
      const jobSubs = jobSubscriptions.get(jobId);
      if (jobSubs) {
        jobSubs.delete(connectionId);
        if (jobSubs.size === 0) {
          jobSubscriptions.delete(jobId);
        }
      }
    }

    connections.delete(connectionId);
  }
}

function generateConnectionId(): string {
  return `mcp_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
}

// Subscribe the MCP broadcasters to the queue's log/state stream. Registered at
// module load: handlerWs.ts imports this module to dispatch /mcp/ws upgrades,
// so importing it is what makes MCP live logs flow.
onJobLogs((logs) => broadcastJobLogsToMCPClients(logs));
onJobStateChange((jobId, state, details) => broadcastJobStatusToMCPClients(jobId, state, details));
