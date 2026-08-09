import type { Context } from "hono";
import type { MCPRequest, MCPResponse } from "./types.ts";
import { getDefaultQueue, handleToolCall, tools } from "./tools.ts";
import { readResource, resources } from "./resources.ts";

/**
 * HTTP MCP Server endpoints - Updated with new tools
 */

export async function handleMCPRequest(c: Context): Promise<Response> {
  // Set proper headers for MCP HTTP transport
  c.header("Content-Type", "application/json");
  c.header("Access-Control-Allow-Origin", "*");
  c.header("Access-Control-Allow-Methods", "POST, OPTIONS");
  c.header("Access-Control-Allow-Headers", "Content-Type");

  if (c.req.method === "OPTIONS") {
    return c.body(null, 200);
  }

  try {
    const request = await c.req.json() as MCPRequest;
    const response = await processMCPRequest(request);
    return c.json(response);
  } catch (error) {
    const errorResponse: MCPResponse = {
      jsonrpc: "2.0",
      id: null, // Add id field for error responses
      error: {
        code: -32700,
        message: "Parse error",
        data: (error as Error).message,
      },
    };
    return c.json(errorResponse, 400);
  }
}

async function processMCPRequest(request: MCPRequest): Promise<MCPResponse> {
  const { method, id } = request;
  // `params` is untyped JSON on the wire; each case checks what it needs.
  const params = request.params as
    | { name?: string; arguments?: Record<string, unknown>; uri?: string }
    | undefined;

  console.log(`[MCP] Processing method: "${method}" (type: ${typeof method})`);

  try {
    switch (method) {
      case "initialize":
        return {
          jsonrpc: "2.0",
          id,
          result: {
            protocolVersion: "2024-11-05",
            capabilities: {
              tools: {
                listChanged: false,
              },
              resources: {
                subscribe: false,
                listChanged: false,
              },
            },
            serverInfo: {
              name: "worker-metapage-mcp",
              version: "1.0.0",
            },
          },
        };

      case "tools/list":
        return {
          jsonrpc: "2.0",
          id,
          result: { tools },
        };

      case "tools/call": {
        if (!params || typeof params !== "object" || !params.name) {
          throw new Error("Invalid tool call parameters");
        }

        const toolResult = await handleToolCall({
          name: params.name,
          arguments: params.arguments || {},
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
        if (!params || typeof params !== "object" || !params.uri) {
          throw new Error("Invalid resource read parameters");
        }

        const resourceResult = await readResource(params.uri);
        return {
          jsonrpc: "2.0",
          id,
          result: resourceResult,
        };
      }

      default:
        console.log(`[MCP DEBUG] Unknown method received: "${method}" (length: ${method?.length})`);
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

// Health check endpoint for MCP server
export function handleMCPHealth(c: Context): Response {
  return c.json({
    status: "healthy",
    server: "worker-metapage-mcp",
    version: "1.0.0",
    capabilities: ["tools", "resources", "iterative-development"],
    queue: getDefaultQueue(),
    timestamp: new Date().toISOString(),
  });
}

// MCP server info endpoint
export function handleMCPInfo(c: Context): Response {
  return c.json({
    server: {
      name: "worker-metapage-mcp",
      version: "1.0.0",
      description: "MCP server for iterative container development",
      queue: getDefaultQueue(),
    },
    workflow: {
      description: "Iterative Container Development",
      steps: [
        "1. create_job - Create a containerized job with Docker image/Dockerfile and inputs",
        "2. execute_job - Run the job and get comprehensive results including logs and outputs",
        "3. inspect_outputs - Analyze outputs, check against expectations, identify issues",
        "4. modify_job - Create iteration with updated code/config based on inspection",
        "5. Repeat steps 2-4 until desired results are achieved",
      ],
      additionalTools: [
        "list_iterations - View development history and iteration chains",
        "get_job_url - Generate shareable URLs containing job definitions and results",
      ],
    },
    capabilities: {
      tools: {
        count: tools.length,
        names: tools.map((t) => t.name),
        iterativeDevelopment: true,
        simplifiedInputs: true,
      },
      resources: {
        count: resources.length,
        patterns: resources.map((r) => r.uri),
      },
      queue: {
        default: getDefaultQueue(),
        configurable: true,
      },
    },
    endpoints: {
      http: "/mcp",
      websocket: "/mcp/ws",
      health: "/mcp/health",
      info: "/mcp/info",
      jobUrls: {
        // Browser pages. Everything under `json` serves JSON instead.
        view: "/j/{jobId}#?queue={queue}",
        viewSelfContained: "/#?job={base64(encodeURIComponent(JSON.stringify(definition)))}&queue={queue}",
        json: {
          pattern: "/j/{jobId}.json",
          queuePattern: "/q/{queue}/j/{jobId}",
          definition: "/q/{queue}/j/{jobId}/definition.json",
          result: "/q/{queue}/j/{jobId}/result.json",
          inputs: "/q/{queue}/j/{jobId}/inputs/",
          outputs: "/q/{queue}/j/{jobId}/outputs/",
        },
      },
    },
    documentation: {
      tools: tools.map((tool) => ({
        name: tool.name,
        description: tool.description,
        required: tool.inputSchema.required || [],
        properties: Object.keys(tool.inputSchema.properties || {}),
      })),
      resources: resources.map((resource) => ({
        uri: resource.uri,
        name: resource.name,
        description: resource.description,
      })),
    },
  });
}
