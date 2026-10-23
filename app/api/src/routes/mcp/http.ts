import { MCP_TOOLS } from "@metapages/compute-queues-mcp/server-factory";
import type { Context } from "hono";

/**
 * Discovery endpoints for the MCP server mounted at /mcp.
 *
 * The server itself is `createMcpServer` (app/mcp), served over Streamable HTTP
 * by streamable.ts. These only describe it, from the one tool list it serves:
 * they used to describe a second, older MCP implementation that lived here, and
 * advertised tools (create_job, execute_job, modify_job) that /mcp did not
 * serve, plus a /mcp/ws transport that has been retired.
 */

export function handleMCPHealth(c: Context): Response {
  return c.json({
    status: "healthy",
    server: "worker-metapage-mcp",
    version: "1.0.0",
    capabilities: ["tools"],
    timestamp: new Date().toISOString(),
  });
}

export function handleMCPInfo(c: Context): Response {
  return c.json({
    server: {
      name: "worker-metapage-mcp",
      version: "1.0.0",
      description: "MCP server for building and running containers on a compute queue",
    },
    capabilities: {
      tools: {
        count: MCP_TOOLS.length,
        names: MCP_TOOLS.map((tool) => tool.name),
      },
    },
    endpoints: {
      http: "/mcp",
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
      tools: MCP_TOOLS.map((tool) => ({
        name: tool.name,
        description: tool.description,
        required: tool.inputSchema.required ?? [],
        properties: Object.keys(tool.inputSchema.properties ?? {}),
      })),
    },
  });
}
