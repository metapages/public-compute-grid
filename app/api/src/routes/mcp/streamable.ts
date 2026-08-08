import { StreamableHTTPTransport } from "@hono/mcp";
import type { Context } from "hono";

import { createMcpServer } from "@metapages/compute-queues-mcp/server-factory";

/**
 * MCP over Streamable HTTP — the transport modern MCP clients (Claude Code
 * among them) speak at a single `/mcp` URL. One route serves all of it: POST
 * for initialize and messages, GET for the notification stream, DELETE to end
 * a session.
 *
 * This is what makes live logs reachable from a normal MCP client. The
 * hand-rolled JSON-RPC handler it replaces could only ever return one response
 * per request, so `follow_job`'s progress notifications had nowhere to go.
 *
 * Stateless: `sessionIdGenerator: undefined` disables session tracking. The
 * tools carry their own state and hit the idempotent job API, so there is
 * nothing for a session to hold, and any instance can serve any request —
 * which matters because the API runs multi-instance behind a load balancer.
 * Progress notifications still stream, over each request's own response.
 */
export const handleMCPStreamableHttp = async (c: Context): Promise<Response> => {
  const origin = new URL(c.req.url).origin;

  // The MCP tools talk to the job API over HTTP. Pointing them back at this
  // same origin keeps everything on one deployment rather than reaching out to
  // production from a local stack.
  const server = createMcpServer({ baseUrl: origin });
  const transport = new StreamableHTTPTransport({ sessionIdGenerator: undefined });
  await server.connect(transport);

  // @hono/mcp is built against its own copy of hono's Context type; the shapes
  // are identical at runtime but nominally distinct across the two copies.
  // deno-lint-ignore no-explicit-any
  const response = await transport.handleRequest(c as any);
  // handleRequest returns undefined only when it has already written the
  // response itself (a streaming GET); surface something valid either way.
  return response ?? c.body(null, 204);
};
