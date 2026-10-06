import { StreamableHTTPTransport } from "@hono/mcp";
import type { Server } from "@modelcontextprotocol/sdk/server/index.js";
import type { Context } from "hono";

/**
 * Hono answers a thrown `HTTPException` with the response the exception
 * carries, but it recognises only its own copy of the class. `@hono/mcp` is
 * built against the npm `hono` while these apps run the deno.land one, so
 * `instanceof HTTPException` is false, the exception escapes Hono entirely, and
 * `Deno.serve`'s `onError` logs a stack trace and returns 500 — instead of the
 * 404 the transport meant to send.
 *
 * That is not hypothetical: a client's first request carries the newest
 * `mcp-protocol-version` it knows (claude-code sends 2026-07-28), the MCP SDK
 * only recognises up to 2025-11-25, and the transport rejects it so the client
 * can negotiate down. Normal, self-healing handshake traffic — but every probe
 * dumped a stack trace into the logs and answered 500.
 *
 * Duck-type on `getResponse()` instead of the class so either copy of hono is
 * handled, and rethrow anything else so real failures still surface.
 */
const errorResponse = (err: unknown): Response | undefined => {
  const getResponse = (err as { getResponse?: () => Response } | null)?.getResponse;
  return typeof getResponse === "function" ? getResponse.call(err) : undefined;
};

/**
 * Serve one MCP Streamable HTTP request (POST messages, GET notification
 * stream, DELETE) against `server`.
 *
 * Stateless: `sessionIdGenerator: undefined` disables session tracking. The
 * tools carry their own state and hit the idempotent job API, so there is
 * nothing for a session to hold, and any instance can serve any request — which
 * matters because the API runs multi-instance behind a load balancer. Progress
 * notifications still stream, over each request's own response.
 */
export const serveMcpStreamableHttp = async (
  c: Context,
  server: Server,
): Promise<Response> => {
  const transport = new StreamableHTTPTransport({ sessionIdGenerator: undefined });
  await server.connect(transport);

  try {
    // @hono/mcp is built against its own copy of hono's Context type; the
    // shapes are identical at runtime but nominally distinct across the two
    // copies.
    // deno-lint-ignore no-explicit-any
    const response = await transport.handleRequest(c as any);
    // handleRequest returns undefined only when it has already written the
    // response itself (a streaming GET); surface something valid either way.
    return response ?? c.body(null, 204);
  } catch (err) {
    const response = errorResponse(err);
    if (!response) {
      throw err;
    }
    return response;
  }
};
