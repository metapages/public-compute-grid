#!/usr/bin/env deno run --allow-net --allow-env --allow-read --allow-write

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { createMcpServer } from "./server-factory.ts";

/**
 * MCP server for worker.metapage.io over stdio.
 *
 * The server itself is built by createMcpServer so that this and the
 * Streamable HTTP mount expose identical tools — including follow_job, whose
 * live logs ride on progress notifications and therefore work on both.
 */
async function main() {
  const server = createMcpServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
  // stdout is the protocol channel; anything logged there corrupts it.
  console.error("worker.metapage.io MCP server running on stdio");
}

if (import.meta.main) {
  main().catch((error) => {
    console.error("Fatal error running MCP server:", error);
    Deno.exit(1);
  });
}
