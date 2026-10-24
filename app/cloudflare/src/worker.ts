/**
 * Cloudflare entry for the compute-queues API (container.mtfm.io).
 *
 * - The Worker routes every request that touches a queue's live state to that
 *   queue's Durable Object, and serves the rest (blobs, job lookups by id, MCP)
 *   statelessly.
 * - QueueDurableObject runs the same API app as the Deno server, but holds
 *   exactly one queue. Every websocket and HTTP call for a queue lands in one
 *   object, so no cross-instance bus is needed.
 * - Static files (browser app, docs, llms.txt) are Workers Static Assets and
 *   never reach this code, except /j/<jobId> (the app, rebased one level down).
 * - The cron trigger retries failed submission webhooks and sweeps expired rows.
 */
import { store } from "./setup.ts";

import { QueueDurableObjectBase } from "./queue-do.ts";

import { createApp } from "@/handlerHono.ts";
import { createMcpServer } from "@metapages/compute-queues-mcp/server-factory";
import { serveMcpStreamableHttp } from "@metapages/compute-queues-mcp/streamable-http";
import { retryUnsuccessfulWebhooks } from "@shared/webhooks.ts";
import { Hono } from "hono";

interface DurableObjectId {
  toString(): string;
}
interface DurableObjectStub {
  fetch(request: Request): Promise<Response>;
}
interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId;
  get(id: DurableObjectId): DurableObjectStub;
}
interface Fetcher {
  fetch(request: Request | string): Promise<Response>;
}

export interface Env {
  QUEUES: DurableObjectNamespace;
  ASSETS: Fetcher;
}

declare const WebSocketPair: { new (): { 0: WebSocket; 1: WebSocket } };

const app = createApp();

const handle = async (request: Request): Promise<Response> =>
  (await app.fetch(request)) ?? new Response("Not found", { status: 404 });

/**
 * Which queue's Durable Object owns this request, if any. Mirrors the queue
 * routes in handlerHono.ts and handlerWs.ts: /q/:queue/... plus the deprecated
 * /:queue/{status,metrics,client,browser,worker}.
 */
export const queueForPath = (pathname: string): string | undefined => {
  const tokens = pathname.split("/").filter((x) => x !== "");
  if (tokens[0] === "q" && tokens[1]) {
    return decodeURIComponent(tokens[1]);
  }
  if (
    tokens.length === 2 &&
    ["status", "metrics", "client", "browser", "worker"].includes(tokens[1]) &&
    !["q", "j", "f", "mcp", "docs", "assets"].includes(tokens[0])
  ) {
    return decodeURIComponent(tokens[0]);
  }
  return undefined;
};

const toQueue = (env: Env, queue: string, request: Request): Promise<Response> =>
  env.QUEUES.get(env.QUEUES.idFromName(queue)).fetch(request);

/**
 * index.html for the job page at /j/<jobId>. The built asset URLs are relative,
 * so they would resolve under /j/; a <base href="/"> repoints them at the root.
 */
const serveJobPage = async (env: Env, request: Request): Promise<Response> => {
  const index = await env.ASSETS.fetch(new URL("/index.html", request.url).toString());
  if (!index.ok) {
    return index;
  }
  const html = await index.text();
  return new Response(
    html.includes("<base ")
      ? html.replace("<base ", '<base href="/" ')
      : html.replace("<head>", '<head><base href="/">'),
    { headers: { "content-type": "text/html; charset=utf-8" } },
  );
};

export class QueueDurableObject extends QueueDurableObjectBase<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const queueName = queueForPath(url.pathname);
    if (!queueName) {
      return new Response(`Not a queue path: ${url.pathname}`, { status: 404 });
    }

    if (request.headers.get("upgrade")?.toLowerCase() === "websocket") {
      // /q/<queue>/worker vs /q/<queue>/client|browser — the only websocket
      // paths routed here (handlerWs.ts has the same mapping for Deno).
      const tokens = url.pathname.split("/").filter((x) => x !== "");
      const kind = tokens[0] === "q" ? tokens[2] : tokens[1];
      const role = kind === "worker" ? "worker" : "client";
      if (kind !== "worker" && kind !== "client" && kind !== "browser") {
        return new Response(`Unknown websocket endpoint: ${url.pathname}`, { status: 404 });
      }
      const pair = new WebSocketPair();
      try {
        await this.acceptSocket(pair[1], role, queueName);
      } catch (err) {
        console.error(`Could not attach a ${role} socket to queue ${queueName}:`, err);
        return new Response(`Could not attach the ${role} socket: ${(err as Error).message}`, { status: 500 });
      }
      await this.scheduleMaintenance();
      return new Response(null, { status: 101, webSocket: pair[0] } as ResponseInit);
    }

    // An HTTP submit or cancel can create work, so re-check afterwards whether
    // maintenance needs to be running.
    await this.ensureQueue(queueName);
    const response = await handle(request);
    await this.scheduleMaintenance();
    return response;
  }
}

/**
 * fetch() for the MCP tools' calls back into this API. A Worker cannot reach
 * itself over the network, so dispatch through the router in-process. Blob
 * routes answer with a redirect to a presigned R2 URL: follow those out.
 */
const selfFetch = (env: Env) => async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
  const request = new Request(input, init);
  const response = await route(request, env);
  const location = response.headers.get("location");
  if (location && response.status >= 301 && response.status <= 308 && request.redirect !== "manual") {
    const method = response.status === 303 ? "GET" : request.method;
    return fetch(new URL(location, request.url), {
      method,
      headers: init?.headers,
      body: method === "GET" || method === "HEAD" ? undefined : init?.body,
    });
  }
  return response;
};

/** MCP over Streamable HTTP (stateless: any isolate can serve any request) */
const serveMcp = (request: Request, env: Env): Response | Promise<Response> => {
  const origin = new URL(request.url).origin;
  const mcpApp = new Hono();
  mcpApp.all("/mcp", (c) =>
    serveMcpStreamableHttp(
      c,
      createMcpServer({ baseUrl: origin, publicUrl: origin, fetch: selfFetch(env) }),
    ));
  return mcpApp.fetch(request);
};

const route = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);

  const queue = queueForPath(url.pathname);
  if (queue) {
    return toQueue(env, queue, request);
  }

  // Copying a job enqueues it on the queue named in the body
  const copy = url.pathname.match(/^\/j\/[^/]+\/copy$/);
  if (copy && request.method === "POST") {
    const body = await request.clone().json().catch(() => undefined) as { queue?: string } | undefined;
    if (!body?.queue) {
      return Response.json({ error: "Missing queue" }, { status: 400 });
    }
    return toQueue(env, body.queue, request);
  }

  if (url.pathname === "/mcp") {
    return serveMcp(request, env);
  }

  if (request.headers.get("upgrade")?.toLowerCase() === "websocket") {
    // Queue websockets were routed above; no other path has one (MCP clients
    // use Streamable HTTP at /mcp).
    return new Response("Websocket path not supported", { status: 404 });
  }

  if (request.method === "GET" && /^\/j\/[^/.]+$/.test(url.pathname)) {
    return serveJobPage(env, request);
  }

  // The Deno server redirects /docs itself, so do it here rather than leave it
  // to the asset layer, which only redirects when a docs build is deployed.
  if (request.method === "GET" && url.pathname === "/docs") {
    return Response.redirect(new URL("/docs/", url).toString(), 301);
  }

  // A missing doc: VitePress' own 404 page, never the browser app
  if (request.method === "GET" && url.pathname.startsWith("/docs/")) {
    return env.ASSETS.fetch(request);
  }

  return handle(request);
};

export default {
  fetch: route,

  async scheduled(): Promise<void> {
    await Promise.all([
      retryUnsuccessfulWebhooks().catch((err) => console.error("Check for webhooks to retry failed:", err)),
      store.deleteExpired().catch((err) => console.error("Deleting expired rows failed:", err)),
    ]);
  },
};
