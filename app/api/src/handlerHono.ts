import { copyJobToQueueHandler } from "@/routes/api/v1/copy.ts";
import { downloadHandler } from "@/routes/api/v1/download.ts";
import { existsHandler } from "@/routes/api/v1/exists.ts";
import { getJobHandler, getQueueJobHandler } from "@/routes/api/v1/job.ts";
import { submitJobToQueueHandler } from "@/routes/api/v1/submit.ts";
import { uploadHandler } from "@/routes/api/v1/upload.ts";
import { downloadHandler as downloadHandlerDeprecated } from "@/routes/deprecated/download.ts";
import { uploadHandler as uploadHandlerDeprecated } from "@/routes/deprecated/upload.ts";
import { metricsHandler } from "@/routes/metrics.ts";
import { cancelJobHandler } from "@/routes/queue/job/cancel.ts";
import { statusHandler } from "@/routes/status.ts";
import { makeStreamHandler } from "@metapages/compute-queues-shared";
import { type Context, Hono } from "hono";
import { serveStatic } from "hono/middleware";
import { cors } from "hono/middleware/cors";

import { getDefinitionHandler } from "./routes/api/v1/definition.ts";
import { getJobInputsHandler } from "./routes/api/v1/jobInputs.ts";
import { getJobOutputsHandler } from "./routes/api/v1/jobOutputs.ts";
import { getJobsHandler } from "./routes/api/v1/jobs.ts";
import { getBuildLogsHandler, getRunLogsHandler } from "./routes/api/v1/logs.ts";
import { getJobNamespacesHandler } from "./routes/api/v1/namespaces.ts";
import { getJobResultHandler } from "./routes/api/v1/result.ts";
import { getApiDockerJobQueue } from "./routes/websocket.ts";
// MCP (Model Context Protocol) handlers
import { handleMCPHealth, handleMCPInfo } from "./routes/mcp/http.ts";
import { handleMCPStreamableHttp } from "./routes/mcp/streamable.ts";

const streamHandler = makeStreamHandler({
  resolveQueue: (c: Context) => getApiDockerJobQueue(c.req.param("queue") || ""),
});

const app = new Hono();

// app.use(logger((message: string, ...rest: string[]) => {
//   if (message.includes('GET /healthz')) {
//     return;
//   }
//   console.log(message, ...rest)
// }))

app.use("/*", cors() // cors({
  // origin: 'http://example.com',
  // allowHeaders: ['X-Custom-Header', 'Upgrade-Insecure-Requests'],
  // allowMethods: ['POST', 'GET', 'OPTIONS'],
  // exposeHeaders: ['Content-Length', 'X-Kuma-Revision'],
  // maxAge: 600,
  // credentials: true,
  // })
);

// Put your custom routes here
app.get("/healthz", (c: Context) => c.text("OK"));

const toImplementPlaceholder = (c: Context) => c.text("Not implemented");

// app.get("/api/v1/download/:key", downloadHandler);
// app.get("/api/v1/exists/:key", existsHandler);
// app.put("/api/v1/upload/:key", uploadHandler);
// app.post("/api/v1/copy", copyJobToQueueHandler);
// app.get("/api/v1/job/:jobId", getJobHandler);
// app.get("/j/:jobId/inputs/:filename", toImplementPlaceholder);
// app.get("/j/:jobId/outputs/:filename", toImplementPlaceholder);
// app.post("/copy/:jobId", copyJobToQueueHandler);
// app.get("/job/:jobId", getJobHandler);
app.get("/f/:key", downloadHandler);
app.get("/f/:key/exists", existsHandler);
app.put("/f/:key", uploadHandler);
app.get("/j/:jobId/build-logs.json", getBuildLogsHandler);
app.get("/j/:jobId/run-logs.json", getRunLogsHandler);
app.get("/j/:jobId/definition.json", getDefinitionHandler);
app.get("/j/:jobId/result.json", getJobResultHandler);
app.get("/j/:jobId/results.json", getJobResultHandler);
app.get("/j/:jobId/outputs/*", getJobOutputsHandler);
app.get("/j/:jobId/inputs/*", getJobInputsHandler);
app.post("/j/:jobId/copy", copyJobToQueueHandler);
// JSON job-state lives at /j/<jobId>.json so the bare /j/<jobId> path can fall
// through to the static SPA below, which loads the job by id from
// /j/<jobId>/definition.json. That short path is what MCP clients hand to users.
app.get("/j/:jobId{[^/]+\\.json}", getJobHandler);
app.post("/q/:queue", submitJobToQueueHandler);
app.post("/q/:queue/j", submitJobToQueueHandler);
app.get("/q/:queue/j", getJobsHandler);
app.get("/q/:queue", getJobsHandler);
app.get("/q/:queue/j/:jobId", getQueueJobHandler);
app.get("/q/:queue/j/:jobId/inputs/*", getJobInputsHandler);
app.get("/q/:queue/j/:jobId/outputs/*", getJobOutputsHandler);
app.get("/q/:queue/j/:jobId/namespaces.json", getJobNamespacesHandler);
app.get("/q/:queue/j/:jobId/definition.json", getDefinitionHandler);
app.get("/q/:queue/j/:jobId/result.json", getJobResultHandler);
app.get("/q/:queue/j/:jobId/results.json", getJobResultHandler);
app.get("/q/:queue/j/:jobId/build-logs.json", getBuildLogsHandler);
app.get("/q/:queue/j/:jobId/run-logs.json", getRunLogsHandler);
// Server-Sent Events: follow one job's build logs, run logs and state to completion
app.get("/q/:queue/j/:jobId/stream", streamHandler);
app.get("/q/:queue/j/:jobId/history.json", toImplementPlaceholder);
app.post("/q/:queue/j/:jobId/cancel", cancelJobHandler);
app.post("/q/:queue/j/:jobId/:namespace/cancel", cancelJobHandler);
// app.get("/q/:queue/namespaces", getJobHandler);

// @deprecated
app.get("/upload/:key", uploadHandlerDeprecated);
// @deprecated
app.get("/download/:key", downloadHandlerDeprecated);

// @deprecated
app.get("/:queue/status", statusHandler);
// @deprecated
app.get("/:queue/metrics", metricsHandler);

app.get("/q/:queue/status", statusHandler);
app.get("/q/:queue/metrics", metricsHandler);

// Serve llms.txt file from public folder
app.get("/llms.txt", serveStatic({ path: "app/browser/public/llms.txt" }));

// MCP (Model Context Protocol) endpoints
// MCP over Streamable HTTP. One route serves POST (messages), GET (the
// notification stream that carries live logs) and DELETE (end session).
app.all("/mcp", handleMCPStreamableHttp);
// /mcp/ws is a websocket upgrade, dispatched in handlerWs.ts — Hono never
// sees websocket upgrades on this server, so it cannot be routed here.
app.get("/mcp/health", handleMCPHealth);
app.get("/mcp/info", handleMCPInfo);

// VitePress docs site (source: docs/, build: `just docs build`).
// The site is built with base=/docs/ into docs/dist/docs, so the extra `docs`
// segment in the request path maps straight onto the directory below the root
// and no path rewriting is needed. Deployment copies docs/dist — see the
// `deploy` recipe in app/api/justfile.
app.get("/docs", (c: Context) => c.redirect("/docs/", 301));
app.get("/docs/*", serveStatic({ root: "docs/dist" }));
// `cleanUrls` means links point at /docs/quickstart, but the build still emits
// quickstart.html, so extensionless requests need the suffix added back.
app.get("/docs/*", serveStatic({ root: "docs/dist", rewriteRequestPath: (path) => `${path}.html` }));
// Everything else under /docs is a missing doc, not a browser-app route: fall
// through to VitePress' own 404 page rather than the client's index.html.
app.get("/docs/*", async (c: Context) => {
  const notFoundPage = await Deno.readTextFile("docs/dist/docs/404.html").catch(() => undefined);
  return notFoundPage ? c.html(notFoundPage, 404) : c.notFound();
});

/**
 * index.html for a page served one directory down (`/j/<jobId>`). The built
 * asset URLs are relative — vite's `base` is empty so the app can also be served
 * from a subpath — which means `./assets/…` would resolve to `/j/assets/…` and
 * 404. Giving the existing `<base>` tag an href repoints them at the origin root
 * without touching the build. Read per request so a rebuilt dist is picked up.
 */
const serveSpaOneLevelDown = async (indexPath: string, c: Context): Promise<Response> => {
  const html = await Deno.readTextFile(indexPath).catch(() => undefined);
  if (!html) {
    return c.notFound();
  }
  return c.html(
    html.includes("<base ")
      ? html.replace("<base ", '<base href="/" ')
      : html.replace("<head>", '<head><base href="/">'),
  );
};

// The bare /j/<jobId> is the browser page for a job (the SPA loads the
// definition by id). It needs its own route: the static handler below answers a
// missing file with 404 rather than falling through, so the index.html catch-all
// at the end never runs. Registered after /j/<jobId>.json so that stays JSON.
app.get("/j/:jobId", (c: Context) => serveSpaOneLevelDown("app/browser/dist/index.html", c));

// Serve static assets, and the index.html as the fallback
app.get("/*", serveStatic({ root: "app/browser/dist" }));
app.get("/", serveStatic({ path: "app/browser/dist/index.html" }));
app.get("*", serveStatic({ path: "app/browser/dist/index.html" }));

export const handlerHttp = app.fetch as (
  request: Request,
) => Promise<Response | undefined>;
