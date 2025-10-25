import { copyJobToQueueHandler } from "/@/routes/api/v1/copy.ts";
import { downloadHandler } from "/@/routes/api/v1/download.ts";
import { existsHandler } from "/@/routes/api/v1/exists.ts";
import { getJobHandler, getQueueJobHandler } from "/@/routes/api/v1/job.ts";
import { submitJobToQueueHandler } from "/@/routes/api/v1/submit.ts";
import { uploadHandler } from "/@/routes/api/v1/upload.ts";
import { downloadHandler as downloadHandlerDeprecated } from "/@/routes/deprecated/download.ts";
import { uploadHandler as uploadHandlerDeprecated } from "/@/routes/deprecated/upload.ts";
import { metricsHandler } from "/@/routes/metrics.ts";
import { cancelJobHandler } from "/@/routes/queue/job/cancel.ts";
import { statusHandler } from "/@/routes/status.ts";
import { type Context, Hono } from "hono";
import { serveStatic } from "hono/middleware";
import { cors } from "hono/middleware/cors";

import { getDefinitionHandler } from "./routes/api/v1/definition.ts";
import { getJobsHandler } from "./routes/api/v1/jobs.ts";
import { getJobNamespacesHandler } from "./routes/api/v1/namespaces.ts";
import { getJobResultHandler } from "./routes/api/v1/result.ts";
import { getJobInputsHandler } from "./routes/api/v1/jobInputs.ts";
import { getJobOutputsHandler } from "./routes/api/v1/jobOutputs.ts";

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
app.get("/j/:jobId/definition.json", getDefinitionHandler);
app.get("/j/:jobId/result.json", getJobResultHandler);
app.get("/j/:jobId/results.json", getJobResultHandler);
app.get("/j/:jobId/outputs/*", getJobOutputsHandler);
app.get("/j/:jobId/inputs/*", getJobInputsHandler);
app.post("/j/:jobId/copy", copyJobToQueueHandler);
app.get("/j/:jobId", getJobHandler);
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

// Serve static assets, and the index.html as the fallback
app.get("/*", serveStatic({ root: "app/browser/dist" }));
app.get("/", serveStatic({ path: "app/browser/dist/index.html" }));
app.get("*", serveStatic({ path: "app/browser/dist/index.html" }));

export const handlerHttp = app.fetch as (
  request: Request,
) => Promise<Response | undefined>;
