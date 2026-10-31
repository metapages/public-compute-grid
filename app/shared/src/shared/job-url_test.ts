import { assertEquals, assertRejects } from "std/assert";
import { type Context, Hono } from "hono";

import type { DB } from "@shared/db.ts";
import { makeJobUrlMiddleware, resolveSubmittedJob } from "@shared/job-url.ts";
import { InvalidJobIdError, shaDockerJob } from "@shared/util.ts";

const RUN = "a".repeat(64);
const HASH = "b".repeat(64);
const INPUTS = "c".repeat(64);

const makeApp = () => {
  const app = new Hono();
  const db = {
    resolveJobRunId: (id: string, opts?: { v?: string; i?: string }) => {
      if (opts?.i === INPUTS) return Promise.resolve(RUN);
      if (opts?.i) return Promise.resolve(undefined);
      return Promise.resolve(opts?.v || (id === "name" ? HASH : id));
    },
  } as unknown as DB;
  app.use("*", makeJobUrlMiddleware({ resolveDb: () => Promise.resolve(db), fetch: (req) => app.fetch(req) }));
  const echo = async (c: Context) =>
    c.json({
      jobId: c.req.param("jobId"),
      queue: c.req.param("queue"),
      method: c.req.method,
      search: new URL(c.req.url).search,
      body: await c.req.text(),
    });
  app.get("/j/:jobId{[^/]+\\.json}", echo);
  app.get("/j/:jobId/result.json", echo);
  app.post("/j/:jobId/copy", echo);
  app.post("/q/:queue/j/:jobId/cancel", echo);
  app.get("/q/:queue/j/:jobId", echo);
  app.get("/j/:jobId", (c) => c.text(`page ${c.req.param("jobId")}${new URL(c.req.url).search}`));
  return app;
};

const get = async (path: string, init?: RequestInit) => {
  const response = await makeApp().request(path, init);
  return response.headers.get("content-type")?.includes("json") ? await response.json() : await response.text();
};

Deno.test("makeJobUrlMiddleware: ?i picks the run, and other query params survive", async () => {
  const { jobId, search } = await get(`/j/${HASH}/result.json?i=${INPUTS}&since=3`);
  assertEquals(jobId, RUN);
  assertEquals(search, "?since=3");
});

Deno.test("makeJobUrlMiddleware: ?v and names resolve, on queue routes too", async () => {
  assertEquals((await get(`/j/name/result.json?v=${RUN}`)).jobId, RUN);
  assertEquals((await get(`/j/name/result.json`)).jobId, HASH);
  const onQueue = await get(`/q/q1/j/name`);
  assertEquals([onQueue.jobId, onQueue.queue], [HASH, "q1"]);
});

Deno.test("makeJobUrlMiddleware: /j/<id>.json keeps its suffix", async () => {
  assertEquals((await get(`/j/name.json`)).jobId, `${HASH}.json`);
});

Deno.test("makeJobUrlMiddleware: a rewritten POST keeps its method and body", async () => {
  const body = JSON.stringify({ queue: "other" });
  const copied = await get(`/j/name/copy?i=${INPUTS}`, { method: "POST", body });
  assertEquals([copied.jobId, copied.method, copied.body], [RUN, "POST", body]);
  const cancelled = await get(`/q/q1/j/name/cancel`, { method: "POST" });
  assertEquals([cancelled.jobId, cancelled.queue, cancelled.method], [HASH, "q1", "POST"]);
});

Deno.test("makeJobUrlMiddleware: inputs that were never submitted are a 404", async () => {
  const response = await makeApp().request(`/j/${HASH}/result.json?i=${"d".repeat(64)}`);
  assertEquals(response.status, 404);
  await response.body?.cancel();
});

Deno.test("makeJobUrlMiddleware: the bare /j/<id> is the page, and keeps its query", async () => {
  assertEquals(await get(`/j/name?i=${INPUTS}`), `page name?i=${INPUTS}`);
});

Deno.test("resolveSubmittedJob: refuses a stored definition that does not hash to the sha256 asked for", async () => {
  const definition = { image: "alpine", command: "b" };
  const db = {
    resolveJobRunId: (id: string) => Promise.resolve(id),
    getJobDefinition: () => Promise.resolve(definition),
  } as unknown as DB;
  await assertRejects(
    () => resolveSubmittedJob(db, { jobId: HASH, inputs: { a: { value: "1" } } }),
    InvalidJobIdError,
  );
  const hash = await shaDockerJob(definition);
  assertEquals((await resolveSubmittedJob(db, { jobId: hash, inputs: { a: { value: "1" } } }))?.jobHash, hash);
});
