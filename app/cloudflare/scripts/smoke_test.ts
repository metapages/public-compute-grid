import { assert, assertEquals, assertRejects } from "std/assert";

import { runChecks, waitForSettle } from "./smoke.ts";

/**
 * A stand-in deployment: healthy unless a route is named in `broken`, or in
 * `hung` (accepts the request and answers nothing until stop())
 */
const serve = (broken: Set<string>, { notReadyFor = 0, hung = new Set<string>() } = {}) => {
  const blobs = new Map<string, string>();
  const { promise: stopped, resolve: release } = Promise.withResolvers<void>();
  let requests = 0;
  const server = Deno.serve({ port: 0, onListen: () => {} }, async (request) => {
    const { pathname } = new URL(request.url);
    requests++;
    if (requests <= notReadyFor) return new Response("restarting", { status: 503 });
    const route = pathname.startsWith("/f/") ? "/f/" : pathname.startsWith("/q/") ? "/q/" : pathname;
    if (broken.has(route)) return new Response("broken", { status: 500 });
    if (hung.has(route)) {
      await stopped;
      return new Response("too late");
    }
    if (pathname === "/") {
      return new Response("<!doctype html><html></html>", { headers: { "content-type": "text/html" } });
    }
    if (pathname === "/docs/") return new Response("<html>docs</html>");
    if (pathname === "/llms.txt") return new Response("# compute queues");
    if (pathname.startsWith("/q/")) return Response.json({ jobs: {} });
    if (pathname === "/mcp") {
      const { id } = await request.json();
      const message = { jsonrpc: "2.0", id, result: { serverInfo: { name: "stub", version: "1" } } };
      return new Response(`event: message\ndata: ${JSON.stringify(message)}\n\n`, {
        headers: { "content-type": "text/event-stream" },
      });
    }
    const blob = pathname.match(/^\/f\/([0-9a-f]+)(\/exists)?$/);
    if (blob) {
      const [, key, exists] = blob;
      if (request.method === "PUT") {
        blobs.set(key, await request.text());
        return Response.json({ ok: true });
      }
      if (exists) return Response.json({ exists: blobs.has(key) }, { status: blobs.has(key) ? 200 : 404 });
      return blobs.has(key) ? new Response(blobs.get(key)) : new Response("missing", { status: 404 });
    }
    return new Response("not found", { status: 404 });
  });
  // Hung requests would otherwise keep shutdown() waiting forever
  const stop = () => {
    release();
    return server.shutdown();
  };
  return { url: `http://localhost:${server.addr.port}`, server: { shutdown: stop } };
};

Deno.test("smoke: a healthy deployment passes every check", async () => {
  const { url, server } = serve(new Set());
  try {
    const results = await runChecks(url);
    assertEquals(results.filter((r) => !r.ok), []);
    assertEquals(results.length, 6);
  } finally {
    await server.shutdown();
  }
});

Deno.test("smoke: each broken route fails its check, and only that one", async () => {
  for (const route of ["/", "/docs/", "/llms.txt", "/q/", "/mcp", "/f/"]) {
    const { url, server } = serve(new Set([route]));
    try {
      const failed = (await runChecks(url)).filter((r) => !r.ok);
      assertEquals(failed.length, 1, `${route}: ${JSON.stringify(failed)}`);
      assert(failed[0].error);
    } finally {
      await server.shutdown();
    }
  }
});

Deno.test("smoke: waits for a deployment that is still restarting", async () => {
  const { url, server } = serve(new Set(), { notReadyFor: 3 });
  try {
    const logs: string[] = [];
    await waitForSettle(url, { pollMs: 10, settleMs: 0, timeoutMs: 5_000, log: (m) => logs.push(m) });
    assert(logs.some((m) => m.includes("503")), logs.join("\n"));
  } finally {
    await server.shutdown();
  }
});

Deno.test("smoke: gives up on a deployment that never answers", async () => {
  const { url, server } = serve(new Set(["/"]));
  try {
    await assertRejects(
      () => waitForSettle(url, { pollMs: 10, settleMs: 0, timeoutMs: 100, log: () => {} }),
      Error,
      "not answering",
    );
  } finally {
    await server.shutdown();
  }
});

Deno.test("smoke: a route that never answers fails its check instead of hanging", async () => {
  const { url, server } = serve(new Set(), { hung: new Set(["/q/"]) });
  try {
    const started = Date.now();
    const failed = (await runChecks(url, undefined, { requestTimeoutMs: 200 })).filter((r) => !r.ok);
    assertEquals(failed.map((r) => r.name), ["GET /q/<queue>/status"]);
    assert(failed[0].error?.includes("no answer after 0.2s"), failed[0].error);
    assert(Date.now() - started < 5_000);
  } finally {
    await server.shutdown();
  }
});

Deno.test("smoke: the settle wait gives up on a deployment that never answers", async () => {
  const { url, server } = serve(new Set(), { hung: new Set(["/q/"]) });
  try {
    const logs: string[] = [];
    await assertRejects(
      () =>
        waitForSettle(url, {
          pollMs: 10,
          settleMs: 0,
          timeoutMs: 500,
          requestTimeoutMs: 100,
          log: (m) => logs.push(m),
        }),
      Error,
      "timed out",
    );
    assert(logs.some((m) => m.includes("timed out")), logs.join("\n"));
  } finally {
    await server.shutdown();
  }
});
