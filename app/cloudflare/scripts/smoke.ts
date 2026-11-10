/**
 * Smoke-check a deployment, after it has settled (issue #342).
 *
 * A deploy answers within seconds, but its Durable Objects keep restarting on
 * the new version for a while longer: checking straight away fails jobs that
 * pass a few minutes later. So first wait until / and a queue status answer
 * 200, then wait a while more, then check the routes once. No worker is
 * needed, and nothing is enqueued.
 *
 * Every request gives up after --request-timeout: a Durable Object stuck
 * restarting can accept a connection and never answer, and that has to fail
 * the deploy, not hang it until the Actions job times out hours later.
 *
 * Each run writes one tiny blob (deploy-smoke-<nonce>) to the bucket, since
 * there is no delete route. The bucket's expire-35-days lifecycle rule
 * (`just cloudflare setup`) removes it with every other blob.
 *
 *   deno run --allow-net scripts/smoke.ts <url> [--settle=45] [--timeout=300] [--request-timeout=30]
 */

/** fetch, but giving up after the request timeout */
export type Fetch = (input: string | URL, init?: RequestInit) => Promise<Response>;

export type SmokeCheck = { name: string; run: (url: string, fetch: Fetch) => Promise<void> };

export type SmokeResult = { name: string; ok: boolean; error?: string };

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export const DefaultRequestTimeoutMs = 30_000;

/** fetch that aborts after timeoutMs, including reading the body */
const timedFetch = (timeoutMs: number): Fetch => async (input, init) => {
  try {
    return await fetch(input, { ...init, signal: AbortSignal.timeout(timeoutMs) });
  } catch (err) {
    if (err instanceof DOMException && err.name === "TimeoutError") {
      throw new Error(`${new URL(input).pathname}: no answer after ${timeoutMs / 1000}s`);
    }
    throw err;
  }
};

const nonce = () => `${Date.now()}-${crypto.randomUUID().slice(0, 8)}`;

const expectStatus = async (response: Response, status: number, what: string): Promise<string> => {
  const text = await response.text();
  if (response.status !== status) {
    throw new Error(`${what}: status ${response.status} (wanted ${status}): ${text.slice(0, 200)}`);
  }
  return text;
};

const sha256 = async (text: string): Promise<string> => {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
};

/** Status of GET url, or why there is none */
const statusOf = async (url: string, timeoutMs: number): Promise<number | string> => {
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
    await response.body?.cancel();
    return response.status;
  } catch (err) {
    return err instanceof DOMException && err.name === "TimeoutError" ? "timed out" : "unreachable";
  }
};

/**
 * Wait until / and a queue status both answer 200, then another settleMs.
 * Throws when they don't within timeoutMs.
 */
export const waitForSettle = async (
  url: string,
  {
    timeoutMs = 300_000,
    settleMs = 45_000,
    pollMs = 5_000,
    requestTimeoutMs = DefaultRequestTimeoutMs,
    log = console.log,
  } = {},
): Promise<void> => {
  const paths = ["/", `/q/smoke-${nonce()}/status`];
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    // Never wait on a request past the deadline
    const wait = Math.max(1, Math.min(requestTimeoutMs, deadline - Date.now()));
    const statuses = await Promise.all(paths.map((path) => statusOf(`${url}${path}`, wait)));
    if (statuses.every((s) => s === 200)) break;
    const summary = paths.map((p, i) => `${p} ${statuses[i]}`).join(", ");
    if (Date.now() >= deadline) {
      throw new Error(`${url} not answering after ${timeoutMs / 1000}s: ${summary}`);
    }
    log(`waiting for ${url}: ${summary}`);
    await sleep(pollMs);
  }
  if (settleMs > 0) {
    log(`${url} answers; letting it settle for ${settleMs / 1000}s`);
    await sleep(settleMs);
  }
};

export const checks: SmokeCheck[] = [
  {
    name: "GET / (browser app)",
    run: async (url, fetch) => {
      const response = await fetch(`${url}/`);
      const text = await expectStatus(response, 200, "/");
      if (!text.toLowerCase().includes("<html")) throw new Error("/ is not an html page");
    },
  },
  {
    name: "GET /docs/",
    run: async (url, fetch) => {
      await expectStatus(await fetch(`${url}/docs/`), 200, "/docs/");
    },
  },
  {
    name: "GET /llms.txt",
    run: async (url, fetch) => {
      const text = await expectStatus(await fetch(`${url}/llms.txt`), 200, "/llms.txt");
      if (!text.trim()) throw new Error("/llms.txt is empty");
    },
  },
  {
    name: "GET /q/<queue>/status",
    run: async (url, fetch) => {
      const text = await expectStatus(await fetch(`${url}/q/smoke-${nonce()}/status`), 200, "queue status");
      const body = JSON.parse(text);
      if (!body || typeof body !== "object" || !("jobs" in body)) {
        throw new Error(`queue status is not a status: ${text.slice(0, 200)}`);
      }
    },
  },
  {
    name: "POST /mcp initialize",
    run: async (url, fetch) => {
      const response = await fetch(`${url}/mcp`, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json, text/event-stream" },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "initialize",
          params: {
            protocolVersion: "2025-03-26",
            capabilities: {},
            clientInfo: { name: "deploy-smoke", version: "1.0.0" },
          },
        }),
      });
      const text = await expectStatus(response, 200, "/mcp initialize");
      // Streamable HTTP answers with either plain JSON or one SSE message
      const json = response.headers.get("content-type")?.includes("text/event-stream")
        ? text.split("\n").find((line) => line.startsWith("data:"))?.slice(5)
        : text;
      const message = json ? JSON.parse(json) : undefined;
      if (!message?.result?.serverInfo?.name) {
        throw new Error(`/mcp initialize gave no serverInfo: ${text.slice(0, 200)}`);
      }
    },
  },
  {
    name: "PUT/exists/GET /f/<key> (blob store)",
    run: async (url, fetch) => {
      const content = `deploy-smoke-${nonce()}`;
      const key = await sha256(content);
      // PUT either stores directly or redirects to a presigned URL
      let response = await fetch(`${url}/f/${key}`, { method: "PUT", body: content, redirect: "manual" });
      if (response.status >= 300 && response.status < 400) {
        await response.body?.cancel();
        const location = response.headers.get("location");
        if (!location) throw new Error("upload redirect without location");
        response = await fetch(new URL(location, url), { method: "PUT", body: content });
      }
      const uploaded = await response.text();
      if (!response.ok) throw new Error(`upload: status ${response.status}: ${uploaded.slice(0, 200)}`);

      const exists = JSON.parse(await expectStatus(await fetch(`${url}/f/${key}/exists`), 200, "exists"));
      if (exists?.exists !== true) throw new Error(`exists: ${JSON.stringify(exists)}`);

      const downloaded = await expectStatus(await fetch(`${url}/f/${key}`), 200, "download");
      if (downloaded !== content) throw new Error("downloaded blob differs from the uploaded one");
    },
  },
];

/** Run every check (not stopping at the first failure) */
export const runChecks = async (
  url: string,
  list: SmokeCheck[] = checks,
  { requestTimeoutMs = DefaultRequestTimeoutMs } = {},
): Promise<SmokeResult[]> => {
  const results: SmokeResult[] = [];
  for (const check of list) {
    try {
      await check.run(url, timedFetch(requestTimeoutMs));
      results.push({ name: check.name, ok: true });
    } catch (err) {
      results.push({ name: check.name, ok: false, error: err instanceof Error ? err.message : String(err) });
    }
  }
  return results;
};

const flag = (name: string, fallback: number): number => {
  const arg = Deno.args.find((a) => a.startsWith(`--${name}=`));
  const value = arg ? Number(arg.split("=")[1]) : fallback;
  if (!Number.isFinite(value) || value < 0) {
    console.error(`--${name} must be a number of seconds`);
    Deno.exit(1);
  }
  return value;
};

if (import.meta.main) {
  const url = Deno.args.find((a) => !a.startsWith("--"))?.replace(/\/+$/, "");
  if (!url || !/^https?:\/\//.test(url)) {
    console.error("usage: smoke.ts <url> [--settle=45] [--timeout=300] [--request-timeout=30]");
    Deno.exit(1);
  }
  const requestTimeoutMs = flag("request-timeout", DefaultRequestTimeoutMs / 1000) * 1000;
  try {
    await waitForSettle(url, {
      settleMs: flag("settle", 45) * 1000,
      timeoutMs: flag("timeout", 300) * 1000,
      requestTimeoutMs,
    });
  } catch (err) {
    console.error(`❌ ${err instanceof Error ? err.message : err}`);
    Deno.exit(1);
  }
  const results = await runChecks(url, checks, { requestTimeoutMs });
  for (const r of results) {
    console.log(r.ok ? `✅ ${r.name}` : `❌ ${r.name}: ${r.error}`);
  }
  const failed = results.filter((r) => !r.ok).length;
  if (failed > 0) {
    console.error(`❌ smoke check of ${url}: ${failed}/${results.length} failed`);
    Deno.exit(1);
  }
  console.log(`✅ smoke check of ${url}: ${results.length}/${results.length} passed`);
}
