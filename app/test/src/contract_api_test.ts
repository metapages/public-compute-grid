/**
 * API contract tests: the HTTP/SSE/websocket surface every server
 * implementation must keep, exercised over the wire only.
 *
 * These run against any API_URL — the local-mode worker, the compose stack
 * (Deno), the Cloudflare stack (wrangler dev) and staging — so they must not
 * import server internals or touch the database. They are the safety net for
 * the Deno Deploy → Cloudflare migration (issue #308) and for the later
 * server/worker/cli unification: if two servers pass this file, clients can't
 * tell them apart.
 *
 * Routes only one implementation serves today are marked `remoteOnly`, so the
 * gap between the local handler and the api is visible in one place.
 */
import { assert, assertEquals, assertStringIncludes } from "std/assert";

import {
  type BroadcastJobStates,
  createNewContainerJobMessage,
  DockerJobState,
  fetchRobust,
  type WebsocketMessageServerBroadcast,
  WebsocketMessageTypeServerBroadcast,
} from "@metapages/compute-queues-shared";

import { API_URL, QUEUE_ID } from "./util.ts";

// Plain fetch for anything asserting a status: fetchRobust retries every 4xx/5xx
const fetch = globalThis.fetch;

const isLocal = QUEUE_ID === "local";
// Routes the local-mode worker does not serve (yet)
const remoteOnly = isLocal;

const nonce = () => `${Date.now()}-${Math.floor(Math.random() * 1_000_000_000)}`;

type SseEvent = { event: string; data: unknown };

/**
 * Read a Server-Sent Events response until `final` (or timeout), returning
 * every event in order.
 */
const readSse = async (url: string, timeoutMs = 120_000): Promise<SseEvent[]> => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const events: SseEvent[] = [];
  try {
    const response = await fetch(url, {
      headers: { Accept: "text/event-stream" },
      signal: controller.signal,
    });
    assertEquals(response.status, 200, `SSE ${url} status`);
    assertStringIncludes(response.headers.get("content-type") || "", "text/event-stream");
    const reader = response.body!.pipeThrough(new TextDecoderStream()).getReader();
    let buffer = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += value;
      let split: number;
      while ((split = buffer.indexOf("\n\n")) >= 0) {
        const block = buffer.slice(0, split);
        buffer = buffer.slice(split + 2);
        let event = "message";
        const dataLines: string[] = [];
        for (const line of block.split("\n")) {
          if (line.startsWith("event:")) event = line.slice(6).trim();
          else if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
        }
        if (dataLines.length === 0) continue;
        const raw = dataLines.join("\n");
        let data: unknown = raw;
        try {
          data = JSON.parse(raw);
        } catch { /* keep raw text */ }
        events.push({ event, data });
        if (event === "final") {
          await reader.cancel();
          return events;
        }
      }
    }
    return events;
  } finally {
    clearTimeout(timer);
  }
};

/** Submit a job over HTTP, as a script or another server would. */
const submitJob = async (command: string, namespace?: string) => {
  const { jobId, queuedJob } = await createNewContainerJobMessage({
    definition: { image: "alpine:3.18.5", command },
    control: namespace ? { namespace } : undefined,
  });
  const response = await fetchRobust(`${API_URL}/q/${QUEUE_ID}`, {
    method: "POST",
    body: JSON.stringify(queuedJob!.enqueued),
    headers: { "Content-Type": "application/json" },
  });
  const body = await response.text();
  assert(response.ok, `submit failed: ${response.status} ${body}`);
  return { jobId };
};

const getJson = async <T = unknown>(path: string): Promise<{ status: number; body: T }> => {
  const response = await fetch(`${API_URL}${path}`);
  const text = await response.text();
  let body: unknown = text;
  try {
    body = JSON.parse(text);
  } catch { /* not json */ }
  return { status: response.status, body: body as T };
};

type LogsResponse = {
  data: [string, number, boolean?][];
  sliceStart: number;
  nextCursor: number;
  isFinal: boolean;
};

Deno.test("contract: healthz", async () => {
  const response = await fetch(`${API_URL}/healthz`);
  assertEquals(response.status, 200);
  assertEquals(await response.text(), "OK");
});

Deno.test(
  "contract: job submitted over HTTP is followed to completion over SSE, then served by every job endpoint",
  async () => {
    const marker = nonce();
    const namespace = `contract-${marker}`;
    const { jobId } = await submitJob(
      `sh -c 'echo out-${marker}; echo err-${marker} >&2'`,
      namespace,
    );

    // SSE: one request follows the job to the end
    const events = await readSse(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/stream`);
    const final = events.find((e) => e.event === "final");
    assert(final, `no final event, got: ${JSON.stringify(events.map((e) => e.event))}`);
    assertEquals((final.data as { state: string }).state, DockerJobState.Finished);
    assertEquals((final.data as { reason: string }).reason, "Success");
    const runLines = events
      .filter((e) => e.event === "run-log")
      .flatMap((e) => (e.data as { lines: [string][] }).lines.map((l) => l[0]));
    assert(
      runLines.some((l) => l.includes(`out-${marker}`)),
      `stdout missing from run-log events: ${JSON.stringify(runLines)}`,
    );

    // result.json, queue-scoped and global
    for (const path of [`/q/${QUEUE_ID}/j/${jobId}/result.json`, `/j/${jobId}/result.json`]) {
      const { status, body } = await getJson<{ data: { finished?: { result?: { StatusCode?: number } } } }>(path);
      assertEquals(status, 200, path);
      assert(body.data, `${path} has no data`);
    }

    // definition.json
    for (const path of [`/q/${QUEUE_ID}/j/${jobId}/definition.json`, `/j/${jobId}/definition.json`]) {
      const { status, body } = await getJson<{ command?: string; image?: string }>(path);
      assertEquals(status, 200, path);
      assertStringIncludes(JSON.stringify(body), `out-${marker}`, path);
    }

    // run logs: separated from build logs, paged by cursor, final once finished
    for (const prefix of [`/q/${QUEUE_ID}/j/${jobId}`, `/j/${jobId}`]) {
      const { status, body } = await getJson<LogsResponse>(`${prefix}/run-logs.json`);
      assertEquals(status, 200, `${prefix}/run-logs.json`);
      const text = body.data.map((l) => l[0]).join("\n");
      assertStringIncludes(text, `out-${marker}`);
      assertStringIncludes(text, `err-${marker}`);
      assertEquals(body.isFinal, true);
      assertEquals(body.sliceStart, 0);
      assertEquals(body.nextCursor, body.data.length);

      const paged = await getJson<LogsResponse>(`${prefix}/run-logs.json?since=${body.nextCursor}`);
      assertEquals(paged.body.data.length, 0, "since=nextCursor returns nothing new");
      assertEquals(paged.body.sliceStart, body.nextCursor);

      const build = await getJson<LogsResponse>(`${prefix}/build-logs.json`);
      assertEquals(build.status, 200, `${prefix}/build-logs.json`);
      assert(Array.isArray(build.body.data));
      assert(
        !build.body.data.some((l) => l[0].includes(`out-${marker}`)),
        "program output leaked into build logs",
      );
    }

    const badSince = await getJson(`/q/${QUEUE_ID}/j/${jobId}/run-logs.json?since=abc`);
    assertEquals(badSince.status, 400);

    // job state json
    const job = await getJson<{ data?: unknown }>(`/j/${jobId}.json`);
    assertEquals(job.status, 200);

    // the queue lists it (finished jobs stay in the queue for a while)
    const queue = await getJson<{ data: Record<string, { state: string }> }>(`/q/${QUEUE_ID}`);
    assertEquals(queue.status, 200);
    assert(queue.body.data[jobId], "finished job not listed in queue");

    // Replaying the stream of a finished job yields its history and final at once
    const replay = await readSse(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/stream`, 15_000);
    assert(replay.some((e) => e.event === "final"), "replay of finished job has no final");
  },
);

Deno.test("contract: namespaces.json", { ignore: remoteOnly }, async () => {
  const marker = nonce();
  const namespace = `contract-ns-${marker}`;
  const { jobId } = await submitJob(`echo ns-${marker}`, namespace);
  await readSse(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/stream`);
  const { status, body } = await getJson<unknown>(`/q/${QUEUE_ID}/j/${jobId}/namespaces.json`);
  assertEquals(status, 200);
  assertStringIncludes(JSON.stringify(body), namespace);
});

Deno.test(
  "contract: websocket client gets a snapshot on connect and state updates for a job submitted over HTTP",
  async () => {
    // The websocket and the HTTP submit can land on different server instances
    // (or, on Cloudflare, the Worker and the queue's Durable Object). The client
    // must still see the job go all the way to Finished.
    const wsUrl = `${API_URL.replace(/^http/, "ws")}/q/${QUEUE_ID}/client`;
    const socket = new WebSocket(wsUrl);
    const states = new Map<string, string>();
    let gotSnapshot = false;
    let targetJobId: string | undefined;
    const { promise: finished, resolve: resolveFinished, reject } = Promise.withResolvers<void>();
    const timer = setTimeout(() => reject(new Error(`timed out, states=${JSON.stringify([...states])}`)), 120_000);

    socket.onmessage = (event: MessageEvent) => {
      const message: WebsocketMessageServerBroadcast = JSON.parse(event.data.toString());
      if (
        message.type !== WebsocketMessageTypeServerBroadcast.JobStates &&
        message.type !== WebsocketMessageTypeServerBroadcast.JobStateUpdates
      ) {
        return;
      }
      if (message.type === WebsocketMessageTypeServerBroadcast.JobStates) gotSnapshot = true;
      const payload = message.payload as BroadcastJobStates;
      for (const [jobId, job] of Object.entries(payload?.state?.jobs || {})) {
        states.set(jobId, job.state);
        if (jobId === targetJobId && job.state === DockerJobState.Finished) resolveFinished();
      }
    };
    const { promise: opened, resolve: resolveOpened } = Promise.withResolvers<void>();
    socket.onopen = () => resolveOpened();
    await opened;

    try {
      const marker = nonce();
      const { jobId } = await submitJob(`echo ws-${marker}`, `contract-ws-${marker}`);
      targetJobId = jobId;
      if (states.get(jobId) === DockerJobState.Finished) resolveFinished();
      await finished;
      assert(gotSnapshot, "no JobStates snapshot received");
    } finally {
      clearTimeout(timer);
      const { promise: closed, resolve: resolveClosed } = Promise.withResolvers<void>();
      socket.onclose = () => resolveClosed();
      socket.close();
      await closed;
    }
  },
);

Deno.test("contract: blob upload, exists, download via /f/<key>", async () => {
  const content = `blob-${nonce()}`;
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(content));
  const key = Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");

  // PUT either stores directly or redirects to a presigned URL
  let response = await fetch(`${API_URL}/f/${key}`, { method: "PUT", body: content, redirect: "manual" });
  if (response.status >= 300 && response.status < 400) {
    await response.body?.cancel();
    const location = response.headers.get("location");
    assert(location, "upload redirect without location");
    response = await fetch(new URL(location, API_URL), { method: "PUT", body: content });
  }
  const uploadBody = await response.text();
  assert(response.ok, `upload failed: ${response.status} ${uploadBody}`);

  const exists = await getJson<{ exists: boolean }>(`/f/${key}/exists`);
  assertEquals(exists.status, 200);
  assertEquals(exists.body.exists, true);

  const download = await fetch(`${API_URL}/f/${key}`);
  assertEquals(download.status, 200);
  assertEquals(await download.text(), content);

  const missing = await getJson<{ exists: boolean }>(`/f/${"0".repeat(64)}/exists`);
  assertEquals(missing.status, 404);
});

Deno.test("contract: queue status", async () => {
  const { status, body } = await getJson<unknown>(`/q/${QUEUE_ID}/status`);
  assertEquals(status, 200);
  assert(body && typeof body === "object");
});

Deno.test("contract: browser app routes", async (t) => {
  const index = await fetch(`${API_URL}/`);
  const indexHtml = await index.text();
  if (index.status === 404) {
    // Test stacks may run without a browser build; there is nothing to check
    console.log("contract: no browser build served, skipping static route checks");
    return;
  }
  assertEquals(index.status, 200);
  assertStringIncludes(indexHtml.toLowerCase(), "<html");

  await t.step("/j/<jobId> serves the app with assets rebased to /", async () => {
    const response = await fetch(`${API_URL}/j/${"a".repeat(64)}`);
    assertEquals(response.status, 200);
    assertStringIncludes(await response.text(), '<base href="/"');
  });
});

Deno.test("contract: llms.txt", { ignore: remoteOnly }, async () => {
  const response = await fetch(`${API_URL}/llms.txt`);
  assertEquals(response.status, 200);
  assert((await response.text()).length > 0);
});

Deno.test("contract: docs routes", { ignore: remoteOnly }, async () => {
  const redirect = await fetch(`${API_URL}/docs`, { redirect: "manual" });
  await redirect.body?.cancel();
  assertEquals(redirect.status, 301);
  assert(redirect.headers.get("location")?.endsWith("/docs/"));

  // A missing doc is a 404, never the browser app's index.html
  const missing = await fetch(`${API_URL}/docs/no-such-page-${nonce()}`);
  await missing.body?.cancel();
  assertEquals(missing.status, 404);
});
