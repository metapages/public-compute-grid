/**
 * Deterministic coverage for the HTTP surface the `compute-queues` skill
 * depends on. No LLM involved — these assert that the endpoints the skill (and
 * its `cq` helper) call actually behave the way the skill documents:
 *
 *   - build logs and run logs are separate, retrievable streams
 *   - a failed build surfaces the docker error in the BUILD log
 *   - `?since=` paging returns only the tail
 *   - the SSE stream emits build-log / run-log / state / final
 *   - a locally-built tarball works as a build context, including when the URL
 *     has no archive suffix and the archive has no wrapper directory
 *   - inputs (inline and uploaded) reach /inputs, outputs come back out
 *
 * The LLM-in-the-loop counterpart is `just test-skill-ai`.
 */
import { assert, assertEquals, assertExists } from "std/assert";
import { delay } from "std/async/delay";
import { join } from "std/path";

import { type ConsoleLogLine, fetchRobust } from "@metapages/compute-queues-shared";

const fetch = fetchRobust;

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL") ||
  (QUEUE_ID === "local" ? "http://worker:8000" : "http://api1:8081");

const JOB_TIMEOUT_MS = 5 * 60 * 1000;

interface LogSlice {
  data: ConsoleLogLine[];
  sliceStart: number;
  nextCursor: number;
  isFinal: boolean;
}

// deno-lint-ignore no-explicit-any
type Definition = Record<string, any>;

/** Every test needs a distinct job: jobId is the hash of the definition. */
const withNonce = (definition: Definition): Definition => ({
  ...definition,
  env: { ...(definition.env || {}), TEST_NONCE: crypto.randomUUID() },
});

/**
 * A nonce in `env` makes the JOB unique but not the IMAGE: the built image is
 * keyed on the `build` block alone, so an unchanged Dockerfile is served from
 * the worker's image cache and emits no build logs at all. Tests that assert on
 * build output must therefore vary the Dockerfile itself.
 */
const uniqueDockerfile = (lines: string[]): string =>
  [...lines, `RUN echo cache-buster-${crypto.randomUUID()} > /dev/null`].join("\n");

const submit = async (definition: Definition): Promise<string> => {
  const res = await fetch(`${API_URL}/q/${QUEUE_ID}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ definition }),
  });
  assertEquals(res.status, 200, `submit failed: ${res.status}`);
  const body = await res.json();
  assert(body.jobId, `submit returned no jobId: ${JSON.stringify(body)}`);
  return body.jobId as string;
};

const getJson = async (path: string) => {
  const res = await fetch(`${API_URL}${path}`, { redirect: "follow" });
  assert(res.ok, `GET ${path} -> ${res.status}`);
  return await res.json();
};

const getLogs = (jobId: string, kind: "build" | "run", since = 0): Promise<LogSlice> =>
  getJson(`/q/${QUEUE_ID}/j/${jobId}/${kind}-logs.json?since=${since}`);

// deno-lint-ignore no-explicit-any
const waitForResult = async (jobId: string): Promise<any> => {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const { data } = await getJson(`/q/${QUEUE_ID}/j/${jobId}/result.json`);
    if (data?.state === "Finished") return data;
    await delay(500);
  }
  throw new Error(`timed out waiting for job ${jobId}`);
};

const logText = (lines: ConsoleLogLine[]): string => lines.map((l) => l[0]).join("\n");

/** Upload bytes under an arbitrary key; returns the URL the worker will fetch. */
const uploadBlob = async (bytes: Uint8Array, key: string): Promise<string> => {
  const res = await fetch(`${API_URL}/f/${key}`, {
    method: "PUT",
    body: bytes,
    redirect: "follow",
  });
  assert(res.ok, `upload ${key} -> ${res.status}`);
  await res.body?.cancel();
  return `${API_URL}/f/${key}`;
};

Deno.test("skill: build from an inline Dockerfile — build logs, run logs, outputs", async () => {
  const jobId = await submit(
    withNonce({
      build: {
        dockerfile: uniqueDockerfile([
          "FROM alpine:3.19.1",
          "RUN echo BUILD_STEP_MARKER",
        ]),
      },
      command: `sh -c 'echo RUN_STEP_MARKER; echo payload > /outputs/out.txt'`,
    }),
  );

  const result = await waitForResult(jobId);
  assertEquals(result.finishedReason, "Success");
  assertEquals(result.finished?.result?.StatusCode, 0, "container should exit 0");

  // Build and run logs must be separate, and each must contain only its own marker.
  const build = await getLogs(jobId, "build");
  const run = await getLogs(jobId, "run");
  assert(build.isFinal, "build logs should report isFinal for a finished job");
  assert(
    logText(build.data).includes("BUILD_STEP_MARKER"),
    `build log missing the build marker:\n${logText(build.data)}`,
  );
  assert(
    !logText(build.data).includes("RUN_STEP_MARKER"),
    "run output leaked into the build log",
  );
  assert(
    logText(run.data).includes("RUN_STEP_MARKER"),
    `run log missing the run marker:\n${logText(run.data)}`,
  );

  // The output file is retrievable by name, already decoded.
  const res = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/outputs/out.txt`, { redirect: "follow" });
  assert(res.ok, `outputs/out.txt -> ${res.status}`);
  assertEquals((await res.text()).trim(), "payload");
});

Deno.test("skill: a failed build reports Error and puts the docker error in the build log", async () => {
  const jobId = await submit(
    withNonce({
      build: {
        dockerfile: uniqueDockerfile(["FROM alpine:3.19.1", "RUN this-command-does-not-exist"]),
      },
      command: "echo never-reached",
    }),
  );

  const result = await waitForResult(jobId);
  assertEquals(result.finishedReason, "Error", "a build failure is a job-level Error");

  const build = await getLogs(jobId, "build");
  const text = logText(build.data);
  assert(
    text.includes("this-command-does-not-exist"),
    `build log should name the failing command:\n${text}`,
  );
  assert(text.includes("not found") || text.includes("exit code"), `build log should show the failure:\n${text}`);

  // The program never ran, so there is nothing in the run log. This is the
  // signal that tells a caller to fix the Dockerfile rather than the program.
  const run = await getLogs(jobId, "run");
  assertEquals(run.data.length, 0, `expected no run logs, got:\n${logText(run.data)}`);
});

Deno.test("skill: a crashed program still finishes Successfully but with a non-zero StatusCode", async () => {
  const jobId = await submit(
    withNonce({
      image: "alpine:3.19.1",
      command: `sh -c 'echo CRASH_MARKER >&2; exit 7'`,
    }),
  );

  const result = await waitForResult(jobId);
  // This is the trap the skill warns about: the JOB succeeded, the PROGRAM did not.
  assertEquals(result.finishedReason, "Success");
  assertEquals(result.finished?.result?.StatusCode, 7);

  const run = await getLogs(jobId, "run");
  assert(logText(run.data).includes("CRASH_MARKER"), "stderr should reach the run log");
});

Deno.test("skill: ?since= returns only the tail of the log", async () => {
  const jobId = await submit(
    withNonce({
      image: "alpine:3.19.1",
      command: `sh -c 'for i in 1 2 3 4 5; do echo LINE_$i; done'`,
    }),
  );
  await waitForResult(jobId);

  const all = await getLogs(jobId, "run");
  assert(all.nextCursor > 0, "expected some run log lines");

  const tail = await getLogs(jobId, "run", all.nextCursor - 1);
  assertEquals(tail.sliceStart, all.nextCursor - 1);
  assertEquals(tail.nextCursor, all.nextCursor);
  assertEquals(tail.data.length, 1, "since=nextCursor-1 should return exactly the last line");

  const past = await getLogs(jobId, "run", all.nextCursor);
  assertEquals(past.data.length, 0, "since=nextCursor should return nothing");
});

Deno.test("skill: the SSE stream emits build-log, run-log, state and final", async () => {
  const jobId = await submit(
    withNonce({
      build: { dockerfile: uniqueDockerfile(["FROM alpine:3.19.1", "RUN echo SSE_BUILD_MARKER"]) },
      command: `sh -c 'echo SSE_RUN_MARKER'`,
    }),
  );

  const res = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/stream`, {
    headers: { accept: "text/event-stream" },
  });
  assertEquals(res.status, 200, "stream endpoint should exist");
  assertExists(res.body);

  const events: { event: string; data: string }[] = [];
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  let sawFinal = false;

  while (!sawFinal && Date.now() < deadline) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let split: number;
    while ((split = buffer.indexOf("\n\n")) !== -1) {
      const frame = buffer.slice(0, split);
      buffer = buffer.slice(split + 2);
      let event = "message";
      const data: string[] = [];
      for (const line of frame.split("\n")) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) data.push(line.slice(5).trim());
      }
      if (data.length) events.push({ event, data: data.join("\n") });
      if (event === "final") sawFinal = true;
    }
  }
  await reader.cancel().catch(() => {});

  assert(sawFinal, `stream never emitted a final event; got: ${events.map((e) => e.event).join(", ")}`);

  const kinds = events.map((e) => e.event);
  assert(kinds.includes("state"), "expected at least one state event");
  assert(kinds.includes("build-log"), `expected build-log events, got: ${kinds.join(", ")}`);
  assert(kinds.includes("run-log"), `expected run-log events, got: ${kinds.join(", ")}`);

  const buildText = events.filter((e) => e.event === "build-log").map((e) => e.data).join("\n");
  const runText = events.filter((e) => e.event === "run-log").map((e) => e.data).join("\n");
  assert(buildText.includes("SSE_BUILD_MARKER"), "build marker missing from streamed build logs");
  assert(runText.includes("SSE_RUN_MARKER"), "run marker missing from streamed run logs");

  const final = JSON.parse(events[events.length - 1].data);
  assertEquals(final.state, "Finished");
  assertEquals(final.reason, "Success");
});

Deno.test("skill: a locally-built tarball works as a build context (no suffix, no wrapper dir)", async () => {
  const contextDir = await Deno.makeTempDir({ prefix: "cq-ctx-" });
  const archivePath = await Deno.makeTempFile({ suffix: ".tar.gz" });
  try {
    await Deno.mkdir(join(contextDir, "src"), { recursive: true });
    await Deno.writeTextFile(
      join(contextDir, "src", "hello.txt"),
      "CONTEXT_FILE_MARKER\n",
    );
    await Deno.writeTextFile(
      join(contextDir, "Dockerfile"),
      ["FROM alpine:3.19.1", "COPY src/hello.txt /hello.txt"].join("\n"),
    );

    // Built with the system `tar`, exactly as the skill's cq helper does. (The
    // `compress` module's tgz.compress relies on Deno APIs removed in Deno 2;
    // the worker only ever calls its uncompress side.) `-C dir .` puts entries
    // at the archive root, so there is no wrapper directory to hoist.
    const tar = await new Deno.Command("tar", {
      args: ["czf", archivePath, "-C", contextDir, "."],
      stderr: "piped",
    }).output();
    assert(
      tar.success,
      `failed to create the test tarball with \`tar\`: ${new TextDecoder().decode(tar.stderr)}`,
    );
    const bytes = await Deno.readFile(archivePath);

    // Deliberately no archive suffix on the key: the worker must identify the
    // format from the gzip magic bytes, the way a content-addressed blob URL
    // forces it to.
    const contextUrl = await uploadBlob(bytes, `${crypto.randomUUID().replace(/-/g, "")}`);

    const jobId = await submit(
      withNonce({
        build: { context: contextUrl },
        command: `sh -c 'cat /hello.txt'`,
      }),
    );

    const result = await waitForResult(jobId);
    const build = await getLogs(jobId, "build");
    assertEquals(
      result.finishedReason,
      "Success",
      `build from tarball context failed:\n${logText(build.data)}`,
    );
    assertEquals(result.finished?.result?.StatusCode, 0);

    const run = await getLogs(jobId, "run");
    assert(
      logText(run.data).includes("CONTEXT_FILE_MARKER"),
      `the context file was not copied into the image:\n${logText(run.data)}`,
    );
  } finally {
    await Deno.remove(contextDir, { recursive: true }).catch(() => {});
    await Deno.remove(archivePath).catch(() => {});
  }
});

Deno.test("skill: inputs arrive in /inputs, inline and uploaded alike", async () => {
  const smallValue = "inline-input-marker";
  // Comfortably over the 200-byte inline threshold, so this one must round-trip
  // through blob storage.
  const bigValue = "X".repeat(4096);
  const bigUrl = await uploadBlob(new TextEncoder().encode(bigValue), await sha256Hex(bigValue));

  const jobId = await submit(
    withNonce({
      image: "alpine:3.19.1",
      inputs: {
        "small.txt": { type: "base64", value: btoa(smallValue) },
        "big.txt": { type: "url", value: bigUrl },
      },
      command:
        `sh -c 'ls /inputs; cat /inputs/small.txt; wc -c < /inputs/big.txt; cp /inputs/small.txt /outputs/echoed.txt'`,
    }),
  );

  const result = await waitForResult(jobId);
  const run = await getLogs(jobId, "run");
  assertEquals(result.finishedReason, "Success", `job failed:\n${logText(run.data)}`);
  assertEquals(result.finished?.result?.StatusCode, 0, `container failed:\n${logText(run.data)}`);

  const text = logText(run.data);
  assert(text.includes(smallValue), `inline input missing from /inputs:\n${text}`);
  assert(text.includes("4096"), `uploaded input did not arrive at full size:\n${text}`);

  const res = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/outputs/echoed.txt`, { redirect: "follow" });
  assert(res.ok, `outputs/echoed.txt -> ${res.status}`);
  assertEquals((await res.text()).trim(), smallValue);
});

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
