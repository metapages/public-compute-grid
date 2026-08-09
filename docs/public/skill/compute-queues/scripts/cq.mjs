#!/usr/bin/env node
/**
 * cq — build, run and debug Docker containers on a compute queue.
 *
 * Zero dependencies. Runs under node >= 18 or deno.
 *
 *   node cq.mjs run --image alpine:3.19.1 --command 'echo hi'
 *   deno run -A cq.mjs run --dockerfile ./Dockerfile --command 'python /app/go.py'
 *
 * Environment:
 *   CQ_API     API base URL   (default https://container.mtfm.io)
 *   CQ_QUEUE   queue name     (default public1)
 *
 * Exit codes: 0 success · 1 usage/transport/job-level failure ·
 * otherwise the container's own exit code.
 */

import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { readFileSync as readSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * A development checkout can redirect the defaults at a local stack, so an
 * agent using the symlinked skill cannot accidentally submit real jobs to the
 * shared production queue. `just dev-install-skill` writes dev-target.json next
 * to this script; it is gitignored and never part of a published release, so
 * for everyone else this is a no-op and the defaults stay production.
 *
 * Precedence: explicit --api/--queue > CQ_API/CQ_QUEUE > dev-target.json >
 * production.
 */
const readDevTarget = () => {
  try {
    const here = dirname(fileURLToPath(import.meta.url));
    const raw = readSync(join(here, "dev-target.json"), "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed?.api) return undefined;
    return parsed;
  } catch {
    return undefined;
  }
};

const DEV_TARGET = readDevTarget();

const DEFAULT_API = process.env.CQ_API || DEV_TARGET?.api ||
  "https://container.mtfm.io";
const DEFAULT_QUEUE = process.env.CQ_QUEUE || DEV_TARGET?.queue || "public1";

/**
 * Announce a redirected default loudly and once. Silently talking to a
 * different API than the docs advertise is exactly the kind of surprise that
 * wastes an hour.
 */
const noteDevTarget = () => {
  if (!DEV_TARGET || process.env.CQ_API) return;
  process.stderr.write(
    `cq: development target active — ${DEFAULT_API} (queue ${DEFAULT_QUEUE})\n` +
      `    from ${DEV_TARGET.source || "dev-target.json"}; production is NOT being used.\n`,
  );
};

/** Payloads at or under this many bytes ride inline in the definition. */
const INLINE_MAX_BYTES = 200;

// ---------------------------------------------------------------------------
// tiny utils
// ---------------------------------------------------------------------------

const die = (msg, code = 1) => {
  process.stderr.write(`cq: ${msg}\n`);
  process.exit(code);
};

const log = (msg) => process.stderr.write(`${msg}\n`);
const out = (msg) => process.stdout.write(`${msg}\n`);

const sha256 = async (bytes) => {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0"))
    .join("");
};

const toBytes = (
  v,
) => (typeof v === "string" ? new TextEncoder().encode(v) : new Uint8Array(v));

/** base64 of a Uint8Array, without Buffer (so it works identically under deno). */
const b64 = (bytes) => {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * The API is chatty over HTTP and this is often run against a local dev stack
 * with a self-signed cert, so surface transport failures clearly rather than
 * letting an opaque fetch error escape.
 */
const api = async (url, init) => {
  let res;
  try {
    res = await fetch(url, init);
  } catch (err) {
    die(
      `cannot reach ${url}: ${err?.message || err}\n` +
        `  Is the API up? For a local stack, pass --api https://worker-metaframe.localhost:<port> ` +
        `and set NODE_TLS_REJECT_UNAUTHORIZED=0 if it uses a self-signed cert.`,
    );
  }
  return res;
};

/**
 * Like apiJson, but returns undefined instead of exiting when the response is
 * not JSON. Used where a missing endpoint is a survivable difference between
 * API versions rather than a fatal error.
 */
const maybeJson = async (url, init) => {
  const res = await api(url, init);
  if (!res.ok) {
    res.body?.cancel().catch(() => {});
    return undefined;
  }
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
};

const apiJson = async (url, init) => {
  const res = await api(url, init);
  const text = await res.text();
  if (!res.ok) {
    die(
      `${init?.method || "GET"} ${url} -> ${res.status}: ${text.slice(0, 400)}`,
    );
  }
  try {
    return JSON.parse(text);
  } catch {
    die(`${url} did not return JSON: ${text.slice(0, 200)}`);
  }
};

// ---------------------------------------------------------------------------
// arg parsing
// ---------------------------------------------------------------------------

/**
 * Flags are `--name value` or `--name=value`; `--flag` alone is boolean true.
 * Repeatable flags (env, input, config, build-arg) collect into arrays.
 */
const REPEATABLE = new Set(["env", "input", "config", "build-arg"]);

const parseArgs = (argv) => {
  const flags = {};
  const positional = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith("--")) {
      positional.push(arg);
      continue;
    }
    let name = arg.slice(2);
    let value;
    const eq = name.indexOf("=");
    if (eq !== -1) {
      value = name.slice(eq + 1);
      name = name.slice(0, eq);
    } else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
      value = argv[++i];
    } else {
      value = true;
    }
    if (REPEATABLE.has(name)) {
      (flags[name] ||= []).push(value);
    } else {
      flags[name] = value;
    }
  }
  return { flags, positional };
};

// ---------------------------------------------------------------------------
// blob upload
// ---------------------------------------------------------------------------

/**
 * Upload bytes and return a DataRef. The blob key is the sha256 of the
 * content, so uploading the same bytes twice is a no-op and every caller that
 * has the same file converges on the same URL.
 */
const uploadBytes = async (apiUrl, bytes, { suffix = "" } = {}) => {
  const hash = await sha256(bytes);
  const key = `${hash}${suffix}`;
  const exists = await api(`${apiUrl}/f/${key}/exists`);
  if (exists.status !== 200) {
    const res = await api(`${apiUrl}/f/${key}`, {
      method: "PUT",
      body: bytes,
      redirect: "follow",
    });
    if (!res.ok) {
      die(`upload of ${key} failed: ${res.status} ${await res.text()}`);
    }
  }
  return {
    hash,
    key,
    url: `${apiUrl}/f/${key}`,
    ref: { type: "url", value: `${apiUrl}/f/${key}` },
  };
};

/**
 * Turn `name=value` / `name=@path` into a DataRef, inlining small payloads and
 * uploading anything bigger. Inlining matters: a definition that carries its
 * inputs is self-contained and its jobId still hashes deterministically.
 */
const resolveDataRef = async (apiUrl, spec, kind) => {
  const eq = spec.indexOf("=");
  if (eq === -1) {
    die(`--${kind} expects name=value or name=@path, got: ${spec}`);
  }
  const name = spec.slice(0, eq);
  const raw = spec.slice(eq + 1);

  let bytes;
  if (raw.startsWith("@")) {
    const path = raw.slice(1);
    try {
      bytes = new Uint8Array(await readFile(path));
    } catch (err) {
      die(`--${kind} ${name}: cannot read ${path}: ${err?.message || err}`);
    }
  } else {
    bytes = toBytes(raw);
  }

  if (bytes.length <= INLINE_MAX_BYTES) {
    return [name, { type: "base64", value: b64(bytes) }];
  }
  const { ref } = await uploadBytes(apiUrl, bytes);
  return [name, ref];
};

/**
 * Package a local directory as a gzipped tar and upload it, yielding a URL
 * usable as `build.context`. Uses the system `tar`; the archive has no wrapper
 * directory, so the worker treats the archive root as the build context.
 */
const uploadContextDir = async (apiUrl, dir) => {
  const info = await stat(dir).catch(() => undefined);
  if (!info?.isDirectory()) die(`--context-dir: not a directory: ${dir}`);

  const archive = join(await mkdtempish(), "context.tar.gz");
  try {
    execFileSync("tar", ["czf", archive, "-C", dir, "."], {
      stdio: ["ignore", "ignore", "pipe"],
    });
  } catch (err) {
    die(
      `failed to tar ${dir} (is \`tar\` installed?): ${err?.stderr?.toString?.() || err?.message || err}`,
    );
  }
  const bytes = new Uint8Array(await readFile(archive));
  // The .tar.gz suffix is belt-and-braces: current workers sniff the archive's
  // magic bytes, but an older worker keys off the URL suffix.
  const { url } = await uploadBytes(apiUrl, bytes, { suffix: ".tar.gz" });
  log(`📦 build context ${dir} (${bytes.length} bytes) -> ${url}`);
  return url;
};

const mkdtempish = async () => {
  const dir = join(tmpdir(), `cq-${Math.random().toString(36).slice(2)}`);
  await mkdir(dir, { recursive: true });
  return dir;
};

// ---------------------------------------------------------------------------
// job definition
// ---------------------------------------------------------------------------

const buildDefinition = async (apiUrl, flags) => {
  const definition = {};

  if (flags.image) definition.image = flags.image;

  const build = {};
  if (flags.dockerfile) {
    try {
      build.dockerfile = await readFile(flags.dockerfile, "utf8");
    } catch (err) {
      die(
        `--dockerfile: cannot read ${flags.dockerfile}: ${err?.message || err}`,
      );
    }
  }
  if (flags["dockerfile-inline"]) build.dockerfile = flags["dockerfile-inline"];
  if (flags["context-dir"]) {
    build.context = await uploadContextDir(apiUrl, flags["context-dir"]);
  }
  if (flags.context) build.context = flags.context;
  if (flags["build-context"]) build.buildContext = flags["build-context"];
  if (flags.filename) build.filename = flags.filename;
  if (flags.target) build.target = flags.target;
  if (flags.platform) build.platform = flags.platform;
  if (flags["build-arg"]) build.buildArgs = flags["build-arg"];
  if (Object.keys(build).length) definition.build = build;

  if (!definition.image && !definition.build) {
    die(
      "need --image, or one of --dockerfile / --dockerfile-inline / --context / --context-dir",
    );
  }

  if (flags.command) definition.command = flags.command;
  if (flags.entrypoint) definition.entrypoint = flags.entrypoint;
  if (flags.workdir) definition.workdir = flags.workdir;
  if (flags["shm-size"]) definition.shmSize = flags["shm-size"];
  if (flags["max-duration"]) definition.maxDuration = flags["max-duration"];

  const env = {};
  for (const spec of flags.env || []) {
    const eq = spec.indexOf("=");
    if (eq === -1) die(`--env expects NAME=VALUE, got: ${spec}`);
    env[spec.slice(0, eq)] = spec.slice(eq + 1);
  }
  // jobId is the sha256 of the definition, so an identical definition returns
  // the cached result instead of running again. --nonce perturbs the definition
  // when a genuine re-run is wanted.
  if (flags.nonce) {
    env.CQ_NONCE = flags.nonce === true ? String(Date.now()) : String(flags.nonce);
  }
  if (Object.keys(env).length) definition.env = env;

  const inputs = {};
  for (const spec of flags.input || []) {
    const [name, ref] = await resolveDataRef(apiUrl, spec, "input");
    inputs[name] = ref;
  }
  if (Object.keys(inputs).length) definition.inputs = inputs;

  const configFiles = {};
  for (const spec of flags.config || []) {
    const [name, ref] = await resolveDataRef(apiUrl, spec, "config");
    configFiles[name] = ref;
  }
  if (Object.keys(configFiles).length) definition.configFiles = configFiles;

  const requirements = {};
  if (flags.cpus) requirements.cpus = Number(flags.cpus);
  if (flags.gpus) requirements.gpus = Number(flags.gpus);
  if (flags.memory) requirements.memory = flags.memory;
  if (flags["max-duration"]) requirements.maxDuration = flags["max-duration"];
  if (Object.keys(requirements).length) definition.requirements = requirements;

  return definition;
};

const submitJob = async (apiUrl, queue, definition, flags) => {
  const body = { definition };
  if (flags.namespace) body.control = { namespace: flags.namespace };
  const res = await apiJson(`${apiUrl}/q/${encodeURIComponent(queue)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.jobId) die(`submit did not return a jobId: ${JSON.stringify(res)}`);
  return res.jobId;
};

/**
 * Short, paste-friendly browser URL: the job id in the path. The client loads
 * the definition from /j/<jobId>/definition.json, so this only works once the
 * job has been submitted.
 */
const shortViewUrl = (apiUrl, jobId, queue) => `${apiUrl}/j/${jobId}#?queue=${encodeURIComponent(queue)}`;

/**
 * Self-contained browser URL: the whole definition in the hash, which the client
 * reads as `#?job=<base64(encodeURIComponent(json))>&queue=<q>`. Needs no server
 * lookup, so it works for a definition that was never submitted.
 */
const viewUrl = (apiUrl, definition, queue) => {
  const encoded = b64(toBytes(encodeURIComponent(JSON.stringify(definition))));
  return `${apiUrl}/#?job=${encoded}&queue=${encodeURIComponent(queue)}`;
};

// ---------------------------------------------------------------------------
// following a job
// ---------------------------------------------------------------------------

const renderLine = (line) => {
  const [text] = line;
  return text.endsWith("\n") ? text.slice(0, -1) : text;
};

/**
 * Follow one job over SSE, printing build and run logs as they arrive.
 * Resolves with the terminal state. Falls back to polling if the stream
 * endpoint is missing (older API deployment).
 */
const followJob = async (apiUrl, queue, jobId, { quiet = false } = {}) => {
  const url = `${apiUrl}/q/${encodeURIComponent(queue)}/j/${jobId}/stream`;
  const res = await api(url, { headers: { accept: "text/event-stream" } });
  // Detect a missing endpoint by content-type, not status. This API serves a
  // single-page app from a catch-all route, so an unknown path comes back as
  // 200 text/html rather than 404 — checking the status alone would send us on
  // to parse HTML as an event stream and hang until the job ended.
  const isEventStream = (res.headers.get("content-type") || "").includes("text/event-stream");
  if (!res.ok || !res.body || !isEventStream) {
    res.body?.cancel().catch(() => {});
    if (!quiet) {
      log("⚠️  this API has no /stream endpoint; falling back to polling");
    }
    return pollJob(apiUrl, queue, jobId, { quiet });
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let final;
  let sawBuild = false;
  let sawRun = false;

  const handle = (event, data) => {
    let payload;
    try {
      payload = JSON.parse(data);
    } catch {
      return;
    }
    if (event === "build-log") {
      if (!sawBuild && !quiet) log("── build ──────────────────────────────");
      sawBuild = true;
      for (const line of payload.lines || []) if (!quiet) out(renderLine(line));
    } else if (event === "run-log") {
      if (!sawRun && !quiet) log("── run ────────────────────────────────");
      sawRun = true;
      for (const line of payload.lines || []) if (!quiet) out(renderLine(line));
    } else if (event === "state") {
      if (!quiet) {
        log(
          `◆ ${payload.state}${payload.reason ? ` (${payload.reason})` : ""}`,
        );
      }
    } else if (event === "final") {
      final = payload;
    }
  };

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    // SSE frames are separated by a blank line.
    let split;
    while ((split = buffer.indexOf("\n\n")) !== -1) {
      const frame = buffer.slice(0, split);
      buffer = buffer.slice(split + 2);
      let event = "message";
      const dataLines = [];
      for (const raw of frame.split("\n")) {
        if (raw.startsWith("event:")) event = raw.slice(6).trim();
        else if (raw.startsWith("data:")) dataLines.push(raw.slice(5).trim());
      }
      if (dataLines.length) handle(event, dataLines.join("\n"));
    }
    if (final) break;
  }
  reader.cancel().catch(() => {});
  return final ||
    { state: "Unknown", reason: "stream ended without a final event" };
};

/** Cursor-paged polling: works anywhere, survives a dropped connection. */
const pollJob = async (
  apiUrl,
  queue,
  jobId,
  { quiet = false, timeoutMs = 30 * 60 * 1000 } = {},
) => {
  const base = `${apiUrl}/q/${encodeURIComponent(queue)}/j/${jobId}`;
  const deadline = Date.now() + timeoutMs;
  let buildCursor = 0;
  let runCursor = 0;
  let interval = 500;
  // An older deployment has no *-logs.json either. Stop asking after the first
  // response that isn't a log slice, rather than dying — the job still runs and
  // the result is still fetchable, we just cannot show progress.
  let logsAvailable = true;

  while (Date.now() < deadline) {
    for (const kind of logsAvailable ? ["build", "run"] : []) {
      const cursor = kind === "build" ? buildCursor : runCursor;
      const slice = await maybeJson(`${base}/${kind}-logs.json?since=${cursor}`);
      if (!slice || !Array.isArray(slice.data)) {
        logsAvailable = false;
        if (!quiet) log("⚠️  this API has no per-job log endpoints; waiting without live logs");
        break;
      }
      for (const line of slice.data) if (!quiet) out(renderLine(line));
      if (kind === "build") buildCursor = slice.nextCursor;
      else runCursor = slice.nextCursor;
    }
    const { data } = await apiJson(`${base}/result.json`);
    if (data?.state === "Finished") {
      return { state: "Finished", reason: data.finishedReason };
    }
    await sleep(interval);
    interval = Math.min(interval * 1.5, 5000);
  }
  die(`timed out after ${Math.round(timeoutMs / 1000)}s waiting for ${jobId}`);
};

const getResult = async (apiUrl, queue, jobId) => {
  const { data } = await apiJson(
    `${apiUrl}/q/${encodeURIComponent(queue)}/j/${jobId}/result.json`,
  );
  return data;
};

const downloadOutputs = async (apiUrl, queue, jobId, result, dir) => {
  const names = Object.keys(result?.finished?.result?.outputs || {});
  if (!names.length) {
    log("no outputs to download");
    return [];
  }
  await mkdir(dir, { recursive: true });
  const written = [];
  for (const name of names) {
    const res = await api(
      `${apiUrl}/q/${encodeURIComponent(queue)}/j/${jobId}/outputs/${name}`,
    );
    if (!res.ok) {
      log(`⚠️  could not download output ${name}: ${res.status}`);
      continue;
    }
    const bytes = new Uint8Array(await res.arrayBuffer());
    const target = join(dir, name);
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, bytes);
    written.push(target);
    log(`⬇️  ${target} (${bytes.length} bytes)`);
  }
  return written;
};

/**
 * Report the two separate verdicts a caller must not conflate: did the JOB
 * complete (finishedReason), and did the PROGRAM succeed (StatusCode). A
 * container that crashed still reports finishedReason "Success".
 */
const reportAndExit = (result, { json = false } = {}) => {
  const reason = result?.finishedReason;
  const inner = result?.finished?.result || {};
  const statusCode = inner.StatusCode;

  if (json) {
    out(
      JSON.stringify(
        {
          state: result?.state,
          finishedReason: reason,
          StatusCode: statusCode,
          duration: inner.duration,
          outputs: Object.keys(inner.outputs || {}),
          error: inner.error ?? null,
        },
        null,
        2,
      ),
    );
  }

  if (reason !== "Success") {
    // inner.error frequently just restates the reason; only add it when it says
    // something new.
    const detail = inner.error && String(inner.error) !== String(reason)
      ? ` — ${typeof inner.error === "string" ? inner.error : JSON.stringify(inner.error)}`
      : "";
    log(`✗ job did not complete: ${reason || "unknown"}${detail}`);
    if (reason === "Error") {
      log(
        `  the image likely failed to build — see the build log above, or: cq logs <jobId> --kind build`,
      );
    }
    process.exit(1);
  }
  if (statusCode !== 0) {
    log(`✗ container exited ${statusCode}`);
    process.exit(
      typeof statusCode === "number" && statusCode !== 0 ? statusCode : 1,
    );
  }
  log(
    `✓ success (exit 0, ${inner.duration ?? "?"}ms, outputs: ${Object.keys(inner.outputs || {}).join(", ") || "none"})`,
  );
  process.exit(0);
};

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

const USAGE = `cq — build, run and debug Docker containers on a compute queue

USAGE
  cq run    [job flags]        submit, stream logs, report result   (the usual one)
  cq submit [job flags]        submit only, print the jobId
  cq logs <jobId>              print logs; --follow to stream
  cq wait <jobId>              wait for completion, then report
  cq result <jobId>            print the result JSON
  cq outputs <jobId>           download output files
  cq upload <path>             upload a file, print its DataRef URL
  cq status                    queue status (are any workers attached?)
  cq url [job flags]           print a self-contained browser URL for a definition

COMMON
  --api <url>                  default $CQ_API or https://container.mtfm.io
  --queue <name>               default $CQ_QUEUE or public1
  --json                       machine-readable output

JOB (for run / submit / url)
  --image <ref>                use an existing image
  --dockerfile <path>          build from a local Dockerfile (contents are inlined)
  --dockerfile-inline <text>   build from a Dockerfile passed as a string
  --context-dir <dir>          tar + upload a local dir as the build context
  --context <url>              git repo or archive URL as the build context
  --build-context <subdir>     subdirectory of the context to build from
  --filename <name>            Dockerfile name within the context
  --target <stage>             multi-stage build target
  --platform <plat>            e.g. linux/amd64
  --build-arg K=V              repeatable
  --command <cmd>              container command
  --entrypoint <cmd>           container entrypoint
  --workdir <dir>              working directory
  --env K=V                    repeatable
  --input name=@path|value     file into /inputs        (repeatable)
  --config name=@path|value    config file into /inputs (repeatable, part of the job hash)
  --max-duration <dur>         e.g. 20m
  --cpus / --gpus / --memory   resource requirements
  --shm-size <size>            /dev/shm size
  --namespace <name>           replace any previous job in this namespace
  --nonce [value]              force a re-run of an otherwise identical job

RUN
  --output-dir <dir>           download outputs here when the job succeeds
  --no-follow                  don't stream logs
  --quiet                      suppress log output

LOGS
  --kind build|run|both        default both
  --since <n>                  start from line n
  --follow                     stream until the job finishes
`;

const cmdRun = async (flags) => {
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);

  const definition = await buildDefinition(apiUrl, flags);
  const jobId = await submitJob(apiUrl, queue, definition, flags);

  log(`▶ job ${jobId}`);
  log(`  queue ${queue}  ·  ${apiUrl}/q/${queue}/j/${jobId}/result.json`);
  log(`  view  ${shortViewUrl(apiUrl, jobId, queue)}`);

  if (flags["no-follow"]) {
    out(jobId);
    return;
  }

  await followJob(apiUrl, queue, jobId, { quiet: !!flags.quiet });
  const result = await getResult(apiUrl, queue, jobId);
  if (!result) die(`job ${jobId} finished but has no result`);

  if (flags["output-dir"] && result.finishedReason === "Success") {
    await downloadOutputs(
      apiUrl,
      queue,
      jobId,
      result,
      String(flags["output-dir"]),
    );
  }
  reportAndExit(result, { json: !!flags.json });
};

const cmdSubmit = async (flags) => {
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);
  const definition = await buildDefinition(apiUrl, flags);
  const jobId = await submitJob(apiUrl, queue, definition, flags);
  if (flags.json) {
    out(
      JSON.stringify(
        {
          jobId,
          queue,
          api: apiUrl,
          view: shortViewUrl(apiUrl, jobId, queue),
          viewSelfContained: viewUrl(apiUrl, definition, queue),
        },
        null,
        2,
      ),
    );
  } else {
    out(jobId);
  }
};

const cmdLogs = async (flags, jobId) => {
  if (!jobId) die("usage: cq logs <jobId>");
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);

  if (flags.follow) {
    const final = await followJob(apiUrl, queue, jobId, {});
    log(`◆ ${final.state} ${final.reason || ""}`);
    return;
  }

  const kind = String(flags.kind || "both");
  const kinds = kind === "both" ? ["build", "run"] : [kind];
  const since = Number(flags.since || 0);
  for (const k of kinds) {
    const slice = await maybeJson(
      `${apiUrl}/q/${encodeURIComponent(queue)}/j/${jobId}/${k}-logs.json?since=${since}`,
    );
    // A deployment without the log endpoints serves its SPA here, so say what
    // is actually wrong rather than reporting unparseable HTML.
    if (!slice || !Array.isArray(slice.data)) {
      die(
        `this API has no ${k}-logs endpoint (it is a newer feature).\n` +
          `  Fall back to: cq result ${jobId} — the result carries the container's logs.`,
      );
    }
    if (flags.json) {
      out(JSON.stringify({ kind: k, ...slice }, null, 2));
      continue;
    }
    if (!slice.data?.length) continue;
    if (kinds.length > 1) log(`── ${k} ──────────────────────────────`);
    for (const line of slice.data) out(renderLine(line));
  }
};

const cmdWait = async (flags, jobId) => {
  if (!jobId) die("usage: cq wait <jobId>");
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);
  await followJob(apiUrl, queue, jobId, { quiet: !!flags.quiet });
  const result = await getResult(apiUrl, queue, jobId);
  if (!result) die(`job ${jobId} has no result`);
  if (flags["output-dir"]) {
    await downloadOutputs(
      apiUrl,
      queue,
      jobId,
      result,
      String(flags["output-dir"]),
    );
  }
  reportAndExit(result, { json: !!flags.json });
};

const cmdResult = async (flags, jobId) => {
  if (!jobId) die("usage: cq result <jobId>");
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);
  const result = await getResult(apiUrl, queue, jobId);
  out(JSON.stringify(result, null, 2));
};

const cmdOutputs = async (flags, jobId) => {
  if (!jobId) die("usage: cq outputs <jobId> [--output-dir dir]");
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);
  const result = await getResult(apiUrl, queue, jobId);
  if (!result) die(`job ${jobId} has no result yet`);
  const files = await downloadOutputs(
    apiUrl,
    queue,
    jobId,
    result,
    String(flags["output-dir"] || "./outputs"),
  );
  if (flags.json) out(JSON.stringify({ files }, null, 2));
};

const cmdUpload = async (flags, path) => {
  if (!path) die("usage: cq upload <path>");
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const bytes = new Uint8Array(await readFile(path));
  const { url, hash } = await uploadBytes(apiUrl, bytes);
  if (flags.json) {
    out(
      JSON.stringify(
        { path, hash, url, ref: { type: "url", value: url } },
        null,
        2,
      ),
    );
  } else out(url);
};

const cmdStatus = async (flags) => {
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);
  const status = await apiJson(
    `${apiUrl}/q/${encodeURIComponent(queue)}/status`,
  );
  const workers = Object.keys(status.localWorkers || {}).length +
    Object.keys(status.otherServers || {}).length;
  if (flags.json) {
    out(JSON.stringify(status, null, 2));
  } else {
    out(
      `queue ${queue}: ${Object.keys(status.jobs || {}).length} job(s), ${workers} worker source(s)`,
    );
    if (!workers) {
      log(`⚠️  no workers on ${queue} — jobs will sit in Queued forever`);
    }
  }
};

const cmdUrl = async (flags) => {
  const apiUrl = String(flags.api || DEFAULT_API).replace(/\/$/, "");
  const queue = String(flags.queue || DEFAULT_QUEUE);
  const definition = await buildDefinition(apiUrl, flags);
  out(viewUrl(apiUrl, definition, queue));
};

// ---------------------------------------------------------------------------

const main = async () => {
  const argv = process.argv.slice(2);
  const { flags, positional } = parseArgs(argv);
  const command = positional[0];
  noteDevTarget();

  if (!command || flags.help || command === "help") {
    process.stdout.write(USAGE);
    process.exit(command ? 0 : 1);
  }

  switch (command) {
    case "run":
      return cmdRun(flags);
    case "submit":
      return cmdSubmit(flags);
    case "logs":
      return cmdLogs(flags, positional[1]);
    case "wait":
      return cmdWait(flags, positional[1]);
    case "result":
      return cmdResult(flags, positional[1]);
    case "outputs":
      return cmdOutputs(flags, positional[1]);
    case "upload":
      return cmdUpload(flags, positional[1]);
    case "status":
      return cmdStatus(flags);
    case "url":
      return cmdUrl(flags);
    default:
      die(`unknown command: ${command}\n\n${USAGE}`);
  }
};

main().catch((err) => die(err?.stack || err?.message || String(err)));
