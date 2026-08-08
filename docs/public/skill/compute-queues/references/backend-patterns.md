# Backend patterns

Complete, runnable shapes for putting a compute queue behind a service. Plain HTTP — no SDK.

## Choosing

| Pattern   | Latency        | Restart-safe | Use when                                    |
| --------- | -------------- | ------------ | ------------------------------------------- |
| Polling   | your interval  | ✅           | Default. Batch, serverless, cron.           |
| WebSocket | instant + logs | ❌           | Live progress, interactive UIs.             |
| Callback  | instant        | ✅           | Durable "accepted" only — never "finished". |

## Reusable client module

```js
// compute-queue.mjs — Node 18+ / Deno
import { createHash } from "node:crypto";

const API = process.env.COMPUTE_API ?? "https://container.mtfm.io";

export async function submitJob(queue, definition, control) {
  const res = await fetch(`${API}/q/${queue}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ definition, control }),
  });
  if (!res.ok) throw new Error(`submit ${res.status}: ${await res.text()}`);
  return (await res.json()).jobId;
}

export async function waitForJob(queue, jobId, { timeoutMs = 600_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let interval = 500;
  while (Date.now() < deadline) {
    const res = await fetch(`${API}/q/${queue}/j/${jobId}/result.json`);
    if (res.ok) {
      const { data } = await res.json();
      if (data?.state === "Finished") return data;
    }
    await new Promise((r) => setTimeout(r, interval));
    interval = Math.min(interval * 1.5, 5000);
  }
  throw new Error(`timeout waiting for ${jobId}`);
}

export function assertOk(data) {
  if (data.finishedReason !== "Success") {
    throw new Error(`job ${data.finishedReason}`);
  }
  const result = data.finished?.result;
  if (result?.StatusCode !== 0) {
    const stderr = (result?.logs ?? []).filter((l) => l[2]).map((l) => l[0])
      .join("");
    throw new Error(`exit ${result?.StatusCode}: ${stderr.slice(0, 2000)}`);
  }
  return result;
}

export const stdout = (result) => (result.logs ?? []).filter((l) => !l[2]).map((l) => l[0]).join("");

export async function uploadInput(bytes) {
  const hash = createHash("sha256").update(bytes).digest("hex");
  if ((await fetch(`${API}/f/${hash}/exists`)).status !== 200) {
    const put = await fetch(`${API}/f/${hash}`, {
      method: "PUT",
      body: bytes,
      redirect: "follow",
    });
    if (!put.ok) throw new Error(`upload ${put.status}`);
  }
  return { type: "url", value: `${API}/f/${hash}` };
}

export async function readOutput(outputs, name) {
  const ref = outputs?.[name];
  if (!ref) return undefined;
  switch (ref.type) {
    case "base64":
      return Buffer.from(ref.value, "base64");
    case "utf8":
      return Buffer.from(ref.value, "utf8");
    case "json":
      return Buffer.from(JSON.stringify(ref.value), "utf8");
    case "url":
      return Buffer.from(await (await fetch(ref.value)).arrayBuffer());
    default:
      throw new Error(`unknown dataref type ${ref.type}`);
  }
}

export async function queueHealthy(queue) {
  const s = await (await fetch(`${API}/q/${queue}/status`)).json();
  return Object.keys(s.localWorkers ?? {}).length > 0;
}
```

Usage:

```js
const jobId = await submitJob(QUEUE, {
  image: "python:3.12-slim",
  command: "python /inputs/main.py",
  inputs: {
    "main.py": { type: "utf8", value: script },
    "data.csv": await uploadInput(csvBytes),
  },
  requirements: { cpus: 2, memory: "4g", maxDuration: "10m" },
});

const result = assertOk(await waitForJob(QUEUE, jobId));
const report = await readOutput(result.outputs, "report.json");
```

## WebSocket waiter with live logs

Node 22+ or Deno have a global `WebSocket`; on Node 18–20 use `npm i ws` and `import WebSocket from "ws"`.

```js
export function waitForJobWs(
  queue,
  jobId,
  { timeoutMs = 600_000, onLog } = {},
) {
  const API_WS = API.replace(/^http/, "ws");
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(`${API_WS}/q/${queue}/client`);
    const timer = setTimeout(() => {
      socket.close();
      reject(new Error("timeout"));
    }, timeoutMs);
    const finish = (fn, arg) => {
      clearTimeout(timer);
      socket.close();
      fn(arg);
    };

    socket.addEventListener(
      "open",
      () => socket.send(JSON.stringify({ type: "QueryJob", payload: { jobId } })),
    );
    socket.addEventListener("error", (e) => finish(reject, e));

    socket.addEventListener("message", async (event) => {
      if (event.data === "PONG") return;
      const msg = JSON.parse(event.data);

      if (msg.type === "JobStates" || msg.type === "JobStateUpdates") {
        if (msg.payload?.state?.jobs?.[jobId]?.state === "Finished") {
          const { data } = await (await fetch(`${API}/q/${queue}/j/${jobId}/result.json`))
            .json();
          finish(resolve, data);
        }
      }
      if (
        msg.type === "JobStatusPayload" && msg.payload?.jobId === jobId && onLog
      ) {
        for (const [line, , isErr] of msg.payload.logs ?? []) {
          onLog(line, !!isErr);
        }
      }
    });
  });
}
```

Rules: one socket per queue (not per job); open it _before_ submitting; always `QueryJob` on open to cover the
finished-before-connect race; reconnect with backoff and re-query.

## Callback receiver

```js
// Fires on ENQUEUE, retried every minute until 2xx. Not a completion hook.
app.post("/hooks/job-queued", async (req, res) => {
  const { jobId, queue, namespace, config } = req.body;
  res.status(200).end(); // stop the retry loop first
  await markAccepted(config.requestId, jobId); // must be idempotent
});
```

The URL must be reachable from the API server — no `localhost` unless tunnelled.

## Restart-safe hybrid

The shape to use for anything real:

1. `submitJob(...)` with `control.callbacks.queued`, and **persist `jobId`** against your request row before doing
   anything else.
2. The callback is the durable "the queue has it" signal.
3. One websocket per queue provides live state and logs.
4. On boot, poll `result.json` for every persisted unfinished `jobId`. That reconciliation loop is what makes it survive
   crashes and redeploys.

```js
// boot reconciliation
for (const { jobId, queue } of await db.unfinishedJobs()) {
  waitForJob(queue, jobId).then((data) => db.complete(jobId, data)).catch(
    () => {},
  );
}
```

## Deno service, end to end

```ts
// deno run --allow-net --allow-env server.ts
const API = Deno.env.get("COMPUTE_API") ?? "https://container.mtfm.io";
const QUEUE = Deno.env.get("COMPUTE_QUEUE")!;

Deno.serve({ port: 8080 }, async (req) => {
  if (req.method !== "POST") {
    return new Response("POST { script }", { status: 405 });
  }
  const { script } = await req.json();

  const submit = await fetch(`${API}/q/${QUEUE}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      definition: {
        image: "python:3.12-slim",
        command: "python /inputs/main.py",
        inputs: { "main.py": { type: "utf8", value: script } },
        requirements: { cpus: 1, maxDuration: "5m" },
      },
    }),
  });
  const { jobId } = await submit.json();

  let data, interval = 500;
  const deadline = Date.now() + 300_000;
  while (Date.now() < deadline) {
    ({ data } = await (await fetch(`${API}/q/${QUEUE}/j/${jobId}/result.json`))
      .json());
    if (data?.state === "Finished") break;
    await new Promise((r) => setTimeout(r, interval));
    interval = Math.min(interval * 1.5, 5000);
  }

  const result = data?.finished?.result;
  return Response.json({
    jobId,
    ok: data?.finishedReason === "Success" && result?.StatusCode === 0,
    reason: data?.finishedReason,
    exitCode: result?.StatusCode,
    durationMs: result?.duration,
    stdout: (result?.logs ?? []).filter((l) => !l[2]).map((l) => l[0]).join(""),
    outputs: Object.keys(result?.outputs ?? {}),
  });
});
```

## Job recipes

**GPU**

```json
{
  "image": "nvidia/cuda:12.4.1-runtime-ubuntu22.04",
  "command": "nvidia-smi",
  "requirements": { "gpus": 1 }
}
```

Only workers started with GPUs take it; the container always sees its device as `CUDA_VISIBLE_DEVICES=0`.

**Build from git**

```json
{
  "build": {
    "context": "https://github.com/me/tool#main",
    "filename": "Dockerfile"
  },
  "command": "tool --run"
}
```

Build logs stream as `JobStatusPayload` with `step: "docker build"`. Images are cached per worker.

**Force a re-run** (defeat content-hash dedup)

```json
{
  "image": "alpine:3.19.1",
  "command": "date",
  "env": { "NONCE": "2026-07-24T09:14:22Z" }
}
```

**Cache model weights across jobs**

```py
import os, pathlib
cache = pathlib.Path(os.environ["JOB_CACHE"]) / "weights.pt"
if not cache.exists():
    download_to(cache)     # first job pays, later jobs on this worker don't
```

**One live job per user/tab**

```json
{ "control": { "namespace": "user-42:doc-7" } }
```

A new submit into the namespace evicts the previous job.

## Failure modes and what they mean

| Symptom                                   | Cause                                                          |
| ----------------------------------------- | -------------------------------------------------------------- |
| Job stuck in `Queued`                     | No worker on that queue. Check `/q/<queue>/status`.            |
| `{"data": null}` forever                  | Same as above, or a wrong/typo'd `jobId`.                      |
| Second run returns instantly, stale data  | Content-hash dedup. Add a nonce.                               |
| `finishedReason: "Success"` but no output | Container exited non-zero. Check `StatusCode` and stderr logs. |
| `WorkerLost`                              | Worker died mid-job; the job requeues automatically.           |
| `TimedOut`                                | Hit `maxDuration` or the worker's `--max-job-duration`.        |
| Webhook keeps firing                      | Handler didn't return 2xx. Retries every minute.               |
| Output file missing                       | Written outside `/outputs`.                                    |
| Result gone after weeks                   | ~1 month retention on the public instance.                     |

## Self-hosting and privacy

- `--mode=local` on a worker: self-contained API on `http://localhost:8000`, nothing uploaded, identical REST surface —
  point `COMPUTE_API` at it.
- Or deploy the whole stack: <https://github.com/metapages/compute-queues>.
- On the public instance, treat the queue name as a credential.
