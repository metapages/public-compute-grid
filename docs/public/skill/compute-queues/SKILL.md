---
name: compute-queues
description: >
  Run Docker containers on a private compute queue (worker.metapage.io /
  container.mtfm.io) from any backend or script — no account, no API key. Load
  when the user wants to run containerized work from a Node/Deno/browser app,
  offload compute to their own machines, build a job queue with workers, or
  asks about container.mtfm.io, metaframe-docker-worker, compute queues, or
  submitting a Docker job by URL.
---

# Compute queues: run Docker jobs on machines you own

A queue is a URL. A job is a Docker container. Anyone can push a job with no authentication, and anyone can add compute
with one `docker run`.

- API: `https://container.mtfm.io` (self-hostable; `--mode=local` worker serves the identical API on
  `http://localhost:8000`)
- Docs: <https://container.mtfm.io/docs/>
- Source: <https://github.com/metapages/compute-queues>

## The five facts that prevent most mistakes

1. **`jobId = sha256(definition)`.** Resubmitting an identical definition returns the same id and its cached result — no
   new run. Add a nonce to `env` when a real re-run is required.
2. **Submitting ≠ running.** With no worker attached, jobs sit in `Queued` forever. Verify with `GET /q/<queue>/status`
   → `localWorkers`.
3. **Two success checks.** `finishedReason === "Success"` means the _job_ completed; `result.StatusCode === 0` means the
   _program_ succeeded. A crashed container still reports `finishedReason: "Success"`.
4. **The queue name is the only access control.** Unguessable = private. Never suggest a name like `test` or `jobs` for
   anything real.
5. **There is no completion webhook.** `control.callbacks.queued` fires on _enqueue_. `control.callbacks.finished` is in
   the types but nothing reads it. Get results by polling or websocket.

## Submit a job

```js
const API = "https://container.mtfm.io";

const res = await fetch(`${API}/q/${QUEUE}`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    definition: {
      image: "alpine:3.19.1",
      command: 'sh -c "echo hello > /outputs/greeting.txt"',
    },
  }),
});
const { jobId } = await res.json(); // { success: true, jobId }
```

Runtime: Deno, or Node 18+ (Node 22+ for built-in `WebSocket`). No SDK exists — it is plain HTTP, so do not go looking
for a package to install.

## Get the result — pick one

### Polling — the default, always works

`{"data": null}` means not finished yet.

```js
async function waitForJob(queue, jobId, { timeoutMs = 600_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let interval = 500;
  while (Date.now() < deadline) {
    const r = await fetch(`${API}/q/${queue}/j/${jobId}/result.json`);
    if (r.ok) {
      const { data } = await r.json();
      if (data?.state === "Finished") return data;
    }
    await new Promise((res) => setTimeout(res, interval));
    interval = Math.min(interval * 1.5, 5000);
  }
  throw new Error("timeout");
}

const data = await waitForJob(QUEUE, jobId);
if (data.finishedReason !== "Success") throw new Error(data.finishedReason);
const { StatusCode, logs, outputs, duration } = data.finished.result;
```

Stateless, so it survives process restarts. Choose this unless the user needs live logs.

### WebSocket — live state and streaming logs

Connect to `wss://<api>/q/<queue>/client`. The socket carries **state, never results** — fetch `result.json` after
seeing `Finished`.

```js
const socket = new WebSocket(`${API.replace(/^http/, "ws")}/q/${queue}/client`);
socket.addEventListener("open", () =>
  // covers the race where the job finished before the socket opened
  socket.send(JSON.stringify({ type: "QueryJob", payload: { jobId } })));
socket.addEventListener("message", async (event) => {
  const msg = JSON.parse(event.data);
  if (msg.type === "JobStates" || msg.type === "JobStateUpdates") {
    if (msg.payload?.state?.jobs?.[jobId]?.state === "Finished") { /* fetch result.json */ }
  }
  if (msg.type === "JobStatusPayload" && msg.payload?.jobId === jobId) {
    for (const [line] of msg.payload.logs ?? []) process.stdout.write(line);
  }
});
```

One socket per queue, not per job. Send `"PING"` (gets `"PONG"`) as a heartbeat. Does not survive a restart — persist
`jobId` and reconcile by polling on boot.

### Callback — durable "accepted", not "finished"

```js
control: { callbacks: { queued: { url: "https://my.app/hook", payload: { requestId } } } }
```

POSTs `{ jobId, queue, namespace, config }` on enqueue and retries every minute until 2xx. Handlers must be idempotent
and should answer 200 before doing work. Still requires polling or a websocket to learn the job _finished_.

## Add compute

```sh
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp \
  metapage/metaframe-docker-worker:0.54.83 run \
    --cpus=4 --max-job-duration=20m --data-directory=/tmp/worker-metapage-io \
    "$QUEUE"
```

Start it on more machines to scale. `--gpus=2` (or `--gpus="device=1,3"`) offers GPUs. `--mode=local` runs a
self-contained API on `localhost:8000` and nothing leaves the machine.

## Files

Inline under ~200 bytes; upload anything larger and reference it by URL. The blob key is the sha256 of the content, so
uploads are idempotent.

```js
const hash = createHash("sha256").update(bytes).digest("hex");
if ((await fetch(`${API}/f/${hash}/exists`)).status !== 200) {
  await fetch(`${API}/f/${hash}`, { method: "PUT", body: bytes, redirect: "follow" });
}
const ref = { type: "url", value: `${API}/f/${hash}` };
```

Container paths: `/inputs` (read), `/outputs` (collected into the result), `/job-cache` (persists between jobs on that
worker — use it for model weights). Read an output without decoding refs: `GET /q/<queue>/j/<jobId>/outputs/<file>`.

## References

Load these when the task needs more than the above:

- `references/rest-api.md` — every endpoint, request/response shapes, job definition fields, error semantics.
- `references/backend-patterns.md` — complete Node and Deno services, the restart-safe hybrid pattern, GPU and
  build-from-git jobs, production checklist.

## Checklist before shipping

- [ ] Unguessable queue name, stored as a secret
- [ ] A worker is attached (`localWorkers` non-empty) — assert it in a healthcheck
- [ ] `maxDuration` set on the job _and_ `--max-job-duration` on the worker
- [ ] Both `finishedReason` and `StatusCode` checked
- [ ] `jobId` persisted before waiting, so restarts can reconcile
- [ ] `control.namespace` if clients can resubmit faster than jobs finish
- [ ] Results copied out — public-instance data expires after ~1 month
