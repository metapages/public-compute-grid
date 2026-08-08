# REST + WebSocket reference

Base URL: `https://container.mtfm.io`, your own deployment, or `http://localhost:8000` (worker in `--mode=local`). No
auth. CORS open.

## Job endpoints

| Method | Path                                   | Description                             |
| ------ | -------------------------------------- | --------------------------------------- |
| `POST` | `/q/:queue`                            | Submit. Creates the queue if needed.    |
| `POST` | `/q/:queue/j`                          | Alias.                                  |
| `GET`  | `/q/:queue`, `/q/:queue/j`             | List jobs.                              |
| `GET`  | `/q/:queue/j/:jobId`                   | Definition + results.                   |
| `GET`  | `/q/:queue/j/:jobId/result.json`       | Result + queue state. **Use this one.** |
| `GET`  | `/j/:jobId/result.json`                | Result without queue context.           |
| `GET`  | `/j/:jobId/definition.json`            | Definition.                             |
| `GET`  | `/j/:jobId/outputs/*`                  | Raw output bytes.                       |
| `GET`  | `/j/:jobId/inputs/*`                   | Raw input bytes.                        |
| `GET`  | `/q/:queue/j/:jobId/build-logs.json`   | Build/pull/clone logs. `?since=N`.      |
| `GET`  | `/q/:queue/j/:jobId/run-logs.json`     | Container stdout/stderr. `?since=N`.    |
| `GET`  | `/q/:queue/j/:jobId/stream`            | SSE: logs + state until finished.       |
| `GET`  | `/q/:queue/j/:jobId/namespaces.json`   | Namespaces waiting on the job.          |
| `POST` | `/q/:queue/j/:jobId/cancel`            | Cancel → `finishedReason: "Cancelled"`. |
| `POST` | `/q/:queue/j/:jobId/:namespace/cancel` | Cancel for one namespace.               |
| `POST` | `/j/:jobId/copy`                       | Copy into another queue.                |

Every path above returns JSON — none is a page to send a person to. For that, see the browser URL below. Note the
job-state endpoint is `/j/:jobId.json`, **with** the suffix: the bare `/j/:jobId` is the browser page.

## Browser view URL

The base URL also serves the browser client, which renders live logs, outputs and an editable definition. Two forms:

```
<base>/j/<jobId>#?queue=<queue>                                          # short
<base>/#?job=<base64(encodeURIComponent(JSON.stringify(definition)))>&queue=<queue>   # self-contained
```

```js
const shortViewUrl = (base, jobId, queue) => `${base}/j/${jobId}#?queue=${encodeURIComponent(queue)}`;

const viewUrl = (base, definition, queue) =>
  `${base}/#?job=${btoa(encodeURIComponent(JSON.stringify(definition)))}` +
  `&queue=${encodeURIComponent(queue)}`;
```

**Short** — ~100 chars, so it pastes into a terminal. The SPA is served at that path and fetches the definition from
`/j/<jobId>/definition.json` at boot, so it only works once the job has been submitted, and stops working when the
stored data expires.

**Self-contained** — the client re-derives `jobId = sha256(definition)` from the hash, so it opens the run that
definition produced; the empty `env`/`configFiles` the client adds are dropped from the hash blob and do not change the
id. Needs no server lookup, so it links to jobs that were never submitted and outlives data expiry. Large inline inputs
make it unusable — upload those (`PUT /f/:key`) and reference them instead.

Editing a short-URL page rewrites it to the self-contained form, so the edited job stays shareable.

Omit `queue` and the client watches the default queue instead of yours. Other hash params: `inputs` (a `{name: string}`
map, surfaced in the container as `configFiles`), `control`, `maxJobDuration`, `autostart`, `terminal`, `debug`.

## Files

| Method | Path             | Notes                                        |
| ------ | ---------------- | -------------------------------------------- |
| `PUT`  | `/f/:key`        | Upload. Redirects to signed URL — follow it. |
| `GET`  | `/f/:key`        | Download. Redirects — follow it.             |
| `GET`  | `/f/:key/exists` | `200 {"exists":true}` or `404`.              |

`:key` = sha256 hex of the content.

## Queue

| Method | Path                | Notes                                |
| ------ | ------------------- | ------------------------------------ |
| `GET`  | `/q/:queue/status`  | `localWorkers`, `jobs`, `queueInfo`. |
| `GET`  | `/q/:queue/metrics` | Prometheus.                          |
| `GET`  | `/healthz`          | `OK`.                                |

## Submit body

```json
{
  "id": "optional; defaults to sha256(definition)",
  "definition": {},
  "control": {},
  "debug": false
}
```

→ `{"success": true, "jobId": "…"}`

### `definition`

| Field          | Type     | Notes                                                            |
| -------------- | -------- | ---------------------------------------------------------------- |
| `image`        | string   | Docker image, or a git URL to build.                             |
| `build`        | object   | `{ context, filename, dockerfile, buildArgs, target, platform }` |
| `command`      | string   | Shell syntax needs explicit `sh -c "…"`.                         |
| `entrypoint`   | string   | Overrides image entrypoint.                                      |
| `env`          | object   | `{ KEY: "value" }`. Also the place to put a re-run nonce.        |
| `workdir`      | string   | Working directory.                                               |
| `inputs`       | object   | filename → DataRef. Materialised at `/inputs`.                   |
| `configFiles`  | object   | Like `inputs`, but part of the job hash.                         |
| `shmSize`      | string   | e.g. `"2g"` for PyTorch dataloaders.                             |
| `maxDuration`  | string   | `"30s"`, `"20m"`, `"2h"`.                                        |
| `requirements` | object   | `{ cpus, gpus, memory, maxDuration }`.                           |
| `tags`         | string[] | Reserved — declared but not matched on today.                    |

### `control`

| Field                | Notes                                                                     |
| -------------------- | ------------------------------------------------------------------------- |
| `namespace`          | One live job per namespace; a new submit evicts the previous job.         |
| `maxDuration`        | Queue-side kill switch.                                                   |
| `callbacks.queued`   | `{ url, payload }` POSTed on **enqueue**, retried every minute until 2xx. |
| `callbacks.finished` | Present in the types, **not implemented**. Do not rely on it.             |

Callback body: `{ jobId, queue, namespace, config }` where `config` is your `payload` echoed back.

## Result shape

```json
{
  "data": {
    "state": "Finished",
    "finishedReason": "Success",
    "queuedTime": 1784941340915,
    "time": 1784941344264,
    "worker": "3a645624-…",
    "namespaces": ["_"],
    "finished": {
      "reason": "Success",
      "result": {
        "StatusCode": 0,
        "duration": 184,
        "isTimedOut": false,
        "logs": [["stdout-line\n", 1784941344026]],
        "outputs": { "out.txt": { "type": "base64", "value": "aGVsbG8K" } }
      }
    }
  }
}
```

- `{"data": null}` = unfinished **or** unknown job. Indistinguishable — track your own ids.
- `finishedReason`: `Success | Error | TimedOut | Cancelled | WorkerLost | JobReplacedByClient | Deleted`
- `logs`: `[text, timestampMs, isStdErr?]`; third element `true` for stderr.
- `StatusCode`: container exit code — check separately from `finishedReason`.

## DataRef

```ts
{ value: string, type?: "utf8" | "base64" | "json" | "url" | "key", hash?: string }
```

Inline under 200 bytes (`utf8`/`json`/`base64`), otherwise `url`. `url` refs may point at any URL the worker can reach,
not only at `/f/…`.

## Container environment

`JOB_ID`, `JOB_INPUTS=/inputs`, `JOB_OUTPUTS=/outputs`, `JOB_CACHE=/job-cache`, `JOB_URL_PREFIX`,
`JOB_INPUTS_URL_PREFIX`, `JOB_OUTPUTS_URL_PREFIX`, and `CUDA_VISIBLE_DEVICES=0` when a GPU is allocated.

## Job states

`Queued → Running → Finished` (`Removed` is a short-lived tombstone). A lost worker returns the job to `Queued`.

## Logs over HTTP

Two separate streams: **build** logs (`docker build`, image pull/push, repo cloning) and **run** logs (the container's
own stdout/stderr). A build failure and a program failure are different problems, so do not conflate them.

```
GET /q/:queue/j/:jobId/build-logs.json?since=28
→ {"data": [[text, timestampMs, isStderr?]], "sliceStart": 28, "nextCursor": 31, "isFinal": true}
```

Poll with `since=<previous nextCursor>`; `isFinal: true` means no more lines are coming.

For live following without the websocket, one SSE request follows a job to completion:

```
GET /q/:queue/j/:jobId/stream

event: build-log   data: {"lines":[[...]],"cursor":28}
event: run-log     data: {"lines":[[...]],"cursor":1}
event: state       data: {"state":"Running"}
event: final       data: {"state":"Finished","reason":"Success"}
```

Everything already known is replayed on connect, so a stream opened against a finished job returns its whole history and
`final` immediately. Logs are retained about a week — much less than results.

## WebSocket

`wss://<api>/q/<queue>/client` (`/worker` is for the worker binary).

On connect the server sends `Workers`, then `JobStates` (full snapshot).

Server → client: `JobStates`, `JobStateUpdates`, `JobStatusPayload`, `Workers`, `BroadcastJobDefinitions`.

Client → server: `{type:"StateChange", payload}` (submit), `{type:"QueryJob", payload:{jobId}}`,
`{type:"QueryJobStates"}`, and the literal string `"PING"` → `"PONG"`.

`JobStatusPayload = { jobId, step, logs }` where `step` is one of `docker image pull`, `cloning repo`, `docker build`,
`Running`, `docker image push`.

State messages never include the result body — fetch `result.json`.

## Worker CLI

```
run [queue]
  -c --cpus N                 default 1
  -g --gpus N|"device=1,3"    default 0
  -m --mode remote|local      default remote
  -t --max-job-duration 20m   default 5m
  -d --data-directory PATH    default /tmp/worker-metapage-io
  -a --api-address URL        default https://container.mtfm.io
  -p --port N                 local mode, default 8000
  --id ID    --debug
```

Env equivalents are prefixed `METAPAGE_IO_WORKER_` (`CPUS`, `GPUS`, `MODE`, `QUEUE`, `PORT`, `API_ADDRESS`,
`JOB_MAX_DURATION`, `DEBUG`).
