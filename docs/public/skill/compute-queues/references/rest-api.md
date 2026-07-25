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
| `GET`  | `/q/:queue/j/:jobId/namespaces.json`   | Namespaces waiting on the job.          |
| `POST` | `/q/:queue/j/:jobId/cancel`            | Cancel → `finishedReason: "Cancelled"`. |
| `POST` | `/q/:queue/j/:jobId/:namespace/cancel` | Cancel for one namespace.               |
| `POST` | `/j/:jobId/copy`                       | Copy into another queue.                |

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
