# REST API

Base URL: `https://container.mtfm.io` (or your own deployment, or `http://localhost:8000` for a worker in
`--mode=local`).

No authentication. CORS is open, so browsers can call it directly.

## Jobs

| Method | Path                                   | Description                                 |
| ------ | -------------------------------------- | ------------------------------------------- |
| `POST` | `/q/:queue`                            | Submit a job. Creates the queue if needed.  |
| `POST` | `/q/:queue/j`                          | Alias of the above.                         |
| `GET`  | `/q/:queue`                            | List jobs in the queue.                     |
| `GET`  | `/q/:queue/j`                          | Alias of the above.                         |
| `GET`  | `/q/:queue/j/:jobId`                   | Definition + results.                       |
| `GET`  | `/j/:jobId`                            | Definition + results, without the queue.    |
| `GET`  | `/j/:jobId/definition.json`            | Just the definition.                        |
| `GET`  | `/j/:jobId/result.json`                | Just the result. `results.json` also works. |
| `GET`  | `/q/:queue/j/:jobId/result.json`       | Queue-scoped result — includes queue state. |
| `GET`  | `/j/:jobId/inputs/*`                   | Raw input file bytes.                       |
| `GET`  | `/j/:jobId/outputs/*`                  | Raw output file bytes.                      |
| `GET`  | `/q/:queue/j/:jobId/build-logs.json`   | Image build / pull / clone logs.             |
| `GET`  | `/q/:queue/j/:jobId/run-logs.json`     | The container's own stdout/stderr.            |
| `GET`  | `/q/:queue/j/:jobId/stream`            | SSE: logs and state, live, until finished.   |
| `GET`  | `/q/:queue/j/:jobId/namespaces.json`   | Namespaces waiting on this job.             |
| `POST` | `/q/:queue/j/:jobId/cancel`            | Cancel.                                     |
| `POST` | `/q/:queue/j/:jobId/:namespace/cancel` | Cancel for one namespace only.              |
| `POST` | `/j/:jobId/copy`                       | Copy a job into another queue.              |

### `POST /q/:queue`

```json
{
  "id": "optional, defaults to sha256(definition)",
  "definition": { "image": "alpine:3.19.1", "command": "echo hi" },
  "control": { "namespace": "user-42", "callbacks": { "queued": { "url": "…", "payload": {} } } },
  "debug": false
}
```

→ `200 {"success": true, "jobId": "c0320f…"}`

### `GET /q/:queue/j/:jobId/result.json`

`{"data": null}` while the job is unfinished or unknown. Once finished:

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
      "type": "Finished",
      "reason": "Success",
      "time": 1784941344264,
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

- `finishedReason`: `Success | Error | TimedOut | Cancelled | WorkerLost | JobReplacedByClient | Deleted`
- `logs`: `[text, timestampMs, isStdErr?]` — third element `true` for stderr.
- `StatusCode`: the container's exit code. **Check it separately** — a non-zero exit still gives
  `finishedReason: "Success"`.

The unscoped `/j/:jobId/result.json` returns `{"data": {definition, results}}` instead; prefer the queue-scoped form
when you have the queue.

### `GET /q/:queue/j/:jobId/build-logs.json` · `run-logs.json`

Logs, split by which half of the job produced them. **Build logs** cover `docker build`, image pull/push and repo
cloning; **run logs** are the container's own stdout/stderr. Keeping them apart is what lets a caller tell "the image
failed to build" from "the program failed".

```
GET /q/:queue/j/:jobId/build-logs.json?since=28

{"data": [["#6 DONE 0.1s", 1785940953827, true]], "sliceStart": 28, "nextCursor": 31, "isFinal": true}
```

Poll with `since=<the previous nextCursor>` to follow a running job without re-reading what you already have.
`isFinal: true` means the job has finished and no more lines are coming.

These read the live in-memory buffer while a job runs and the persisted copy afterwards, so they work during and after
execution. Logs are retained about a week — much less than results.

An unqueued `/j/:jobId/build-logs.json` also exists, but it can only see persisted logs, so it returns nothing until the
job finishes.

### `GET /q/:queue/j/:jobId/stream`

Server-Sent Events: one request that follows a single job to completion. Useful when you want live logs without the
stateful, per-queue websocket protocol.

```
event: build-log
data: {"lines":[["#5 DONE 1.3s",1785940953488,true]],"cursor":28}

event: run-log
data: {"lines":[["hello\n",1785940954287]],"cursor":1}

event: state
data: {"state":"Running"}

event: final
data: {"state":"Finished","reason":"Success"}
```

Everything already known is replayed on connect, so opening the stream against a job that has already finished returns
its full history and `final` in one short read. `cursor` is the running count of lines emitted for that kind. The server
sends `final` and closes on a terminal state, or after 30 minutes.

## Files

| Method | Path             | Description                                  |
| ------ | ---------------- | -------------------------------------------- |
| `PUT`  | `/f/:key`        | Upload. Redirects to a signed storage URL.   |
| `GET`  | `/f/:key`        | Download. Redirects to a signed storage URL. |
| `GET`  | `/f/:key/exists` | `200 {"exists":true}` / `404`.               |

`:key` is the sha256 hex digest of the content. **Follow redirects** on both upload and download.

## Queue

| Method | Path                | Description                      |
| ------ | ------------------- | -------------------------------- |
| `GET`  | `/q/:queue/status`  | Jobs, connected workers, counts. |
| `GET`  | `/q/:queue/metrics` | Prometheus metrics.              |
| `GET`  | `/healthz`          | `OK`.                            |

```json
{
  "jobs": { "c0320f…": { "state": "Finished" } },
  "localWorkers": { "3a645624-…": { "cpus": 4, "gpus": 0, "queue": {} } },
  "clientCount": 0,
  "queueInfo": { "address": "public1", "jobCount": 1, "localWorkerCount": 2 }
}
```

`localWorkerCount === 0` means nothing will run.

## Deprecated

`GET /upload/:key`, `GET /download/:key`, `GET /:queue/status`, `GET /:queue/metrics` — use the `/f/…` and `/q/…` forms
instead.

## Errors

| Code  | Meaning                                |
| ----- | -------------------------------------- |
| `400` | Malformed body or missing queue.       |
| `404` | Unknown job or missing path parameter. |
| `500` | Server error; the body is the message. |

A job that does not exist returns `{"data": null}` from `result.json` rather than a 404 — indistinguishable from "not
finished yet", so track your own `jobId`s.
