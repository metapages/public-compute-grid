# The job definition

A job is a JSON document. `cq` builds it for you from flags, but you need to know the shape to reason about caching, to
edit a definition by hand, or to submit from something other than `cq`.

## Submitting

```
POST /q/<queue>
content-type: application/json

{
  "definition": { ...DockerJobDefinition... },
  "control": { "namespace": "...", "callbacks": { ... } }
}

→ 200 {"success": true, "jobId": "<sha256>"}
```

`id` is optional on submit — the server computes `sha256(definition)` if you omit it, and that is the `jobId`.

## definition

| Field          | Type     | Notes                                                                            |
| -------------- | -------- | -------------------------------------------------------------------------------- |
| `image`        | string   | Existing image reference, e.g. `python:3.12-slim`. Mutually usable with `build`. |
| `build`        | object   | Build the image on the worker. See below.                                        |
| `command`      | string   | Overrides the image CMD. A single string, shell-quoted.                          |
| `entrypoint`   | string   | Overrides the image ENTRYPOINT.                                                  |
| `workdir`      | string   | Working directory inside the container.                                          |
| `env`          | object   | `{NAME: value}`. Stored with the definition — **not** a secret store.            |
| `inputs`       | object   | `{filename: DataRef}` → written to `/inputs`.                                    |
| `configFiles`  | object   | `{filename: DataRef}` → also written to `/inputs`. See "hashing" below.          |
| `maxDuration`  | string   | e.g. `"20m"`, `"1h"`, `"90s"`. The worker kills the job past this.               |
| `shmSize`      | string   | `/dev/shm` size, e.g. `"2g"`. PyTorch dataloaders need this.                     |
| `requirements` | object   | `{cpus, gpus, memory, maxDuration}` — what the job needs from a worker.          |
| `tags`         | string[] | Reserved. Plumbed through the types but **nothing matches on it yet**.           |

### build

| Field          | Type     | Notes                                                                             |
| -------------- | -------- | --------------------------------------------------------------------------------- |
| `dockerfile`   | string   | The **contents** of a Dockerfile, inline. Simplest option.                        |
| `context`      | string   | URL of the build context: a GitHub repo/tree/commit URL, or a `.tar.gz` / `.zip`. |
| `buildContext` | string   | Subdirectory _within_ the context to build from.                                  |
| `filename`     | string   | Dockerfile name within the context (default `Dockerfile`).                        |
| `target`       | string   | Multi-stage build target.                                                         |
| `platform`     | string   | e.g. `linux/amd64`.                                                               |
| `buildArgs`    | string[] | `["KEY=value"]`.                                                                  |

`dockerfile` and `context` combine: the context supplies the files, the inline `dockerfile` overwrites/creates the
Dockerfile in it. That is how `cq --dockerfile X --context-dir Y` works.

Accepted `context` URLs:

- `https://github.com/owner/repo` — default branch
- `https://github.com/owner/repo/tree/<branch-or-tag>`
- `https://github.com/owner/repo/commit/<sha>` — **prefer this**; it pins the build
- any URL serving a gzip or zip archive (the worker sniffs the magic bytes)

If the archive contains exactly one top-level directory, the worker treats that directory as the context root — this is
what makes GitHub's wrapper folder transparent. An archive with files at the root is used as-is.

The built image is tagged by a hash of the build config, so an unchanged `build` block is not rebuilt.

## DataRef

Every file crossing the boundary is a `DataRef`:

```json
{"type": "base64", "value": "aGVsbG8="}
{"type": "utf8",   "value": "hello"}
{"type": "json",   "value": {"a": 1}}
{"type": "url",    "value": "https://container.mtfm.io/f/<sha256>"}
```

Anything over ~200 bytes must be uploaded and referenced by URL, or it clogs the message pipeline:

```
GET /f/<sha256>/exists     → 200 if already there, skip the upload
PUT /f/<sha256>            → body is the bytes (follow redirects)
```

The key is the sha256 of the content, so uploads are idempotent and identical files converge on one blob.

## Hashing: what changes the jobId

`jobId = sha256(definition)`. Everything in `definition` is part of it — including `inputs`, `configFiles`, `env` and
the whole `build` block.

The practical consequence: **resubmitting an identical definition returns the cached result instead of running.** To
force a real re-run of an otherwise identical job, change something — `cq --nonce` sets an `env.CQ_NONCE` for exactly
this.

The `inputs` / `configFiles` split is about intent, not mechanics — both are hashed, both land in `/inputs`. Use
`configFiles` for what defines the job (a script, a config), `inputs` for the data it processes.

## control

```json
{
  "namespace": "my-tab",
  "callbacks": {
    "queued": {
      "url": "https://my.app/hook",
      "payload": { "requestId": "..." }
    }
  }
}
```

- `namespace` — a sub-partition of the queue that tolerates only **one** live job. Submitting a new job in a namespace
  removes the previous one. Use it when a client can resubmit faster than jobs finish. Default namespace is `_`.
- `callbacks.queued` — POSTed when the job is **enqueued**, retried every minute until it gets a 2xx.
  `callbacks.finished` exists in the type but **nothing reads it — there is no completion webhook.** Poll or stream.

## Container environment

| Path / var                                                             | Meaning                              |
| ---------------------------------------------------------------------- | ------------------------------------ |
| `/inputs` (`$JOB_INPUTS`)                                              | `inputs` + `configFiles` land here   |
| `/outputs` (`$JOB_OUTPUTS`)                                            | everything written here is collected |
| `/job-cache` (`$JOB_CACHE`)                                            | persists across jobs on that worker  |
| `$JOB_ID`                                                              | the job's sha256 id                  |
| `$JOB_URL_PREFIX`, `$JOB_INPUTS_URL_PREFIX`, `$JOB_OUTPUTS_URL_PREFIX` | for fetching/pushing over HTTP       |
| `$CUDA_VISIBLE_DEVICES`                                                | set when a GPU is allocated          |

## The result

```
GET /q/<queue>/j/<jobId>/result.json
```

`{"data": null}` until finished. Once finished:

```json
{
  "data": {
    "state": "Finished",
    "finishedReason": "Success",
    "worker": "...",
    "finished": {
      "result": {
        "StatusCode": 0,
        "duration": 146,
        "logs": [["line", 1785940954287]],
        "outputs": { "summary.json": { "type": "base64", "value": "..." } },
        "isTimedOut": false,
        "error": null
      }
    }
  }
}
```

`finishedReason` is one of `Success | Error | TimedOut | Cancelled | WorkerLost | JobReplacedByClient | Deleted`.

**`finishedReason: "Success"` only means the job ran to completion. Check `StatusCode` for whether the program
succeeded.**

Read a single output file without decoding DataRefs:

```
GET /q/<queue>/j/<jobId>/outputs/<filename>
```
