# Job definition

The body you `POST /q/<queue>`:

```json
{
  "id": "optional — defaults to sha256(definition)",
  "definition": { "...": "the container spec" },
  "control": { "...": "optional queue behaviour" },
  "debug": false
}
```

## `definition`

| Field          | Type   | Notes                                                                  |
| -------------- | ------ | ---------------------------------------------------------------------- |
| `image`        | string | Docker image, e.g. `python:3.12-slim`. Or a git URL to build from.     |
| `build`        | object | Build instead of pull — see below.                                     |
| `command`      | string | Overrides the image `CMD`. Shell syntax needs an explicit `sh -c "…"`. |
| `entrypoint`   | string | Overrides the image entrypoint.                                        |
| `env`          | object | `{ "KEY": "value" }` env vars.                                         |
| `workdir`      | string | Working directory inside the container.                                |
| `inputs`       | object | Filename → [DataRef](/guide/files). Mounted read-only at `/inputs`.    |
| `configFiles`  | object | Same shape as `inputs`, but part of the job hash.                      |
| `shmSize`      | string | e.g. `"2g"` — needed by PyTorch dataloaders and friends.               |
| `maxDuration`  | string | `"30s"`, `"20m"`, `"2h"`.                                              |
| `requirements` | object | `{ cpus, gpus, memory, maxDuration }`.                                 |

`definition.tags` also exists in the types, intended to pin a job to workers with matching tags. Nothing matches on it
yet — treat it as reserved.

::: tip inputs vs configFiles
Both land in the container. `inputs` are the *data* you vary per run; `configFiles` are fixed and folded into the job
hash, so changing one produces a different job id. If you want a change to force a re-run, put it in `configFiles` (or
`env`).
:::

### Minimal

```json
{ "definition": { "image": "alpine:3.19.1", "command": "echo hello" } }
```

### With a script and data

```json
{
  "definition": {
    "image": "python:3.12-slim",
    "command": "python /inputs/analyse.py",
    "inputs": {
      "analyse.py": {
        "type": "utf8",
        "value": "import json,os\nprint(os.listdir('/inputs'))\nopen('/outputs/result.json','w').write(json.dumps({'ok':True}))\n"
      },
      "data.csv": { "type": "url", "value": "https://container.mtfm.io/f/9f86d0…" }
    },
    "requirements": { "cpus": 2, "memory": "4g", "maxDuration": "10m" }
  }
}
```

### GPU

```json
{
  "definition": {
    "image": "nvidia/cuda:12.4.1-runtime-ubuntu22.04",
    "command": "nvidia-smi",
    "requirements": { "gpus": 1 }
  }
}
```

The worker allocates a specific device and sets `CUDA_VISIBLE_DEVICES=0` inside the container — from the container's
point of view its GPU is always index 0. Only workers started with GPUs available will take the job.

### Build an image

`build` makes the worker build the image instead of pulling one. See
[Building containers](/guide/building-containers) for the whole workflow; the fields are:

| Field          | Type     | Notes                                                                          |
| -------------- | -------- | ------------------------------------------------------------------------------ |
| `dockerfile`   | string   | The **contents** of a Dockerfile, inline. Simplest option.                     |
| `context`      | string   | Where the build context comes from — a GitHub URL or an archive URL. See below. |
| `buildContext` | string   | Subdirectory *within* the context to build from.                               |
| `filename`     | string   | Dockerfile name within the context (default `Dockerfile`).                     |
| `target`       | string   | Multi-stage build target.                                                      |
| `buildArgs`    | string[] | `["VERSION=1.2.3"]`.                                                            |
| `platform`     | string   | e.g. `linux/amd64`.                                                             |

```json
{
  "definition": {
    "build": {
      "context": "https://github.com/me/my-tool/commit/9f3c1a2",
      "filename": "Dockerfile",
      "buildArgs": ["VERSION=1.2.3"],
      "platform": "linux/amd64"
    },
    "command": "my-tool --run"
  }
}
```

`dockerfile` and `context` combine: the context supplies the files, and an inline `dockerfile` is written into it,
overwriting any Dockerfile already there.

Accepted `context` values:

- `https://github.com/owner/repo` — the default branch
- `https://github.com/owner/repo/tree/<branch-or-tag>`
- `https://github.com/owner/repo/commit/<sha>` — **prefer this**, it pins the build
- any URL serving a gzip or zip archive — the worker identifies the format from the archive's magic bytes, so a
  content-addressed blob URL with no file extension works

If the archive contains exactly one top-level directory it is treated as the context root, which is what makes
GitHub's wrapper folder transparent. An archive with files at its root is used as-is — so you can tar a local
directory, `PUT` it to [blob storage](/guide/files), and use that URL as the context.

::: warning Two caches, not one
The **job** is cached on the whole definition; the **image** is cached on the `build` block alone. Change only the
command and the container re-runs against the already-built image, producing no build logs at all — that is the cache
working, not a failure. Changing anything inside `build` forces a rebuild.
:::

Build logs are kept separate from the container's own output. Read them at
`/q/<queue>/j/<jobId>/build-logs.json`, follow them live over [SSE](/guide/rest-api), or watch them on the websocket.

## `control`

| Field              | Notes                                                                |
| ------------------ | -------------------------------------------------------------------- |
| `namespace`        | One live job per namespace; submitting evicts the previous one.      |
| `maxDuration`      | Queue-side kill switch.                                              |
| `callbacks.queued` | `{ url, payload }` — POSTed when the job is **enqueued**. See below. |

```json
{
  "definition": { "image": "alpine:3.19.1", "command": "echo hi" },
  "control": {
    "namespace": "user-42",
    "callbacks": { "queued": { "url": "https://my.app/hooks/queued", "payload": { "requestId": "abc" } } }
  }
}
```

::: warning There is no `finished` callback yet
`control.callbacks.finished` appears in the TypeScript types but nothing reads it. Only `callbacks.queued` fires. To
learn that a job *completed*, poll or use the websocket — see [Backend integration](/guide/backend-integration).
:::

## Container environment

Every container gets:

| Variable                 | Value                             |
| ------------------------ | --------------------------------- |
| `JOB_ID`                 | The job id                        |
| `JOB_INPUTS`             | `/inputs`                         |
| `JOB_OUTPUTS`            | `/outputs`                        |
| `JOB_CACHE`              | `/job-cache` — shared across jobs |
| `JOB_URL_PREFIX`         | `<api>/j/<jobId>`                 |
| `JOB_INPUTS_URL_PREFIX`  | `<api>/j/<jobId>/inputs/`         |
| `JOB_OUTPUTS_URL_PREFIX` | `<api>/j/<jobId>/outputs/`        |
| `CUDA_VISIBLE_DEVICES`   | `0`, when a GPU was allocated     |

`/job-cache` is the right place for model weights and datasets: it persists on the worker between jobs, so the second
run of an ML job skips the download.
