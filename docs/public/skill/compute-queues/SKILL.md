---
name: compute-queues
description: >
  Run Docker containers on a compute queue (worker.metapage.io /
  container.mtfm.io) — no account, no API key. Build a container from a
  Dockerfile or image and iterate on it until it works, run someone's script or
  git repo and collect the output files, or wire job submission into a
  Node/Deno/browser backend. Load when the user wants something run in a
  container, wants a container built or debugged, wants to offload compute to
  their own machines, or asks about container.mtfm.io,
  metaframe-docker-worker, compute queues, or the `cq` helper.
license: MIT
metadata:
  author: metapages
  homepage: https://container.mtfm.io
  version: "2.0"
---

# Compute queues: run Docker containers on machines you own

A queue is a URL. A job is a Docker container. Anyone can push a job with no authentication, and anyone can add compute
with one `docker run`.

- API: `https://container.mtfm.io` (self-hostable; a `--mode=local` worker serves the identical API on
  `http://localhost:8000`)
- Default queue: `public1`
- Docs: <https://container.mtfm.io/docs/> · Source: <https://github.com/metapages/compute-queues>

## Which half of this do you need

**Running or building a container** — the user wants a thing to run and its output. Use the `cq` helper below; it is the
whole job. Depth in [references/build-and-iterate.md](references/build-and-iterate.md).

**Integrating into an app** — the user is writing a service that submits jobs. `cq` is not what ships; you need the
result patterns in [references/backend-patterns.md](references/backend-patterns.md). Jump to
[Integrating into an app](#integrating-into-an-app).

Both halves share the facts immediately below. Read those either way.

## The facts that prevent most mistakes

1. **`jobId = sha256(definition)`.** Submitting an identical definition returns the same id and its **cached result** —
   it does not run again. Force a real re-run with `cq --nonce`, or by changing something in the definition. Corollary:
   if a fix seems to have had no effect, check that you actually changed the definition.
2. **Two caches, not one.** The _job_ is cached on the whole definition; the _image_ is cached on the `build` block
   alone. Changing only the command re-runs the container against the already-built image and produces **no build logs
   at all** — expected, not a failure. Empty build logs mean nothing needed building.
3. **Two different success checks.** `finishedReason === "Success"` means the _job_ completed. `StatusCode === 0` means
   the _program_ succeeded. **A container that crashed still reports `finishedReason: "Success"`.** `cq` checks both; if
   you call the API directly you must check both yourself.
4. **Submitting is not running.** With no worker on the queue, jobs sit in `Queued` forever. Check first — `cq status`,
   or `GET /q/<queue>/status` → `localWorkers`. This is the single most common reason "nothing happens".
5. **Nothing is authenticated, and the queue name is the only access control.** An unguessable name is a private queue.
   `public1` is shared and world-readable — never put secrets, credentials or private data in a job on it. For anything
   real, generate a random queue name and tell the user to keep it.
6. **Container paths are fixed.** `/inputs` is input, everything written to `/outputs` is collected into the result,
   `/job-cache` persists between jobs on that worker (model weights go there, not in the image). `JOB_INPUTS`,
   `JOB_OUTPUTS`, `JOB_CACHE` hold these paths.
7. **The image is built on the worker**, not where you are. You never need a local Docker daemon. Pass
   `--platform linux/amd64` if the architecture must match.
8. **There is no completion webhook.** `control.callbacks.queued` fires on _enqueue_; `control.callbacks.finished`
   exists in the types but nothing reads it. Learn that a job finished by polling or streaming.
9. **Data expires** (~1 month on the public instance, logs sooner). Copy anything that matters out.
10. **A job you don't link to is invisible.** A `jobId` is not something a person can open. Every submitted job has a
    short shareable page at `<api>/j/<jobId>#?queue=<queue>`. `cq run` and `cq submit --json` print it; `cq url` builds
    one. Always hand it to the user.

## The `cq` helper

Bundled at `scripts/cq.mjs`, resolved **relative to this skill's directory** — in Claude Code that is
`${CLAUDE_SKILL_DIR}/scripts/cq.mjs`. Needs `node >= 18` or `deno`, nothing to install.

```bash
CQ="node ${CLAUDE_SKILL_DIR}/scripts/cq.mjs"

$CQ status                                  # is a worker attached? do this first
$CQ run --image alpine:3.19.1 --command 'echo hi'
$CQ logs <jobId> --kind build               # why the image didn't build
$CQ result <jobId>                          # the full result JSON
$CQ outputs <jobId> --out ./outputs         # download output files
```

`cq help` lists every flag. Set `CQ_API` / `CQ_QUEUE` to change the defaults, or pass `--api` / `--queue`.

`cq run` submits, streams build logs then run logs live, downloads outputs on success, and **exits with the container's
own exit code** — so `if $CQ run ...; then` is a real test of whether the container works.

Live log streaming needs a recent deployment. Against an older one `cq` says so and falls back to polling: the job still
runs and the result is still correct, you just do not see output until it finishes. If you need the logs in that case,
`cq result <jobId>` carries them.

## Building a container that works

This is an **empirical** loop, not a code-generation task. Do not hand back a Dockerfile you have not run — the whole
point of the queue is that you can run it.

```
author image  →  cq run  →  read logs  →  fix  →  cq run  →  ...  →  exit 0 + outputs
```

Pick the narrowest thing that can work:

| Situation                            | Use                                             |
| ------------------------------------ | ----------------------------------------------- |
| A published image already does it    | `--image python:3.12-slim --command '...'`      |
| Needs packages/setup, no local files | `--dockerfile ./Dockerfile`                     |
| Needs local source files copied in   | `--dockerfile ./Dockerfile --context-dir ./ctx` |
| Build a public git repo              | `--context https://github.com/o/r/commit/<sha>` |

```bash
$CQ run --dockerfile ./Dockerfile --context-dir ./ctx \
        --command 'python /app/main.py' \
        --input data.csv=@./data.csv \
        --output-dir ./outputs
```

When it fails, **which log failed tells you what to fix**: build logs cover `docker build`, image pull/push and repo
cloning; run logs are the container's own stdout/stderr. `cq run` prints them under `── build ──` and `── run ──`, or
fetch them separately with `cq logs <jobId> --kind build|run`.

Write the Dockerfile to disk as a real file the user keeps — that, plus the working command, is the deliverable. Say
what landed in `--output-dir`, and end with the browser view URL (below) so the user can open the run you are
describing.

## Show the user the job

Everything you run is invisible until you link to it. The browser client at the API origin renders a job's live logs,
its outputs and an editable copy of its definition — that page **is** the user-facing artifact of a run.

```bash
$CQ run --image alpine:3.19.1 --command 'echo hi'   # prints "view <url>" alongside the jobId
$CQ url --image alpine:3.19.1 --command 'echo hi'   # the same link, without submitting
```

There are two forms. Prefer the short one — it is ~100 characters and survives a terminal:

```
https://container.mtfm.io/j/<jobId>#?queue=<queue>            # short: id in the path
https://container.mtfm.io/#?job=<base64-definition>&queue=<q>  # self-contained: whole definition in the hash
```

The short form works only after the job is submitted — the page loads the definition by id from
`/j/<jobId>/definition.json`. The self-contained form needs no server lookup, so it is what you use to hand someone a
job that has not been run yet, and it keeps working after the stored data expires; it is also what the page rewrites
itself to as soon as the user edits anything, so the edited job stays shareable.

Include the queue either way, or the client watches the wrong one. Note `/j/<jobId>.json` (with the suffix) is the JSON
state endpoint — not a page.

Quote the URL in your final message, not only in a tool call the user may never expand.

Recipes (git repos, multi-stage, GPU, large files), a failure-signature table, and the raw HTTP behind every `cq`
command: [references/build-and-iterate.md](references/build-and-iterate.md).

## Inputs and outputs

```bash
--input name=value          # literal, inlined into the definition
--input name=@./path        # file; inlined under 200 bytes, uploaded above it
--config name=@./path       # same, but semantically part of what the job *is*
--output-dir ./outputs      # download everything the container wrote to /outputs
```

Inputs appear in `/inputs` under the name you gave. Uploads are content-addressed by sha256, so re-uploading the same
file is free. Both `inputs` and `configFiles` are part of the job hash; use `--config` for the script or config that
defines the job and `--input` for the data it runs on.

## Integrating into an app

`cq` is a tool for _you_, not a dependency for the user's service. There is no SDK — it is plain HTTP, so do not go
looking for a package to install. Runtime: Deno, or Node 18+ (Node 22+ for built-in `WebSocket`).

```js
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
const { jobId } = await res.json();
```

Then pick how the app learns the job finished:

| Pattern                                  | Use when                            | Survives a restart |
| ---------------------------------------- | ----------------------------------- | ------------------ |
| **Poll** `GET /q/<q>/j/<id>/result.json` | the default — always works          | yes                |
| **SSE** `GET /q/<q>/j/<id>/stream`       | you want live logs for one job      | no                 |
| **WebSocket** `wss://<api>/q/<q>/client` | live state for a whole queue        | no                 |
| **Callback** `control.callbacks.queued`  | durable "accepted" (not "finished") | yes                |

`{"data": null}` from `result.json` means not finished yet. The websocket carries **state, never results** — fetch
`result.json` after seeing `Finished`. Full implementations of each, a reusable client module, a complete Deno service
and the restart-safe hybrid: [references/backend-patterns.md](references/backend-patterns.md).

## Add compute

If no worker is attached, the user runs this on any machine with Docker:

```sh
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp \
  metapage/metaframe-docker-worker:latest run \
    --cpus=4 --max-job-duration=20m --data-directory=/tmp/worker-metapage-io \
    "$QUEUE"
```

Start it on more machines to scale. `--gpus=2` (or `--gpus="device=1,3"`) offers GPUs. `--mode=local` instead runs a
self-contained API on `localhost:8000` and nothing leaves the machine — point `cq` at it with
`--api http://localhost:8000 --queue local`.

## Absolute rules

- **Never claim a container works without having run it and seen `exit 0`.** Report the actual exit code and logs.
- **Never report a job without its view URL.** A bare `jobId` gives the user nothing to open.
- **Never put secrets in a job on a shared queue** — definitions and results are readable by anyone who knows the queue
  name, and `env` values are stored with the definition.
- **Never use `latest` as a base image tag** in a Dockerfile the user keeps. Pin it, or it stops being reproducible.
- If the build fails three times on the same error, stop guessing and tell the user what the build log says.

## References

- [references/build-and-iterate.md](references/build-and-iterate.md) — build recipes, failure signatures, raw HTTP.
- [references/job-definition.md](references/job-definition.md) — every definition field, what is and is not part of the
  job hash, result shapes.
- [references/backend-patterns.md](references/backend-patterns.md) — complete Node and Deno services, the restart-safe
  pattern, production checklist.
- [references/rest-api.md](references/rest-api.md) — every endpoint, request/response shapes, worker CLI flags.

## Checklist before shipping

- [ ] It ran, and you saw `StatusCode: 0`
- [ ] The view URL is in your final message, not just in the tool output
- [ ] Unguessable queue name for anything real, stored as a secret
- [ ] A worker is attached — assert it in a healthcheck
- [ ] `maxDuration` on the job _and_ `--max-job-duration` on the worker
- [ ] Both `finishedReason` and `StatusCode` checked
- [ ] `jobId` persisted before waiting, so restarts can reconcile
- [ ] `control.namespace` if clients can resubmit faster than jobs finish
- [ ] Results copied out — public-instance data expires after ~1 month
