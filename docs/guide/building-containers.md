# Building containers

Getting a container to work is a loop: write a Dockerfile, run it, read the logs, fix what broke, run it again. The
queue makes that loop cheap — you never need a local Docker daemon, because the image is built on the worker.

```
author image  →  run  →  read logs  →  fix  →  run  →  ...  →  exit 0 + outputs
```

This page uses `cq`, a small helper that wraps the API. Everything it does is plain HTTP, so if you would rather call
the endpoints directly, see [REST API](/guide/rest-api) — the workflow is identical.

## Setup

`cq` ships with the [Agent Skill](/guide/agent-skill) and needs `node >= 18` or `deno`, with no dependencies to
install.

```sh
curl -fsSL https://container.mtfm.io/docs/skill/install.sh | sh
alias cq="node ~/.claude/skills/compute-queues/scripts/cq.mjs"
```

Point it at an API and a queue once:

```sh
export CQ_API=https://container.mtfm.io
export CQ_QUEUE=public1
```

Then check a worker is actually listening. **This is the first thing to do in any session** — with no worker attached,
jobs sit in `Queued` forever and nothing appears to happen:

```sh
cq status
# queue public1: 0 job(s), 1 worker source(s)
```

If that reports no workers, start one (see [Running workers](/guide/workers)):

```sh
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp \
  metapage/metaframe-docker-worker:latest run \
    --cpus=4 --max-job-duration=20m --data-directory=/tmp/worker-metapage-io \
    "$CQ_QUEUE"
```

::: tip Everything local
A worker started with `--mode=local` serves the same API itself on `localhost:8000` and nothing leaves the machine.
Point `cq` at it with `--api http://localhost:8000 --queue local`.
:::

## The first run

Start with the narrowest thing that can work — often a published image and a command, with no Dockerfile at all:

```sh
cq run --image alpine:3.19.1 --command 'sh -c "echo hi; echo done > /outputs/result.txt"'
```

```
▶ job 9f2400a474880666da9f5e008812d0e61dddc3670c37ed09cb4a0dea5a01952f
  queue public1  ·  https://container.mtfm.io/q/public1/j/9f24…/result.json
◆ Queued
◆ Running
── build ──────────────────────────────
3.19.1: Pulling from library/alpine
Status: Image is up to date for alpine:3.19.1
── run ────────────────────────────────
hi
◆ Finished (Success)
✓ success (exit 0, 199ms, outputs: result.txt)
```

`cq run` submits, streams the logs, and **exits with the container's own exit code** — so `if cq run …; then` is a real
test of whether the container works.

## Building from a Dockerfile

When you need packages or setup, write a real Dockerfile and pass it. Its contents are sent inline, so there is no
context to upload:

```dockerfile
# Dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir polars
```

```sh
cq run --dockerfile ./Dockerfile --command 'python -c "import polars; print(polars.__version__)"'
```

## Building with local source files

To copy your own files into the image, add a build context directory. `cq` tars it, uploads it, and points the build
at it:

```
myproject/
  Dockerfile
  src/main.py
```

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY src/ /app/src/
```

```sh
cq run --dockerfile ./myproject/Dockerfile --context-dir ./myproject \
       --command 'python /app/src/main.py'
```

## Building from a git repo

```sh
cq run --context https://github.com/owner/repo/commit/9f3c1a2 \
       --command './build/bin/thing --version'
```

Pin to `/commit/<sha>` rather than a branch. A branch URL means the job's meaning changes silently over time, and the
definition hash will not reflect it. If the repo already has a usable Dockerfile, name it with `--filename` and point
at its subdirectory with `--build-context`.

## Reading failures

Build logs and run logs are **separate streams**, and which one failed tells you what to fix.

```sh
cq logs <jobId> --kind build     # the image never built — fix the Dockerfile
cq logs <jobId> --kind run       # it built and the program failed — fix the program
```

A failed build looks like this — the docker error names the step and the exit code:

```
#5 [2/2] RUN this-command-does-not-exist
#5 0.143 /bin/sh: this-command-does-not-exist: not found
#5 ERROR: process "/bin/sh -c this-command-does-not-exist" did not complete successfully: exit code: 127
◆ Finished (Error)
✗ job did not complete: Error
```

A failed *program* looks completely different — the job succeeded, the container did not:

```
── run ────────────────────────────────
Traceback (most recent call last):
  File "/app/src/main.py", line 3, in <module>
◆ Finished (Success)
✗ container exited 1
```

::: warning Two success checks
`finishedReason: "Success"` means the **job** completed. `StatusCode: 0` means the **program** succeeded. A container
that crashed still reports `finishedReason: "Success"`. Check both — `cq` does.
:::

Common signatures:

| What you see                                        | What it means                                                          |
| --------------------------------------------------- | ----------------------------------------------------------------------- |
| Job stays `Queued`, no logs at all                  | No worker on the queue. `cq status`.                                    |
| Result returns instantly with old logs              | Cached — the definition is unchanged. Add `--nonce` to force a re-run.  |
| No build logs at all                                | The image was already built and cached. Expected.                       |
| Build log stops at `Downloading context`            | Bad context URL, a private repo, or an archive that is neither gz nor zip. |
| `StatusCode: 137`                                   | OOM-killed. Raise `--memory`.                                            |
| `finishedReason: "TimedOut"`                        | Exceeded `maxDuration`, or the worker's own ceiling.                    |
| `exec format error`                                 | Architecture mismatch. `--platform linux/amd64`.                        |
| Program cannot find its input                       | Inputs land in `/inputs/<name>`, not the working directory.             |

You cannot shell into a job, so when something is confusing, make the container tell you:

```sh
cq run --image <the same image> --command 'sh -c "ls -la /inputs /outputs; env | sort"'
```

## Iterating

Change the Dockerfile or the command and run again. Two caches shape what happens next:

- The **job** is keyed on the whole definition. Resubmitting an identical definition returns the cached result without
  running. `--nonce` forces a genuine re-run.
- The **image** is keyed on the `build` block alone. Changing only the command re-runs against the already-built image
  — fast, and with no build logs.

## Inputs and outputs

```sh
cq run --dockerfile ./Dockerfile --context-dir . \
       --command 'python /app/src/main.py' \
       --input data.csv=@./data.csv \
       --output-dir ./outputs
```

Inputs appear at `/inputs/<name>`; anything the container writes to `/outputs` is collected into the result and
downloaded by `--output-dir`. Files over ~200 bytes are uploaded to content-addressed blob storage automatically — see
[Files in & out](/guide/files) for the details and for `/job-cache`, which persists between jobs on the same worker.

## Command reference

```
cq run       submit, stream logs, report the result   (the usual one)
cq submit    submit only, print the jobId
cq logs      print logs; --follow to stream
cq wait      wait for completion, then report
cq result    print the result JSON
cq outputs   download output files
cq upload    upload a file, print its DataRef URL
cq status    queue status — are any workers attached?
cq url       print a browser URL for a definition
```

`cq help` lists every flag.

## Doing this without `cq`

Every step above is one HTTP call against an unauthenticated API:

```sh
API=https://container.mtfm.io; QUEUE=public1

JOB=$(curl -s -X POST "$API/q/$QUEUE" -H 'content-type: application/json' -d '{
  "definition": {"build": {"dockerfile": "FROM alpine:3.19.1\nRUN apk add --no-cache jq"},
                 "command": "jq --version"}
}' | jq -r .jobId)

curl -sN "$API/q/$QUEUE/j/$JOB/stream"                 # live: build-log, run-log, state, final
curl -s "$API/q/$QUEUE/j/$JOB/build-logs.json?since=0" # or poll with a cursor
curl -s "$API/q/$QUEUE/j/$JOB/result.json"             # {"data": null} until finished
curl -s "$API/q/$QUEUE/j/$JOB/outputs/result.txt"      # one output file
```

Full endpoint list and payload shapes: [REST API](/guide/rest-api).
