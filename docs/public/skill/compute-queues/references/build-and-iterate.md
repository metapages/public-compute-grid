# Building and iterating

Recipes, failure signatures, and the raw HTTP behind every `cq` command.

## Recipes

Assume `CQ="node ${CLAUDE_SKILL_DIR}/scripts/cq.mjs"` and a queue with a worker on it.

### Package a local project

The common case: some source files, a Dockerfile, some input data.

```
ctx/
  Dockerfile
  src/main.py
```

```bash
$CQ run --dockerfile ./ctx/Dockerfile --context-dir ./ctx \
        --command 'python /app/src/main.py' \
        --input data.csv=@./data.csv \
        --output-dir ./outputs
```

`--context-dir` tars the directory, uploads it, and points `build.context` at it. `--dockerfile` inlines the Dockerfile
contents on top, so you can iterate on the Dockerfile without re-uploading the context — but note that changing either
changes the jobId, so the build does re-run.

### Build a public git repo

```bash
$CQ run --context https://github.com/owner/repo/commit/<sha> \
        --dockerfile ./Dockerfile \
        --command './build/bin/thing --help'
```

Pin to `/commit/<sha>` rather than a branch: a branch URL means the job's identity silently changes meaning over time,
and the definition hash will not reflect it.

If the repo already contains a usable Dockerfile, drop `--dockerfile` and use `--filename` to name it, plus
`--build-context` if it lives in a subdirectory:

```bash
$CQ run --context https://github.com/owner/repo/commit/<sha> \
        --build-context docker --filename Dockerfile.gpu \
        --command '...'
```

### Multi-stage build

```dockerfile
FROM golang:1.23 AS build
WORKDIR /src
COPY . .
RUN go build -o /out/app ./cmd/app

FROM debian:bookworm-slim
COPY --from=build /out/app /usr/local/bin/app
```

```bash
$CQ run --dockerfile ./Dockerfile --context-dir . --command 'app --version'
```

Use `--target build` to stop at an intermediate stage — invaluable for debugging a build that fails late.

### Large inputs

Anything over ~200 bytes is uploaded automatically by `--input name=@path`. To reuse one blob across many jobs, upload
once and reference it:

```bash
URL=$($CQ upload ./model.bin)
$CQ run --image ... --env MODEL_URL="$URL" --command 'sh -c "curl -sL $MODEL_URL -o /tmp/m.bin && ./run /tmp/m.bin"'
```

Better for anything big and reused: download it into `/job-cache` inside the container and check for it first. That
directory survives between jobs on the same worker.

### GPU

```bash
$CQ run --image pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime \
        --gpus 1 --shm-size 2g \
        --command 'python -c "import torch; print(torch.cuda.is_available())"'
```

`CUDA_VISIBLE_DEVICES` is set inside the container. A worker must have been started with `--gpus=N` or the job waits
forever.

### Long jobs

```bash
$CQ run ... --max-duration 2h
```

The worker also has its own `--max-job-duration` ceiling; the effective limit is the smaller of the two. If jobs are
being killed early, that worker-side flag is usually why.

## Failure signatures

| What you see                                        | What it means                                                                |
| --------------------------------------------------- | ---------------------------------------------------------------------------- |
| Job stays `Queued`, no logs at all                  | No worker on the queue. `cq status`.                                         |
| Result comes back instantly with old logs           | Cached — identical definition. Add `--nonce` to force a re-run.              |
| Build log ends at `Downloading context` / `cloning` | Bad `context` URL, private repo, or an archive that is neither gzip nor zip. |
| Build log: `failed to solve: ... exit code: N`      | A `RUN` step failed. The lines just above it are the real error.             |
| `finishedReason: "Success"`, `StatusCode: 137`      | OOM-killed. Raise `--memory`, or reduce the workload.                        |
| `finishedReason: "TimedOut"`                        | Exceeded `maxDuration` or the worker's ceiling.                              |
| `finishedReason: "WorkerLost"`                      | The worker died or disconnected mid-job. Resubmit with `--nonce`.            |
| `exec format error`                                 | Architecture mismatch. `--platform linux/amd64`.                             |
| Program cannot find its input                       | Inputs land in `/inputs/<name>`, not the working directory.                  |
| Outputs empty                                       | The program must write to `/outputs` (`$JOB_OUTPUTS`), not anywhere else.    |

### Debugging a container interactively-ish

You cannot shell into a job, so make the container tell you:

```bash
$CQ run --image <same image> --command 'sh -c "ls -la /inputs /outputs; env | sort; which python"'
```

A cheap `ls`/`env` job against the same image usually settles a "why can't it find X" question in one round trip.

## Raw HTTP

Everything `cq` does is plain HTTP against an unauthenticated API. Use this when node and deno are both unavailable, or
when embedding the flow in another program.

```bash
API=https://container.mtfm.io
QUEUE=public1

# submit
JOB=$(curl -s -X POST "$API/q/$QUEUE" -H 'content-type: application/json' -d '{
  "definition": {"image":"alpine:3.19.1","command":"sh -c \"echo hi > /outputs/o.txt\""}
}' | jq -r .jobId)

# follow (SSE): build-log / run-log / state / final events
curl -sN "$API/q/$QUEUE/j/$JOB/stream"

# or poll logs with a cursor
curl -s "$API/q/$QUEUE/j/$JOB/build-logs.json?since=0"
curl -s "$API/q/$QUEUE/j/$JOB/run-logs.json?since=0"

# result — {"data": null} until finished
curl -s "$API/q/$QUEUE/j/$JOB/result.json"

# one output file
curl -s "$API/q/$QUEUE/j/$JOB/outputs/o.txt"

# workers attached?
curl -s "$API/q/$QUEUE/status"
```

### Endpoints

| Method | Path                                        | Purpose                                           |
| ------ | ------------------------------------------- | ------------------------------------------------- |
| POST   | `/q/:queue`                                 | Submit. Body `{definition, control?}` → `{jobId}` |
| GET    | `/q/:queue/status`                          | Queue state and attached workers                  |
| GET    | `/q/:queue/j/:jobId/stream`                 | **SSE**: `build-log`, `run-log`, `state`, `final` |
| GET    | `/q/:queue/j/:jobId/build-logs.json?since=` | Build logs slice → `{data, nextCursor, isFinal}`  |
| GET    | `/q/:queue/j/:jobId/run-logs.json?since=`   | Run logs slice, same shape                        |
| GET    | `/q/:queue/j/:jobId/result.json`            | `{"data": null}` until finished                   |
| GET    | `/q/:queue/j/:jobId/definition.json`        | The submitted definition                          |
| GET    | `/q/:queue/j/:jobId/outputs/*`              | One output file, decoded                          |
| GET    | `/q/:queue/j/:jobId/inputs/*`               | One input file, decoded                           |
| POST   | `/q/:queue/j/:jobId/cancel`                 | Cancel a running job                              |
| PUT    | `/f/:sha256`                                | Upload a blob (follow redirects)                  |
| GET    | `/f/:sha256/exists`                         | 200 if present — check before uploading           |
| GET    | `/f/:sha256`                                | Download a blob                                   |

The `build-logs.json` / `run-logs.json` / `stream` endpoints read a live in-memory buffer while the job runs and the
persisted copy afterwards, so they work both during and after execution. Logs are retained for about a week — much less
than results.

### SSE event shapes

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

A log line is `[text, timestampMs]` or `[text, timestampMs, isStderr]`. `cursor` is the running count of lines emitted
for that kind. Everything already known is replayed on connect, so opening the stream against a finished job returns its
full history and `final` in one short read.

## Checklist before handing back a container

- [ ] It ran, and you saw `StatusCode: 0`
- [ ] The Dockerfile exists as a file the user keeps, with a pinned base image tag
- [ ] Outputs were actually produced and downloaded, and contain what was asked for
- [ ] `maxDuration` set if it is not fast
- [ ] No secrets in `env`, `inputs` or the Dockerfile if the queue is shared
- [ ] For a real workload: an unguessable queue name, not `public1`
