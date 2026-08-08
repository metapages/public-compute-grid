---
name: compute-queues-repo
description: >
  What this repo (worker.metapage.io / compute-queues) is and how it fits
  together: a browser-first Docker client plus private, zero-auth compute
  queues that anyone can add compute to. Load whenever working anywhere in this
  repo — changing the API routes, the worker, the browser client, the CLI, or
  shared types — or when answering "what does this project actually do?".
---

# worker.metapage.io — browser Docker client + private compute queues

**One sentence:** a queue is just a URL, a job is just a URL, anyone can push a Docker job to a queue without
authentication, and anyone can add compute to that queue by running one `docker run` command.

Production API + browser client: <https://container.mtfm.io>\
Public LLM summary served at `/llms.txt` (`app/browser/public/llms.txt`).

## The mental model (say it this way)

| Thing      | What it really is                                                      |
| ---------- | ---------------------------------------------------------------------- |
| **Queue**  | An arbitrary string in a URL: `POST /q/<queue>`. Created on first use. |
| **Job**    | A Docker container spec. Its id is the sha256 of the definition.       |
| **Worker** | Any machine running the worker image, pointed at a queue name.         |
| **Result** | JSON at `GET /q/<queue>/j/<jobId>/result.json`.                        |

Consequences that surprise people, and that you should preserve when editing:

- **No auth.** Security is URL obscurity — a queue name nobody can guess is a private queue. Do not add auth assumptions
  to route handlers.
- **Job id = hash of the definition** (`shaDockerJob` in `app/shared/src/shared/jobtools.ts`). Submitting the same
  definition twice is deduplicated to one job, and its cached result comes straight back. Anything that must produce a
  distinct job has to change the definition.
- **The API stores no compute.** It is a coordination layer (Deno KV) plus S3 blobs. All execution happens on workers
  that other people own.
- **Data expires** (~1 month in production). Nothing here is durable storage.
- **The browser is a first-class client**, not an afterthought. The client is a metaframe (iframe) so arbitrary compute
  can be embedded in a web page safely.

## Layout

```
app/
  api/      Deno + Hono. REST + WebSocket coordination. Deployed to Deno Deploy.
  browser/  React + Vite + Chakra + Zustand. The metaframe client (also the /
            web UI). Built into app/browser/dist and served by the api.
  worker/   Deno + dockerode. Runs the containers. Shipped as the docker image
            metapage/metaframe-docker-worker. Local and remote modes.
  cli/      Deno CLI (cliffy): `job add`, `job await`.
  shared/   Types + the job state machine + datarefs. Imported by everything.
  test/     Functional tests against a real local stack.
  deploy/   Cloud worker fleet deployment examples.
docs/       VitePress docs site, served at /docs (see docs/justfile).
```

### Files to open first

| Question                        | File                                                                       |
| ------------------------------- | -------------------------------------------------------------------------- |
| What routes exist?              | `app/api/src/handlerHono.ts` (the whole route table)                       |
| Job/state/message types         | `app/shared/src/shared/types.ts`                                           |
| The queue state machine         | `app/shared/src/shared/jobqueue.ts` (client+worker socket)                 |
| Enqueue / KV persistence        | `app/shared/src/shared/db.ts`                                              |
| Submission webhooks             | `app/shared/src/shared/webhooks.ts`                                        |
| Container creation, env, mounts | `app/worker/src/queue/DockerJob.ts`                                        |
| Image build / context download  | `app/worker/src/queue/dockerImage.ts`                                      |
| Inputs/outputs → datarefs       | `app/worker/src/queue/IO.ts`, `shared/src/shared/dataref.ts`               |
| Build/run logs over HTTP        | `app/api/src/routes/api/v1/logs.ts`, `shared/src/shared/stream-handler.ts` |
| Browser state                   | `app/browser/src/store.ts`                                                 |

## Job lifecycle

`Queued → Running → Finished` (plus `Removed`, a short-lived tombstone so state propagates before deletion). Enum:
`DockerJobState`. Finish reasons: `Success | Error | TimedOut | Cancelled | WorkerLost | JobReplacedByClient | Deleted`.

Three ways a client learns a job finished — all three are documented for users in `docs/guide/backend-integration.md`,
keep that page true if you change behaviour:

1. **Polling** `GET /q/:queue/j/:jobId/result.json` — `{"data": null}` until finished. Always works.
2. **WebSocket** `wss://<api>/q/:queue/client` — server pushes `JobStates` (full snapshot, on connect) and
   `JobStateUpdates` (deltas). The payload carries _state only_, never the result body; fetch `result.json` after seeing
   `Finished`.
3. **Webhook** `control.callbacks.queued` — fires when the job is **enqueued**, not when it finishes, and retries every
   minute via `Deno.cron` until 2xx. ⚠️ `control.callbacks.finished` exists in the type but **nothing reads it**; there
   is no finished webhook yet.
4. **SSE** `GET /q/:queue/j/:jobId/stream` — one request follows one job (`build-log`, `run-log`, `state`, `final`) and
   closes on a terminal state. Built for scripts and agents that want live logs without the stateful websocket.

### Logs

Logs stream from the worker as `JobStatusLogs` messages tagged with a `step`. `BaseDockerJobQueue` splits them into
**build** (`docker build`, image pull/push, `cloning repo`) and **run** buffers, serves them live from memory, and
flushes both to KV on `stateChangeJobFinished` (1 week TTL). Exposed as `build-logs.json` / `run-logs.json`
(cursor-paged via `?since=`) and over SSE. Keep the two kinds separate — telling "the build broke" from "the program
broke" is the whole point.

## Inputs and outputs

Everything moving in or out of a container is a `DataRef` (`{value, type: base64|utf8|json|url|key, hash?}`).

- Small payloads travel inline as `base64`/`utf8`.
- Anything over `ENV_VAR_DATA_ITEM_LENGTH_MAX` (200 bytes) goes to blob storage: `PUT /f/<sha256>`, then referenced as
  `{type:"url", value:"<api>/f/<sha256>"}`.
- Inside the container: `/inputs` (read), `/outputs` (write), `/job-cache` (shared across jobs on that worker). Env vars
  injected: `JOB_ID`, `JOB_INPUTS`, `JOB_OUTPUTS`, `JOB_CACHE`, `JOB_URL_PREFIX`, `JOB_INPUTS_URL_PREFIX`,
  `JOB_OUTPUTS_URL_PREFIX`, and `CUDA_VISIBLE_DEVICES=0` when a GPU is allocated.

## Namespaces and tags

- `control.namespace`: a sub-partition of a queue that tolerates only **one** live job. Submitting a new job in a
  namespace removes the previous one. This is how a browser tab avoids piling up jobs. Default namespace is `_`.
- `definition.tags`: intended to restrict a job to workers with matching tags — the field is plumbed through the types
  and `InMemoryDockerJob`, but **nothing matches on it and the worker has no `--tags` flag**. Reserved, not working.

## Commands

Always `just`. Root `justfile` delegates to `app/*/justfile`.

```sh
just dev                 # whole local stack (api + browser + worker + minio + denokv)
just app/worker/local    # ⚡ just the local-mode worker — it serves the same API on :8000
                         #    (queue "local"), starts in seconds, no docker compose. Use this
                         #    to test anything that doesn't need the cloud API or the browser.
just test                # full suite against a fresh local stack
just dev-install-skill   # symlink docs/public/skill/* into ~/.claude/skills so repo edits
                         #    are live while iterating (just dev-uninstall-skill to undo)
just test-skill          # compute-queues skill's API surface (needs a worker, see above)
just test-skill-ai       # spawns real `claude -p` sessions; costs tokens, not in `just test`
just check               # typescript compile checks
just fmt / just lint-fix
just worker dev|prod     # worker only, against local or prod api
just browser dev|prod
just api dev
just docs dev            # docs site
just deploy [version]    # bump version + commit; CI publishes
```

Local stack is HTTPS via mkcert at `https://worker-metaframe.localhost`; run `mkcert -install` once.

## Deployment topology

- Push to `main` → `.github/workflows/deploy.yml` → `just app/api/deploy`, which assembles a standalone dir (the api
  `src/`, `app/browser/dist`, `shared/`, `docs/dist`) and runs `deno deploy --prod`. **If you add a new served
  directory, add it to that copy list or it will 404 in production.**
- Bumping `app/worker/mod.json` version → worker binary + docker images published, git tag pushed.

## Editing rules of thumb

- Types live in `app/shared/src/shared/types.ts` and are re-exported through `client.ts` / `mod.ts`; add there, not in a
  single app.
- The state machine is deliberately tiny (4 states). Think hard before adding one — the codebase comment says so, and it
  means it.
- Route handlers stay thin; logic belongs in `shared/`.
- Anything user-facing about the queue protocol has a home in `docs/` and in the distributable skill under
  `docs/public/skill/compute-queues/` — update both. The skill is deliberately ONE skill covering both audiences
  (building a container, and integrating submission into an app); splitting it caused the wrong half to be selected on
  ambiguous prompts. See docs/guide/agent-skill.md § Why one skill and not two.
- `docs/public/skill/compute-queues/scripts/cq.mjs` is shipped to users verbatim. It must stay dependency-free and run
  under both node ≥ 18 and deno — no `Buffer`, no npm imports.
