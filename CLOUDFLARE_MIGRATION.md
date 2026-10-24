# Cloudflare migration: container.mtfm.io API + browser

Deno Deploy shuts down in about 6 months. We're doing this now, while we have the time. The S3 bucket is also being shut
down, so blobs move to R2.

**Overview:** the API becomes a Cloudflare Worker (`app/cloudflare`). Each queue gets its own Durable Object, which
holds that queue's websockets and in-memory state, so the Redis bus goes away. The store is D1. Blobs go to R2 through
the S3 API, with MinIO as the local stand-in. The browser and docs are served as Workers Static Assets. Webhook retry
runs on a Cron Trigger.

**Approach:**

- Write the integration tests first, then work from them. They run against any `API_URL`: the local Deno stack, the
  local Cloudflare stack (`just test cloudflare`), and staging (`just test-url <url>`).
- The local stack has to keep working the whole way through.

**Long-term direction:** the worker moves to Node, and server, worker and CLI eventually become one program. So new core
code uses web-standard APIs behind small interfaces (`getEnv`, `KvStore`, `SqlDriver`, the queue bus, `S3BlobStore`),
with no `Deno.*` or Workers-only calls. Only the runtime adapters are runtime-specific. `SqlKvStore` already runs on
`node:sqlite`.

## 0. Integration tests first

- [x] HTTP contract tests: submit, result, job lookups, namespaces, `/f/` upload/exists/download
      (`app/test/src/contract_api_test.ts`)
- [x] Websocket tests: client snapshot on connect, and state deltas for a job submitted over HTTP
- [x] SSE `/stream` and `build-logs.json` / `run-logs.json` tests
- [x] Static route tests: `/`, `/j/<id>`, `/docs`, `/llms.txt`
- [x] Webhook retry test (`webhooks_retry_test.ts`): the first call fails, and the scheduler must retry (Deno.cron, or a
      Cron Trigger fired by the compose `cron` sidecar)
- [x] Big-log test: ~200 KB of output must be fully kept after the job finishes
- [x] MCP HTTP test (the existing MCP suites pass against Cloudflare)
- [x] `just test-url <url>` runs the contract suite against any deployment, with a temporary local worker
- [x] `KvStore` conformance test, run against both Deno KV and `SqlKvStore`

## 1. S3 → R2

- [x] Staging R2 bucket `compute-queues-staging`, with CORS and a 30-day expiry rule (`just cloudflare setup staging`)
- [x] Blob store on `aws4fetch` (fetch + WebCrypto; R2, MinIO, any runtime) instead of the AWS SDK
- [x] An R2 API token (Object Read & Write) for the Worker's S3 presigning: `just cloudflare secrets staging`. It must
      be scoped to the bucket — a token scoped elsewhere fails every request with `403 AccessDenied`, which looks like a
      signing bug and is not one
- [ ] Production bucket, at cutover (`just cloudflare setup production`)
- [ ] Switch the Deno Deploy env to R2 (it works there too). No copy needed, since data expires after a month

## 2. Runtime-neutral core

- [x] `getEnv()` replaces `Deno.env.get` in shared, api and mcp module scope
- [x] Store interface (`KvStore`, injected with `setKv`), with Deno KV satisfying it unchanged
- [x] Bus interface (`setBroadcastChannelFactory`, `NoopBroadcastChannel`)
- [x] `createApp()` is the API without static files; the Deno server (API + static files from disk) is `handlerDeno.ts`
- [ ] `BaseDockerJobQueue.tick()` so the intervals can be driven from outside (needed for hibernation)
- [x] Switch hono to `npm:hono@4.13.13` (was a `deno.land/x` release candidate): one copy of hono for every runtime,
      shared with `@hono/mcp`, and no `deno.land/x` left in the Worker bundle
- [x] Drop the Deno-only imports from the Worker graph (std/fs → node:fs, the Deno static handler moved out), and delete
      the Deno shim

## 3. Cloudflare app and local stack (`app/cloudflare/`)

- [x] `wrangler.jsonc`: nodejs_compat, assets, DO `QUEUES`, D1, cron, staging env
- [x] Worker entry: route `/q/:queue/*` (HTTP and websockets) and `/j/:id/copy` to `QueueDurableObject`, the rest
      statelessly
- [x] `QueueDurableObject`: websockets, submit, cancel, SSE, logs, status
- [x] Store: `SqlKvStore` over D1. Everything is in D1, because db.ts reads across queues (a job's namespaces span
      queues), so a per-queue DO store doesn't fit yet
- [x] Cron Trigger: webhook retry and the expired-row sweep
- [x] Static assets: `/docs` cleanUrls, VitePress' 404 for missing docs, `/j/<id>` rebased
- [x] MCP `/mcp`: the tools call the API in-process through the router
- [x] `deno bundle` → `wrangler deploy`: `just cloudflare build|dev|deploy`
- [x] Docker compose `cloudflare` mode (`just dev cloudflare`, `just test cloudflare`): the full suite passes
- [x] Logs in blob storage (R2/MinIO, or files in local mode), with a pointer in the store. They used to be one KV
      value, and Deno KV rejects values over 64 KiB ("Value too large"), so long logs were lost
- [x] `/mcp/ws` retired: it served a second MCP implementation (~1900 lines in `app/api/src/routes/mcp/`) that read
      in-process state across every queue, was never served by the local-mode worker, and cannot work with one Durable
      Object per queue. Clients use Streamable HTTP at `/mcp`. `/mcp/info` is now generated from the one tool list
      (`MCP_TOOLS`), which both servers use — it had been advertising tools that no longer existed

## 4. Staging

Live at **https://compute-queues-staging.dion19.workers.dev** (D1 `compute-queues-staging`, R2 `compute-queues-staging`,
one Durable Object per queue, cron every minute).

- [x] Staging D1 database and R2 bucket created; `just cloudflare setup <env>` is idempotent
- [x] R2 API token uploaded as Worker secrets
- [x] `just cloudflare deploy staging`
- [x] `just test-url https://compute-queues-staging.dion19.workers.dev` — 10/10
- [x] A real worker attached, and the OpenThermo repo built and run end to end (`cq` with a git build context): the repo
      cloned at its commit, compiled with gcc in the container, the binary run on a Gaussian log, exit 0, build and run
      logs persisted separately, and the output file served back through the API from R2
- [x] Worker reconnect across a redeploy: re-registered ~2s after `deploy`, with jobs succeeding either side of it
- [x] Long SSE streams: one `cq run` stream followed a multi-minute image build to completion
- [x] Cron verified firing on staging (`wrangler tail`: scheduled invocations, outcome ok, no exceptions)
- [x] Load test, one hot queue: 40 concurrent submissions, all accepted, all finished (21s wall, POST p95 2.9s under
      that concurrency)
- [x] D1 latency through the queue DO: ~360ms of server-side work per submission (sequential submit ~458ms against a
      ~99ms warm-connection baseline). See the latency finding below

## 5. Cutover

- [ ] Lower the DNS TTL
- [ ] Point `container.mtfm.io` at the Worker
- [ ] Monitor for 48h. Rollback is pointing DNS back at Deno Deploy
- [ ] Replace `deploy.yml` with `just cloudflare deploy production`

## 6. Cost: hibernation

- [ ] Hibernatable WebSockets and alarms in place of intervals. Today a queue's DO stays awake while it has a socket or
      a live interval
- [ ] Rehydrate queue state from the store on wake

## 7. Cleanup (before Deno Deploy shuts down)

- [ ] Remove the Redis bus and its env vars
- [ ] Shut down the Deno Deploy app and the S3 bucket
- [ ] Update the repo skill, READMEs and `docs/` ("Deployed to Deno Deploy")

## Later: Node and unification

- [ ] Worker runs on Node
- [ ] Single server/worker/CLI program, sharing the runtime-neutral core. The local-mode worker's
      `app/worker/src/lib/local-handler.ts` duplicates the api routes and is the first thing to fold in

## Findings along the way

- `std/fs` cannot be replaced with plain `node:fs` for the worker's file handling: doing so made a served job input come
  back empty in the docker stack (CI failed on the inputs/outputs and multi-segment-path tests, locally it passed).
  `shared/fs.ts` now dispatches — Deno's own calls on Deno, `node:fs` on Workers and Node — because `std/fs` reads Deno
  globals at import time and `jobtools.ts` is in the Worker's module graph.
- `get_job_status` swallowed both of its lookup failures and returned a job with no state, so an unreachable or
  overloaded API was indistinguishable from an absent job. It now reports the two apart, and `waitForJobCompletion` says
  which of four things happened, so a CI timeout explains itself.
- CI's two jobs run on two different org runners (`mini1` and `mini2`), not on one machine: they share nothing, so
  serializing them would achieve nothing. They are also slower than a dev machine, which is why timing-sensitive
  failures show up there and not locally. (An earlier guess that they shared one workspace was wrong — the repo-level
  runner list shows one runner, but the org-level runners that actually take these jobs are not visible from it.)
- A submit could answer 200 for a job the queue never took: `stateChangeJobEnqueue` returned void and gave up silently
  when `queueJobAdd` declined, so the caller polled forever. It now returns the queued job, and the submit handlers
  answer 409. The MCP client also dropped response bodies from its errors, so the server's explanation never surfaced.

- Submission latency, measured against staging (medians, warm connection): a fresh job **410ms**, resubmitting an
  already-finished job **249ms**. The writes in `queueJobAdd` are already one `kv.atomic()` batch, i.e. a single D1
  round trip, so there is no write batching left to do. The fresh path is the expensive one: it PUTs `definition.json`
  to R2 and does a handful of D1 reads, where a resubmission short-circuits on the cached record and skips both.
- `isFinishedJobMissingResults` is not on the expensive path, despite looking like it: it returns with no I/O at all
  unless the record is already Finished with Success/Error/TimedOut, and its one read is R2, not D1 — so it cannot join
  a D1 batch, and a fresh submission never pays for it. Nothing to win there; the numbers above are why.
- None of this is blocking: jobs run for seconds to minutes.

- Fixed: the worker lost all job output after the first ~32 KB. Its log-grabbing transform streams were never read, so
  they stopped accepting data once their buffers filled (`DockerJob.ts`, now drained with `resume()`).
- Fixed: logs over 64 KiB failed to persist (above).
- Fixed: log lines arriving after a job finished (e.g. the image push) replaced its full build log in `build-logs.json`,
  which then showed only the push lines.
- Production deploys from main have failed since Oct 6: Deno Deploy reports "The revision failed". The last successful
  deploy was Aug 18.
- A finished record whose results are gone is re-run correctly on a single-instance server (the local-mode worker, and
  Cloudflare) but not on the multi-instance Deno stack: `resolveMostCorrectJob` prefers the _earliest_ Success, so the
  dead record keeps winning over the fresh one the re-run produces. One more reason a queue belongs in one place.
- One unexplained one-off: `Test exists API` saw a never-uploaded key report `exists: true` on the Cloudflare stack. It
  did not recur on a re-run or reproduce against the same code by hand, and the run's api logs were gone by then.
- Pre-existing flakes, reproduced on main on the same machine: MCP jobs reported "not on queue" or "unknown", and the
  local inputs/outputs and multi-segment path tests. Each stack passes in full on most runs (local 60/60, remote 64/64,
  cloudflare 63/64, the one failure an MCP flake).
