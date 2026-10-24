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

Assumes PR #313 is merged. Today `container.mtfm.io` is an **A record to 69.67.170.170** (Deno Deploy,
`server:
deployd`) with a **14400s (4h) TTL**, and `mtfm.io` is hosted on **Google Cloud DNS**
(`ns-cloud-*.googledomains.com`), not Cloudflare. `js.mtfm.io` points at the same Deno Deploy IP, so the zone carries
other services too.

### Decisions needed before starting

- **How `container.mtfm.io` reaches a Worker.** A Worker can only serve a hostname that belongs to a zone on this
  Cloudflare account, and `mtfm.io` is not one. Two routes:
  1. **Delegate just the subdomain** (recommended): add `container.mtfm.io` to Cloudflare as its own zone and add NS
     records for it in Google Cloud DNS. Nothing else under `mtfm.io` moves, so `js.mtfm.io` and the rest are untouched.
     Confirm subdomain zones are available on the account's plan before committing to this.
  2. **Move the whole `mtfm.io` zone** to Cloudflare (nameserver change at the registrar). More blast radius: every
     record has to be copied first, and `js.mtfm.io` and anything else in the zone move with it.
- **Old job results.** Results live in the current S3 bucket; after cutover the Worker reads R2, so results from before
  the cutover return 404 until they would have expired anyway (~1 month). A job id is the hash of its definition, so
  resubmitting re-runs the job and produces a fresh result. Accept that, or copy the bucket, or add a temporary
  read-through to S3. Confirm nothing depends on month-old results.
- **`/mcp/ws` is gone.** Production still advertises the legacy tool list (`create_job`, `execute_job`, …) at
  `/mcp/info` and still serves `/mcp/ws`; both go away. `/mcp` (Streamable HTTP) is unaffected. Confirm no client is
  pointed at `/mcp/ws`.

### Infrastructure to create

- [ ] `just cloudflare setup production` — D1 `compute-queues`, R2 `compute-queues` with CORS and the 35-day expiry
      rule, and the real `database_id` written into `wrangler.jsonc`. **Commit that id.**
- [ ] An R2 API token (Object Read & Write, scoped to `compute-queues`), then
      `R2_ACCESS_KEY_ID=… R2_SECRET_ACCESS_KEY=… just cloudflare secrets production`
- [ ] The zone/delegation chosen above, then a Workers **Custom Domain** for `container.mtfm.io` on the `compute-queues`
      Worker (add it to `wrangler.jsonc` as a route so it is in version control)
- [ ] GitHub repo secrets `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`, and a `production` option in
      `.github/workflows/deploy-cloudflare.yml`

### Before any traffic moves

- [ ] Publish a worker release so deployed workers pick up the quiet-while-idle cadence (bump `app/worker/mod.json`).
      Without it queues stay resident: old workers re-register every 10s. They keep working either way
- [ ] `just cloudflare deploy production` — live on its own hostname, serving no real traffic yet
- [ ] Verify production **before** the DNS flip: `just test-url https://<the custom domain>` (or via a hosts override
      while DNS still points at Deno Deploy), then attach a real worker and run the OpenThermo job end to end
- [ ] Check the blob path specifically: upload, `exists`, download, and a job's outputs, since this is the first use of
      the production R2 bucket and token
- [ ] Lower the `container.mtfm.io` TTL in Google Cloud DNS to 60s and **wait out the old 4h TTL** before cutting over

### Cutover

- [ ] Point `container.mtfm.io` at the Worker (A record → the Custom Domain). Watch `wrangler tail`, queue status, and
      that workers reconnect — they use ReconnectingWebSocket, so they reattach on their own
- [ ] Confirm, on production: a job submitted and followed to completion, build and run logs, outputs served from R2,
      `/`, `/docs`, `/mcp`, and the cron firing (scheduled invocations in `wrangler tail`)
- [ ] Watch for 48h. Leave Deno Deploy running and untouched for ~2 weeks

**Rollback:** restore the A record. Deno Deploy is current — its deploys from main were broken from Oct 6 and fixed on
Oct 21 (#310/#311), and the last two runs succeeded — so rollback lands on current code. Jobs submitted during the
Cloudflare window live in D1/R2 and will not be visible after a rollback; resubmitting re-runs them.

## 6. Cost: hibernation

**Done.** An idle queue now sleeps: verified on staging by watching the queue's `serverId` (regenerated per isolate)
change across an idle window, with a job run before and after to prove the rebuilt queue works.

Measured first, because the docs are ambiguous and the design depends on it:

| probe on real Cloudflare                   | result                                                  |
| ------------------------------------------ | ------------------------------------------------------- |
| hibernatable socket + a 5s repeating alarm | one instance across 20 alarms — never hibernated        |
| hibernatable socket, no timers             | hibernated, in-memory state gone                        |
| pokes every 8s / 20s / 45s                 | resident at 8s; hibernated between pokes at 20s and 45s |

So the eviction threshold is roughly 10-15s of total silence, _any_ pending timer prevents hibernation, and swapping
`setInterval` for `setAlarm` saves nothing.

- [x] `periodicWork: false` for the Durable Object. Five timers kept it resident: four in the queue constructor plus a
      per-connection health check. Deno keeps all of them, unchanged, by default
- [x] `maintenance()` runs from an alarm, scheduled only while `hasWorkInFlight()` — a job queued, running, or finished
      within the last minute and still to be evicted
- [x] Sockets accepted with `ctx.acceptWebSocket`, with `webSocketMessage`/`Close`/`Error` dispatching into a shim that
      looks like the WebSocket `BaseDockerJobQueue` expects, so the queue itself is untouched
- [x] A worker's registration is kept in its socket attachment and replayed on wake, so a rebuilt queue knows its
      workers before it does any maintenance and does not requeue their running jobs
- [x] `webSocketClose` completes the closing handshake — without it the peer waits forever for its own close
- [x] The worker is quiet while idle: pings every 30s instead of 5s and re-registers every 2 minutes instead of every
      10s, switching back to the fast cadence the moment it has jobs. Its reconnect watchdogs follow the same cadence,
      and the JobStates watchdog only applies while it has work, since a sleeping server pushes nothing
- [x] Backwards compatible: no protocol change, so older workers keep working exactly as before. They ping every 10s,
      which keeps their queue resident — they lose the saving, not the function
- [ ] Log buffers do **not** need incremental persistence after all: a running job keeps `hasWorkInFlight()` true, so
      the object stays resident for the whole job and the buffers cannot be lost to an eviction

`setWebSocketAutoResponse` looked like the answer and is not usable here: the worker reads its own id back out of
`PONG <id>` and re-registers when it is missing, and auto-response can only reply with a fixed string. Making the reply
static made every worker re-register on every ping.

## 7. Cleanup (after the watch period, before Deno Deploy shuts down)

- [ ] Replace `.github/workflows/deploy.yml` (Deno Deploy) with the Cloudflare deploy on push to main
- [ ] Add `just test cloudflare` to CI, so the Worker path cannot rot
- [ ] Remove the Redis bus: `REDIS_URL`/`DENO_BROADCAST_REDIS_URL`, the compose service, and the
      `@metapages/deno-redis-broadcastchannel` dependency. One Durable Object per queue means there is nothing to
      coordinate between instances
- [ ] Delete the Deno Deploy app and the old S3 bucket
- [ ] Update everything that says the API is deployed to Deno Deploy: the `compute-queues-repo` skill, `README`s,
      `docs/`, and `app/api/justfile`'s deploy recipe
- [ ] Consider a stricter worker version gate once workers have upgraded, so a stale worker cannot keep a queue awake

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
