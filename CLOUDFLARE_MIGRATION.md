# Cloudflare migration: container.mtfm.io API + browser

Deno Deploy shuts down in about 6 months. We're doing this now, while we have the time. The S3 bucket is also being shut
down, so blobs move to R2.

**This is a replacement, not a parallel run.** PR #313 makes Cloudflare _the_ deployment: push to `main` deploys the
Worker, nothing deploys to Deno Deploy any more, and no Deno-Deploy-specific plumbing stays behind. The only step that
cannot land in the PR is DNS, because `container.mtfm.io` has to be in a zone on this Cloudflare account first — see
[The domain](#the-domain).

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

## Next steps

The migration is merged and deployed; what is left lives in its own issues.

| issue | what                                                          | when                             |
| ----- | ------------------------------------------------------------- | -------------------------------- |
| #346  | A finished job is started again on the same worker            | **first** — it runs twice        |
| #330  | A double-claimed job runs to completion twice                 | with #346                        |
| #342  | Smoke-test each Cloudflare deploy, after it settles           | next: #346 is only visible there |
| #341  | Run `just test cloudflare` in CI                              | any time                         |
| #347  | Concurrent first requests to a new queue get a half-built one | any time, small                  |
| #320  | Delete the Deno Deploy app and the old S3 bucket              | once the domain has settled      |
| #315  | Worker can go deaf (ping/pong weaknesses)                     | when it next recurs              |
| #322  | Stricter worker version gate                                  | when it costs                    |
| #343  | Pre-existing test flakes                                      | when they bite                   |
| #344  | Worker on Node, one server/worker/CLI program                 | the long-term direction          |

Done since this table was written: #317 (deploy token rotated), #321 (docs-only pushes), #314 (two suites on one queue),
#316 (`container.mtfm.io` is served by the Worker as of 2026-11-07), #318 (0.54.87 published), #319 (Deno Deploy era env
vars). `just test cloudflare` passes 71/71, and `just test-url https://container.mtfm.io` 9/10 — the failure is #346.

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
- [x] Production bucket: `just cloudflare setup production` created `compute-queues` with CORS and the 35-day expiry
      rule. Deno Deploy was never switched to R2 and does not need to be — it is no longer the deployment

## 2. Runtime-neutral core

- [x] `getEnv()` replaces `Deno.env.get` in shared, api and mcp module scope
- [x] Store interface (`KvStore`, injected with `setKv`), with Deno KV satisfying it unchanged
- [x] Bus interface (`setBroadcastChannelFactory`, `NoopBroadcastChannel`)
- [x] `createApp()` is the API without static files; the Deno server (API + static files from disk) is `handlerDeno.ts`
- [x] The queue's periodic work is drivable from outside, for hibernation: `periodicWork: false` plus a public
      `maintenance()` and `hasWorkInFlight()`, rather than the `tick()` this originally planned
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

## 5. Deploy

**Cloudflare is the deployment.** Merging PR #313 replaces the Deno Deploy workflow with the Cloudflare one, so push to
`main` → `.github/workflows/deploy.yml` → `just cloudflare deploy production`. Nothing deploys to Deno Deploy any more;
there is no parallel-run period and no split between "old" and "new" production.

Production infrastructure exists and is validated:

- [x] An R2 API token with **Object Read & Write**. Scoping it to _all_ buckets in the account covers staging and
      production in one go and avoids the mis-scoped-token trap (a token scoped elsewhere fails every request with
      `403 AccessDenied`, which reads like a signing bug)
- [x] `just cloudflare setup production` — D1 `compute-queues`, R2 `compute-queues` with CORS and the 35-day expiry
      rule, and the real `database_id` written into `wrangler.jsonc`. **Commit that id**
- [x] `R2_ACCESS_KEY_ID=… R2_SECRET_ACCESS_KEY=… just cloudflare secrets production`
- [x] `just cloudflare deploy production` → live at **https://compute-queues.dion19.workers.dev**
- [x] Validated there, against the real production infrastructure:
  - [x] `just test-url https://compute-queues.dion19.workers.dev` — 10/10 in 24s
  - [x] Blobs specifically — upload, `exists`, download, and a job's outputs — the first use of the production bucket
        and token
  - [x] The OpenThermo build end to end through `cq` — built from the git commit, ran `tests/C2v.log`, exit 0, output
        file returned
  - [x] Cron firing (`wrangler tail` shows the `* * * * *` invocations) and hibernation (after a 60s idle window the
        object was evicted and rebuilt — the log shows the socket being re-attached — and the next job ran, exit 0)
  - [x] MCP over Streamable HTTP at `/mcp`: `initialize`, `tools/list` (the six current tools) and a real `tools/call`
- [ ] Publish a worker release so deployed workers pick up the quiet-while-idle cadence (bump `app/worker/mod.json`).
      Without it queues stay resident: older workers re-register every 10s. They keep working either way

**Rollback** is `wrangler rollback`, or a redeploy of a previous Worker version — not a return to Deno Deploy.

### The two tokens

Two unrelated credentials, easy to confuse:

**`CLOUDFLARE_API_TOKEN`** — deploys the Worker. Must be available to Actions (org or repo secret) or the merge's deploy
run fails. Cloudflare's own GitHub Actions guide says to use the **Edit Cloudflare Workers** template, which grants:

| permission                | scope   | what we use it for                                                             |
| ------------------------- | ------- | ------------------------------------------------------------------------------ |
| Workers Scripts: Write    | account | the deploy itself: script, static assets, DO migration, cron, `secret put`     |
| Account Settings: Read    | account | `wrangler.jsonc` has no `account_id`, so wrangler resolves the account         |
| User Details: Read        | user    | `wrangler whoami`, which `just cloudflare secrets` parses for the account id   |
| User Memberships: Read    | user    | same                                                                           |
| Workers Routes: Write     | zone    | unused until the Custom Domain for `container.mtfm.io` is added, then required |
| Workers R2 Storage: Write | account | `just cloudflare setup` (bucket, CORS, expiry rule) — not the deploy           |
| Workers Tail: Read        | account | `wrangler tail` — not the deploy                                               |
| Workers KV Storage: Write | account | nothing; we use D1, so this can be dropped when trimming                       |

**Add D1: Edit** on top of the template for a token that runs `just cloudflare setup` (`wrangler d1 create`, and the
`d1 info` idempotency check). A CI token that only deploys does not need it. Create it with the account as the account
resource and _all_ zones for the zone resource, so it keeps working once `container.mtfm.io` becomes a zone here.

**Mind the expiry.** The token currently in the local `.env` runs `2026-10-22` → **`2026-11-01`**. A 10-day token is
fine for hand-driven setup and wrong for CI: the deploy would start failing a week after merge. Give the Actions token
no expiry, or a long one, and treat the expiry date as a thing to diarise.

**`R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY`** — an **R2 API token** (Object Read & Write, all buckets), not a
Cloudflare API token. It never goes near Actions: it is uploaded once to the Worker as the `AWS_*` secrets, which is how
the Worker signs S3 requests to R2.

### The domain

One manual step remains, and it needs DNS access rather than code. Today `container.mtfm.io` is an **A record to
69.67.170.170** (Deno Deploy, `server: deployd`) with a **14400s (4h) TTL**, and `mtfm.io` is hosted on **Google Cloud
DNS** (`ns-cloud-*.googledomains.com`). `js.mtfm.io` points at the same Deno Deploy IP, so the zone carries other
services too.

A Worker can only serve a hostname in a zone on this Cloudflare account, and `mtfm.io` is not one. Two routes:

1. **Delegate just the subdomain** (recommended): add `container.mtfm.io` to Cloudflare as its own zone and add NS
   records for it in Google Cloud DNS. Nothing else under `mtfm.io` moves, so `js.mtfm.io` and the rest are untouched.
   Confirm subdomain zones are available on the account's plan first
2. **Move the whole `mtfm.io` zone** to Cloudflare (nameserver change at the registrar). Every record has to be copied
   first, and `js.mtfm.io` and anything else in the zone move with it

**Done 2026-11-07, by route 2**: the whole `mtfm.io` zone is on Cloudflare (`jacob`/`aida.ns.cloudflare.com`), and
`container.mtfm.io` is a Workers Custom Domain on the `compute-queues` Worker.

- [x] The Custom Domain, and `routes` in `wrangler.jsonc` so it is in version control
- [x] TTL: moot in the end — the zone moved to Cloudflare, which left the record at TTL 300 well before the flip
- [x] Smoke-tested first on a throwaway hostname (`flip-probe.mtfm.io`) rather than via a hosts override: attach,
      confirm the Worker answers, delete. That also established what rollback looks like
- [x] Flipped
- [x] Confirmed on the real hostname: `just test-url https://container.mtfm.io` 10/10 — a job submitted and followed to
      completion, build and run logs, outputs from R2, `/`, `/docs`, `/llms.txt`, `/mcp/health`, and `/mcp/ws` gone
      (404)

**Keep `workers_dev: true` after the domain is live.** Uploaded inputs are stored as absolute `url` DataRefs
(`${address}/f/<hash>`, `app/shared/src/shared/jobtools.ts`), so jobs submitted against the workers.dev origin carry
input URLs on that hostname and resolve only while it answers. The blobs themselves are in the same R2 bucket either
way. Job ids are hashes of the definition and the hostname is inside it for jobs with file inputs, so the same job
submitted after the domain change hashes differently and simply runs again — which is fine, nothing needs carrying over.

### Decisions

- **Old job results: nothing to carry over.** Decided — jobs have a 30-day lifespan anyway, so everything in the old
  bucket is disposable. No bucket copy and no read-through to S3: results from before the switch return 404 (job ids are
  content hashes, so resubmitting re-runs them).
- **`/mcp/ws` is gone.** The old deployment serves it and advertises the legacy tool list (`create_job`, `execute_job`,
  …) at `/mcp/info`; both go away with it. `/mcp` (Streamable HTTP) is the replacement and is verified on the Worker:
  `initialize`, `tools/list` (the six current tools) and a real `tools/call` all answer. Confirm no client uses
  `/mcp/ws`

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
      which keeps their queue resident — they lose the saving, not the function Log buffers turned out not to need
      incremental persistence: a running job keeps `hasWorkInFlight()` true, so the object stays resident for the whole
      job and the buffers cannot be lost to an eviction.

`setWebSocketAutoResponse` looked like the answer and is not usable here: the worker reads its own id back out of
`PONG <id>` and re-registers when it is missing, and auto-response can only reply with a fixed string. Making the reply
static made every worker re-register on every ping.

## 7. Cleanup

Nothing here is required for the migration — this PR already removes the Deno Deploy workflow, deploy recipes and
`DENO_DEPLOY_TOKEN`, and updates the docs and the repo skill.

- [ ] Remove the Deno Deploy era environment variables and secrets from wherever they are still set (the Deno Deploy
      project, local `.env` files, CI secrets): `DENO_DEPLOY_TOKEN`, `DENO_KV_URL`, `DENO_KV_ACCESS_TOKEN`, `REDIS_URL`
      / `DENO_BROADCAST_REDIS_URL`. **None of these exist on Cloudflare and none should be added**: the Worker has
      exactly `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT` as secrets and `AWS_REGION`, `AWS_S3_BUCKET`
      as vars (verified with `wrangler secret list`). D1 replaces Deno KV and one Durable Object per queue replaces the
      Redis bus, so there is nothing to configure in their place. #319 removed the remaining references from the code
      comments and docs; the compose stacks keep their own `DENO_KV_*` / `REDIS_URL` values for their `denokv` and
      `redis` services. What is left is outside the repo: delete the `DENO_DEPLOY_TOKEN` Actions secret (repo and org),
      remove these variables from local `.env` files, and delete the Deno Deploy project (next item)
- [ ] Delete the Deno Deploy app and the old S3 bucket whenever convenient — jobs have a 30-day lifespan, so the
      bucket's contents are all disposable
- [ ] Add `just test cloudflare` to CI, so the Worker path cannot rot
- [ ] Consider a stricter worker version gate once workers have upgraded, so a stale worker cannot keep a queue awake

Note: the Redis bus and the Deno api server stay in the tree. They are what the `local` and `remote` docker stacks and
the local-mode worker run, and those are development and test targets, not deployments.

## Later: Node and unification

- [ ] Worker runs on Node
- [ ] Single server/worker/CLI program, sharing the runtime-neutral core. The local-mode worker's
      `app/worker/src/lib/local-handler.ts` duplicates the api routes and is the first thing to fold in

## Findings along the way

- The domain flip, for anyone doing the next one: `override_existing_dns_record: true` on
  `PUT /accounts/{id}/workers/domains` does **not** override an externally managed record — it still answers
  `Hostname 'container.mtfm.io' already has externally managed DNS records (A, CNAME, etc). Delete them first`. So the
  sequence is delete the records, then attach. Deleting a Custom Domain removes the record Cloudflare created for it,
  which makes rollback clean: delete the domain, recreate the old records.
- `container.mtfm.io` had an **AAAA** record as well as the A record, both pointing at Deno Deploy. Attaching the Custom
  Domain only displaces the record it conflicts with, so leaving the AAAA in place would have sent IPv6 clients to the
  old origin and IPv4 clients to the Worker — a split that no smoke test on one address would have caught. List every
  record for the hostname before a flip, not just the A.
- The token for all of this is **account-owned**, so it is managed on the account's API Tokens page and is invisible on
  `/profile/api-tokens`; `GET /user/tokens/verify` rejects it outright while `GET /accounts/{id}/tokens/verify`
  describes it. Worth knowing before hunting for permissions that look missing. It has no expiry, unlike the one that
  lapsed on 2026-11-01.
- `wrangler deploy` with several environments defined warns that it is picking the top-level environment by implication.
  Production now names it explicitly (`--env ""`), because that warning describes exactly how a staging deploy reaches
  production by accident.
- The big-log contract test read **3485** log lines where the job wrote 3000 — duplicated, not truncated. First seen
  once and written off as intermittent, with the late-log append path as the suspect. That was wrong, and worth
  recording as a lesson: a surplus of output is evidence about _execution_, not only about storage. Against the real
  hostname it now reproduces every run (3485, then 3005), and the worker log says why — the job **ran to completion
  twice on one worker**. The worker finishes a job and drops its local entry, the server still broadcasts the job as
  `Running` on that worker for about a second, and `_claimJobs` reads "running on me but not local" as a job to restart.
  The late-log append path then faithfully appends a second execution's output. Filed as #346. It needs real round-trip
  latency, so neither `just test cloudflare` nor a local-mode worker shows it: only a deployed stack does, which is the
  argument for #342.

- A queue's Durable Object could drop its worker the moment the object was rebuilt, leaving every job Queued:
  `getApiDockerJobQueue` memoizes queues in a module-level map, which is per _isolate_, not per Durable Object. A new
  instantiation in a reused isolate got back a queue still holding the previous one's websockets, and Cloudflare refuses
  to let one Durable Object use another's I/O (`Cannot perform I/O on behalf of a different Durable Object`). The queue
  read that as a dead worker and removed it. The object now discards whatever the map holds and builds its own queue,
  whose sockets come from `ctx.getWebSockets()`. It only reproduced on production because it needs the isolate to be
  reused after an eviction — staging never hit it, and neither did `wrangler dev`.
- The local `just test cloudflare` stack failed exactly one test per run (64/65), a different one each time, always a
  job test. It looked like saturation (~15 jobs Queued when a test's 120s ran out), but the cause was two whole suites
  running at once: `docker-compose-test-cloudflare.yml` never got the idle `TEST_SERVICE_COMMAND` that the local and
  remote stacks have, so its `deno test --watch` raced the `exec`'d run. Their jobs superseded each other in shared
  namespaces and queued behind each other. With one suite the queue peaks at 3 Queued, so tests still share one queue.
- MinIO's Docker Hub images stopped being publicly pullable — every tag, `:latest` included, answers
  `pull access denied` — which broke the `remote mode` CI job on every branch, main included, and every dev machine on a
  cache miss. The stacks now run SeaweedFS, which is maintained, freely pullable, and verifies real SigV4 for both
  header-signed and presigned requests. The traefik hostname stays `minio.worker-metaframe.localhost` on purpose: it is
  in the mkcert SANs and in `/etc/hosts` on dev machines and both CI runners, and only the service behind it changed.
  Internally it is `s3:9000`.
- The bucket the local stacks use was only ever created by the `create-s3-bucket` compose step, whose guard required the
  endpoint to contain "minio" — and on a fresh machine nothing else makes it, so the suite was relying on the
  self-hosted runners keeping the S3 data directory between runs. `ensureBucket()` now lives on `S3BlobStore` (a
  CreateBucket that treats 409 as success), the init step refuses to run against a hosted endpoint, and the api waits
  for it (`service_completed_successfully`) instead of racing it.
- Seen once, unexplained: a worker registered (the server listed it in `/status`) and then received nothing at all — no
  PONG, no JobStates — while all four job tests timed out. Its own 90s watchdog reported "no PONG since 15 minutes",
  i.e. 15 minutes of silence surfaced in one step with no intervening check, which is what a suspended host looks like
  rather than a lost socket. Not reproducible: the same suite then passed 10/10 against both staging and production. If
  it recurs with the host demonstrably awake, the suspect is a send from the Durable Object reaching a socket that is no
  longer delivering.
- Validating within seconds of a deploy gives false failures: the first production contract run went 6/4 with jobs stuck
  Queued while the objects were still restarting on the new version. The same suite passed 10/10 a few minutes later.
  Give a deploy a moment before testing it.
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
- Two workers claiming the same job is expected — the queue is fast and reliable, so claiming is optimistic — and the
  losing worker does notice immediately. It then starts its container anyway and runs it to completion, because the kill
  is deferred by a fixed `setTimeout(..., 6s)` that outlasts most jobs. One `just test cloudflare` run: 13 jobs claimed
  by both workers, 10 where both actually started a container, 9 of those finishing before any kill. The conflict is
  logged one line _before_ `container started`, so the information to avoid it entirely is already there. #330, which
  also records a latent comparison bug in the same block: `isRunningElsewhereStill` compares a raw worker id against
  `workerIdShort`, an ANSI-colorized display string, so that guard is always true.
- Fixed: the api's cancel handler read `?namespace=` but not `?message=`, recording "Job cancelled by API" for every
  cancel, so a stray cancel could not be attributed to anything. The local-mode worker had always honoured the caller's
  message — one more place the two servers had drifted apart. Attribution is what ruled out a cross-test cancel as the
  cause of #314.
- Fixed: the test failure dump tailed only the worker service. On the cloudflare stack the queue lives in the api (one
  Durable Object per queue), so worker logs alone cannot explain a job that vanished, and the evidence needed for #314
  was never captured. It now dumps the api too, under its per-stack service name.
- Removed: `CONTAINER_MTFM_IO_DISABLE_KILLING_UNKNOWN_JOBS`, set on every test worker in two compose files and read
  nowhere in the codebase. It reads as a guarantee that test workers will not kill jobs they do not recognise; what
  actually provides that is `killOrphanedJobs` skipping worker ids starting with `test-worker`.
