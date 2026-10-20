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

- [x] Blob store on `aws4fetch` (fetch + WebCrypto; R2, MinIO, any runtime) instead of the AWS SDK
- [ ] Create R2 buckets for prod and staging, with an API token (needs Cloudflare account access)
- [ ] CORS on R2 for browser PUTs
- [ ] R2 lifecycle rule: objects expire after about a month
- [ ] Switch the Deno Deploy env to R2 (it works there too). No copy needed, since data expires after a month

## 2. Runtime-neutral core

- [x] `getEnv()` replaces `Deno.env.get` in shared, api and mcp module scope
- [x] Store interface (`KvStore`, injected with `setKv`), with Deno KV satisfying it unchanged
- [x] Bus interface (`setBroadcastChannelFactory`, `NoopBroadcastChannel`)
- [x] `createApp()` is the API without static files; Deno's static routes live in `staticDeno.ts`
- [ ] `BaseDockerJobQueue.tick()` so the intervals can be driven from outside (needed for hibernation)
- [ ] Switch hono to `npm:hono@4`, drop `handlerDeno`
- [ ] Drop the remaining Deno-only imports from the shared graph, then delete `app/cloudflare/src/deno-shim.ts`

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
- [ ] `/mcp/ws` (the legacy MCP websocket) is not supported: it reads in-process queue state. Retire it or route it

## 4. Staging

- [ ] Cloudflare account: D1 databases, R2 buckets and secrets, staging subdomain route
- [ ] `just cloudflare deploy staging`
- [ ] `just test-url https://<staging>`
- [ ] Attach a real worker and run the OpenThermo job end to end
- [ ] Check reconnects after redeploy, long SSE streams, D1 latency from the queue DO
- [ ] Load test a single hot queue

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

- Fixed: the worker lost all job output after the first ~32 KB. Its log-grabbing transform streams were never read, so
  they stopped accepting data once their buffers filled (`DockerJob.ts`, now drained with `resume()`).
- Fixed: logs over 64 KiB failed to persist (above).
- Production deploys from main have failed since Oct 6: Deno Deploy reports "The revision failed". The last successful
  deploy was Aug 18.
- Pre-existing flakes, reproduced on main on the same machine: MCP jobs reported "not on queue" or "unknown", and the
  local inputs/outputs and multi-segment path tests. Each stack passes in full on most runs (local 60/60, remote 64/64,
  cloudflare 63/64, the one failure an MCP flake).
