# Cloudflare migration: container.mtfm.io API + browser

Deno Deploy shuts down in about 6 months. We're doing this now, while we have the time. The S3 bucket is also being shut
down, so blobs move to R2.

**Overview:** the API becomes a Cloudflare Worker. Each queue gets its own Durable Object, which holds that queue's
websockets and in-memory state, so the Redis bus goes away. Global keys go to D1. Blobs go to R2 through the S3 API
(MinIO stays as the local stand-in). The browser and docs are served as Workers Static Assets. Webhook retry runs on a
Cron Trigger.

**Approach:**

- Write the integration tests first, then work from them. They run against any `API_URL`: the local Deno stack, the
  local Cloudflare stack (`wrangler dev`), and staging.
- The local stack has to keep working the whole way through.

**Long-term direction:** the worker moves to Node, and server, worker and CLI eventually become one program. So new core
code uses web-standard APIs behind small interfaces (config, store, bus, scheduler, blob store), with no `Deno.*` or
Workers-only calls. Only the runtime adapters are runtime-specific.

## 0. Integration tests first

- [ ] HTTP contract tests: submit, result, job lookups, exists, namespaces, cancel, `/f/` upload/download
- [ ] Websocket tests: client snapshot on connect, state deltas, worker registration
- [ ] SSE `/stream` and `build-logs.json` / `run-logs.json` tests
- [ ] Static route tests: `/`, SPA fallback, `/docs`, `/llms.txt`
- [ ] Webhook retry test (cron)
- [ ] MCP HTTP test
- [ ] `just test-url <url>` runs the suite against any deployment

## 1. S3 → R2 (ships independently, on Deno Deploy)

- [ ] Create R2 buckets for prod and staging, with an API token
- [ ] Make the S3 client R2-compatible (endpoint, `region: auto`, path-style) and keep MinIO for local
- [ ] CORS on R2 for browser PUTs
- [ ] R2 lifecycle rule: objects expire after about a month
- [ ] Switch prod env to R2. No copy needed, since data expires after a month

## 2. Runtime-neutral core

- [ ] `getConfig()` replaces direct `Deno.env.get`
- [ ] Store interface over the Deno.Kv subset (get/set/delete/list/atomic), with a `DenoKvStore` implementation
- [ ] Bus interface around `CustomBroadcastChannel`
- [ ] `BaseDockerJobQueue.tick()` so the intervals can be driven from outside
- [ ] Switch hono to `npm:hono@4`, drop `handlerDeno`
- [ ] `just check`, `just test local`, `just test` all pass, then merge (still on Deno Deploy)

## 3. Cloudflare app and local stack (`app/cloudflare/`)

- [ ] `wrangler.jsonc`: nodejs_compat, assets, DO `QUEUE`, D1, cron
- [ ] Worker entry: route `/q/:queue/*` to `QueueDO`, handle everything else statelessly
- [ ] `QueueDO`: websockets, submit, cancel, SSE
- [ ] Store: queue-scoped keys in DO SQLite, global keys in D1, expiry sweep on cron
- [ ] Large build/run logs go to R2, with a pointer in D1
- [ ] Cron Trigger for webhook retry
- [ ] Static assets: SPA fallback and `/docs` `.html` rewrite
- [ ] Bundle → `wrangler deploy`, plus `just cloudflare dev|deploy`
- [ ] Docker compose `cloudflare` mode: `wrangler dev` + worker + MinIO, and `just test cloudflare` passes

## 4. Staging

- [ ] Deploy to the staging subdomain
- [ ] `just test-url` against staging
- [ ] Attach a real worker and run the OpenThermo job end to end
- [ ] Check reconnects after redeploy, long SSE streams, MCP
- [ ] Load test a single hot queue

## 5. Cutover

- [ ] Lower the DNS TTL
- [ ] Point `container.mtfm.io` at the Worker
- [ ] Monitor for 48h. Rollback is pointing DNS back at Deno Deploy
- [ ] Replace `deploy.yml` with `wrangler deploy`

## 6. Cost: hibernation

- [ ] Hibernatable WebSockets and alarms in place of intervals
- [ ] Rehydrate queue state from DO SQLite on wake

## 7. Cleanup (before Deno Deploy shuts down)

- [ ] Remove the Redis bus and its env vars
- [ ] Shut down the Deno Deploy app and the S3 bucket
- [ ] Update the repo skill, READMEs and `docs/` ("Deployed to Deno Deploy")

## Later: Node and unification

- [ ] Worker runs on Node
- [ ] Single server/worker/CLI program, sharing the runtime-neutral core
