/**
 * Runtime wiring for Cloudflare. Imported first by the entry module, so it runs
 * before any API module loads (the api's db module opens the store at import).
 */
import "./deno-shim.ts";

import { env } from "cloudflare:workers";

import { NoopBroadcastChannel, setBroadcastChannelFactory } from "@shared/jobqueue.ts";
import { setKv } from "@shared/kv.ts";
import { SqlKvStore } from "@shared/store/sql-kv.ts";

import { type D1Database, D1Driver } from "./d1-driver.ts";

export const store = new SqlKvStore(new D1Driver((env as { DB: D1Database }).DB));
setKv(store);

// Each queue lives in exactly one Durable Object: no other instance to tell.
setBroadcastChannelFactory((name) => new NoopBroadcastChannel(name) as unknown as BroadcastChannel);
