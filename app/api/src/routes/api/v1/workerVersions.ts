import { getWorkerVersionGate } from "@metapages/compute-queues-shared";
import type { Context } from "hono";

import mod from "../../../../../worker/mod.json" with { type: "json" };

/**
 * The worker versions this server accepts. Workers running with --auto-update
 * read this before upgrading, so a release reaches the server first.
 */
export const workerVersionsHandler = (c: Context) => c.json(getWorkerVersionGate(mod.version));
