import { getWorkerVersionGate } from "@metapages/compute-queues-shared";
import type { Context } from "hono";

/**
 * The worker versions this server accepts. Workers running with --auto-update
 * read this before upgrading, so a release reaches the server first.
 *
 * `serverVersion` is the worker release this server was built alongside
 * (app/worker/mod.json). Each runtime supplies it: the api must not import the
 * worker's files itself, because the docker stacks do not mount app/worker
 * into the api containers.
 */
export const makeWorkerVersionsHandler = (serverVersion: string) => (c: Context) =>
  c.json(getWorkerVersionGate(serverVersion));
