import { getEnv } from "@shared/env.ts";

export const SERVER_INSTANCE_ID_LENGTH = 6;

const ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

let serverInstanceId: string | undefined;

/** A fresh random id. Call it in a handler, not at import: Workers refuse random values in global scope. */
export const generateInstanceId = (): string =>
  Array.from(
    crypto.getRandomValues(new Uint8Array(SERVER_INSTANCE_ID_LENGTH)),
    (byte) => ALPHANUMERIC[byte % ALPHANUMERIC.length],
  ).join("");

/**
 * This server instance's id: METAPAGE_IO_SERVER_ID, else random. Generated on
 * first use, not at import: Workers refuse random values in global scope.
 *
 * Per *process* (or per isolate on Cloudflare), which is the right identity
 * where one process serves many queues and Redis is the bus between processes.
 * A Cloudflare Durable Object instead gives each queue instance its own id, see
 * ensureQueue in app/cloudflare/src/queue-do.ts.
 */
export const getServerInstanceId = (): string => {
  if (!serverInstanceId) {
    serverInstanceId = getEnv("METAPAGE_IO_SERVER_ID") || generateInstanceId();
  }
  return serverInstanceId;
};
