import { getEnv } from "@shared/env.ts";

export const SERVER_INSTANCE_ID_LENGTH = 6;

const ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

let serverInstanceId: string | undefined;

/**
 * This server instance's id: METAPAGE_IO_SERVER_ID, else random. Generated on
 * first use, not at import: Workers refuse random values in global scope.
 */
export const getServerInstanceId = (): string => {
  if (!serverInstanceId) {
    serverInstanceId = getEnv("METAPAGE_IO_SERVER_ID") ||
      Array.from(
        crypto.getRandomValues(new Uint8Array(SERVER_INSTANCE_ID_LENGTH)),
        (byte) => ALPHANUMERIC[byte % ALPHANUMERIC.length],
      ).join("");
  }
  return serverInstanceId;
};
