import { ensureFileSync } from "std/fs";

import { getEnv } from "./env.ts";
import type { KvStore } from "./store/kv.ts";

export type { KvStore } from "./store/kv.ts";

let localkv: KvStore | undefined = undefined;

/**
 * Use this store instead of opening Deno KV. A runtime without Deno KV
 * (Cloudflare, Node) calls this before anything touches the db.
 */
export const setKv = (store: KvStore): void => {
  localkv = store;
};

/**
 * The process-wide store: whatever setKv installed, else Deno KV at
 * DENO_KV_URL (a file path, a KV Connect URL, or unset for the default).
 */
export const getKv = async (): Promise<KvStore> => {
  if (localkv === undefined) {
    const kvPath = getEnv("DENO_KV_URL") || undefined;
    if (
      kvPath &&
      !kvPath?.startsWith("http")
    ) {
      ensureFileSync(kvPath);
    }

    const thiskv = await Deno.openKv(kvPath) as unknown as KvStore;
    if (localkv) {
      thiskv.close();
      return localkv;
    }
    localkv = thiskv;
  }
  return localkv;
};

export const closeKv = (): void => {
  if (localkv) {
    localkv.close();
    localkv = undefined;
  }
};
