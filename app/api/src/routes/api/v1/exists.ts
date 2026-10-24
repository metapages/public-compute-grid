import type { Context } from "hono";

import { getBlobStore } from "@/routes/s3config.ts";

export const existsHandler = async (c: Context) => {
  const key: string | undefined = c.req.param("key");
  if (!key) {
    c.status(400);
    return c.text("Missing key");
  }

  try {
    if (await getBlobStore().exists(key)) {
      return c.json({ exists: true });
    }
    c.status(404);
    return c.json({ exists: false });
  } catch (err: unknown) {
    console.error("Error checking file:", err);
    return c.text((err as Error).message, 500);
  }
};
