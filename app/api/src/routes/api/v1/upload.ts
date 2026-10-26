import type { Context } from "hono";
import { ms } from "ms";

import { getBlobStore } from "@/routes/s3config.ts";

const OneWeekInSeconds = (ms("1 week") as number) / 1000;

export const uploadHandler = async (c: Context) => {
  const key: string | undefined = c.req.param("key");

  if (!key) {
    c.status(400);
    return c.text("Missing key");
  }

  try {
    const url = await getBlobStore().presignPut(key, OneWeekInSeconds);
    return c.redirect(url);
  } catch (err) {
    console.error("uploadHandler error", err);
    c.status(500);
    return c.text("Failed to get signed URL");
  }
};
