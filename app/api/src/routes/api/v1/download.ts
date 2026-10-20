import type { Context } from "hono";
import { ms } from "ms";

import { getBlobStore } from "@/routes/s3config.ts";

const OneWeekInSeconds = (ms("1 week") as number) / 1000;

export const downloadHandler = async (c: Context) => {
  const key: string | undefined = c.req.param("key");
  if (!key) {
    c.status(400);
    return c.text("Missing key");
  }
  try {
    const url = await getDownloadPresignedUrl(key);
    return c.redirect(url);
  } catch (err) {
    console.error("Error downloading file:", err);
    return c.text((err as Error).message, 500);
  }
};

export const getDownloadPresignedUrl = (key: string): Promise<string> =>
  getBlobStore().presignGet(key, OneWeekInSeconds);
