import { bucketParams, s3Client } from "@/routes/s3config.ts";
import { HeadObjectCommand } from "aws-sdk/client-s3";
import type { Context } from "hono";

export const existsHandler = async (c: Context) => {
  const key: string | undefined = c.req.param("key");
  if (!key) {
    c.status(400);
    return c.text("Missing key");
  }

  try {
    const command = new HeadObjectCommand({ ...bucketParams, Key: key });
    await s3Client.send(command);
    return c.json({ exists: true });
  } catch (err: unknown) {
    if ((err as { name: string }).name === "NotFound") {
      c.status(404);
      return c.json({ exists: false });
    }
    console.error("Error checking file:", err);
    return c.text((err as Error).message, 500);
  }
};
