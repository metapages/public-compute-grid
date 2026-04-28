import type { Context } from "hono";

import { db } from "@/db/db.ts";

export const getJobNamespacesHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    const queue: string | undefined = c.req.param("queue");
    if (!jobId || !queue) {
      c.status(404);
      return c.json({ error: "both jobId and queue are required" });
    }

    const namespaces = await db.queueJobGetNamespaces({ queue, jobId });

    return c.json({ data: namespaces || null });
  } catch (err) {
    console.error("Error getting namespaces", err);
    return c.text((err as Error).message, 500);
  }
};
