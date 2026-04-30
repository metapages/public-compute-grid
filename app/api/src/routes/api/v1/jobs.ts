import { db } from "@/db/db.ts";
import type { Context } from "hono";

import type { InMemoryDockerJob } from "@metapages/compute-queues-shared";

export const getJobsHandler = async (c: Context) => {
  try {
    const queue: string | undefined = c.req.param("queue");
    if (!queue) {
      c.status(404);
      return c.json({ error: "No queue specified" });
    }

    const data: Record<string, InMemoryDockerJob> = await db.queueGetJobs(queue);

    return c.json({ data });
  } catch (err) {
    console.error("Error getting jobs:", err);
    return c.text((err as Error).message, 500);
  }
};
