import type { Context } from "hono";

import { getApiDockerJobQueue } from "@/routes/websocket.ts";

export const statusHandler = async (c: Context) => {
  const queue: string | undefined = c.req.param("queue");

  if (!queue) {
    c.status(400);
    return c.text("Missing queue");
  }

  const jobQueue = await getApiDockerJobQueue(queue);

  if (!jobQueue) {
    c.status(400);
    return c.json({
      queue: null,
    });
  }

  const response = await jobQueue.status();

  return c.json(response as unknown);
};
