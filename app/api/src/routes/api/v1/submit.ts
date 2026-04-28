import type { Context } from "hono";
import { getApiDockerJobQueue } from "@/routes/websocket.ts";

import { type EnqueueJob, shaDockerJob } from "@metapages/compute-queues-shared";

export const submitJobToQueueHandler = async (c: Context) => {
  try {
    const queue: string | undefined = c.req.param("queue");
    if (!queue) {
      c.status(404);
      return c.json({ error: "No queue specified" });
    }
    const jobToQueue = await c.req.json<EnqueueJob>();
    console.log("jobToQueue", jobToQueue);
    if (!jobToQueue) {
      c.status(400);
      return c.json({ error: "No job provided" });
    }
    jobToQueue.control = jobToQueue.control || {};
    jobToQueue.id = jobToQueue.id || (await shaDockerJob(jobToQueue.definition));

    const jobQueue = await getApiDockerJobQueue(queue);

    // This needs to assume that a job submitted with a stateChange
    // like this will have an expectation of persistance
    await jobQueue.stateChangeJobEnqueue(jobToQueue);

    c.status(200);
    return c.json({ success: true, jobId: jobToQueue.id });
  } catch (err) {
    console.error("Error submitting job:", err);
    return c.text((err as Error).message, 500);
  }
};
