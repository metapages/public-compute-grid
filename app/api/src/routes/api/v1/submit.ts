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
    if (!jobToQueue?.definition) {
      c.status(400);
      return c.json({ error: "No job definition provided" });
    }
    jobToQueue.control = jobToQueue.control || {};
    jobToQueue.id = jobToQueue.id || (await shaDockerJob(jobToQueue.definition));

    const jobQueue = await getApiDockerJobQueue(queue);

    // This needs to assume that a job submitted with a stateChange
    // like this will have an expectation of persistance
    const queuedJob = await jobQueue.stateChangeJobEnqueue(jobToQueue);
    if (!queuedJob) {
      // Answering 200 here would hand back an id for a job the queue does not
      // have, and the caller would poll it forever.
      c.status(409);
      return c.json({
        error: "The job was not added to the queue: it was removed or replaced while being submitted. Submit it again.",
        jobId: jobToQueue.id,
        queue,
      });
    }

    c.status(200);
    return c.json({ success: true, jobId: jobToQueue.id });
  } catch (err) {
    const jobId = (err as { jobId?: string })?.jobId;
    console.error(`Error submitting job${jobId ? ` ${jobId}` : ""} to queue ${c.req.param("queue")}:`, err);
    c.status(500);
    return c.json({
      error: `Failed to submit the job to queue ${c.req.param("queue")}: ${(err as Error).message}`,
    });
  }
};
