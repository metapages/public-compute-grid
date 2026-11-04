import type { Context } from "hono";

import {
  DefaultNamespace,
  DockerJobFinishedReason,
  DockerJobState,
  getJobColorizedString,
  getQueueColorizedString,
  type StateChange,
} from "@metapages/compute-queues-shared";

import { db } from "../../../db/db.ts";
import { getApiDockerJobQueue } from "../../websocket.ts";

export const cancelJobHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    const queue: string | undefined = c.req.param("queue");
    let { namespace, message } = c.req.query();
    if (!namespace) {
      namespace = DefaultNamespace;
    } else {
      namespace = decodeURIComponent(namespace);
    }

    if (!jobId) {
      c.status(404);
      return c.json({ error: "No job provided" });
    }
    if (!queue) {
      c.status(404);
      return c.json({ error: "No queue provided" });
    }

    let job = await db.queueJobGet({ queue, jobId });
    if (!job) {
      c.status(200);
      return c.json({ message: "Job not found" });
    }

    if (job.state === DockerJobState.Finished || job.state === DockerJobState.Removed) {
      c.status(200);
      return c.json({ message: "Job already Finished|Removed" });
    }

    const jobQueue = await getApiDockerJobQueue(queue);

    while (true) {
      job = await db.queueJobGet({ queue, jobId });
      if (!job) {
        c.status(200);
        return c.json({ message: "Job not found" });
      }
      if (job.state === DockerJobState.Finished || job.state === DockerJobState.Removed) {
        break;
      }
      const stateChange: StateChange = {
        job: jobId,
        tag: "api",
        state: DockerJobState.Finished,
        value: {
          type: DockerJobState.Finished,
          reason: DockerJobFinishedReason.Cancelled,
          // Keep the caller's reason: it is the only record of who cancelled a
          // job and why. The local-mode worker has always done this; dropping
          // it here made every cancelled job read "Job cancelled by API",
          // which is what made a stray cancel impossible to attribute.
          message: message ? decodeURIComponent(message) : "Job cancelled by API",
          time: Date.now(),
          namespace: namespace,
        },
      };

      console.log(
        `${getQueueColorizedString(queue)} ${getJobColorizedString(jobId)} 🚫 cancel [namespace=${namespace}] ` +
          `${message ? decodeURIComponent(message) : "(no message)"}`,
      );
      await jobQueue.stateChange(stateChange);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    c.status(200);
    return c.json({ success: true, jobId, namespace });
  } catch (err) {
    console.error("Error getting job", err);
    return c.text((err as Error).message, 500);
  }
};
