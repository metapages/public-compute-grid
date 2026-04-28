import { db } from "@/db/db.ts";
import { getApiDockerJobQueue } from "@/routes/websocket.ts";
import type { Context } from "hono";

import {
  type DockerApiCopyJobToQueuePayload,
  DockerJobState,
  type StateChange,
  type StateChangeValueQueued,
} from "@metapages/compute-queues-shared";

export const copyJobToQueueHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    const post = await c.req.json<DockerApiCopyJobToQueuePayload>();
    const { queue, control } = post;

    const definition = await db.getJobDefinition(jobId);
    if (!definition) {
      c.status(404);
      return c.json({ error: "Job definition not found" });
    }

    const jobQueue = await getApiDockerJobQueue(queue);

    const stateChangeValue: StateChangeValueQueued = {
      type: DockerJobState.Queued,
      time: Date.now(),
      enqueued: {
        id: jobId,
        definition,
        debug: false,
        control,
      },
    };
    const stateChange: StateChange = {
      job: jobId,
      tag: "",
      state: DockerJobState.Queued,
      value: stateChangeValue,
    };

    await jobQueue.stateChange(stateChange);

    c.status(200);
    return c.json({ success: true });
  } catch (err) {
    console.error("Error downloading file:", err);
    return c.text((err as Error).message, 500);
  }
};
