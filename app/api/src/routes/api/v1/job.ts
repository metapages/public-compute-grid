import { db } from "@/db/db.ts";
import type { Context } from "hono";

export const getQueueJobHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    if (!jobId) {
      c.status(404);
      return c.json({ error: "No job provided" });
    }
    const queue: string | undefined = c.req.param("queue");
    if (!queue) {
      c.status(404);
      return c.json({ error: "No queue provided" });
    }

    const [definition, results] = await Promise.all([db.getJobDefinition(jobId), db.getJobFinishedResults(jobId)]);

    if (!definition) {
      c.status(404);
      return c.json({ error: "Job not found" });
    }

    return c.json({ data: { definition, results } });
  } catch (err) {
    console.error("Error getting job", err);
    return c.text((err as Error).message, 500);
  }
};

export const getJobHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    if (!jobId) {
      c.status(404);
      return c.json({ error: "No job provided" });
    }
    const [definition, results] = await Promise.all([db.getJobDefinition(jobId), db.getJobFinishedResults(jobId)]);

    if (!definition) {
      c.status(404);
      return c.json({ error: "Job not found" });
    }

    return c.json({ data: { definition, results } });
  } catch (err) {
    console.error("Error getting job", err);
    return c.text((err as Error).message, 500);
  }
};
