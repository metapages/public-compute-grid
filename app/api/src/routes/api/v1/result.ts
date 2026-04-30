import { db } from "@/db/db.ts";
import type { Context } from "hono";

export const getJobResultHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    if (!jobId) {
      c.status(404);
      return c.json({ error: "No job provided" });
    }

    // this stores the job, but without the full (large) results
    const jobWithoutMaybeLargeResults = await db.getFinishedJob(jobId);
    if (!jobWithoutMaybeLargeResults) {
      return c.json({ data: null });
    }

    // if the above exists, then get the full from s3
    const finishedJobFull = await db.getJobFinishedResults(jobId);
    // const jobWithResults = { ...jobWithoutMaybeLargeResults, results };

    return c.json({ data: finishedJobFull });
  } catch (err) {
    console.error("Error getting results", err);
    return c.text((err as Error).message, 500);
  }
};
