import type { Context } from "hono";

import { db } from "@/db/db.ts";

export const getDefinitionHandler = async (c: Context) => {
  try {
    const jobId: string | undefined = c.req.param("jobId");
    if (!jobId) {
      c.status(404);
      return c.json({ error: "No job provided" });
    }

    const job = await db.getJobDefinition(jobId);

    return c.json({ data: job || null });
  } catch (err) {
    console.error("Error getting job", err);
    return c.text((err as Error).message, 500);
  }
};
