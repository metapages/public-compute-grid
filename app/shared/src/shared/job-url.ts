import type { Context, MiddlewareHandler, Next } from "hono";

import type { DB } from "@shared/db.ts";
import type { EnqueueJob } from "@shared/types.ts";
import { formatJobUrlPath, isSha256, prepareEnqueueJob } from "@shared/util.ts";

const JOB_PATH = /^(?:\/q\/([^/]+))?\/j\/([^/]+?)(\.json)?(\/.*)?$/;

/**
 * Queues, results and logs are all keyed by run id, so the job routes need only
 * one id. This turns the public forms of a job URL into that id before they run:
 *
 *   /j/<sha256 job>/result.json?i=<sha256 inputs>
 *   /j/<named job>/result.json
 *   /j/<named job>/result.json?v=<sha256 job>&i=<sha256 inputs>
 *
 * The same goes for /q/<queue>/j/<id>/... and /j/<id>.json. The bare /j/<id> is
 * the browser page, which reads `v` and `i` itself.
 */
export const makeJobUrlMiddleware = (opts: {
  resolveDb: (queue?: string) => Promise<DB>;
  fetch: (request: Request) => Response | Promise<Response>;
}): MiddlewareHandler => {
  return async (c: Context, next: Next) => {
    const match = JOB_PATH.exec(c.req.path);
    if (!match) {
      return next();
    }
    const [, queue, id, json = "", rest = ""] = match;
    if (!queue && !json && !rest) {
      return next();
    }
    const url = new URL(c.req.url);
    const v = url.searchParams.get("v") || undefined;
    const i = url.searchParams.get("i") || undefined;
    if (!v && !i && isSha256(id)) {
      return next();
    }

    const db = await opts.resolveDb(queue);
    const runId = await db.resolveJobRunId(id, { v, i });
    if (!runId) {
      return c.json({ error: `Job ${id} has not been submitted with inputs ${i}` }, 404);
    }
    if (runId === id && !v && !i) {
      return next();
    }
    url.pathname = `${queue ? `/q/${queue}` : ""}/j/${runId}${json}${rest}`;
    url.searchParams.delete("v");
    url.searchParams.delete("i");
    return opts.fetch(new Request(url, c.req.raw));
  };
};

/**
 * The body of a job submission. The id is optional, and the definition can be
 * left out to run an existing job, named by `jobId`, on new inputs.
 */
export type SubmittedJob = Omit<EnqueueJob, "id" | "definition"> & Partial<Pick<EnqueueJob, "id" | "definition">>;

/**
 * Undefined when there is no definition, given or stored. Throws
 * InvalidJobIdError when the `jobId` cannot be used with that definition.
 */
export const resolveSubmittedJob = async (db: DB, submitted: SubmittedJob): Promise<EnqueueJob | undefined> => {
  let definition = submitted?.definition;
  if (!definition && submitted?.jobId) {
    const jobHash = await db.resolveJobRunId(submitted.jobId);
    definition = jobHash ? await db.getJobDefinition(jobHash) : undefined;
  }
  return definition ? prepareEnqueueJob({ ...submitted, definition }) : undefined;
};

export type SubmitResponse = {
  success: true;
  jobId: string;
  runId: string;
  jobHash?: string;
  inputsHash?: string;
  path: string;
};

export const formatSubmitResponse = (job: EnqueueJob): SubmitResponse => ({
  success: true,
  jobId: job.jobId ?? job.id,
  // What to poll: with inputs this is not the jobId
  runId: job.id,
  jobHash: job.jobHash,
  inputsHash: job.inputsHash,
  path: formatJobUrlPath(job),
});
