import type { Resource } from "./types.ts";
import { userJobQueues, type WorkerRegistration } from "@metapages/compute-queues-shared";

/**
 * MCP Resources for job queue data
 */

export const resources: Resource[] = [
  {
    uri: "queue://*/jobs",
    name: "Queue Jobs",
    description: "List of jobs in any queue (replace * with queue name)",
    mimeType: "application/json",
  },
  {
    uri: "job://*/status",
    name: "Job Status",
    description: "Status and details of any job (replace * with job ID)",
    mimeType: "application/json",
  },
  {
    uri: "system://queues",
    name: "Active Queues",
    description: "List of all active queues in the system",
    mimeType: "application/json",
  },
  {
    uri: "system://workers",
    name: "Active Workers",
    description: "List of all active workers across all queues",
    mimeType: "application/json",
  },
];

export async function readResource(uri: string): Promise<{
  contents: Array<{
    type: "text" | "image" | "resource";
    text?: string;
    data?: string;
    uri?: string;
  }>;
}> {
  try {
    if (uri.startsWith("queue://")) {
      return await readQueueResource(uri);
    }

    if (uri.startsWith("job://")) {
      return await readJobResource(uri);
    }

    if (uri.startsWith("system://")) {
      return await readSystemResource(uri);
    }

    throw new Error(`Unknown resource URI: ${uri}`);
  } catch (error) {
    return {
      contents: [
        {
          type: "text",
          text: JSON.stringify(
            {
              error: "Failed to read resource",
              uri,
              message: (error as Error).message,
            },
            null,
            2,
          ),
        },
      ],
    };
  }
}

function readQueueResource(uri: string): {
  contents: Array<{
    type: "text";
    text: string;
  }>;
} {
  // Parse: queue://queueName/jobs
  const parts = uri.replace("queue://", "").split("/");
  const queueName = parts[0];
  const resource = parts[1];

  if (resource === "jobs") {
    if (!userJobQueues[queueName]) {
      throw new Error(`Queue '${queueName}' not found`);
    }

    const queue = userJobQueues[queueName];
    const jobs = Object.entries(queue.getJobsSnapshot()).map(([jobId, job]) => ({
      jobId,
      state: job.state,
      worker: job.worker,
      finishedReason: job.finishedReason,
      time: job.time,
      queuedTime: job.queuedTime,
      namespaces: job.namespaces,
    }));

    const response = {
      queue: queueName,
      totalJobs: jobs.length,
      jobs,
      workers: queue.getWorkerRegistrations(),
      lastUpdated: new Date().toISOString(),
    };

    return {
      contents: [
        {
          type: "text",
          text: JSON.stringify(response, null, 2),
        },
      ],
    };
  }

  throw new Error(`Unknown queue resource: ${resource}`);
}

async function readJobResource(uri: string): Promise<{
  contents: Array<{
    type: "text";
    text: string;
  }>;
}> {
  // Parse: job://jobId/status
  const parts = uri.replace("job://", "").split("/");
  const jobId = parts[0];
  const resource = parts[1];

  if (resource === "status") {
    // Find job across all queues
    let foundJob = null;
    let foundQueue = null;

    for (const [queueName, queue] of Object.entries(userJobQueues)) {
      const job = queue.getCurrentJobState(jobId);
      if (job) {
        foundJob = job;
        foundQueue = queueName;
        break;
      }
    }

    if (!foundJob) {
      throw new Error(`Job '${jobId}' not found in any queue`);
    }

    // Try to get job result if finished
    let result = null;
    if (foundJob.state === "Finished") {
      try {
        const finishedJob = await userJobQueues[foundQueue!].db.getJobFinishedResults(jobId);
        result = finishedJob?.finished?.result ?? null;
      } catch (error) {
        console.warn(`Could not fetch result for job ${jobId}:`, (error as Error).message);
      }
    }

    const response = {
      jobId,
      queue: foundQueue,
      status: {
        state: foundJob.state,
        worker: foundJob.worker,
        finishedReason: foundJob.finishedReason,
        time: foundJob.time,
        queuedTime: foundJob.queuedTime,
        namespaces: foundJob.namespaces,
      },
      result: result
        ? {
          outputs: result.outputs,
          logs: result.logs,
          statusCode: result.StatusCode,
          duration: result.duration,
          isTimedOut: result.isTimedOut,
        }
        : null,
      lastUpdated: new Date().toISOString(),
    };

    return {
      contents: [
        {
          type: "text",
          text: JSON.stringify(response, null, 2),
        },
      ],
    };
  }

  throw new Error(`Unknown job resource: ${resource}`);
}

function readSystemResource(uri: string): {
  contents: Array<{
    type: "text";
    text: string;
  }>;
} {
  const resource = uri.replace("system://", "");

  if (resource === "queues") {
    const queues = Object.entries(userJobQueues).map(([queueName, queue]) => ({
      name: queueName,
      jobCount: Object.keys(queue.getJobsSnapshot()).length,
      workerCount: queue.getWorkerRegistrations().length,
      clientCount: queue.getClientCount(),
    }));

    const response = {
      totalQueues: queues.length,
      queues,
      lastUpdated: new Date().toISOString(),
    };

    return {
      contents: [
        {
          type: "text",
          text: JSON.stringify(response, null, 2),
        },
      ],
    };
  }

  if (resource === "workers") {
    const allWorkers: (WorkerRegistration & { queue: string })[] = [];

    for (const [queueName, queue] of Object.entries(userJobQueues)) {
      const workers = queue.getWorkerRegistrations().map((registration) => ({
        ...registration,
        queue: queueName,
      }));
      allWorkers.push(...workers);
    }

    const response = {
      totalWorkers: allWorkers.length,
      workers: allWorkers,
      lastUpdated: new Date().toISOString(),
    };

    return {
      contents: [
        {
          type: "text",
          text: JSON.stringify(response, null, 2),
        },
      ],
    };
  }

  throw new Error(`Unknown system resource: ${resource}`);
}
