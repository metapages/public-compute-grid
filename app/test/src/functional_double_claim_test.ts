/**
 * Two workers on one queue claim optimistically, so the same job is sometimes
 * claimed by both (#330). The worker that loses must drop its copy before its
 * container gets anywhere, otherwise a short job runs to completion twice:
 * twice the compute, twice the side effects, and every log line twice.
 *
 * Black box: only the API is used. Each job prints a marker and its
 * container's hostname (unique per container), after a 1s sleep that leaves
 * a losing copy plenty of time to be killed before it prints anything. A
 * duplicate shows up as the marker twice in the job's run log, or as a
 * hostname in the logs or outputs that is not the one in the stored result.
 *
 * Needs two workers, so it only runs against the remote/cloudflare stacks.
 */
import { assert, assertEquals } from "std/assert";
import { delay } from "std/async/delay";

import {
  createNewContainerJobMessage,
  DockerJobFinishedReason,
  DockerJobState,
  type InMemoryDockerJob,
} from "@metapages/compute-queues-shared";
import { closeKv } from "../../shared/src/shared/kv.ts";
import { API_URL, cancelJobOnQueue, QUEUE_ID, withDeadline } from "./util.ts";

// Plain fetch: fetchRobust retries 4xx/5xx, and a missing output must be seen
const fetch = globalThis.fetch;

// Enough concurrent short jobs that both workers race for some of them
const JOB_COUNT = 16;
const TIMEOUT = 180_000;

type LogLine = [string, number, boolean?];

const getJson = async <T>(path: string): Promise<T> => {
  const response = await fetch(`${API_URL}${path}`);
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`${path}: ${response.status}`);
  }
  return await response.json() as T;
};

const runLogLines = async (jobId: string): Promise<string[]> => {
  const { data } = await getJson<{ data: LogLine[] }>(`/q/${QUEUE_ID}/j/${jobId}/run-logs.json`);
  return data.map((l) => l[0]);
};

const output = async (jobId: string, name: string): Promise<string> => {
  const response = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/outputs/${name}`);
  assertEquals(response.status, 200, `${jobId} outputs/${name}`);
  return (await response.text()).trim();
};

Deno.test("a double-claimed short job runs to completion exactly once", async () => {
  if (QUEUE_ID === "local") {
    console.log("Skipping test - needs two workers on one queue (remote mode only)");
    return;
  }

  const runId = `${Date.now()}-${Math.floor(Math.random() * 1_000_000_000)}`;
  const jobs = await Promise.all(
    Array.from({ length: JOB_COUNT }, async (_, i) => {
      const marker = `once-${runId}-${i}`;
      const { jobId, queuedJob } = await createNewContainerJobMessage({
        definition: {
          image: "alpine:3.18.5",
          command: `sh -c 'sleep 1 && echo "${marker} $(hostname)" && hostname > /outputs/hostname.txt'`,
        },
      });
      return { jobId, marker, enqueued: queuedJob!.enqueued };
    }),
  );

  try {
    // Submit them all at once, so both workers see a burst of queued jobs
    await Promise.all(jobs.map(async ({ enqueued }) => {
      const response = await fetch(`${API_URL}/q/${QUEUE_ID}`, {
        method: "POST",
        body: JSON.stringify(enqueued),
        headers: { "Content-Type": "application/json" },
      });
      assertEquals(response.status, 200, await response.text());
    }));

    const finished = new Map<string, InMemoryDockerJob>();
    await withDeadline(
      (async () => {
        while (finished.size < jobs.length) {
          const { data } = await getJson<{ data: Record<string, InMemoryDockerJob> }>(`/q/${QUEUE_ID}`);
          for (const { jobId } of jobs) {
            if (data[jobId]?.state === DockerJobState.Finished) {
              finished.set(jobId, data[jobId]);
            }
          }
          await delay(500);
        }
      })(),
      () => `only ${finished.size}/${jobs.length} jobs finished on queue ${QUEUE_ID}`,
      TIMEOUT,
    );

    // A losing copy that was not killed finishes up to a few seconds later:
    // give it the time to stream its lines and upload its outputs.
    await delay(8_000);

    for (const { jobId, marker } of jobs) {
      const job = finished.get(jobId)!;
      assertEquals(job.finishedReason, DockerJobFinishedReason.Success, `${jobId} ${JSON.stringify(job)}`);

      const { data: result } = await getJson<{ data: InMemoryDockerJob }>(`/q/${QUEUE_ID}/j/${jobId}/result.json`);
      assertEquals(result.finished?.result?.StatusCode, 0, jobId);
      // the finished record names a worker, and it is the one the queue holds
      assert(result.finished?.worker, `${jobId} finished record names no worker`);
      assertEquals(result.finished?.worker, job.worker, `${jobId} recorded worker`);

      const resultLines = (result.finished?.result?.logs ?? []).map((l) => l[0]).filter((l) => l.includes(marker));
      assertEquals(resultLines.length, 1, `${jobId} result logs: ${JSON.stringify(resultLines)}`);
      const hostname = resultLines[0].trim().split(" ").pop()!;

      // exactly one container streamed this job's output, the one whose result was stored
      const runLines = (await runLogLines(jobId)).filter((l) => l.includes(marker));
      assertEquals(
        runLines.map((l) => l.trim()),
        [`${marker} ${hostname}`],
        `${jobId} ran more than once (run-logs.json)`,
      );

      // and its outputs were not overwritten by a second copy
      assertEquals(await output(jobId, "hostname.txt"), hostname, `${jobId} outputs written by another container`);
    }
  } finally {
    await Promise.all(
      jobs.map(({ jobId }) => cancelJobOnQueue({ queue: QUEUE_ID, jobId, message: "from-double-claim-test" })),
    );
    closeKv();
  }
});
