/**
 * Two workers on one queue claim optimistically, so the same job is sometimes
 * claimed by both (#330). The worker that loses must drop its copy before its
 * container gets anywhere, otherwise a short job runs to completion twice:
 * twice the compute, twice the side effects, and every log line twice.
 *
 * Black box: only the API is used. The server already drops log lines and
 * results from a worker it does not record as running the job, so a losing
 * copy cannot be seen in the job's logs or result. What can be seen is the
 * worker still holding it: /q/:queue/status asks every worker which jobs it
 * has. While the jobs run that is polled, and a job held by a worker other
 * than the one that finished it, in two polls in a row (each poll takes
 * seconds), is a copy that was not dropped. The old code held it for 6s+.
 *
 * Each job also prints a marker and its container's hostname (unique per
 * container), and writes the hostname to /outputs, so the stored result, run
 * log and outputs must all come from one container.
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

/**
 * jobId -> ids of the workers holding it right now. Walks the whole status
 * response: worker statuses sit under localWorkers and under each
 * otherServers entry, in shapes that differ per server implementation.
 */
const jobsHeldByWorkers = async (): Promise<Map<string, Set<string>>> => {
  const held = new Map<string, Set<string>>();
  const walk = (value: unknown) => {
    if (!value || typeof value !== "object") {
      return;
    }
    const v = value as { id?: unknown; queue?: unknown };
    if (typeof v.id === "string" && v.queue && typeof v.queue === "object") {
      for (const jobId of Object.keys(v.queue)) {
        if (!held.has(jobId)) held.set(jobId, new Set());
        held.get(jobId)!.add(v.id);
      }
    }
    Object.values(value).forEach(walk);
  };
  walk(await getJson<unknown>(`/q/${QUEUE_ID}/status`));
  return held;
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

    // Poll which workers hold which of our jobs until the jobs are done and
    // any losing copy has had the old 6s delay to be dropped.
    const jobIds = new Set(jobs.map((j) => j.jobId));
    const polls: Map<string, Set<string>>[] = [];
    let polling = true;
    const poller = (async () => {
      while (polling) {
        try {
          const held = await jobsHeldByWorkers();
          polls.push(new Map([...held].filter(([jobId]) => jobIds.has(jobId))));
        } catch (err) {
          console.log(`status poll failed: ${err}`);
          await delay(500);
        }
      }
    })();

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
    polling = false;
    await poller;
    assert(polls.length >= 2, `only ${polls.length} status polls`);

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

      // no other worker kept a copy of it around
      const winner = job.worker;
      for (let i = 1; i < polls.length; i++) {
        const losers = [...(polls[i].get(jobId) ?? [])].filter((w) => w !== winner && polls[i - 1].get(jobId)?.has(w));
        assertEquals(losers, [], `${jobId} finished on ${winner} but still held by ${losers} across two status polls`);
      }
    }
  } finally {
    await Promise.all(
      jobs.map(({ jobId }) => cancelJobOnQueue({ queue: QUEUE_ID, jobId, message: "from-double-claim-test" })),
    );
    closeKv();
  }
});
