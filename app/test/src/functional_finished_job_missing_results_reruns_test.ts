import { assert, assertEquals } from "std/assert";

import { closed, open } from "@korkje/wsi";
import {
  createNewContainerJobMessage,
  DockerJobFinishedReason,
  DockerJobState,
  fetchRobust,
  getJobColorizedString,
  type InMemoryDockerJob,
  type StateChange,
  type WebsocketMessageClientToServer,
  WebsocketMessageTypeClientToServer,
} from "@metapages/compute-queues-shared";

import { API_URL, QUEUE_ID } from "./util.ts";
import { closeKv } from "../../shared/src/shared/kv.ts";

const fetch = fetchRobust;

/**
 * A job id is a hash of the job definition, so submitting the same job twice is
 * a cache hit: the finished record is handed back and the job is not run again.
 *
 * The record and the results it refers to are stored separately, with their own
 * expiries, and the results can also be deleted on their own. A record that
 * outlives its results used to be served as a cache hit anyway, so every later
 * submission was answered with "Finished" and nothing behind it: no exit code,
 * no outputs. Permanently, and for everyone - the id is the same hash for every
 * client of every queue - so that job could never be run again. A metapage
 * whose container leg hit this just stopped producing output, with the queue
 * insisting the job had finished.
 */

/** The finished record and its results, as any client reads them back */
const jobResults = async (
  jobId: string,
): Promise<InMemoryDockerJob | undefined> => {
  const response = await fetch(
    `${API_URL}/q/${QUEUE_ID}/j/${jobId}/result.json`,
    { redirect: "follow" },
  );
  if (!response.ok) {
    await response.body?.cancel();
    return undefined;
  }
  const { data } = await response.json() as { data?: InMemoryDockerJob };
  return data;
};

const hasRunResult = async (jobId: string): Promise<boolean> => {
  const results = await jobResults(jobId);
  return results?.finished?.result?.StatusCode === 0;
};

const waitFor = async (
  what: string,
  predicate: () => Promise<boolean>,
  timeoutMs: number,
): Promise<void> => {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for: ${what}`);
};

Deno.test(
  "a finished job whose results are gone is run again, not served from the record",
  async () => {
    const socket = new WebSocket(
      `${API_URL.replace("http", "ws")}/${QUEUE_ID}/client`,
    );
    await open(socket);

    const definition = {
      image: "alpine:3.18.5",
      // a definition nothing else has run, so this test owns the job id
      command: `echo missing-results-${Math.floor(Math.random() * 1000000)}`,
    };
    const { message, jobId } = await createNewContainerJobMessage({
      definition,
    });

    try {
      // 1. Run it normally. Resubmitted until the queue has it, the way browser
      //    clients do: a submission sent while the socket is still settling is
      //    otherwise just lost and the test would fail for the wrong reason.
      let seen = false;
      const submitting = (async () => {
        while (!seen) {
          socket.send(JSON.stringify(message));
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      })();
      await waitFor(
        `${getJobColorizedString(jobId)} to finish with a result`,
        async () => {
          seen = await hasRunResult(jobId);
          return seen;
        },
        90000,
      );
      await submitting;

      const firstFinish = await jobResults(jobId);
      const firstFinishTime = firstFinish?.finished?.time;
      assert(firstFinishTime, "the first run should have a finish time");

      // 2. Make the record outlive its results, which is what expiry or a
      //    deletion does in production: the record still says Finished/Success,
      //    but there is no result behind it.
      //
      //    Sent with an earlier time than the real finish so that it wins:
      //    between two Success records the earliest is the most correct one
      //    (resolveMostCorrectJob), so this replaces what the worker stored.
      socket.send(JSON.stringify({
        type: WebsocketMessageTypeClientToServer.StateChange,
        payload: {
          job: jobId,
          tag: "test",
          state: DockerJobState.Finished,
          value: {
            type: DockerJobState.Finished,
            reason: DockerJobFinishedReason.Success,
            time: firstFinishTime - 1000,
            worker: "test-missing-results",
          },
        } as StateChange,
      } as WebsocketMessageClientToServer));

      await waitFor(
        `${getJobColorizedString(jobId)} to have a record with no results`,
        async () => {
          const results = await jobResults(jobId);
          return !!results && !results?.finished?.result;
        },
        30000,
      );

      // The job now looks finished and successful, with nothing behind it
      const withoutResults = await jobResults(jobId);
      assertEquals(withoutResults?.state, DockerJobState.Finished);
      assertEquals(
        withoutResults?.finished?.reason,
        DockerJobFinishedReason.Success,
      );
      assertEquals(withoutResults?.finished?.result, undefined);

      // 3. Submit the identical job again. One submission is enough: the server
      //    must notice the record promises results that are gone, drop it, and
      //    run the job - rather than answer with the empty record forever.
      socket.send(JSON.stringify(message));

      await waitFor(
        `${getJobColorizedString(jobId)} to be run again and have a result`,
        () => hasRunResult(jobId),
        90000,
      );

      const rerun = await jobResults(jobId);
      assertEquals(rerun?.finished?.result?.StatusCode, 0);
      assertEquals(rerun?.finished?.reason, DockerJobFinishedReason.Success);
    } finally {
      socket.close();
      await closed(socket);
      closeKv();
    }
  },
);
