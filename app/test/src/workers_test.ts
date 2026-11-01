import { assert, assertEquals } from "std/assert";

import { open } from "@korkje/wsi";
import {
  type BroadcastJobStates,
  createNewContainerJobMessage,
  DockerJobFinishedReason,
  DockerJobState,
  fetchRobust,
  type InMemoryDockerJob,
  type WebsocketMessageServerBroadcast,
  WebsocketMessageTypeServerBroadcast,
} from "@metapages/compute-queues-shared";
import { closeKv } from "../../shared/src/shared/kv.ts";
import { closeSocket, resubmitUntilSeen, withDeadline } from "./util.ts";

const fetch = fetchRobust;

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL") ||
  (QUEUE_ID === "local" ? "http://worker:8000" : "http://api1:8081");

Deno.test(
  "submit a job: verify worker state transitions and completion",
  async () => {
    const socket = new WebSocket(
      `${API_URL.replace("http", "ws")}/${QUEUE_ID}/client`,
    );

    const outputText = `text-${Math.floor(Math.random() * 1000000)}`;
    // Create a simple job that should work reliably
    const definition = {
      image: "alpine:3.18.5",
      // sleep should be long enough for workers to fight over it and
      // the correct worker to eventually claim it
      command: `sh -c 'sleep 3 && echo "${outputText}"'`,
    };
    const { message, jobId } = await createNewContainerJobMessage({
      definition,
    });

    const {
      promise: jobCompleteDeferred,
      resolve,
      // reject,
    } = Promise.withResolvers<{
      result: string;
      finalWorker: string;
      stateTransitions: Array<{ state: DockerJobState; worker?: string }>;
    }>();

    let jobSuccessfullySubmitted = false;
    let stateTransitions: Array<{ state: DockerJobState; worker?: string }> = [];
    let finalWorker = "";

    socket.onmessage = async (message: MessageEvent) => {
      const messageString = message.data.toString();
      const possibleMessage: WebsocketMessageServerBroadcast = JSON.parse(
        messageString,
      );
      switch (possibleMessage.type) {
        case WebsocketMessageTypeServerBroadcast.JobStates:
        case WebsocketMessageTypeServerBroadcast.JobStateUpdates: {
          const someJobsPayload = possibleMessage.payload as BroadcastJobStates;
          if (!someJobsPayload) {
            break;
          }

          const jobState = someJobsPayload.state.jobs[jobId];
          if (!jobState) {
            break;
          }

          // console.log(`👺 ${getJobColorizedString(jobId)}: `, jobState);

          jobSuccessfullySubmitted = true;

          // Track state transitions
          const worker = jobState.state === DockerJobState.Running
            ? jobState?.worker
            : jobState.state === DockerJobState.Finished
            ? jobState?.worker
            : undefined;

          // remove previous state transitions that are the same
          // state, this is likely from a different worker
          stateTransitions = stateTransitions.filter((s) => {
            return s.state !== jobState.state;
          });

          stateTransitions.push({
            state: jobState.state,
            worker,
          });

          if (jobState.state === DockerJobState.Finished) {
            assertEquals(jobState.finishedReason, DockerJobFinishedReason.Success);

            const { data: finishedState }: { data: InMemoryDockerJob } =
              await (await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/result.json`))
                .json();
            assertEquals(finishedState?.finished?.result?.StatusCode, 0);

            // jobState.value as StateChangeValueFinished;
            const lines: string = finishedState?.finished?.result?.logs?.map(
              (l) => l[0],
            )[0]!;
            finalWorker = finishedState.worker || "";
            resolve({
              result: lines,
              finalWorker,
              stateTransitions,
            });
          }

          break;
        }
        default:
          //ignored
      }
    };

    try {
      await open(socket);

      // Submit the job. Job submissson should confirm the job is submitted.
      // Browser clients kinda do this already by resubmitting if the job is
      // not on the results.
      await resubmitUntilSeen(
        socket,
        message,
        () => jobSuccessfullySubmitted,
        () => `${jobId} never appeared on queue ${QUEUE_ID}`,
      );

      // Wait for job to complete
      const result = await withDeadline(
        jobCompleteDeferred,
        () =>
          `${jobId} did not finish on queue ${QUEUE_ID}; states seen: ${
            stateTransitions.map((s) => s.state).join(" -> ") || "none"
          }`,
      );

      // Verify the job completed successfully
      assertEquals(
        result.result.trim(),
        outputText.trim(),
      );

      // Verify that the job went through the expected state transitions
      assert(
        stateTransitions.length === 3,
        "Should have at Queued, Running, and Finished states",
      );

      // Should start with Queued state, go to Running, and then to Finished
      assertEquals(stateTransitions[0].state, DockerJobState.Queued);
      assertEquals(stateTransitions[1].state, DockerJobState.Running);
      assertEquals(stateTransitions[2].state, DockerJobState.Finished);

      // Should end with Finished state
      assertEquals(
        stateTransitions[stateTransitions.length - 1].state,
        DockerJobState.Finished,
      );

      // ⚠️ These tests are not correct, and it's worth leaving them here
      // to remind us: it IS possible for two workers to start Running a job,
      // and for the queue to decide who gets it, but the "wrong" worker finishes
      // it first. The queue prioritizes speed so let's take the finished work, even
      // if the Running.worker is different from the Finished.worker.
      // Verify that the job state shows the correct worker information
      // The job should have been claimed by one worker and finished by that same worker
      // const finishedState = stateTransitions.find((s) => s.state === DockerJobState.Finished);
      // assert(finishedState?.worker, "Finished state should have a worker");
      // assertEquals(
      //   finishedState.worker,
      //   result.finalWorker,
      //   "Final worker should match the worker in finished state",
      // );

      // // Verify that the same worker that started the job also finished it
      // // (in a single-worker environment, this should always be true)
      // assertEquals(
      //   runningState.worker,
      //   result.finalWorker,
      //   "The worker that started the job should be the same one that finished it",
      // );
    } finally {
      await closeSocket(socket);
      closeKv();
    }
  },
);
