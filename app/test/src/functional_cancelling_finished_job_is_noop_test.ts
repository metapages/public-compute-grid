import { open } from "@korkje/wsi";
import {
  type BroadcastJobStates,
  createNewContainerJobMessage,
  DockerJobFinishedReason,
  DockerJobState,
  type StateChange,
  type WebsocketMessageClientToServer,
  type WebsocketMessageServerBroadcast,
  WebsocketMessageTypeClientToServer,
  WebsocketMessageTypeServerBroadcast,
} from "@metapages/compute-queues-shared";
import { closeKv } from "../../shared/src/shared/kv.ts";
import { closeSocket, resubmitUntilSeen, withDeadline } from "./util.ts";

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL") ||
  (QUEUE_ID === "local" ? "http://worker:8000" : "http://api1:8081");

Deno.test(
  "submit job, allow to finish with success, then cancel, cancel has no effect",
  async () => {
    const socket = new WebSocket(
      `${API_URL.replace("http", "ws")}/${QUEUE_ID}/client`,
    );

    const definition = {
      image: "alpine:3.18.5",
      command: `echo ${Math.floor(Math.random() * 10000)}`,
    };

    const { message, jobId /* , stageChange */ } = await createNewContainerJobMessage({
      definition,
    });

    const {
      promise: jobCompleteDeferred,
      resolve,
      /* reject, */
    } = Promise.withResolvers<void>();

    let jobSuccessfullySubmitted = false;
    let jobFinishedSuccessfully = false;
    socket.onmessage = (message: MessageEvent) => {
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
          jobSuccessfullySubmitted = true;
          if (jobState.state === DockerJobState.Finished) {
            if (jobState.finishedReason === DockerJobFinishedReason.Success) {
              jobFinishedSuccessfully = true;
              resolve();
            }

            if (jobState.finishedReason === DockerJobFinishedReason.Cancelled) {
              throw new Error(`Job should not be cancelled jobFinishedSuccessfully=${jobFinishedSuccessfully}`);
            }
          }
          break;
        }
        default:
          //ignored
      }
    };

    try {
      await open(socket);

      // Job submisison should confirm the job is submitted.
      // Browser clients kinda do this already by resubmitting if the job is
      // not on the results.
      await resubmitUntilSeen(
        socket,
        message,
        () => jobSuccessfullySubmitted,
        () => `${jobId} never appeared on queue ${QUEUE_ID}`,
      );

      // finished after this
      await withDeadline(
        jobCompleteDeferred,
        () => `${jobId} did not finish on queue ${QUEUE_ID} (reached the queue: ${jobSuccessfullySubmitted})`,
      );

      // now cancel
      socket.send(JSON.stringify({
        type: WebsocketMessageTypeClientToServer.StateChange,
        payload: {
          job: jobId,
          tag: "api",
          state: DockerJobState.Finished,
          value: {
            type: DockerJobState.Finished,
            reason: DockerJobFinishedReason.Cancelled,
            time: Date.now(),
            message: "Job cancelled test operation",
          },
        } as StateChange,
      } as WebsocketMessageClientToServer));

      // allow some time to possibly get a cancellation
      await new Promise((resolve) => setTimeout(resolve, 2000));
      // we should NOT see a cancellation
    } finally {
      await closeSocket(socket);
      closeKv();
    }
  },
);
