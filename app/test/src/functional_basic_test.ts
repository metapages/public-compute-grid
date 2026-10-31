import { assertEquals, assertExists } from "std/assert";

import { open } from "@korkje/wsi";
import {
  type BroadcastJobStates,
  createNewContainerJobMessage,
  DockerJobFinishedReason,
  DockerJobState,
  fetchRobust,
  getJobColorizedString,
  type InMemoryDockerJob,
  type StateChangeValueFinished,
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
  "pretend to be a client: submit job and get expected results",
  async () => {
    const socket = new WebSocket(
      `${API_URL.replace("http", "ws")}/${QUEUE_ID}/client`,
    );

    const definition = {
      image: "alpine:3.18.5",
      command: `sh -c 'sleep 1.${Math.floor(Math.random() * 10000)} && ls -a'`,
    };

    const { message, jobId /* , stageChange */ } = await createNewContainerJobMessage({
      definition,
    });

    const {
      promise: jobCompleteDeferred,
      resolve,
      /* reject, */
    } = Promise.withResolvers<string>();

    let jobSuccessfullySubmitted = false;
    let jobFinished = false;
    let finalJobState: StateChangeValueFinished | undefined;
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
          jobSuccessfullySubmitted = true;
          if (jobState.state === DockerJobState.Finished) {
            if (jobFinished) {
              break;
            }
            jobFinished = true;
            assertEquals(
              jobState.finishedReason,
              DockerJobFinishedReason.Success,
              `${getJobColorizedString(jobId)} not a success:${JSON.stringify(jobState, null, 2)}`,
            );
            const { data: finishedState }: { data: InMemoryDockerJob } =
              await (await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}/result.json`, { redirect: "follow" }))
                .json();
            assertExists(
              finishedState,
              `${getJobColorizedString(jobId)} no finishedState:${JSON.stringify(jobState, null, 2)}`,
            );
            const lines: string = finishedState?.finished?.result?.logs?.map(
              (l) => l[0],
            ).join("")!;
            finalJobState = finishedState?.finished;
            resolve(lines);
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

      const result = await withDeadline(
        jobCompleteDeferred,
        () => `${jobId} did not finish on queue ${QUEUE_ID} (reached the queue: ${jobSuccessfullySubmitted})`,
      );
      const expectedResult =
        ".\n..\n.dockerenv\nbin\ndev\netc\nhome\ninputs\njob-cache\nlib\nmedia\nmnt\nopt\noutputs\nproc\nroot\nrun\nsbin\nsrv\nsys\ntmp\nusr\nvar\n";
      if (result !== expectedResult) {
        console.log(
          `${getJobColorizedString(jobId)} unexpected result 💥`,
          finalJobState,
        );
      }
      assertEquals(
        result,
        expectedResult,
      );
      assertEquals(
        finalJobState?.result?.StatusCode,
        0,
      );
    } finally {
      await closeSocket(socket);
      closeKv();
    }
  },
);
