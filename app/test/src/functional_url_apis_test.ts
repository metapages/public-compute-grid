import { assertEquals, assertExists } from "std/assert";

import { closed, open } from "@korkje/wsi";
import {
  type BroadcastJobStates,
  createNewContainerJobMessage,
  type DockerJobDefinitionInputRefs,
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
import { assertDefined } from "retry/misc.ts";

const fetch = fetchRobust;

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL") ||
  (QUEUE_ID === "local" ? "http://worker:8000" : "http://api1:8081");

Deno.test(
  "Check the local and remote API endpoints",
  async () => {
    const socket = new WebSocket(
      `${API_URL.replace("http", "ws")}/${QUEUE_ID}/client`,
    );

    const definition = {
      image: "alpine:3.18.5",
      command: `sh -c 'sleep 0.0${Math.floor(Math.random() * 10000)} && ls -a'`,
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

    await open(socket);

    // Job submisison should confirm the job is submitted.
    // Browser clients kinda do this already by resubmitting if the job is
    // not on the results.
    while (!jobSuccessfullySubmitted) {
      socket.send(JSON.stringify(message));
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    const result = await jobCompleteDeferred;
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

    // Check the local API endpoint
    const localJob = await fetch(`${API_URL}/j/${jobId}`);
    const localJobJson: { data: { results: unknown; definition: DockerJobDefinitionInputRefs } } = await localJob
      .json();

    assertEquals(localJobJson.data.definition, definition);
    assertDefined(localJobJson.data.results, "results not defined");

    const localQueueJob = await fetch(`${API_URL}/q/${QUEUE_ID}/j/${jobId}`);
    const localQueueJobJson: { data: { results: unknown; definition: DockerJobDefinitionInputRefs } } =
      await localQueueJob
        .json();

    assertEquals(localQueueJobJson.data.definition, definition);
    assertDefined(localQueueJobJson.data.results, "results not defined");

    socket.close();
    await closed(socket);
    closeKv();
  },
);
