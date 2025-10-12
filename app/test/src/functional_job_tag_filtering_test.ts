import { assertEquals } from "std/assert";

import { closed, open } from "@korkje/wsi";
import {
  type BroadcastJobStates,
  createNewContainerJobMessage,
  DockerJobState,
  getJobColorizedString,
  type WebsocketMessageServerBroadcast,
  WebsocketMessageTypeServerBroadcast,
} from "@metapages/compute-queues-shared";
import { closeKv } from "../../shared/src/shared/kv.ts";

const QUEUE_ID = Deno.env.get("QUEUE_ID") || "local1";
const API_URL = Deno.env.get("API_URL") ||
  (QUEUE_ID === "local" ? "http://worker:8000" : "http://api1:8081");

Deno.test(
  "job with unmatched tags remains queued and is not taken by worker",
  async () => {
    const socket = new WebSocket(
      `${API_URL.replace("http", "ws")}/q/${QUEUE_ID}/client`,
    );

    // Create a job with a random tag that no worker should have
    const randomTag = `test-unique-tag-${Math.random().toString(36).substring(2, 15)}`;
    const definition = {
      image: "alpine:3.18.5",
      command: "echo 'This job should not be picked up'",
      tags: [randomTag], // This tag should not match any worker
    };

    const { message, jobId } = await createNewContainerJobMessage({
      definition,
    });

    console.log(`🏷️  Created job ${getJobColorizedString(jobId)} with unmatched tag: ${randomTag}`);

    const {
      promise: testCompleteDeferred,
      resolve,
    } = Promise.withResolvers<void>();

    let jobSuccessfullySubmitted = false;
    let jobStillQueued = false;
    const maxWaitTime = 15000; // Wait 15 seconds
    let checkCount = 0;

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
          checkCount++;

          console.log(
            `🔍 Check #${checkCount}: Job ${getJobColorizedString(jobId)} state: ${jobState.state}${
              jobState.worker ? ` (worker: ${jobState.worker})` : ""
            }`,
          );

          if (jobState.state === DockerJobState.Queued) {
            jobStillQueued = true;
          } else if (jobState.state === DockerJobState.Running || jobState.state === DockerJobState.Finished) {
            // This should not happen - if it does, the test fails
            console.error(
              `❌ Job ${getJobColorizedString(jobId)} was unexpectedly picked up by a worker!`,
            );
            assertEquals(
              jobState.state,
              DockerJobState.Queued,
              `Job with unmatched tag [${randomTag}] should NOT be picked up by any worker, but it was in state: ${jobState.state}`,
            );
          }
          break;
        }
        default:
          // ignored
      }
    };

    await open(socket);

    // Submit the job
    while (!jobSuccessfullySubmitted) {
      socket.send(JSON.stringify(message));
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    // Wait for a reasonable amount of time to ensure workers had a chance to pick up the job
    setTimeout(() => {
      console.log(`⏰ Test timeout reached after ${maxWaitTime}ms`);
      resolve();
    }, maxWaitTime);

    await testCompleteDeferred;

    // Verify the job is still queued and was never picked up
    assertEquals(
      jobStillQueued,
      true,
      `Job ${getJobColorizedString(jobId)} with unmatched tag [${randomTag}] should remain queued`,
    );

    console.log(
      `✅ Test passed: Job ${
        getJobColorizedString(jobId)
      } with tag [${randomTag}] correctly remained queued and was not picked up by any worker`,
    );

    socket.close();
    await closed(socket);
    closeKv();
  },
);
