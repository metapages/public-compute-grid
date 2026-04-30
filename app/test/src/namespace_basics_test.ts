import { assertEquals, equal } from "std/assert";

import { closed, open } from "@korkje/wsi";
import {
  createNewContainerJobMessage,
  DockerJobFinishedReason,
  DockerJobState,
  type JobMessagePayload,
  type StateChange,
  type WebsocketMessageClientToServer,
  WebsocketMessageTypeClientToServer,
} from "@metapages/compute-queues-shared";

import { API_URL, cancelJobOnQueue, QUEUE_ID, queueJobs } from "./util.ts";
import { closeKv } from "../../shared/src/shared/kv.ts";

Deno.test(
  {
    name: "submit the same job from multiple namespaces: the job from the db shows the namespaces",
    // Disabled due to Deno.cron in webhooks.ts (imported transitively via mod.ts -> db.ts)
    // leaking op_cron_next and "database" resources
    sanitizeResources: false,
    sanitizeOps: false,
    fn: async () => {
      const socket = new WebSocket(
        `${API_URL.replace("http", "ws")}/q/${QUEUE_ID}/client`,
      );

      const namespaces = [...new Array(3)].map(() => `namespace-${Math.floor(Math.random() * 1000000)}`).toSorted();
      // const namespacesSet = new Set(namespaces);
      // none of these jobs will finished, we are only testing replacement on the queue
      const command = `sleep 30.${Math.floor(Math.random() * 1000000)}`;

      const messages: JobMessagePayload[] = await Promise.all(
        namespaces.map(async (namespace) => {
          const message = await createNewContainerJobMessage({
            definition: {
              image: "alpine:3.18.5",

              command,
            },
            control: {
              namespace,
            },
          });
          return message;
        }),
      );

      const jobId = messages[0].jobId;
      assertEquals(jobId, messages[1].jobId);
      assertEquals(jobId, messages[2].jobId);

      const timeoutInterval = setTimeout(async () => {
        const jobs = await queueJobs(QUEUE_ID);
        console.log(
          `Test timed out: 👺 namespaces: ${namespaces} job:`,
          jobs[jobId],
        );

        await Promise.all(
          Array.from(messages).map((message) =>
            cancelJobOnQueue({
              queue: QUEUE_ID,
              jobId: message.jobId,
              namespace: message?.queuedJob?.enqueued?.control?.namespace,
              message: "from-namespace-remove-one-timeout-timeout",
            })
          ),
        );
        throw `Test timed out`;
      }, 15000);

      await open(socket);
      for (const messagePayload of messages) {
        socket.send(JSON.stringify(messagePayload.message));
      }

      let namespacesOnQueue: string[] = [];
      while (true) {
        const jobs = await queueJobs(QUEUE_ID);
        if (jobs) {
          namespacesOnQueue = [...new Set(jobs[jobId]?.namespaces || [])].toSorted();
          if (equal([...namespacesOnQueue].toSorted(), namespaces)) {
            break;
          }
        }

        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      assertEquals(namespacesOnQueue, namespaces);

      clearTimeout(timeoutInterval);

      socket.close();
      await closed(socket);
      closeKv();

      await Promise.all(
        Array.from(messages).map((message) =>
          cancelJobOnQueue({
            queue: QUEUE_ID,
            jobId: message.jobId,
            namespace: "*",
            message: "from-namespace-basics",
          })
        ),
      );
    },
  },
);

Deno.test(
  {
    name: "submit the same job from multiple namespaces: then set one cancelled, the namespace should be removed",
    // Disabled due to Deno.cron in webhooks.ts (imported transitively via mod.ts -> db.ts)
    // leaking op_cron_next and "database" resources
    sanitizeResources: false,
    sanitizeOps: false,
    fn: async () => {
      const socket = new WebSocket(
        `${API_URL.replace("http", "ws")}/q/${QUEUE_ID}/client`,
      );

      const namespaces = [...new Array(3)].map(() => `namespace-${Math.floor(Math.random() * 1000000)}`);
      const namespaceToKeep = [...namespaces];
      const namespaceToCancel = namespaceToKeep.pop();

      const namespaceToKeepSet = new Set(namespaceToKeep);
      // none of these jobs will finished, we are only testing replacement on the queue
      const command = `sleep 30.${Math.floor(Math.random() * 1000000)}`;

      const messages: JobMessagePayload[] = await Promise.all(
        namespaces.map(async (namespace) => {
          const message = await createNewContainerJobMessage({
            definition: {
              image: "alpine:3.18.5",
              command,
            },
            control: {
              namespace,
            },
          });
          return message;
        }),
      );

      const timeoutInterval = setTimeout(async () => {
        await Promise.all(
          Array.from(messages).map((message) =>
            cancelJobOnQueue({
              queue: QUEUE_ID,
              jobId: message.jobId,
              namespace: message?.queuedJob?.enqueued?.control?.namespace,
              message: "from-namespace-basics-timeout",
            })
          ),
        );
        throw "Test timed out";
      }, 10000);

      assertEquals(messages[0].jobId, messages[1].jobId);
      assertEquals(messages[1].jobId, messages[2].jobId);
      const jobId = messages[messages.length - 1].jobId;

      await open(socket);
      for (const messagePayload of messages) {
        socket.send(JSON.stringify(messagePayload.message));
      }

      // then cancel the last job
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
            namespace: namespaceToCancel,
            message: "Job cancelled test operation",
          },
        } as StateChange,
      } as WebsocketMessageClientToServer));

      let namespacesOnQueue: Set<string> = new Set();
      while (true) {
        const jobs = await queueJobs(QUEUE_ID);
        if (jobs) {
          namespacesOnQueue = new Set(jobs[jobId]?.namespaces || []);
          if (equal(namespacesOnQueue, namespaceToKeepSet)) {
            break;
          }
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      assertEquals(namespacesOnQueue, namespaceToKeepSet);

      clearTimeout(timeoutInterval);

      socket.close();
      await closed(socket);
      closeKv();

      await Promise.all(
        Array.from(messages).map((message) =>
          cancelJobOnQueue({
            queue: QUEUE_ID,
            jobId: message.jobId,
            namespace: message?.queuedJob?.enqueued?.control?.namespace,
            message: "from-namespace-basics",
          })
        ),
      );
    },
  },
);
