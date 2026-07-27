import {
  type BroadcastJobDefinitions,
  type DockerJobDefinitionInputRefs,
  type WebsocketMessageSenderWorker,
  type WebsocketMessageServerBroadcast,
  WebsocketMessageTypeServerBroadcast,
  WebsocketMessageTypeWorkerToServer,
} from "@metapages/compute-queues-shared";

// export class JobDefinitionCacheLocal {
//   kv: Deno.Kv;
//   constructor(args: {
//     kv: Deno.Kv;
//   }) {
//     const { kv } = args;
//     this.kv = kv;
//   }

//   async put(jobId: string, definition: DockerJobDefinitionInputRefs): Promise<void> {
//     await this.kv.set(["job-definition", jobId], definition);
//   }

//   async get(jobId: string): Promise<DockerJobDefinitionInputRefs | null> {
//     const dbEntry = await this.kv.get(["job-definition", jobId]);
//     if (dbEntry?.value) {
//       return dbEntry.value as DockerJobDefinitionInputRefs;
//     }
//     return null;
//   }
// }

export class JobDefinitionCache {
  sender: WebsocketMessageSenderWorker;
  kv: Deno.Kv;
  constructor(args: {
    kv: Deno.Kv;
    sender: WebsocketMessageSenderWorker;
  }) {
    const { sender, kv } = args;
    this.sender = sender;
    this.kv = kv;
  }

  public async onWebsocketMessage(message: WebsocketMessageServerBroadcast) {
    switch (message.type) {
      case WebsocketMessageTypeServerBroadcast.BroadcastJobDefinitions: {
        const definitions = (message.payload as BroadcastJobDefinitions)?.definitions;
        if (!definitions) {
          return;
        }
        for (const [jobId, definition] of Object.entries(definitions)) {
          await this.kv.set(["job-definition", jobId], definition);
        }
        break;
      }
      default:
        break;
    }
  }

  prefetch(jobIds: string[]) {
    for (const jobId of jobIds) {
      // Fire and forget, but swallow the rejection: an unhandled rejection here
      // takes down the worker process.
      this.get(jobId).catch(() => {});
    }
  }

  async get(jobId: string): Promise<DockerJobDefinitionInputRefs | null> {
    const dbEntry = await this.kv.get(["job-definition", jobId]);
    if (dbEntry?.value) {
      return dbEntry.value as DockerJobDefinitionInputRefs;
    }

    // Request the job definition and poll for it
    this.sender({
      type: WebsocketMessageTypeWorkerToServer.RequestJobDefinitions,
      payload: {
        jobIds: [jobId],
      },
    });

    // Poll for the job definition with retry logic
    const maxRetries = 12; // Minimum number of retries before throwing error
    const pollIntervalMs = 1000; // 1 second pause between iterations
    let attempt = 0;

    while (attempt < maxRetries) {
      // Wait before polling (except on first iteration)
      if (attempt > 0) {
        await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
      }

      // Check if the job definition is now available
      const polledEntry = await this.kv.get(["job-definition", jobId]);
      if (polledEntry?.value) {
        return polledEntry.value as DockerJobDefinitionInputRefs;
      }
      attempt++;
    }

    // If we've exhausted all retries, throw an error
    throw new Error(`Job definition for jobId '${jobId}' not found after ${maxRetries} polling attempts`);
  }
}
