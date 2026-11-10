import { ms } from "ms";

import type { DockerRunResultWithOutputs, WebsocketMessageWorkerToServer } from "@metapages/compute-queues-shared";
import type { KvStore } from "../../../shared/src/shared/kv.ts";

/** Long enough to outlive any server outage worth surviving, short enough not to accumulate. */
const FinishedReportedExpiry = ms("7d") as number;
/** Deno KV rejects values over 64 KiB; leave room for the key and encoding. */
const MaxPersistedBytes = 60 * 1024;

/**
 * The Finished messages a worker has sent, persisted in its local KV (#358).
 *
 * DockerJobQueue keeps the same record in memory (finishedAndReported, #346),
 * but memory does not survive a restart, and an --auto-update successor is a
 * different process sharing the worker id and the data directory. Either
 * one, seeing a stale "Running on <this worker>" broadcast for a job that was
 * already finished, would otherwise run it a second time. The persisted
 * message also carries a result the server never got (the worker died with
 * Finished still queued in its socket) across the restart, so it is resent
 * rather than recomputed.
 */
export class FinishedJobsStore {
  constructor(private readonly kv: KvStore, private readonly workerId: string) {}

  private key(jobId: string): string[] {
    return ["worker-finished-reported", this.workerId, jobId];
  }

  async set(jobId: string, message: WebsocketMessageWorkerToServer): Promise<void> {
    await this.kv.set(this.key(jobId), fitForKv(message), { expireIn: FinishedReportedExpiry });
  }

  async get(jobId: string): Promise<WebsocketMessageWorkerToServer | null> {
    return (await this.kv.get<WebsocketMessageWorkerToServer>(this.key(jobId))).value;
  }

  async delete(jobId: string): Promise<void> {
    await this.kv.delete(this.key(jobId));
  }
}

/**
 * The message, with the oldest result log lines dropped until it fits in a KV
 * value. The run and build logs reach the server separately as they stream;
 * the result's own copy is what can be large, and its tail is where an error
 * shows.
 */
export const fitForKv = (message: WebsocketMessageWorkerToServer): WebsocketMessageWorkerToServer => {
  const size = (m: unknown) => new TextEncoder().encode(JSON.stringify(m)).length;
  if (size(message) <= MaxPersistedBytes) {
    return message;
  }
  const payload = message.payload as { value?: { result?: DockerRunResultWithOutputs } };
  const result = payload?.value?.result;
  if (!result?.logs?.length) {
    return message;
  }
  const trimmed = structuredClone(message);
  const trimmedResult = (trimmed.payload as { value: { result: DockerRunResultWithOutputs } }).value.result;
  let logs = result.logs;
  while (logs.length > 0) {
    logs = logs.slice(Math.ceil(logs.length / 4));
    trimmedResult.logs = logs;
    if (size(trimmed) <= MaxPersistedBytes) {
      break;
    }
  }
  return trimmed;
};
